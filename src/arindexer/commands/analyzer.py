import asyncio
import json
import os
import stat
from abc import ABC
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

import mmh3
import msgpack
import plyvel

from ..utils.processor import Processor
from ..utils.varint import encode_varint, decode_varint
from ..utils.directory_listener import DirectoryListenerCoordinator, DirectoryListener, ChildTaskException
from ..utils.walker import FileContext
from ..store.archive_store import ArchiveStore


class DuplicateMatchRule:
    """Defines which metadata properties are considered for exact identity matching.

    This rule determines which metadata fields must match for two files to be considered
    identical (beyond content equivalence). Different rules can be used for different
    analysis scenarios.

    Attributes:
        include_mtime: Whether modification time must match for identity
        include_atime: Whether access time must match for identity
        include_ctime: Whether change time must match for identity
        include_mode: Whether file permissions/mode must match for identity
        include_owner: Whether file owner (UID) must match for identity
        include_group: Whether file group (GID) must match for identity
    """

    def __init__(self, *, include_mtime: bool = True, include_atime: bool = False,
                 include_ctime: bool = True, include_mode: bool = True,
                 include_owner: bool = True, include_group: bool = True):
        self.include_mtime = include_mtime
        self.include_atime = include_atime
        self.include_ctime = include_ctime
        self.include_mode = include_mode
        self.include_owner = include_owner
        self.include_group = include_group

    def __eq__(self, other: object) -> bool:
        """Check if two rules are identical."""
        if not isinstance(other, DuplicateMatchRule):
            return False
        return (self.include_mtime == other.include_mtime and
                self.include_atime == other.include_atime and
                self.include_ctime == other.include_ctime and
                self.include_mode == other.include_mode and
                self.include_owner == other.include_owner and
                self.include_group == other.include_group)

    def __hash__(self) -> int:
        """Allow rule to be used as dict key."""
        return hash((self.include_mtime, self.include_atime, self.include_ctime,
                    self.include_mode, self.include_owner, self.include_group))

    def __repr__(self) -> str:
        """String representation for debugging."""
        included = []
        if self.include_mtime: included.append('mtime')
        if self.include_atime: included.append('atime')
        if self.include_ctime: included.append('ctime')
        if self.include_mode: included.append('mode')
        if self.include_owner: included.append('owner')
        if self.include_group: included.append('group')
        return f"DuplicateMatchRule({', '.join(included)})"

    def calculate_is_identical(self, *, mtime_match: bool, atime_match: bool,
                              ctime_match: bool, mode_match: bool,
                              owner_match: bool, group_match: bool) -> bool:
        """Calculate whether files are identical based on this rule.

        Args:
            mtime_match: Whether mtimes match
            atime_match: Whether atimes match
            ctime_match: Whether ctimes match
            mode_match: Whether modes match
            owner_match: Whether owners match
            group_match: Whether groups match

        Returns:
            True if all included metadata fields match
        """
        return ((not self.include_mtime or mtime_match) and
                (not self.include_atime or atime_match) and
                (not self.include_ctime or ctime_match) and
                (not self.include_mode or mode_match) and
                (not self.include_owner or owner_match) and
                (not self.include_group or group_match))

    def to_dict(self) -> dict[str, bool]:
        """Convert rule to dictionary for JSON serialization."""
        return {
            'include_mtime': self.include_mtime,
            'include_atime': self.include_atime,
            'include_ctime': self.include_ctime,
            'include_mode': self.include_mode,
            'include_owner': self.include_owner,
            'include_group': self.include_group
        }

    @classmethod
    def from_dict(cls, data: dict[str, bool]) -> 'DuplicateMatchRule':
        """Load rule from dictionary."""
        return cls(
            include_mtime=data.get('include_mtime', True),
            include_atime=data.get('include_atime', False),
            include_ctime=data.get('include_ctime', True),
            include_mode=data.get('include_mode', True),
            include_owner=data.get('include_owner', True),
            include_group=data.get('include_group', True)
        )


class DuplicateMatch:
    """A duplicate found in the archive with metadata comparison results.

    Attributes:
        path: Path to the duplicate file or directory in the archive, relative to archive root.

        mtime_match: Whether modification times match exactly (nanosecond precision).
                     For files: compares the file's mtime directly.
                     For directories: True only if both the directory's mtime AND all child files'
                     mtimes match. False if ANY child file has mtime_match=False.

        atime_match: Whether access times match exactly (nanosecond precision).
                     For files: compares the file's atime directly.
                     For directories: True only if both the directory's atime AND all child files'
                     atimes match. False if ANY child file has atime_match=False.

        ctime_match: Whether change times (ctime) match exactly (nanosecond precision).
                     For files: compares the file's ctime directly.
                     For directories: True only if both the directory's ctime AND all child files'
                     ctimes match. False if ANY child file has ctime_match=False.

        mode_match: Whether file permissions/mode match exactly.
                    For files: compares the file's mode directly.
                    For directories: True only if both the directory's mode AND all child files'
                    modes match. False if ANY child file has mode_match=False.

        owner_match: Whether file owner (UID) matches exactly.
                     For files: compares the file's owner directly.
                     For directories: True only if both the directory's owner AND all child files'
                     owners match. False if ANY child file has owner_match=False.

        group_match: Whether file group (GID) matches exactly.
                     For files: compares the file's group directly.
                     For directories: True only if both the directory's group AND all child files'
                     groups match. False if ANY child file has group_match=False.

        duplicated_size: Total size in bytes of files within this specific archive path
                        that have content-equivalent files in the analyzed path.
                        For files: the file size if content matches.
                        For directories: sum of all child file sizes that are content-equivalent.
                        NOTE: When a file in the analyzed path has multiple content-equivalent files in the archive,
                        each DuplicateMatch counts that file's size independently.

        duplicated_items: Count of individual items that are duplicated.
                         - Regular files with matching content: count as 1
                         - Special files (symlinks, devices, pipes, sockets) with no difference: count as 1
                         - Directories themselves: count as 0 (only their contents are counted)
                         For directories: sum of duplicated_items from all matching children.

        is_identical: Whether duplicate_path is exactly identical to the analyzed item.
                     For files: True when all metadata matches (mtime_match AND atime_match AND ctime_match AND
                                mode_match AND owner_match AND group_match).
                     For directories: True when:
                       - All children match with identical metadata (is_identical=True for each)
                       - No extra files exist in either directory
                       - Directory-level metadata also matches

        is_superset: Whether duplicate_path contains all content from the analyzed item (possibly with extras).
                    For files: Always equals is_identical (files are atomic, no partial matches).
                    For directories: True when duplicate contains all files from analyzed directory.
                    The duplicate may have additional files not in the analyzed directory.
    """

    def __init__(self, path: Path, *, mtime_match: bool,
                 atime_match: bool, ctime_match: bool, mode_match: bool,
                 owner_match: bool = False, group_match: bool = False,
                 duplicated_size: int = 0, duplicated_items: int = 0,
                 is_identical: bool = False, is_superset: bool = False,
                 rule: DuplicateMatchRule | None = None):
        self.path = path
        self.mtime_match = mtime_match
        self.atime_match = atime_match
        self.ctime_match = ctime_match
        self.mode_match = mode_match
        self.owner_match = owner_match
        self.group_match = group_match
        self.duplicated_size = duplicated_size
        self.duplicated_items = duplicated_items
        self.is_identical = is_identical
        self.is_superset = is_superset
        self.rule = rule


class DuplicateRecord:
    """Record of a file and its duplicates in the archive with metadata comparisons.

    Attributes:
        path: Path relative to the analyzed target (including target's base name)
        duplicates: List of tuples (Path, DuplicateMatch) for each duplicate file
        total_size: Total size in bytes of all content within this path.
                   For files: the file size.
                   For directories: sum of all child file sizes (whether or not they have duplicates).
        duplicated_size: Total size in bytes of this analyzed file/directory's content that
                        has content-equivalent files in the archive. This is the deduplicated size - each
                        file is counted once regardless of how many content-equivalent files exist in the archive.
                        For files: the file size (if content-equivalent files exist in the archive).
                        For directories: sum of all child file sizes that have any content-equivalent files.

                        IMPORTANT: Semantic difference from DuplicateMatch.duplicated_size:
                        - DuplicateRecord.duplicated_size: Global deduplicated size. Each file in the analyzed
                          path is counted once, regardless of how many content-equivalent files exist across
                          all archive paths.
                        - DuplicateMatch.duplicated_size: Localized size for a specific archive path.
                          When a file in the analyzed path has multiple content-equivalent files in different
                          archive directories, each DuplicateMatch counts that file's size independently.
    """

    def __init__(self, path: Path,
                 duplicates: list[tuple[Path, DuplicateMatch]] | None = None,
                 total_size: int = 0,
                 duplicated_size: int = 0):
        self.path = path
        self.duplicates: list[tuple[Path, DuplicateMatch]] = duplicates or []
        self.total_size: int = total_size
        self.duplicated_size: int = duplicated_size

    def to_msgpack(self) -> bytes:
        """Serialize to msgpack format for storage.

        Returns:
            Msgpack-encoded bytes containing [path_components, duplicate_data_list, total_size, duplicated_size]
            where duplicate_data_list is a list of [path_components, mtime_match, atime_match, ctime_match, mode_match,
            owner_match, group_match, duplicated_size, duplicated_items, is_identical, is_superset]
        """
        path_components = [str(part) for part in self.path.parts]

        duplicate_data = []
        for dup_path, comparison in self.duplicates:
            dup_path_components = [str(part) for part in dup_path.parts]
            duplicate_data.append([
                dup_path_components,
                comparison.mtime_match,
                comparison.atime_match,
                comparison.ctime_match,
                comparison.mode_match,
                comparison.owner_match,
                comparison.group_match,
                comparison.duplicated_size,
                comparison.duplicated_items,
                comparison.is_identical,
                comparison.is_superset
            ])

        return msgpack.dumps([path_components, duplicate_data, self.total_size, self.duplicated_size])

    @classmethod
    def from_msgpack(cls, data: bytes) -> 'DuplicateRecord':
        """Deserialize from msgpack format.

        Args:
            data: Msgpack-encoded bytes

        Returns:
            DuplicateRecord instance
        """
        decoded = msgpack.loads(data)
        path_components, duplicate_data, total_size, duplicated_size = decoded

        path = Path(*path_components)

        duplicates: list[tuple[Path, DuplicateMatch]] = []

        for dup_data in duplicate_data:
            dup_path_components, mtime_match, atime_match, ctime_match, mode_match, owner_match, group_match, dup_duplicated_size, duplicated_items, is_identical, is_superset = dup_data
            dup_path = Path(*dup_path_components)
            comparison = DuplicateMatch(
                dup_path,
                mtime_match=mtime_match, atime_match=atime_match, ctime_match=ctime_match, mode_match=mode_match,
                owner_match=owner_match, group_match=group_match,
                duplicated_size=dup_duplicated_size, duplicated_items=duplicated_items,
                is_identical=is_identical, is_superset=is_superset
            )
            duplicates.append((dup_path, comparison))

        return cls(path, duplicates, total_size, duplicated_size)


class FileAnalysisResult(ABC):
    """Abstract base class for file analysis results.

    Subclasses represent different types of analysis outcomes for files and directories.
    """
    pass


class ImmediateResult(FileAnalysisResult):
    """Immediate result containing a duplicate record.

    Attributes:
        base_name: Base name of the file or directory (e.g., 'file.txt' or 'dirname')
        duplicate_record: DuplicateRecord for the analyzed file/directory, or None if no duplicates found.
    """

    def __init__(self, base_name: str, duplicate_record: 'DuplicateRecord | None' = None):
        self.base_name = base_name
        self.duplicate_record = duplicate_record


class DeferredResult(FileAnalysisResult):
    """Deferred result for items that will be analyzed later.

    Attributes:
        base_name: Base name of the file or directory
    """

    def __init__(self, base_name: str):
        self.base_name = base_name


class AnalyzeArgs(NamedTuple):
    """Arguments for analyze command operations."""
    processor: Processor  # File processing backend for content comparison
    input_paths: list[Path]  # List of files/directories to analyze
    hash_algorithm: tuple[int, Any]  # Hash algorithm configuration (digest_size, calculator)
    archive_id: str  # Current archive identifier
    archive_path: Path  # Path to the archive
    comparison_rule: DuplicateMatchRule  # Rule defining which metadata must match for identity


@dataclass
class ReportManifest:
    """Simple manifest structure for duplicate analysis reports.

    This is persisted as manifest.json in the .report directory.
    """
    version: str = "1.0"
    """Report format version"""

    archive_path: str = ""
    """Absolute path to the archive that was used for analysis"""

    archive_id: str = ""
    """Identifier of the archive, used to validate report is still valid"""

    timestamp: str = ""
    """ISO format timestamp when analysis was performed"""

    comparison_rule: dict[str, bool] | None = None
    """Rule used for determining identical matches (which metadata fields must match)"""

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'ReportManifest':
        """Load manifest from dictionary."""
        return cls(**data)


class ReportWriter:
    """Handles writing analysis reports to .report directories with LevelDB storage."""

    def __init__(self, report_dir: Path) -> None:
        """Initialize report writer.

        Args:
            report_dir: Path to .report directory where report will be written
        """
        self.report_dir: Path = report_dir
        self.manifest_path: Path = report_dir / 'manifest.json'
        self.database_path: Path = report_dir / 'database'
        self._database: plyvel.DB | None = None

    def create_report_directory(self) -> None:
        """Create the .report directory if it doesn't exist."""
        self.report_dir.mkdir(exist_ok=True)

    def open_database(self) -> None:
        """Open the LevelDB database for storing duplicate records."""
        self.database_path.mkdir(parents=True, exist_ok=True)
        self._database = plyvel.DB(str(self.database_path), create_if_missing=True)

    def close_database(self) -> None:
        """Close the LevelDB database."""
        if self._database is not None:
            self._database.close()
            self._database = None

    def __enter__(self) -> 'ReportWriter':
        """Context manager entry."""
        self.open_database()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close_database()

    def write_duplicate_record(self, record: DuplicateRecord) -> None:
        """Write a duplicate record to the database.

        Uses the same key scheme as FileSignature storage:
        - Key: <16-byte path hash><varint sequence number>
        - Value: msgpack([path_components, duplicate_path_components_list])

        Args:
            record: The duplicate record to write
        """
        if self._database is None:
            raise RuntimeError("Database not opened. Use context manager or call open_database().")

        path_hash = self._compute_path_hash(record.path)
        prefixed_db = self._database.prefixed_db(path_hash)

        # Check if this path already exists
        next_seq_num = 0
        found_existing = False
        for key, _ in prefixed_db.iterator():
            seq_num, _ = decode_varint(key, 0)
            next_seq_num = max(next_seq_num, seq_num + 1)

            # Check if this is the same path
            existing_record = DuplicateRecord.from_msgpack(prefixed_db.get(key))
            if existing_record.path == record.path:
                # Update existing entry
                prefixed_db.put(key, record.to_msgpack())
                found_existing = True
                break

        # If path doesn't exist, insert with new sequence number
        if not found_existing:
            seq_num_bytes = encode_varint(next_seq_num)
            prefixed_db.put(seq_num_bytes, record.to_msgpack())

    def write_manifest(self, manifest: ReportManifest) -> None:
        """Write report manifest to manifest.json.

        Args:
            manifest: The report manifest to write
        """
        with open(self.manifest_path, 'w') as f:
            json.dump(manifest.to_dict(), f, indent=2)

    def read_manifest(self) -> ReportManifest:
        """Read existing report manifest.

        Returns:
            The report manifest loaded from manifest.json

        Raises:
            FileNotFoundError: If manifest.json doesn't exist
        """
        with open(self.manifest_path, 'r') as f:
            data = json.load(f)
        return ReportManifest.from_dict(data)

    def validate_report(self, current_archive_id: str) -> bool:
        """Validate that report matches current archive state.

        Args:
            current_archive_id: Current archive identifier to check against

        Returns:
            True if report is valid for current archive, False otherwise
        """
        try:
            manifest = self.read_manifest()
            return manifest.archive_id == current_archive_id
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            return False

    @staticmethod
    def _compute_path_hash(path: Path) -> bytes:
        """Compute 128-bit Murmur3 hash for a path.

        Args:
            path: File path to hash

        Returns:
            16 bytes representing the 128-bit hash value
        """
        path_str = '\0'.join(str(part) for part in path.parts)
        hash_value = mmh3.hash128(path_str.encode('utf-8'), signed=False)
        return hash_value.to_bytes(16, byteorder='big')


class AnalyzeProcessor:
    """Processor for analysis operations that encapsulates state and logic."""

    def __init__(self, store: ArchiveStore, args: AnalyzeArgs, input_path: Path, writer: ReportWriter) -> None:
        """Initialize the analyze processor.

        Args:
            store: Archive store for accessing indexed files
            args: Analysis arguments
            input_path: Path to analyze
            writer: Report writer with open database connection
        """
        self._store: ArchiveStore = store
        self._processor: Processor = args.processor
        self._input_path: Path = input_path
        self._writer: ReportWriter = writer
        self._archive_path: Path = store.archive_path
        _, self._calculate_digest = args.hash_algorithm
        self._comparison_rule: DuplicateMatchRule = args.comparison_rule
        self._listener_coordinator: DirectoryListenerCoordinator | None = None

    async def run(self) -> None:
        """Execute the analysis operation."""
        from asyncio import TaskGroup
        import stat
        from ..utils.throttler import Throttler

        async with TaskGroup() as tg:
            # Create coordinator bound to this task group
            self._listener_coordinator = DirectoryListenerCoordinator(tg)

            throttler = Throttler(tg, self._processor.concurrency * 2)

            # Walk the input path
            for file_path, context in self._store.walk(self._input_path):
                if stat.S_ISDIR(context.stat.st_mode):
                    await self._handle_directory(file_path, context)
                elif stat.S_ISREG(context.stat.st_mode):
                    await self._handle_file(file_path, context, throttler)
                else:
                    # Defer non-regular files for directory handler comparison
                    await self._defer_for_parent_directory(context)

    async def _handle_directory(self, dir_path: Path, context: FileContext) -> None:
        """Handle a directory by registering a completion listener.

        Args:
            dir_path: Absolute path to the directory
            context: File context for the directory
        """
        # Register a DirectoryListener on the context
        listener: DirectoryListener = self._listener_coordinator.register_directory(context)

        # Create a callback that binds the context to the analysis method
        async def analyze_with_context(results: list[Any]) -> FileAnalysisResult:
            return await self._analyze_directory_with_children(dir_path, context, results)

        # schedule_callback returns a task/future containing the callback's return value
        result_task: asyncio.Task[FileAnalysisResult] = listener.schedule_callback(analyze_with_context)

        # Register this directory's result task with its parent
        await self._register_file_with_parent(context, result_task)

    async def _analyze_directory_with_children(
            self,
            dir_path: Path,
            context: FileContext,
            results: list[Any]
    ) -> FileAnalysisResult:
        """Analyze a directory after all its children have been processed.

        Aggregates results from child files and directories to produce an ImmediateResult
        for the directory that can be passed to its parent.

        Args:
            dir_path: Absolute path to the directory being analyzed
            context: File context for the directory
            results: List of ImmediateResult or DeferredResult from child items

        Returns:
            ImmediateResult containing duplicate information for this directory
        """
        base_name: str = context.name or "unknown"

        # Extract parent directories from child DuplicateRecords and build directory contents
        candidate_archive_dirs: set[Path] = set()
        child_files: dict[str, ImmediateResult] = {}  # base_name -> ImmediateResult
        deferred_items: dict[str, DeferredResult] = {}  # base_name -> DeferredResult

        for result in results:
            if isinstance(result, ChildTaskException):
                # Child task failed - this shouldn't happen for normal files, but handle it
                # For directory duplicate detection, we'll skip failed children
                continue
            elif isinstance(result, ImmediateResult):
                child_files[result.base_name] = result
                # Extract parent directories from duplicates if this child has a record
                if result.duplicate_record:
                    for dup_path, _ in result.duplicate_record.duplicates:
                        # dup_path is the path to the duplicate file in the archive
                        # Get its parent directory as a candidate
                        # Skip root-level files (where parent would be Path('.'))
                        if dup_path.parent != Path('.'):
                            candidate_archive_dirs.add(dup_path.parent)
            elif isinstance(result, DeferredResult):
                deferred_items[result.base_name] = result

        # Early return if this directory has no potential duplicates
        if not candidate_archive_dirs and not deferred_items:
            return ImmediateResult(base_name)

        # Defer this directory if we have deferred items but no candidate directories yet
        # (the parent directory handler may be able to match it)
        if not candidate_archive_dirs:
            return DeferredResult(base_name)

        # Compare this directory with each candidate archive directory
        metadata_comparisons: list[DuplicateMatch] = []

        for candidate_dir in candidate_archive_dirs:
            full_candidate_path: Path = self._archive_path / candidate_dir

            # Verify the candidate directory is actually a directory
            candidate_stat = full_candidate_path.stat()
            if not stat.S_ISDIR(candidate_stat.st_mode):
                continue  # Not a directory, skip

            # Compare this candidate directory with the analyzed directory
            comparison = await self._compare_directory_with_candidate(
                dir_path, candidate_dir, child_files, deferred_items
            )

            if comparison is not None:
                metadata_comparisons.append(comparison)

        # Return early if no matching directories were found
        if not metadata_comparisons:
            return ImmediateResult(base_name)

        # Calculate relative path for this directory
        if dir_path == self._input_path:
            path: Path = Path(self._input_path.name)
        else:
            path = Path(self._input_path.name) / dir_path.relative_to(self._input_path)

        # Extract paths and calculate sizes for DuplicateRecord
        matching_archive_dirs = [comp.path for comp in metadata_comparisons]

        # Calculate total_size: sum of ALL child file sizes
        total_size = 0
        for base_name, child_result in child_files.items():
            if child_result.duplicate_record:
                total_size += child_result.duplicate_record.total_size

        # Calculate duplicated_size: sum of child files that have ANY duplicates (deduplicated)
        duplicated_size = 0
        files_with_any_duplicates: set[str] = set()
        for base_name, child_result in child_files.items():
            if child_result.duplicate_record and base_name not in files_with_any_duplicates:
                files_with_any_duplicates.add(base_name)
                duplicated_size += child_result.duplicate_record.duplicated_size

        # Create duplicates list as tuples
        duplicates: list[tuple[Path, DuplicateMatch]] = list(zip(matching_archive_dirs, metadata_comparisons))

        # Create and write duplicate record
        record: DuplicateRecord = DuplicateRecord(
            path, duplicates, total_size, duplicated_size
        )
        self._writer.write_duplicate_record(record)

        return ImmediateResult(base_name, record)

    def _compare_deferred_item(
            self,
            analyzed_item_path: Path,
            candidate_item_path: Path,
            analyzed_stat: os.stat_result,
            candidate_stat: os.stat_result
    ) -> tuple[bool, int]:
        """Compare a single deferred item (symlink, device, pipe, socket, or subdirectory).

        Args:
            analyzed_item_path: Absolute path to the analyzed item
            candidate_item_path: Absolute path to the candidate item
            analyzed_stat: stat result for the analyzed item (from lstat)
            candidate_stat: stat result for the candidate item (from lstat)

        Returns:
            Tuple of (items_match, duplicated_items_count)
            - items_match: True if items match
            - duplicated_items_count: Count of matching items (1 for single items, more for subdirectories)
        """
        # Check if both have the same file type
        if stat.S_IFMT(analyzed_stat.st_mode) != stat.S_IFMT(candidate_stat.st_mode):
            return False, 0

        # Compare based on file type
        if stat.S_ISLNK(analyzed_stat.st_mode):
            # Symlinks: compare targets
            if analyzed_item_path.readlink() != candidate_item_path.readlink():
                return False, 0
            return True, 1

        elif stat.S_ISBLK(analyzed_stat.st_mode) or stat.S_ISCHR(analyzed_stat.st_mode):
            # Device files: compare major/minor numbers
            if (os.major(analyzed_stat.st_rdev) != os.major(candidate_stat.st_rdev) or
                os.minor(analyzed_stat.st_rdev) != os.minor(candidate_stat.st_rdev)):
                return False, 0
            return True, 1

        elif stat.S_ISFIFO(analyzed_stat.st_mode) or stat.S_ISSOCK(analyzed_stat.st_mode):
            # Pipes/sockets: existence check is sufficient
            return True, 1

        elif stat.S_ISDIR(analyzed_stat.st_mode):
            # Subdirectory: recursively check if structure matches
            return self._check_deferred_subdirectory_structure_matches(analyzed_item_path, candidate_item_path)

        else:
            # Unknown file type
            return False, 0

    def _check_deferred_subdirectory_structure_matches(
            self,
            analyzed_dir: Path,
            candidate_dir: Path
    ) -> tuple[bool, int]:
        """Check if directory structure matches for deferred subdirectory comparison.

        This is called when a parent directory gets a DeferredResult for a subdirectory,
        which happens when:
        1. The subdirectory contains only regular files (no duplicates found yet), OR
        2. The subdirectory contains deferred items (symlinks, devices, etc.) but no regular
           files with duplicates

        In both cases, the subdirectory couldn't be compared during its own analysis phase
        because there were no content-verified duplicates to provide candidate archive paths.
        The parent directory handler must check if the subdirectory's structure exists in
        the candidate archive directory.

        IMPORTANT: Regular files in deferred subdirectories are NOT counted as duplicated_items
        because they haven't been content-verified. Only structural items (symlinks, devices,
        pipes, sockets) and recursively-verified subdirectories are counted.

        Args:
            analyzed_dir: Absolute path to the analyzed directory
            candidate_dir: Absolute path to the candidate archive directory

        Returns:
            Tuple of (structure_matches, verified_items_count)
            - structure_matches: True if directory structure exists in candidate
            - verified_items_count: Count of verified non-file items (symlinks, devices, subdirs)
        """
        analyzed_items = {item.name: item for item in analyzed_dir.iterdir()}
        candidate_items = {item.name: item for item in candidate_dir.iterdir()}

        verified_items = 0

        for name, analyzed_item in analyzed_items.items():
            if name not in candidate_items:
                return False, 0  # Item missing in candidate

            candidate_item = candidate_items[name]
            analyzed_stat = analyzed_item.lstat()
            candidate_stat = candidate_item.lstat()

            # Compare based on file type
            if stat.S_ISREG(analyzed_stat.st_mode):
                # Regular files: only verify size matches for structural comparison
                # Do NOT count as duplicated since content wasn't verified
                if analyzed_stat.st_size != candidate_stat.st_size:
                    return False, 0
                # Note: Not incrementing verified_items for regular files
            else:
                # Use common helper for all other file types (symlinks, devices, pipes, sockets, subdirectories)
                match, item_count = self._compare_deferred_item(
                    analyzed_item, candidate_item, analyzed_stat, candidate_stat
                )
                if not match:
                    return False, 0
                verified_items += item_count

        return True, verified_items

    async def _compare_directory_with_candidate(
            self,
            dir_path: Path,
            candidate_dir: Path,
            child_files: dict[str, ImmediateResult],
            deferred_items: dict[str, DeferredResult]
    ) -> DuplicateMatch | None:
        """Compare a directory with a candidate archive directory.

        Args:
            dir_path: Path to the directory being analyzed
            candidate_dir: Relative path to the candidate archive directory
            child_files: Map of base_name -> ImmediateResult for child files
            deferred_items: Map of base_name -> DeferredResult for deferred items

        Returns:
            DuplicateMatch if the candidate matches, None otherwise.
            For directories, ?_match fields are false if any child file has false value.
        """
        full_candidate_path: Path = self._archive_path / candidate_dir

        # Track metadata matches across all child files
        # Start with True, set to False if any child has False
        all_mtime_match: bool = True
        all_atime_match: bool = True
        all_ctime_match: bool = True
        all_mode_match: bool = True
        all_owner_match: bool = True
        all_group_match: bool = True

        # Check that regular files with duplicates have at least one in this candidate directory
        files_matched: int = 0
        files_with_identical_duplicates: int = 0  # Files where duplicate is identical
        total_files_with_duplicates: int = 0
        duplicated_items: int = 0
        total_duplicate_data_size: int = 0

        # Track the first comparison rule we encounter to validate consistency
        first_rule: DuplicateMatchRule | None = None

        for base_name, child_result in child_files.items():
            if child_result.duplicate_record:
                total_files_with_duplicates += 1
                # Check if any duplicate is in this candidate directory
                for dup_path, comparison in child_result.duplicate_record.duplicates:
                    if dup_path.parent == candidate_dir:
                        files_matched += 1

                        # Validate that all children use the same comparison rule
                        if comparison.rule is not None:
                            if first_rule is None:
                                first_rule = comparison.rule
                            elif first_rule != comparison.rule:
                                raise ValueError(
                                    f"Inconsistent comparison rules within directory {dir_path}: "
                                    f"expected {first_rule} but found {comparison.rule} for child {base_name}"
                                )

                        # Track metadata matches from child
                        if not comparison.mtime_match:
                            all_mtime_match = False
                        if not comparison.atime_match:
                            all_atime_match = False
                        if not comparison.ctime_match:
                            all_ctime_match = False
                        if not comparison.mode_match:
                            all_mode_match = False
                        if not comparison.owner_match:
                            all_owner_match = False
                        if not comparison.group_match:
                            all_group_match = False
                        # Add child's duplicated_items count
                        duplicated_items += comparison.duplicated_items
                        # Add child's data size
                        total_duplicate_data_size += child_result.duplicate_record.duplicated_size
                        # Check if this child's duplicate is identical
                        if comparison.is_identical:
                            files_with_identical_duplicates += 1
                        break

        # Check deferred items (symlinks, devices, subdirectories, etc.)
        deferred_items_match: bool = True
        for base_name, deferred_result in deferred_items.items():
            # Construct paths for both analyzed and candidate items
            analyzed_item_path: Path = dir_path / base_name
            candidate_item_path: Path = full_candidate_path / base_name

            # Get file stats (don't follow symlinks)
            analyzed_stat: os.stat_result = analyzed_item_path.lstat()
            candidate_stat: os.stat_result = candidate_item_path.lstat()

            # Use common helper to compare the deferred item
            match, item_count = self._compare_deferred_item(
                analyzed_item_path, candidate_item_path, analyzed_stat, candidate_stat
            )
            if not match:
                return None  # Deferred item doesn't match

            duplicated_items += item_count

        # Check for extra files in candidate directory that aren't in analyzed directory
        candidate_items: set[str] = set()
        for item in full_candidate_path.iterdir():
            candidate_items.add(item.name)

        analyzed_items: set[str] = set(child_files.keys()) | set(deferred_items.keys())
        extra_items: set[str] = candidate_items - analyzed_items
        has_extra_items: bool = len(extra_items) > 0

        # Verify at least some files matched before considering this a duplicate
        if files_matched == 0:
            return None

        # Determine match type
        # is_superset: all analyzed files exist in candidate (may have extras)
        is_superset: bool = files_matched == total_files_with_duplicates and deferred_items_match

        # For is_identical, first check structural requirements (content match, no extras)
        # Then use the comparison rule to determine if metadata matches
        is_identical: bool = (is_superset and
                             not has_extra_items and
                             files_with_identical_duplicates == total_files_with_duplicates)

        # Compare directory-level metadata
        dir_stat: os.stat_result = dir_path.stat()
        dup_stat: os.stat_result = full_candidate_path.stat()

        dir_mtime_match: bool = dir_stat.st_mtime_ns == dup_stat.st_mtime_ns
        dir_atime_match: bool = dir_stat.st_atime_ns == dup_stat.st_atime_ns
        dir_ctime_match: bool = dir_stat.st_ctime_ns == dup_stat.st_ctime_ns
        dir_mode_match: bool = dir_stat.st_mode == dup_stat.st_mode
        dir_owner_match: bool = dir_stat.st_uid == dup_stat.st_uid
        dir_group_match: bool = dir_stat.st_gid == dup_stat.st_gid

        # For directories, metadata must match both directory AND all children
        # If is_identical is already False due to content, directory metadata can't make it True
        # But if content is identical, use the comparison rule to check directory-level metadata
        if is_identical:
            # Combine child and directory-level metadata matches
            combined_mtime = all_mtime_match and dir_mtime_match
            combined_atime = all_atime_match and dir_atime_match
            combined_ctime = all_ctime_match and dir_ctime_match
            combined_mode = all_mode_match and dir_mode_match
            combined_owner = all_owner_match and dir_owner_match
            combined_group = all_group_match and dir_group_match

            # Use the comparison rule to determine if metadata matches
            is_identical = self._comparison_rule.calculate_is_identical(
                mtime_match=combined_mtime, atime_match=combined_atime, ctime_match=combined_ctime,
                mode_match=combined_mode, owner_match=combined_owner, group_match=combined_group
            )

        return DuplicateMatch(
            candidate_dir,
            mtime_match=all_mtime_match and dir_mtime_match,
            atime_match=all_atime_match and dir_atime_match,
            ctime_match=all_ctime_match and dir_ctime_match,
            mode_match=all_mode_match and dir_mode_match,
            owner_match=all_owner_match and dir_owner_match,
            group_match=all_group_match and dir_group_match,
            duplicated_size=total_duplicate_data_size,
            duplicated_items=duplicated_items,
            is_identical=is_identical,
            is_superset=is_superset,
            rule=self._comparison_rule
        )

    async def _handle_file(self, file_path: Path, context: FileContext, throttler: Any) -> None:
        """Handle a file by scheduling analysis.

        Args:
            file_path: Path to the file
            context: File context for the file
            throttler: Throttler for concurrency control
        """
        # Schedule file analysis
        file_task: asyncio.Task[FileAnalysisResult] = await throttler.schedule(self._analyze_file(file_path, context))
        await self._register_file_with_parent(context, file_task)

    async def _defer_for_parent_directory(self, context: FileContext) -> None:
        """Defer a non-regular file for comparison by its parent directory handler.

        Non-regular files are not analyzed immediately but are registered with their parent
        directory so that _analyze_directory_with_children can process them when necessary.

        Args:
            context: File context for the file
        """
        # Create an immediately resolved future with a deferred result
        future: asyncio.Future[FileAnalysisResult] = asyncio.Future()
        base_name: str = context.name or "unknown"
        future.set_result(DeferredResult(base_name))
        await self._register_file_with_parent(context, future)

    async def _register_file_with_parent(
            self,
            context: FileContext,
            file_task: asyncio.Future[FileAnalysisResult] | asyncio.Task[FileAnalysisResult]
    ) -> None:
        """Register a file task/future with its parent directory's listener.

        Args:
            context: File context for the file
            file_task: Task or Future representing the file's analysis
        """
        # Add the task to parent directory's listener if it exists
        try:
            parent_context = context.parent
        except LookupError:
            # No parent context (root level file)
            return

        context_key = self._listener_coordinator.context_key
        if context_key in parent_context:
            parent_listener: DirectoryListener = parent_context[context_key]
            parent_listener.add_child(file_task)

    async def _analyze_file(self, file_path: Path, context: FileContext) -> ImmediateResult:
        """Analyze a single file and write duplicate record to database if duplicates found.

        Args:
            file_path: Path to the file being analyzed
            context: File context for the file

        Returns:
            ImmediateResult containing all duplicate information
        """
        # Calculate digest
        digest: bytes = await self._calculate_digest(file_path)

        # Find matching files in the archive
        duplicates_found: list[Path] = []

        for ec_id, paths in self._store.list_content_equivalent_classes(digest):
            # Verify content actually matches (handle hash collisions)
            if await self._processor.compare_content(self._archive_path / paths[0], file_path):
                # All files in this EC class have identical content
                duplicates_found = paths
                break

        if not duplicates_found:
            # No duplicates found, return immediate result with empty list
            return ImmediateResult(file_path.name)

        # Calculate relative path including the base name of input_path
        if file_path == self._input_path:
            # File is the input itself
            path: Path = Path(self._input_path.name)
        else:
            # File is inside input directory
            path = Path(self._input_path.name) / file_path.relative_to(self._input_path)

        # Get metadata for the analyzed file
        analyzed_stat: os.stat_result = file_path.stat()
        file_size: int = analyzed_stat.st_size

        # Compare metadata with each duplicate
        metadata_comparisons: list[DuplicateMatch] = []
        for dup_path in duplicates_found:
            full_dup_path: Path = self._archive_path / dup_path
            dup_stat: os.stat_result = full_dup_path.stat()

            # Compare metadata attributes
            mtime_match: bool = analyzed_stat.st_mtime_ns == dup_stat.st_mtime_ns
            atime_match: bool = analyzed_stat.st_atime_ns == dup_stat.st_atime_ns
            ctime_match: bool = analyzed_stat.st_ctime_ns == dup_stat.st_ctime_ns
            mode_match: bool = analyzed_stat.st_mode == dup_stat.st_mode
            owner_match: bool = analyzed_stat.st_uid == dup_stat.st_uid
            group_match: bool = analyzed_stat.st_gid == dup_stat.st_gid

            # For files: is_identical means all metadata matches (content already verified)
            # is_superset equals is_identical for files
            # Use the comparison rule to determine which metadata fields must match
            is_identical: bool = self._comparison_rule.calculate_is_identical(
                mtime_match=mtime_match, atime_match=atime_match, ctime_match=ctime_match,
                mode_match=mode_match, owner_match=owner_match, group_match=group_match
            )
            is_superset: bool = is_identical

            metadata_comparisons.append(DuplicateMatch(
                dup_path,
                mtime_match=mtime_match, atime_match=atime_match, ctime_match=ctime_match, mode_match=mode_match,
                owner_match=owner_match, group_match=group_match,
                duplicated_size=file_size, duplicated_items=1,
                is_identical=is_identical, is_superset=is_superset,
                rule=self._comparison_rule
            ))

        # For files, both total_size and duplicated_size are the file size
        # total_size: size of this file
        # duplicated_size: size of this file (since it has duplicates)
        total_size: int = file_size
        duplicated_size: int = file_size

        # Create duplicates list as tuples
        duplicates: list[tuple[Path, DuplicateMatch]] = list(zip(duplicates_found, metadata_comparisons))

        # Create and write duplicate record
        record: DuplicateRecord = DuplicateRecord(
            path, duplicates, total_size, duplicated_size
        )
        self._writer.write_duplicate_record(record)

        # Return immediate result with the duplicate record
        return ImmediateResult(file_path.name, record)


async def do_analyze(
        store: ArchiveStore,
        args: AnalyzeArgs) -> None:
    """Async implementation of analysis report generation.

    This function analyzes each input path and generates a .report directory
    containing a LevelDB database with duplicate records.

    Args:
        store: Archive store for accessing indexed files
        args: Analysis arguments including paths to analyze

    Raises:
        FileExistsError: If a file exists at the report directory path
    """
    # Process each input path
    for input_path in args.input_paths:
        # Create report directory
        report_dir: Path = get_report_directory_path(input_path)

        # Check if a file with the same name already exists
        if report_dir.exists() and report_dir.is_file():
            raise FileExistsError(
                f"Cannot create report directory '{report_dir}': "
                f"a file with this name already exists"
            )

        writer: ReportWriter = ReportWriter(report_dir)
        writer.create_report_directory()

        # Create and write manifest
        manifest: ReportManifest = ReportManifest(
            archive_path=str(args.archive_path.resolve()),
            archive_id=args.archive_id,
            timestamp=datetime.now().isoformat(),
            comparison_rule=args.comparison_rule.to_dict()
        )
        writer.write_manifest(manifest)

        # Analyze the path with database context
        with writer:
            processor: AnalyzeProcessor = AnalyzeProcessor(store, args, input_path, writer)
            await processor.run()


def get_report_directory_path(input_path: Path) -> Path:
    """Generate the .report directory path for a given input path.

    Args:
        input_path: The file or directory being analyzed

    Returns:
        Path to the .report directory (e.g., /path/to/file.report)
    """
    # For /path/to/file.txt -> /path/to/file.txt.report
    # For /path/to/dir -> /path/to/dir.report
    return Path(str(input_path) + '.report')
