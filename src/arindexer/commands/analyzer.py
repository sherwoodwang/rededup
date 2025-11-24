import asyncio
import json
import logging
import os
import stat
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple, Union

import mmh3
import msgpack
import plyvel

logger = logging.getLogger(__name__)

from ..utils.processor import Processor
from ..utils.varint import encode_varint, decode_varint
from ..utils.directory_listener import DirectoryListenerCoordinator, DirectoryListener
from ..utils.walker import FileContext
from ..store.archive_store import ArchiveStore


class MetadataMatchReducer:
    """Stateful reducer for computing metadata match results from stat comparisons.

    This class encapsulates the logic for reducing metadata comparison results
    from multiple stat comparisons (for child items, files, directories, or special files)
    down to aggregate match results. It also works for single stat comparisons.
    It starts with all metadata fields matching (True) and sets them to False if any
    comparison has a non-matching value.

    This implements an AND reduction pattern: all comparisons must have matching metadata
    for the final result to be considered matching.
    """

    def __init__(self, comparison_rule: 'DuplicateMatchRule') -> None:
        """Initialize reducer with all metadata matches set to True.

        Args:
            comparison_rule: Rule defining which metadata fields must match for identity
        """
        self.mtime_match: bool = True
        self.atime_match: bool = True
        self.ctime_match: bool = True
        self.mode_match: bool = True
        self.owner_match: bool = True
        self.group_match: bool = True
        self.duplicated_items: int = 0
        self.duplicated_size: int = 0
        self._comparison_rule: DuplicateMatchRule = comparison_rule

    def aggregate_from_match(self, match: Union['DuplicateMatch', None]) -> None:
        """Aggregate metadata matches from a DuplicateMatch.

        Args:
            match: DuplicateMatch to aggregate from
        """

        if match is None:
            return

        if not match.mtime_match:
            self.mtime_match = False
        if not match.atime_match:
            self.atime_match = False
        if not match.ctime_match:
            self.ctime_match = False
        if not match.mode_match:
            self.mode_match = False
        if not match.owner_match:
            self.owner_match = False
        if not match.group_match:
            self.group_match = False

        self.duplicated_items += match.duplicated_items
        self.duplicated_size += match.duplicated_size

    def aggregate_from_stat(self, analyzed_stat: os.stat_result, candidate_stat: os.stat_result) -> None:
        """Aggregate metadata matches by comparing two stat results.

        Args:
            analyzed_stat: stat result for the analyzed item
            candidate_stat: stat result for the candidate item
        """
        # Create a temporary DuplicateMatch with comparison results and aggregate from it
        mtime_match = analyzed_stat.st_mtime_ns == candidate_stat.st_mtime_ns
        atime_match = analyzed_stat.st_atime_ns == candidate_stat.st_atime_ns
        ctime_match = analyzed_stat.st_ctime_ns == candidate_stat.st_ctime_ns
        mode_match = analyzed_stat.st_mode == candidate_stat.st_mode
        owner_match = analyzed_stat.st_uid == candidate_stat.st_uid
        group_match = analyzed_stat.st_gid == candidate_stat.st_gid

        # Use a temporary match object to leverage aggregate_from_match logic
        temp_match = DuplicateMatch(
            Path('.'),  # Dummy path for aggregation purposes
            mtime_match=mtime_match,
            atime_match=atime_match,
            ctime_match=ctime_match,
            mode_match=mode_match,
            owner_match=owner_match,
            group_match=group_match,
            duplicated_size=0,
            duplicated_items=0
        )
        self.aggregate_from_match(temp_match)

    def create_duplicate_match(
            self,
            path: Path,
            *,
            non_identical: bool,
            non_superset: bool
    ) -> 'DuplicateMatch':
        """Create a DuplicateMatch using aggregated metadata and calculate is_identical.

        Args:
            path: Path to the duplicate (relative to archive root)
            non_identical: If True, forces is_identical to False (content/structure differs).
                          If False, calculates is_identical using the comparison rule.
            non_superset: If True, forces is_superset to False (not all items present).
                         If False, sets is_superset equal to is_identical.

        Returns:
            DuplicateMatch with aggregated metadata and calculated is_identical
        """
        # Calculate metadata match using comparison rule
        metadata_matches = self._comparison_rule.calculate_is_identical(
            mtime_match=self.mtime_match,
            atime_match=self.atime_match,
            ctime_match=self.ctime_match,
            mode_match=self.mode_match,
            owner_match=self.owner_match,
            group_match=self.group_match
        )

        # Calculate is_identical: requires both structure match AND metadata match
        is_identical = (not non_identical) and metadata_matches

        # Calculate is_superset: requires all analyzed items present AND metadata match
        is_superset = (not non_superset) and metadata_matches

        return DuplicateMatch(
            path,
            mtime_match=self.mtime_match,
            atime_match=self.atime_match,
            ctime_match=self.ctime_match,
            mode_match=self.mode_match,
            owner_match=self.owner_match,
            group_match=self.group_match,
            duplicated_size=self.duplicated_size,
            duplicated_items=self.duplicated_items,
            is_identical=is_identical,
            is_superset=is_superset,
            rule=self._comparison_rule
        )


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
                 include_ctime: bool = False, include_mode: bool = True,
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
            include_ctime=data.get('include_ctime', False),
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
        duplicates: List of DuplicateMatch objects for each duplicate found in the archive.
                   Each DuplicateMatch contains the path and metadata comparison results.
        total_size: Total size in bytes of all content within this path.
                   For files: the file size.
                   For directories: sum of all descendant file sizes, aggregated recursively from child results
                                   (whether or not they have duplicates).
        total_items: Total count of items within this path, using the same counting as duplicated_items.
                    For files: 1 (the file itself).
                    For directories: count of all descendant items, aggregated recursively from child results
                                    (counted the same way as duplicated_items:
                                    regular files count as 1, special files count as 1, directories count as 0).
        duplicated_size: Total size in bytes of this analyzed file/directory's content that
                        has content-equivalent files in the archive. This is the deduplicated size - each
                        file is counted once regardless of how many content-equivalent files exist in the archive.
                        For files: the file size (if content-equivalent files exist in the archive).
                        For directories: sum of all descendant file sizes that have any content-equivalent files,
                                       aggregated recursively from child results across all archive paths
                                       (simple sum without requiring matching files to be at the same location
                                       in any hierarchy).

                        IMPORTANT: Semantic difference from DuplicateMatch.duplicated_size:
                        - DuplicateRecord.duplicated_size: Global deduplicated size. Each file in the analyzed
                          path is counted once, regardless of how many content-equivalent files exist across
                          all archive paths. This is a simple sum of duplicated_size from all child results,
                          without structural requirements.
                        - DuplicateMatch.duplicated_size: Localized size for a specific archive path.
                          When a file in the analyzed path has multiple content-equivalent files in different
                          archive directories, each DuplicateMatch counts that file's size independently.
                          For directories, only includes files that exist at the same relative location
                          within the specific archive directory's hierarchy (hierarchy must match).
        duplicated_items: Total count of items within this analyzed file/directory's content that
                         have content-equivalent files in the archive. This is the deduplicated count - each
                         item is counted once regardless of how many content-equivalent files exist in the archive.
                         For files: 1 (if content-equivalent files exist in the archive).
                         For directories: count of all descendant items that have any content-equivalent files,
                                        aggregated recursively from child results across all archive paths
                                        (simple sum without requiring matching items to be at the same location
                                        in any hierarchy).

                         IMPORTANT: Semantic difference from DuplicateMatch.duplicated_items:
                         - DuplicateRecord.duplicated_items: Global deduplicated count. Each item in the analyzed
                           path is counted once, regardless of how many content-equivalent files exist across
                           all archive paths. This is a simple sum of duplicated_items from all child results,
                           without structural requirements.
                         - DuplicateMatch.duplicated_items: Localized count for a specific archive path.
                           When an item in the analyzed path has multiple content-equivalent files in different
                           archive directories, each DuplicateMatch counts that item independently.
                           For directories, only includes items that exist at the same relative location
                           within the specific archive directory's hierarchy (hierarchy must match).
    """

    def __init__(
            self,
            path: Path,
            duplicates: list[DuplicateMatch] | None = None,
            total_size: int = 0,
            total_items: int = 0,
            duplicated_size: int = 0,
            duplicated_items: int = 0):
        self.path = path
        self.duplicates: list[DuplicateMatch] = duplicates or []
        self.total_size: int = total_size
        self.total_items: int = total_items
        self.duplicated_size: int = duplicated_size
        self.duplicated_items: int = duplicated_items

    def to_msgpack(self) -> bytes:
        """Serialize to msgpack format for storage.

        Returns:
            Msgpack-encoded bytes containing [path_components, duplicate_data_list, total_size, total_items, duplicated_size, duplicated_items]
            where duplicate_data_list is a list of [path_components, mtime_match, atime_match, ctime_match, mode_match,
            owner_match, group_match, duplicated_size, duplicated_items, is_identical, is_superset]
        """
        path_components = [str(part) for part in self.path.parts]

        duplicate_data = []
        for comparison in self.duplicates:
            dup_path_components = [str(part) for part in comparison.path.parts]
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

        result = msgpack.dumps([path_components, duplicate_data, self.total_size, self.total_items, self.duplicated_size,
                              self.duplicated_items])
        assert isinstance(result, bytes)
        return result

    @classmethod
    def from_msgpack(cls, data: bytes) -> 'DuplicateRecord':
        """Deserialize from msgpack format.

        Args:
            data: Msgpack-encoded bytes

        Returns:
            DuplicateRecord instance
        """
        decoded = msgpack.loads(data)
        assert isinstance(decoded, list)
        path_components: list[str] = decoded[0]
        duplicate_data: list[list[Any]] = decoded[1]
        total_size: int = decoded[2]
        total_items: int = decoded[3]
        duplicated_size: int = decoded[4]
        duplicated_items: int = decoded[5]

        path = Path(*path_components)

        duplicates: list[DuplicateMatch] = []

        for dup_data in duplicate_data:
            dup_path_components, mtime_match, atime_match, ctime_match, mode_match, owner_match, group_match, \
                dup_duplicated_size, dup_duplicated_items, is_identical, is_superset = dup_data
            dup_path = Path(*dup_path_components)
            comparison = DuplicateMatch(
                dup_path,
                mtime_match=mtime_match, atime_match=atime_match, ctime_match=ctime_match, mode_match=mode_match,
                owner_match=owner_match, group_match=group_match,
                duplicated_size=dup_duplicated_size, duplicated_items=dup_duplicated_items,
                is_identical=is_identical, is_superset=is_superset
            )
            duplicates.append(comparison)

        return cls(path, duplicates, total_size, total_items, duplicated_size, duplicated_items)


class FileAnalysisResult(ABC):
    """Abstract base class for file analysis results.

    Subclasses represent different types of analysis outcomes for files and directories.
    """
    pass


class ImmediateResult(FileAnalysisResult):
    """Immediate result exposing DuplicateRecord interface with zero values if no duplicates found.

    This class represents the immediate analysis result for a file or directory. It provides
    a consistent interface for accessing duplicate information, returning sensible defaults
    (empty list or zero) when no duplicates are found.

    The report_path represents the path to the analyzed item relative to the parent of the
    input path (the path that was passed to the analyzer). This allows consistent tracking
    of paths across both files and directories during analysis.
    """

    def __init__(
            self,
            report_path: Path,
            duplicates: list[DuplicateMatch],
            total_size: int,
            total_items: int,
            duplicated_size: int,
            duplicated_items: int
    ):
        """Initialize an ImmediateResult.

        Args:
            report_path: Path relative to the parent of the input path being analyzed
            duplicates: List of DuplicateMatch objects for each duplicate found in the archive
            total_size: Total size in bytes of all content within this path
            total_items: Total count of items within this path
            duplicated_size: Total size in bytes of content that has content-equivalent files in the archive
            duplicated_items: Total count of items that have content-equivalent files in the archive
        """
        self.report_path = report_path
        self.duplicates = duplicates
        self.total_size = total_size
        self.total_items = total_items
        self.duplicated_size = duplicated_size
        self.duplicated_items = duplicated_items
        self.base_name = report_path.name

    @classmethod
    def from_duplicate_record(cls, duplicate_record: DuplicateRecord) -> 'ImmediateResult':
        """Create an ImmediateResult from a DuplicateRecord.

        Args:
            duplicate_record: Complete duplicate record with path and duplicate information

        Returns:
            ImmediateResult instance constructed from the duplicate record
        """
        return cls(
            report_path=duplicate_record.path,
            duplicates=duplicate_record.duplicates,
            total_size=duplicate_record.total_size,
            total_items=duplicate_record.total_items,
            duplicated_size=duplicate_record.duplicated_size,
            duplicated_items=duplicate_record.duplicated_items
        )


class DeferredResult(FileAnalysisResult):
    """Deferred result for items that will be analyzed later.

    Used for non-regular files (symlinks, devices, etc.) that cannot be analyzed
    immediately and must be deferred for comparison by their parent directory handler.

    Attributes:
        base_name: Base name of the file or directory, deduced from report_path
    """

    def __init__(self, report_path: Path, total_size: int, total_items: int, duplicated_size: int,
                 duplicated_items: int):
        """Initialize a DeferredResult.

        Args:
            report_path: Path relative to the parent of the input path being analyzed.
                        The base_name is derived from the name component of this path.
            total_size: Total size in bytes
            total_items: Total count of items
            duplicated_size: Total duplicated size in bytes
            duplicated_items: Total count of duplicated items
        """
        self.report_path = report_path
        self.base_name = report_path.name
        self.total_size = total_size
        self.total_items = total_items
        self.duplicated_size = duplicated_size
        self.duplicated_items = duplicated_items


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


class ReportStore:
    """Handles reading and writing analysis reports to .report directories with LevelDB storage."""

    def __init__(self, report_dir: Path, analyzed_path: Path | None = None) -> None:
        """Initialize report store.

        Args:
            report_dir: Path to .report directory where report is stored
            analyzed_path: The path that was analyzed (the root of the analysis).
                          Required for reading operations, optional for write-only usage.
        """
        self.report_dir: Path = report_dir
        self.manifest_path: Path = report_dir / 'manifest.json'
        self.database_path: Path = report_dir / 'database'
        self.analyzed_path: Path | None = analyzed_path
        self._database: plyvel.DB | None = None

    def create_report_directory(self) -> None:
        """Create the .report directory if it doesn't exist."""
        self.report_dir.mkdir(exist_ok=True)

    def open_database(self, *, create_if_missing: bool = False) -> None:
        """Open the LevelDB database.

        Args:
            create_if_missing: If True, create the database if it doesn't exist.
                              If False, raise FileNotFoundError if database doesn't exist.
        """
        if create_if_missing:
            self.database_path.mkdir(parents=True, exist_ok=True)
        elif not self.database_path.exists():
            raise FileNotFoundError(f"Database directory not found: {self.database_path}")
        self._database = plyvel.DB(str(self.database_path), create_if_missing=create_if_missing)

    def close_database(self) -> None:
        """Close the LevelDB database."""
        if self._database is not None:
            self._database.close()
            self._database = None

    def __enter__(self) -> 'ReportStore':
        """Context manager entry."""
        self.open_database()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close_database()

    def read_duplicate_record(self, path: Path) -> DuplicateRecord | None:
        """Read a duplicate record from the database by path.

        Args:
            path: Path to look up (relative to the analyzed target, including target's base name)

        Returns:
            DuplicateRecord if found, None otherwise
        """
        if self._database is None:
            raise RuntimeError("Database not opened. Use context manager or call open_database().")

        path_hash = self._compute_path_hash(path)
        prefixed_db = self._database.prefixed_db(path_hash)

        # Iterate through all records with this hash prefix
        for key, value in prefixed_db.iterator():
            record = DuplicateRecord.from_msgpack(value)
            if record.path == path:
                return record

        return None

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

    def __init__(self, store: ArchiveStore, args: AnalyzeArgs, input_path: Path, report_store: ReportStore) -> None:
        """Initialize the analyze processor.

        Args:
            store: Archive store for accessing indexed files
            args: Analysis arguments
            input_path: Path to analyze
            report_store: Report store with open database connection
        """
        self._store: ArchiveStore = store
        self._processor: Processor = args.processor
        self._input_path: Path = input_path
        self._report_store: ReportStore = report_store
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
                    await self._defer_for_parent_directory(file_path, context)

    async def _handle_directory(self, dir_path: Path, context: FileContext) -> None:
        """Handle a directory by registering a completion listener.

        Args:
            dir_path: Absolute path to the directory
            context: File context for the directory
        """
        logger.info("Handling directory: %s", dir_path)

        # Register a DirectoryListener on the context
        assert self._listener_coordinator is not None
        listener: DirectoryListener = self._listener_coordinator.register_directory(context)

        result_task: asyncio.Task[FileAnalysisResult] = listener.schedule_callback(
            lambda results: self._analyze_directory_with_children(dir_path, context, results)
        )

        # Register this directory's result with its parent directory's listener
        self._listener_coordinator.register_child_with_parent(context, result_task)

        logger.info("Completed handling directory: %s", dir_path)

    async def _handle_file(self, file_path: Path, context: FileContext, throttler: Any) -> None:
        """Handle a file by scheduling analysis.

        Args:
            file_path: Path to the file
            context: File context for the file
            throttler: Throttler for concurrency control
        """
        logger.info("Handling file: %s", file_path)

        file_task: asyncio.Task[FileAnalysisResult] = await throttler.schedule(self._analyze_file(file_path, context))
        # Register this file's analysis result with its parent directory's listener
        assert self._listener_coordinator is not None
        self._listener_coordinator.register_child_with_parent(context, file_task)

        logger.info("Completed handling file: %s", file_path)

    async def _defer_for_parent_directory(self, file_path: Path, context: FileContext) -> None:
        """Defer a non-regular file for comparison by its parent directory handler.

        Non-regular files are not analyzed immediately but are registered with their parent
        directory so that _analyze_directory_with_children can process them when necessary.

        Args:
            file_path: Absolute path to the file being deferred
            context: File context for the file
        """
        # Calculate relative path from parent of input_path
        relative_path: Path = file_path.relative_to(self._input_path.parent)

        future: asyncio.Future[FileAnalysisResult] = asyncio.Future()
        # Even through it is feasible to get file size for some special files, like symlinks, on some systems, this
        # is avoided here for consistency and simplicity.
        future.set_result(DeferredResult(relative_path, 0, 1, 0, 0))
        # Register this deferred item with its parent directory's listener
        assert self._listener_coordinator is not None
        self._listener_coordinator.register_child_with_parent(context, future)

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
            dir_path: Path to the directory being analyzed (can be used directly from working directory)
            context: File context for the directory being analyzed
            results: List of ImmediateResult or DeferredResult from child items

        Returns:
            ImmediateResult containing duplicate information for this directory
        """
        assert context.relative_path is not None, "Directory context must have a relative path"
        logger.info("Analyzing directory: %s", context.relative_path)

        # Accumulate totals from all child results
        # total_size: sum of all child file sizes (whether duplicated or not)
        # total_items: count of all child items, including nested ones (files and special files, not directories)
        # duplicated_size: sum of child file sizes that have content-equivalent files in archive
        # duplicated_items: count of child items that have content-equivalent files in archive
        total_size = 0
        total_items = 0
        duplicated_size = 0
        duplicated_items = 0

        # Build map of candidate archive directories to their pending file comparisons
        # candidate_matches[archive_dir][base_name] = DuplicateMatch for file in archive_dir
        candidate_matches: dict[Path, dict[str, DuplicateMatch]] = {}
        # base names of deferred items
        deferred_items: set[str] = set()
        # all base names of child items that are directly under this directory
        all_items: set[str] = set()

        for result in results:
            if isinstance(result, ImmediateResult):
                # Extract parent directories from duplicates if this child has a record
                for comparison in result.duplicates:
                    # Only process duplicates that match the current file name
                    if result.base_name == comparison.path.name:
                        # comparison.path is the path to the duplicate file in the archive
                        # Get its parent directory as a candidate
                        # Skip root-level files (where parent would be Path('.'))
                        if comparison.path.parent != Path('.'):
                            parent_dir = comparison.path.parent
                            if parent_dir not in candidate_matches:
                                candidate_matches[parent_dir] = {}
                            candidate_matches[parent_dir][result.base_name] = comparison
            elif isinstance(result, DeferredResult):
                deferred_items.add(result.base_name)
            else:
                raise TypeError(f"Unexpected result type {type(result)}")

            total_size += result.total_size
            total_items += result.total_items
            duplicated_size += result.duplicated_size
            duplicated_items += result.duplicated_items
            all_items.add(result.base_name)

        # Early return or defer based on presence of candidate dirs and deferred items
        if not candidate_matches:
            if not deferred_items:
                return ImmediateResult(context.relative_path, [], total_size, total_items, duplicated_size,
                                       duplicated_items)
            else:
                return DeferredResult(context.relative_path, total_size, total_items, duplicated_size, duplicated_items)

        # Process all deferred items across all candidate directories, aggregating in place
        deferred_reducers: dict[Path, MetadataMatchReducer] = {
            d: MetadataMatchReducer(self._comparison_rule) for d in candidate_matches.keys()
        }
        # Prepare a list for stable ordering of candidate directories
        candidate_dirs = list(candidate_matches.keys())

        for base_name in deferred_items:
            matched_count, results = self._compare_deferred_item(
                dir_path / base_name,
                [self._archive_path / candidate_dir / base_name for candidate_dir in candidate_dirs]
            )

            duplicated_items += matched_count

            for candidate_dir, result in zip(candidate_dirs, results):
                deferred_reducers[candidate_dir].aggregate_from_match(result)

        deferred_results = {
            candidate_dir: deferred_reducers[candidate_dir].create_duplicate_match(
                candidate_dir, non_identical=False, non_superset=False)
            for candidate_dir in candidate_dirs}

        # Compare this directory with each candidate archive directory
        metadata_comparisons: list[DuplicateMatch] = []

        for candidate_dir, child_matches in candidate_matches.items():
            candidate_full_path: Path = self._archive_path / candidate_dir

            # Track metadata matches using reducer
            reducer = MetadataMatchReducer(self._comparison_rule)

            # Track the first comparison rule we encounter to validate consistency
            comparison_rule: DuplicateMatchRule | None = None

            for base_name, matching_comparison in child_matches.items():
                # Validate that all children use the same comparison rule
                if matching_comparison.rule is not None:
                    if comparison_rule is not None and matching_comparison.rule != comparison_rule:
                        raise ValueError(
                            f"Inconsistent comparison rules within directory {context.relative_path}: "
                            f"expected {self._comparison_rule} but found {matching_comparison.rule} for child {base_name}"
                        )
                    comparison_rule = matching_comparison.rule

                # Aggregate metadata matches from child
                reducer.aggregate_from_match(matching_comparison)

            # Aggregate deferred item results for this candidate directory
            reducer.aggregate_from_match(deferred_results[candidate_dir])

            # Get items in candidate directory for comparison
            candidate_items: set[str] = set(i.name for i in candidate_full_path.iterdir())

            # Aggregate directory-level metadata with child metadata
            reducer.aggregate_from_stat(context.stat, candidate_full_path.stat())

            # Create the DuplicateMatch with identity determined by set comparison
            # non_identical: True if the item sets differ (different structure)
            # non_superset: True if analyzed items are not a subset of candidate items
            comparison = reducer.create_duplicate_match(
                candidate_dir,
                non_identical=all_items != candidate_items,
                non_superset=not all_items.issubset(candidate_items)
            )

            if comparison is not None:
                metadata_comparisons.append(comparison)

        # Return early if no matching directories were found
        if not metadata_comparisons:
            return ImmediateResult(context.relative_path, [], total_size, total_items, duplicated_size,
                                   duplicated_items)

        # Create and write duplicate record
        record = DuplicateRecord(
            context.relative_path, metadata_comparisons, total_size, total_items, duplicated_size, duplicated_items)
        self._report_store.write_duplicate_record(record)

        logger.info("Completed analyzing directory: %s", context.relative_path)

        return ImmediateResult.from_duplicate_record(record)

    def _compare_deferred_item(
            self,
            analyzed_item_path: Path,
            candidate_item_paths: list[Path],
    ) -> tuple[int, list[DuplicateMatch | None]]:
        """Compare a single deferred item against multiple candidates.

        Compares a deferred item (symlink, device, pipe, socket, or subdirectory) against
        a list of candidate paths in a single pass. For subdirectories, recursion happens
        only once with all candidate subitems aggregated.

        Args:
            analyzed_item_path: Absolute path to the analyzed item
            candidate_item_paths: List of absolute paths to candidate items in the archive

        Returns:
            A tuple of (matched_count, results) where:
            - matched_count: Number of items in the analyzed tree that matched any candidate.
                            For single items (symlinks, devices, pipes, sockets): 1 if any candidate matched, 0 otherwise.
                            For directories: sum of matched_count from all subitems, plus 1 if any candidate matched
                            this directory itself.
            - results: List of DuplicateMatch | None in the same order as candidate_item_paths.
                      Each element is a DuplicateMatch if the candidate matches, None otherwise.
                      For single items, match has duplicated_items=1.
                      For subdirectories, match has aggregated statistics from recursive comparison.
                      Returns None for candidates that don't exist or have mismatched types.
        """
        # Get file stats for analyzed item (don't follow symlinks)
        analyzed_stat: os.stat_result = analyzed_item_path.lstat()
        analyzed_mode = stat.S_IFMT(analyzed_stat.st_mode)

        # Track valid candidates as list parallel to candidate_item_paths
        # Each entry is (candidate_path, candidate_stat, reducer) or None if invalidated
        candidate_states: list[tuple[Path, os.stat_result, MetadataMatchReducer] | None] = []

        # First pass: stat each candidate and filter by type match
        for candidate_path in candidate_item_paths:
            try:
                candidate_stat: os.stat_result = candidate_path.lstat()
            except FileNotFoundError:
                candidate_states.append(None)
                continue

            # Check if both have the same file type
            if stat.S_IFMT(candidate_stat.st_mode) != analyzed_mode:
                candidate_states.append(None)
                continue

            # Create reducer and aggregate initial stat comparison
            reducer = MetadataMatchReducer(self._comparison_rule)
            reducer.aggregate_from_stat(analyzed_stat, candidate_stat)
            candidate_states.append((candidate_path, candidate_stat, reducer))

        # Check if any valid candidates remain
        if all(entry is None for entry in candidate_states):
            return 0, [None] * len(candidate_item_paths)

        # Track total matched items across all subitems
        total_matched: int = 0

        # Per-candidate identity/superset flags (populated for directories, False for other types)
        non_identical_flags: list[bool] = [False] * len(candidate_states)
        non_superset_flags: list[bool] = [False] * len(candidate_states)

        # Compare based on file type
        if stat.S_ISLNK(analyzed_stat.st_mode):
            # Symlinks: compare targets
            analyzed_target = analyzed_item_path.readlink()
            for idx, entry in enumerate(candidate_states):
                if entry is None:
                    continue
                candidate_path, _, _ = entry
                if analyzed_target != candidate_path.readlink():
                    candidate_states[idx] = None

        elif stat.S_ISBLK(analyzed_stat.st_mode) or stat.S_ISCHR(analyzed_stat.st_mode):
            # Device files: compare major/minor numbers
            analyzed_major = os.major(analyzed_stat.st_rdev)
            analyzed_minor = os.minor(analyzed_stat.st_rdev)
            for idx, entry in enumerate(candidate_states):
                if entry is None:
                    continue
                _, candidate_stat, _ = entry
                if (os.major(candidate_stat.st_rdev) != analyzed_major or
                        os.minor(candidate_stat.st_rdev) != analyzed_minor):
                    candidate_states[idx] = None

        elif stat.S_ISFIFO(analyzed_stat.st_mode) or stat.S_ISSOCK(analyzed_stat.st_mode):
            # Pipes/sockets: existence check is sufficient
            pass

        elif stat.S_ISDIR(analyzed_stat.st_mode):
            # For directories, recursively compare all subitems
            # Aggregate all subitems across all valid candidates for a single recursive call

            # Track analyzed children names
            analyzed_children: set[str] = set()

            for analyzed_subitem in analyzed_item_path.iterdir():
                subitem_name = analyzed_subitem.name
                analyzed_children.add(subitem_name)

                valid_candidate_states = [entry for entry in candidate_states if entry is not None]
                # Build list of candidate subitem paths (use empty Path for invalidated)
                candidate_subitem_paths: list[Path] = [
                    candidate_path / subitem_name for candidate_path, _, _ in valid_candidate_states
                ]

                # Single recursive call for all candidates
                subitem_matched, subitem_results = self._compare_deferred_item(
                    analyzed_subitem, candidate_subitem_paths)
                total_matched += subitem_matched

                # Aggregate results back to each candidate's reducer
                for idx, (_, _, reducer) in enumerate(valid_candidate_states):
                    reducer.aggregate_from_match(subitem_results[idx])

            # Compute per-candidate identity/superset flags using set operations
            # non_identical_flags[idx]: True if children sets differ
            # non_superset_flags[idx]: True if any analyzed child is missing from candidate
            for idx, entry in enumerate(candidate_states):
                if entry is not None:
                    candidate_children = set(child.name for child in entry[0].iterdir())
                    non_identical_flags[idx] = analyzed_children != candidate_children
                    non_superset_flags[idx] = not analyzed_children.issubset(candidate_children)

        else:
            # Unknown file type - invalidate all candidates
            candidate_states = [None] * len(candidate_item_paths)

        # Build final results by zipping candidates with flags
        results: list[DuplicateMatch | None] = []
        for entry, non_identical, non_superset in zip(candidate_states, non_identical_flags, non_superset_flags):
            if entry is None:
                results.append(None)
                continue

            candidate_path, _, reducer = entry
            results.append(reducer.create_duplicate_match(
                candidate_path.relative_to(self._archive_path),
                non_identical=non_identical,
                non_superset=non_superset,
            ))

        # Count this item as matched if any candidate matched (directories don't count)
        if not stat.S_ISDIR(analyzed_stat.st_mode) and any(result is not None for result in results):
            total_matched += 1

        return total_matched, results

    async def _analyze_file(self, file_path: Path, context: FileContext) -> ImmediateResult:
        """Analyze a single file and write duplicate record to database if duplicates found.

        Args:
            file_path: Path to the file being analyzed

        Returns:
            ImmediateResult containing all duplicate information
        """
        assert context.relative_path is not None, "File context must have a relative path"
        logger.info("Analyzing file: %s", context.relative_path)

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
            return ImmediateResult(context.relative_path, [], context.stat.st_size, 1, 0, 0)

        # Compare metadata with each duplicate
        metadata_comparisons: list[DuplicateMatch] = []
        for dup_path in duplicates_found:
            full_dup_path: Path = self._archive_path / dup_path
            dup_stat: os.stat_result = full_dup_path.stat()

            # Compare metadata attributes
            mtime_match: bool = context.stat.st_mtime_ns == dup_stat.st_mtime_ns
            atime_match: bool = context.stat.st_atime_ns == dup_stat.st_atime_ns
            ctime_match: bool = context.stat.st_ctime_ns == dup_stat.st_ctime_ns
            mode_match: bool = context.stat.st_mode == dup_stat.st_mode
            owner_match: bool = context.stat.st_uid == dup_stat.st_uid
            group_match: bool = context.stat.st_gid == dup_stat.st_gid

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
                duplicated_size=context.stat.st_size, duplicated_items=1,
                is_identical=is_identical, is_superset=is_superset,
                rule=self._comparison_rule
            ))

        # For files, both total_size and duplicated_size are the file size
        # total_size: size of this file
        # duplicated_size: size of this file (since it has duplicates)
        # total_items: 1 (the file itself)
        # duplicated_items: 1 (the file itself, since it has duplicates)

        # Create and write duplicate record
        record = DuplicateRecord(
            context.relative_path, metadata_comparisons, context.stat.st_size, 1, context.stat.st_size, 1)
        self._report_store.write_duplicate_record(record)

        logger.info("Completed analyzing file: %s", context.relative_path)

        # Return immediate result with the duplicate record
        return ImmediateResult.from_duplicate_record(record)


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
    logger.info("Starting analysis for %d path(s)", len(args.input_paths))

    # Process each input path
    for input_path in args.input_paths:
        logger.info("Analyzing path: %s", input_path)

        # Create report directory
        report_dir: Path = get_report_directory_path(input_path)

        # Check if a file with the same name already exists
        if report_dir.exists() and report_dir.is_file():
            raise FileExistsError(
                f"Cannot create report directory '{report_dir}': "
                f"a file with this name already exists"
            )

        report_store: ReportStore = ReportStore(report_dir)
        report_store.create_report_directory()

        # Create and write manifest
        manifest: ReportManifest = ReportManifest(
            archive_path=str(args.archive_path.resolve()),
            archive_id=args.archive_id,
            timestamp=datetime.now().isoformat(),
            comparison_rule=args.comparison_rule.to_dict()
        )
        report_store.write_manifest(manifest)

        # Analyze the path with database context
        report_store.open_database(create_if_missing=True)
        try:
            processor: AnalyzeProcessor = AnalyzeProcessor(store, args, input_path, report_store)
            await processor.run()
        finally:
            report_store.close_database()

        logger.info("Completed analysis for: %s", input_path)

    logger.info("Analysis complete for all %d path(s)", len(args.input_paths))


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


def find_report_for_path(target_path: Path) -> Path | None:
    """Find the analyzed path for a given file or directory.

    Searches upward from the target path to find a .report directory that contains
    analysis data for the target.

    Args:
        target_path: The file or directory to find a report for

    Returns:
        Path that was analyzed (e.g., /path/to/dir for /path/to/dir.report) if found, None otherwise
    """
    target_path = target_path.resolve()

    # Start from the target path and traverse upward
    current = target_path
    while True:
        # Check if there's a .report directory for the current path
        report_dir = get_report_directory_path(current)
        if report_dir.exists() and report_dir.is_dir():
            return current

        # Move to parent directory
        parent = current.parent
        if parent == current:
            # Reached root without finding a report
            return None
        current = parent


def format_size(size_bytes: int) -> str:
    """Format byte size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string (e.g., "1.5 MB")
    """
    size: float = float(size_bytes)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            if unit == 'B':
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


@dataclass
class DescribeOptions:
    """Options for controlling duplicate display in describe command.

    Attributes:
        limit: Maximum number of duplicates to show. None means show all.
        sort_by: Sorting criterion for duplicates - 'size', 'items', 'identical', or 'path'
        sort_children: Sorting criterion for directory children - 'dup-size', 'dup-items', 'total-size', or 'name'
        use_bytes: If True, show sizes in bytes instead of human-readable format
        show_details: If True, show report metadata (Report, Analyzed, Archive, Timestamp)
        directory_only: If True, describe only the directory itself, not its contents
        keep_input_order: If True, keep input order of multiple paths instead of sorting
    """
    limit: int | None = 1
    sort_by: str = 'size'
    sort_children: str = 'dup-size'
    use_bytes: bool = False
    show_details: bool = False
    directory_only: bool = False
    keep_input_order: bool = False


def do_describe(paths: list[Path], options: DescribeOptions | None = None) -> None:
    """Describe duplicate information for files or directories from analysis reports.

    Args:
        paths: List of paths to describe. Single element for single path, multiple for table display
        options: Options for controlling duplicate display
    """
    if options is None:
        options = DescribeOptions()

    # Resolve all paths
    paths = [p.resolve() for p in paths]

    # Check if all paths exist
    for path in paths:
        if not path.exists():
            print(f"Error: Path does not exist: {path}")
            return

    # Find a common analyzed path that works for all paths
    report_info = []
    for path in paths:
        analyzed_path = find_report_for_path(path)
        if analyzed_path is None:
            print(f"No analysis report found for: {path}")
            print(f"Run 'arindexer analyze {path}' to generate a report.")
            return
        report_info.append((path, analyzed_path))

    # Use the first analyzed path as context
    analyzed_path = report_info[0][1]
    report_dir = get_report_directory_path(analyzed_path)

    try:
        with ReportStore(report_dir, analyzed_path) as store:
            manifest = store.read_manifest()

            # Normalize paths to relative paths within the analyzed directory
            relative_paths = []
            for path in paths:
                if path == analyzed_path:
                    relative_path = Path(analyzed_path.name)
                else:
                    relative_path = Path(analyzed_path.name) / path.relative_to(analyzed_path)
                relative_paths.append(relative_path)

            # Decide flow based on number of paths
            if len(paths) > 1:
                # Multiple paths: display as table
                formatter = MultiplePathsDescribeFormatter(store, relative_paths, manifest, options)
                formatter.describe()
            else:
                # Single path: display details
                target_path = paths[0]

                # Check if target is a directory and create appropriate formatter
                if target_path.is_dir():
                    formatter = DirectoryDescribeFormatter(store, relative_paths, manifest, options)
                else:
                    formatter = FileDescribeFormatter(store, relative_paths, manifest, options)

                # Execute the describe operation
                formatter.describe()

    except FileNotFoundError as e:
        print(f"Error reading report: {e}")
    except Exception as e:
        print(f"Error: {e}")


class SortableRowData(NamedTuple):
    """Row data containing both raw values for sorting and formatted strings for display.

    Fields are ordered to match the standard COLUMNS for display purposes.
    """
    # Display fields (in COLUMNS order)
    name: str
    type_str: str
    total_size_str: str
    dup_size_str: str
    size_ratio_str: str
    total_items_str: str
    dup_items_str: str
    items_ratio_str: str
    dups_str: str
    max_match_dup_size_str: str
    max_ratio_str: str
    best_status_str: str
    in_report_str: str
    # Raw values (for sorting)
    is_dir: bool
    total_size: int
    dup_size: int
    dups: int
    match_dup_size: int
    status_rank: int
    total_items: int
    dup_items: int
    size_ratio: float
    items_ratio: float
    max_ratio: float
    in_report: bool


class DescribeFormatter(ABC):
    """Base class for formatting describe output using template pattern."""

    # Column definitions shared by all derived formatters
    # Format: (field_name, display_name, align_right)
    COLUMNS: list[tuple[str, str, bool]] = [
        ('name', 'Name', False),
        ('type_str', 'Type', False),
        ('total_size_str', 'Total Size', True),
        ('dup_size_str', 'Dup Size', True),
        ('size_ratio_str', 'Size %', True),
        ('total_items_str', 'Total Items', True),
        ('dup_items_str', 'Dup Items', True),
        ('items_ratio_str', 'Items %', True),
        ('dups_str', 'Dups', True),
        ('max_match_dup_size_str', 'Max Match', True),
        ('max_ratio_str', 'Max %', True),
        ('best_status_str', 'Status', False),
        ('in_report_str', 'In Report', False),
    ]

    def __init__(self, store: ReportStore, relative_paths: list[Path], manifest: ReportManifest,
                 options: DescribeOptions | None = None):
        """Initialize formatter.

        Args:
            store: ReportStore instance with open database
            relative_paths: Paths relative to the analyzed target
            manifest: Report manifest
            options: Options for controlling duplicate display
        """
        self.store = store
        self.relative_paths = relative_paths
        self.manifest = manifest
        self.options = options if options is not None else DescribeOptions()

    @property
    def relative_path(self) -> Path:
        """Get the first relative path (for single-path formatters)."""
        assert len(self.relative_paths) == 1, "relative_paths must contain exactly 1 element for single-path formatters"
        return self.relative_paths[0]

    @abstractmethod
    def describe(self) -> None:
        pass

    def _describe_item(self) -> None:
        """Template method that describes the item's duplicates.

        Displays report metadata, duplicate records, and item-specific details.
        """
        # Print report metadata if details are requested
        if self.options.show_details:
            self._print_report_metadata()
            print()

        record = self.store.read_duplicate_record(self.relative_path)

        if record is None or not record.duplicates:
            print(f"No duplicates found for: {self.relative_path}")
            return

        # Print definitions if details are requested
        if self.options.show_details:
            self._print_attribute_definitions()
            print()

        # Print unified header
        self._print_header(record)

        # Print unified duplicates section
        self._print_duplicates(record)

        # Skip directory contents table if directory_only is set
        if self.options.directory_only:
            return

        # Print additional details (only for directories)
        self._print_details(record)

    def _print_report_metadata(self) -> None:
        """Print report metadata including path and archive information."""
        print(f"Report: {self.store.report_dir}")
        print(f"Analyzed: {self.store.analyzed_path}")
        print(f"Archive: {self.manifest.archive_path}")
        print(f"Timestamp: {self.manifest.timestamp}")

    def _build_row_data(self, name: str, is_dir: bool, record: DuplicateRecord | None) -> SortableRowData:
        """Build a SortableRowData entry from a DuplicateRecord.

        Args:
            name: The item name
            is_dir: Whether the item is a directory
            record: The DuplicateRecord, or None if not in report

        Returns:
            SortableRowData with all raw and formatted values
        """
        type_str = "Dir" if is_dir else "File"

        if record is None:
            return SortableRowData(
                name=name, is_dir=is_dir,
                total_size=0, dup_size=0, dups=0, match_dup_size=0, status_rank=0,
                total_items=0, dup_items=0, size_ratio=0.0, items_ratio=0.0, max_ratio=0.0, in_report=False,
                type_str=type_str, total_size_str="-", dup_size_str="0" if self.options.use_bytes else "0 B", dups_str="0",
                max_match_dup_size_str="0" if self.options.use_bytes else "0 B", best_status_str="-",
                total_items_str="-", dup_items_str="-", size_ratio_str="-", items_ratio_str="-",
                max_ratio_str="-", in_report_str="No")

        if not record.duplicates:
            total_size_str = str(record.total_size) if self.options.use_bytes else format_size(record.total_size)
            return SortableRowData(
                name=name, is_dir=is_dir,
                total_size=record.total_size, dup_size=0, dups=0, match_dup_size=0, status_rank=0,
                total_items=record.total_items, dup_items=0, size_ratio=0.0, items_ratio=0.0, max_ratio=0.0, in_report=True,
                type_str=type_str, total_size_str=total_size_str, dup_size_str="0" if self.options.use_bytes else "0 B", dups_str="0",
                max_match_dup_size_str="0" if self.options.use_bytes else "0 B", best_status_str="-",
                total_items_str=str(record.total_items), dup_items_str="0", size_ratio_str="0.0%",
                items_ratio_str="0.0%", max_ratio_str="0.0%", in_report_str="Yes")

        # Has duplicates
        total_size_str = str(record.total_size) if self.options.use_bytes else format_size(record.total_size)
        dup_size_str = str(record.duplicated_size) if self.options.use_bytes else format_size(record.duplicated_size)

        # Find the duplicate with highest duplicated_size and use its status
        max_match_dup = max(record.duplicates, key=lambda dup: dup.duplicated_size)
        max_match_dup_size = max_match_dup.duplicated_size
        max_match_dup_size_str = str(max_match_dup_size) if self.options.use_bytes else format_size(max_match_dup_size)

        # Get status from the max match duplicate
        if max_match_dup.is_identical:
            best_status_str = "Identical"
            best_status_rank = 2
        elif max_match_dup.is_superset:
            best_status_str = "Superset"
            best_status_rank = 1
        else:
            best_status_str = "Partial"
            best_status_rank = 0

        # Calculate ratios
        size_ratio = record.duplicated_size / record.total_size if record.total_size > 0 else 0.0
        items_ratio = record.duplicated_items / record.total_items if record.total_items > 0 else 0.0
        max_ratio = max_match_dup_size / record.total_size if record.total_size > 0 else 0.0

        size_ratio_str = self._format_percentage(record.duplicated_size, record.total_size)
        items_ratio_str = self._format_percentage(record.duplicated_items, record.total_items)
        max_ratio_str = self._format_percentage(max_match_dup_size, record.total_size)

        return SortableRowData(
            name=name, is_dir=is_dir,
            total_size=record.total_size, dup_size=record.duplicated_size,
            dups=len(record.duplicates), match_dup_size=max_match_dup_size, status_rank=best_status_rank,
            total_items=record.total_items, dup_items=record.duplicated_items,
            size_ratio=size_ratio, items_ratio=items_ratio, max_ratio=max_ratio, in_report=True,
            type_str=type_str, total_size_str=total_size_str, dup_size_str=dup_size_str,
            dups_str=str(len(record.duplicates)), max_match_dup_size_str=max_match_dup_size_str,
            best_status_str=best_status_str, total_items_str=str(record.total_items),
            dup_items_str=str(record.duplicated_items), size_ratio_str=size_ratio_str,
            items_ratio_str=items_ratio_str, max_ratio_str=max_ratio_str, in_report_str="Yes")

    @staticmethod
    def _format_percentage(numerator: int | float, denominator: int | float) -> str:
        """Format percentage with ~ prefix if result is due to rounding.

        Args:
            numerator: The numerator value
            denominator: The denominator value

        Returns:
            Formatted percentage string with ~ prefix if rounded, otherwise exact percentage
        """
        if denominator == 0:
            return "0.0%"

        ratio = numerator / denominator
        percent_str = f"{ratio:.1%}"

        # Check if 100.0% is due to rounding (numerator != denominator)
        if percent_str == "100.0%" and numerator != denominator:
            return "~100.0%"
        # Check if 0.0% is due to rounding (numerator != 0)
        elif percent_str == "0.0%" and numerator != 0:
            return "~0.0%"

        return percent_str

    @staticmethod
    def _print_formatted_table(columns: list[tuple[str, str, bool]], rows: list[SortableRowData]) -> None:
        """Print a formatted table with the given columns and rows.

        This is the low-level table printing implementation that handles all
        formatting, alignment, and width calculation.

        Args:
            columns: List of (field_name, display_name, align_right) tuples for column definitions.
            rows: List of SortableRowData rows to display
        """
        if not rows:
            return

        # Extract field names and display headers from columns
        field_names = [col[0] for col in columns]
        headers = [col[1] for col in columns]

        # Calculate column widths based on actual field values
        col_widths = [max(max(len(str(getattr(row, field_names[i]))) for row in rows), len(headers[i]))
                      for i in range(len(headers))]

        # Build format template for header and rows
        header_parts = []
        row_format_specs = []
        for i in range(len(columns)):
            align = '>' if columns[i][2] else '<'
            header_parts.append(f"{headers[i]:{align}{col_widths[i]}}")
            row_format_specs.append(f"{{:{align}{col_widths[i]}}}")
        row_template = "  ".join(row_format_specs)

        # Print header
        header = "  ".join(header_parts)
        print(header)
        print("-" * len(header))

        # Print rows using field names to extract values
        for row in rows:
            row_values = [str(getattr(row, field_names[i])) for i in range(len(field_names))]
            print(row_template.format(*row_values))

    def _print_table(self, rows: list[SortableRowData]) -> None:
        """Print a formatted table with standard columns.

        This is a convenience method that uses the class's standard COLUMNS
        definition for table formatting. Use this for displaying data with
        the default column structure.

        Args:
            rows: List of SortableRowData rows to display
        """
        self._print_formatted_table(self.COLUMNS, rows)

    def _sort_and_format_rows(self, rows_data: list[SortableRowData]) -> list[SortableRowData]:
        """Sort rows by the configured sort criteria.

        Args:
            rows_data: List of SortableRowData to sort

        Returns:
            List of SortableRowData sorted by configured criteria
        """
        def sort_key(row: SortableRowData) -> tuple[Any, ...]:
            if self.options.sort_children == 'dup-size':
                return -row.dup_size, -row.dups, -row.total_size
            elif self.options.sort_children == 'dup-items':
                return -row.dups, -row.dup_size, -row.total_size
            elif self.options.sort_children == 'total-size':
                return (-row.total_size,)
            elif self.options.sort_children == 'name':
                return (row.name,)
            else:
                return -row.dup_size, -row.dups, -row.total_size

        return sorted(rows_data, key=sort_key)

    @staticmethod
    def _print_attribute_definitions() -> None:
        """Print definitions of attributes and columns.

        This is shown when --details flag is enabled.
        """
        print("Attribute Definitions:")
        print()
        print("Summary Counts (in report header):")
        print("  Size: Total size and breakdown")
        print("    - duplicated: Each item counted once if it has ANY duplicate in archive")
        print("    - unique: Content with no duplicates in archive")
        print("  Items: Total item count and breakdown (files + special items, not directories)")
        print("    - duplicated: Each item counted once if it has ANY duplicate in archive")
        print("    - unique: Items with no duplicates in archive")
        print()
        print("Duplicate Match Details (per archive location):")
        print("  Status: Match type for this archive location")
        print("    - Identical: All content present with matching metadata")
        print("    - Superset: All analyzed content present (archive may have extras)")
        print("    - Partial: Some analyzed content missing in this location")
        print("  Matching: Which metadata fields match (mtime, atime, ctime, mode, owner, group)")
        print("  Duplicated items/size: Content in this archive location that matches")
        print()
        print("Hierarchical Matching Requirement (for directory duplicates):")
        print("  Files at the SAME relative paths within the directory are counted")
        print("  - Example: Analyzed [a.txt, b.txt] vs. archive 'dir1/' [a.txt, c.txt]")
        print("    - Header totals: a.txt and b.txt both show as duplicated")
        print("    - 'dir1/' entry: Only a.txt counted (b.txt not at same path in dir1/)")
        print()
        print("Report Inclusion:")
        print("  Included: Item has at least one duplicate in the archive")
        print("  Excluded: Item has NO duplicates (In Report column shows 'No')")
        print()
        print("Directory Listing Columns:")
        print("  Name: File or directory name")
        print("  Type: File or Dir (directory)")
        print("  Total Size: All content size")
        print("  Dup Size: Global duplicated size (each item once)")
        print("  Size %: Percentage duplicated (Dup Size / Total Size)")
        print("  Total Items: All items count")
        print("  Dup Items: Global duplicated items (each item once)")
        print("  Items %: Percentage duplicated (Dup Items / Total Items)")
        print("  Dups: Number of duplicate matches found")
        print("  Max Match: Largest per-location duplicated size")
        print("  Max %: Largest match percentage (Max Match / Total Size)")
        print("  Status: Best match status (Identical/Superset/Partial)")
        print("  In Report: Yes if included in analysis report, No otherwise")
        print()

    def _print_header(self, record: DuplicateRecord) -> None:
        """Print header information for the item.

        Args:
            record: DuplicateRecord for the item
        """
        item_type = self._get_item_type()
        unique_size = record.total_size - record.duplicated_size
        unique_items = record.total_items - record.duplicated_items

        total_size_str = str(record.total_size) if self.options.use_bytes else format_size(record.total_size)
        dup_size_str = str(record.duplicated_size) if self.options.use_bytes else format_size(record.duplicated_size)
        unique_size_str = str(unique_size) if self.options.use_bytes else format_size(unique_size)

        if self.options.show_details:
            print(f"{item_type}: {self.relative_path}")
        print(f"Size: {total_size_str} (duplicated: {dup_size_str}, unique: {unique_size_str})")
        print(f"Items: {record.total_items} (duplicated: {record.duplicated_items}, unique: {unique_items})")
        print()
        print(f"Duplicates:")
        print()

    @abstractmethod
    def _get_item_type(self) -> str:
        """Get the item type string (e.g., 'File' or 'Directory').

        Returns:
            String describing the item type
        """
        pass

    def _get_status_message(self, comparison: DuplicateMatch) -> str:
        """Get status message for a duplicate match.

        Args:
            comparison: DuplicateMatch to get status for

        Returns:
            Status message string
        """
        if comparison.is_identical:
            return "Identical"
        elif comparison.is_superset:
            return "Superset (contains all analyzed content, may have extras)"
        else:
            return "Partial match (not all analyzed content present)"

    def _print_duplicate_comparison(self, comparison: DuplicateMatch) -> None:
        """Print information for a single duplicate comparison.

        Args:
            comparison: DuplicateMatch to print
        """
        print(f"  {comparison.path}")
        print(f"    Status: {self._get_status_message(comparison)}")

        # Show metadata matches for non-identical items
        if not comparison.is_identical:
            matches = []
            if comparison.mtime_match:
                matches.append("mtime")
            if comparison.atime_match:
                matches.append("atime")
            if comparison.ctime_match:
                matches.append("ctime")
            if comparison.mode_match:
                matches.append("mode")
            if comparison.owner_match:
                matches.append("owner")
            if comparison.group_match:
                matches.append("group")

            if matches:
                print(f"    Matching: {', '.join(matches)}")

        # Show size and items if relevant (non-zero)
        if comparison.duplicated_items > 0:
            print(f"    Duplicated items: {comparison.duplicated_items}")
        if comparison.duplicated_size > 0:
            size_str = str(comparison.duplicated_size) if self.options.use_bytes else format_size(
                comparison.duplicated_size)
            print(f"    Duplicated size: {size_str}")

    def _sort_duplicates(self, duplicates: list[DuplicateMatch]) -> list[DuplicateMatch]:
        """Sort duplicates according to options.

        Sort priority (for all sort_by options):
        1. Primary criterion (size, items, identical status, or path length)
        2. Identity status (identical > superset > partial)
        3. Path length (shorter is better)

        Args:
            duplicates: List of DuplicateMatch objects to sort

        Returns:
            Sorted list of duplicates
        """

        def sort_key(dup: DuplicateMatch) -> tuple[Any, ...]:
            # Identity status rank: identical=2, superset=1, partial=0
            identity_rank = 2 if dup.is_identical else (1 if dup.is_superset else 0)

            if self.options.sort_by == 'size':
                # Sort by duplicated_size (descending), then identity, then path length
                return (-dup.duplicated_size, -identity_rank, len(str(dup.path)))
            elif self.options.sort_by == 'items':
                # Sort by duplicated_items (descending), then identity, then path length
                return (-dup.duplicated_items, -identity_rank, len(str(dup.path)))
            elif self.options.sort_by == 'identical':
                # Sort by identity status, then path length
                return (-identity_rank, len(str(dup.path)))
            elif self.options.sort_by == 'path':
                # Sort by path length (ascending), then identity
                return (len(str(dup.path)), -identity_rank)
            else:
                # Default to size
                return (-dup.duplicated_size, -identity_rank, len(str(dup.path)))

        return sorted(duplicates, key=sort_key)

    def _print_duplicates(self, record: DuplicateRecord) -> None:
        """Print duplicate information for the item.

        Args:
            record: DuplicateRecord for the item
        """
        # Sort duplicates
        sorted_duplicates = self._sort_duplicates(record.duplicates)

        # Apply limit
        if self.options.limit is not None and len(sorted_duplicates) > self.options.limit:
            displayed_duplicates = sorted_duplicates[:self.options.limit]
            total_count = len(record.duplicates)
        else:
            displayed_duplicates = sorted_duplicates
            total_count = None

        # Print duplicates
        for comparison in displayed_duplicates:
            self._print_duplicate_comparison(comparison)

        # Show count if not all duplicates are displayed
        if total_count is not None:
            print()
            print(f"  Showing {len(displayed_duplicates)} of {total_count} duplicates")

    def _print_details(self, record: DuplicateRecord) -> None:
        """Print additional details for the item.

        Default implementation does nothing. Subclasses can override.

        Args:
            record: DuplicateRecord for the item
        """
        pass


class FileDescribeFormatter(DescribeFormatter):
    """Formatter for describing files."""

    def describe(self) -> None:
        self._describe_item()

    def _get_item_type(self) -> str:
        """Get the item type string."""
        return "File"

    def _get_status_message(self, comparison: DuplicateMatch) -> str:
        """Get status message for a file duplicate match.

        For files, is_superset always equals is_identical, so we only show
        two states: identical or content match with differing metadata.

        Args:
            comparison: DuplicateMatch to get status for

        Returns:
            Status message string
        """
        if comparison.is_identical:
            return "Identical"
        else:
            return "Content match (metadata differs)"


class DirectoryDescribeFormatter(DescribeFormatter):
    """Formatter for describing directories."""

    def describe(self) -> None:
        self._describe_item()

    def _get_item_type(self) -> str:
        """Get the item type string."""
        return "Directory"

    def _print_details(self, record: DuplicateRecord) -> None:
        """Print child files information using filesystem listing first."""
        print()
        print("Files in directory:")
        print()

        # Get the absolute path to the directory being described
        # relative_path includes the analyzed_path's name as first component, so skip it
        assert self.store.analyzed_path is not None, "analyzed_path must be set for directory describe"
        directory_path: Path = self.store.analyzed_path / Path(*self.relative_path.parts[1:])

        # List children from filesystem
        try:
            children = sorted(directory_path.iterdir(), key=lambda p: p.name)
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            print("  Could not list directory contents")
            return

        if not children:
            print("  Directory is empty")
            return

        # Prepare table data using the shared helper method
        rows_data: list[SortableRowData] = []
        for child_path in children:
            child_name = child_path.name

            # Construct the relative path for this child
            child_relative_path: Path = self.relative_path / child_name

            # Query the report for this child
            child_record = self.store.read_duplicate_record(child_relative_path)

            # Build row data using the helper method
            row_data = self._build_row_data(child_name, child_path.is_dir(), child_record)
            rows_data.append(row_data)

        # Sort and format rows using the shared helper method
        rows = self._sort_and_format_rows(rows_data)

        # Print table using the shared method with column alignment
        self._print_table(rows)


class MultiplePathsDescribeFormatter(DescribeFormatter):
    """Formatter for describing multiple paths in a single table."""

    def _get_item_type(self) -> str:
        """Get the item type string."""
        return "Multiple Paths"

    def describe(self) -> None:
        """Display multiple paths in a table format."""
        # Print report metadata if details are requested
        if self.options.show_details:
            self._print_report_metadata()
            print()

        rows_data: list[SortableRowData] = []

        assert self.store.analyzed_path is not None, "analyzed_path must be set for multiple paths describe"
        analyzed_path = self.store.analyzed_path

        for relative_path in self.relative_paths:
            # Reconstruct the path from relative_path using analyzed_path
            if relative_path == Path(analyzed_path.name):
                # Path is the analyzed directory itself
                path = analyzed_path
            else:
                # Path is relative to analyzed directory - reconstruct it
                path = analyzed_path / relative_path.relative_to(analyzed_path.name)

            # Get record from report using pre-computed relative path
            record = self.store.read_duplicate_record(relative_path)

            # Build row data using the helper method
            row_data = self._build_row_data(path.name, path.is_dir(), record)
            rows_data.append(row_data)

        # Sort rows if not keeping input order, otherwise use original order
        if self.options.keep_input_order:
            rows = rows_data
        else:
            rows = self._sort_and_format_rows(rows_data)

        # Print table
        if rows:
            self._print_table(rows)
