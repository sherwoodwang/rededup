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

        return msgpack.dumps([path_components, duplicate_data, self.total_size, self.total_items, self.duplicated_size, self.duplicated_items])

    @classmethod
    def from_msgpack(cls, data: bytes) -> 'DuplicateRecord':
        """Deserialize from msgpack format.

        Args:
            data: Msgpack-encoded bytes

        Returns:
            DuplicateRecord instance
        """
        decoded = msgpack.loads(data)
        path_components, duplicate_data, total_size, total_items, duplicated_size, duplicated_items = decoded

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

    def __init__(self, report_path: Path, total_size: int, total_items: int, duplicated_size: int, duplicated_items: int):
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


class ReportReader:
    """Handles reading analysis reports from .report directories with LevelDB storage."""

    def __init__(self, report_dir: Path, analyzed_path: Path) -> None:
        """Initialize report reader.

        Args:
            report_dir: Path to .report directory where report is stored
            analyzed_path: The path that was analyzed (the root of the analysis)
        """
        self.report_dir: Path = report_dir
        self.manifest_path: Path = report_dir / 'manifest.json'
        self.database_path: Path = report_dir / 'database'
        self.analyzed_path: Path = analyzed_path
        self._database: plyvel.DB | None = None

    def open_database(self) -> None:
        """Open the LevelDB database for reading duplicate records."""
        if not self.database_path.exists():
            raise FileNotFoundError(f"Database directory not found: {self.database_path}")
        self._database = plyvel.DB(str(self.database_path), create_if_missing=False)

    def close_database(self) -> None:
        """Close the LevelDB database."""
        if self._database is not None:
            self._database.close()
            self._database = None

    def __enter__(self) -> 'ReportReader':
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

        path_hash = ReportWriter._compute_path_hash(path)
        prefixed_db = self._database.prefixed_db(path_hash)

        # Iterate through all records with this hash prefix
        for key, value in prefixed_db.iterator():
            record = DuplicateRecord.from_msgpack(value)
            if record.path == path:
                return record

        return None

    def iterate_all_records(self):
        """Iterate through all duplicate records in the database.

        Yields:
            DuplicateRecord instances for all records in the database
        """
        if self._database is None:
            raise RuntimeError("Database not opened. Use context manager or call open_database().")

        for key, value in self._database.iterator():
            # Skip keys that are just hash prefixes without varint sequence numbers
            # Valid keys have format: <16-byte hash><varint sequence>
            if len(key) > 16:
                try:
                    record = DuplicateRecord.from_msgpack(value)
                    yield record
                except Exception:
                    # Skip invalid records
                    continue

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
                    await self._defer_for_parent_directory(file_path, context)

    async def _handle_directory(self, dir_path: Path, context: FileContext) -> None:
        """Handle a directory by registering a completion listener.

        Args:
            dir_path: Absolute path to the directory
            context: File context for the directory
        """
        logger.info("Handling directory: %s", dir_path)

        # Register a DirectoryListener on the context
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
        future.set_result(DeferredResult(relative_path, context.stat.st_size, 1, 0, 0))
        # Register this deferred item with its parent directory's listener
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
                return ImmediateResult(context.relative_path, [], total_size, total_items, duplicated_size, duplicated_items)
            else:
                return DeferredResult(context.relative_path, total_size, total_items, duplicated_size, duplicated_items)

        # Compare this directory with each candidate archive directory
        metadata_comparisons: list[DuplicateMatch] = []

        for candidate_dir, child_matches in candidate_matches.items():
            # Compare this candidate directory with the analyzed directory
            comparison = await self._compare_directory_with_candidate(
                dir_path, context, all_items, candidate_dir, child_matches, deferred_items
            )

            if comparison is not None:
                metadata_comparisons.append(comparison)

        # Return early if no matching directories were found
        if not metadata_comparisons:
            return ImmediateResult(context.relative_path, [], total_size, total_items, duplicated_size, duplicated_items)

        # Create and write duplicate record
        record = DuplicateRecord(
            context.relative_path, metadata_comparisons, total_size, total_items, duplicated_size, duplicated_items)
        self._writer.write_duplicate_record(record)

        logger.info("Completed analyzing directory: %s", context.relative_path)

        return ImmediateResult.from_duplicate_record(record)

    def _compare_deferred_item(
            self,
            analyzed_item_path: Path,
            candidate_item_path: Path,
            analyzed_stat: os.stat_result,
            candidate_stat: os.stat_result,
    ) -> DuplicateMatch | None:
        """Compare a single deferred item (symlink, device, pipe, socket, or subdirectory).

        Args:
            analyzed_item_path: Absolute path to the analyzed item
            candidate_item_path: Absolute path to the candidate item
            analyzed_stat: stat result for the analyzed item (from lstat)
            candidate_stat: stat result for the candidate item (from lstat)

        Returns:
            DuplicateMatch with overall statistics for the entire subtree if items match, None otherwise.
            For single items (symlinks, devices, pipes, sockets), returns match with duplicated_items=1.
            For subdirectories, returns aggregated statistics from recursive comparison.
        """
        # Check if both have the same file type
        if stat.S_IFMT(analyzed_stat.st_mode) != stat.S_IFMT(candidate_stat.st_mode):
            return None

        # Use reducer to compare metadata for special files (symlinks, devices, pipes, sockets)
        reducer = MetadataMatchReducer(self._comparison_rule)
        reducer.aggregate_from_stat(analyzed_stat, candidate_stat)

        # Compare based on file type
        if stat.S_ISLNK(analyzed_stat.st_mode):
            # Symlinks: compare targets
            if analyzed_item_path.readlink() != candidate_item_path.readlink():
                return None

        elif stat.S_ISBLK(analyzed_stat.st_mode) or stat.S_ISCHR(analyzed_stat.st_mode):
            # Device files: compare major/minor numbers
            if (os.major(analyzed_stat.st_rdev) != os.major(candidate_stat.st_rdev) or
                    os.minor(analyzed_stat.st_rdev) != os.minor(candidate_stat.st_rdev)):
                return None

        elif stat.S_ISFIFO(analyzed_stat.st_mode) or stat.S_ISSOCK(analyzed_stat.st_mode):
            # Pipes/sockets: existence check is sufficient
            pass

        elif stat.S_ISDIR(analyzed_stat.st_mode):
            # Check all items in the analyzed directory
            for analyzed_subitem in analyzed_item_path.iterdir():
                candidate_subitem = candidate_item_path / analyzed_subitem.name
                analyzed_subitem_stat = analyzed_subitem.lstat()
                try:
                    candidate_subitem_stat = candidate_subitem.lstat()
                except FileNotFoundError:
                    continue

                # Use common helper for all other file types (symlinks, devices, pipes, sockets) and aggregate metadata
                # matches
                reducer.aggregate_from_match(self._compare_deferred_item(
                    analyzed_subitem, candidate_subitem, analyzed_subitem_stat, candidate_subitem_stat
                ))
        else:
            # Unknown file type
            return None

        # For special files, is_superset equals is_identical (file itself is the only item)
        return reducer.create_duplicate_match(
            candidate_item_path.relative_to(self._archive_path),
            non_identical=False,
            non_superset=False,
        )

    async def _compare_directory_with_candidate(
            self,
            dir_path: Path,
            context: FileContext,
            all_items: set[str],
            candidate_dir: Path,
            child_matches: dict[str, DuplicateMatch],
            deferred_items: set[str]
    ) -> DuplicateMatch | None:
        """Compare a directory with a candidate archive directory.

        Args:
            dir_path: Path to the directory being analyzed (can be used directly from working directory)
            context: File context for the directory being analyzed
            all_items: Set of all item names (base names) in the analyzed directory
            candidate_dir: Relative path to the candidate archive directory
            child_matches: Map of base_name -> DuplicateMatch for files in candidate directory
            deferred_items: Set of base names for deferred items

        Returns:
            DuplicateMatch if the candidate matches, None otherwise.
            For directories, ?_match fields are false if any child file has false value.
        """
        logger.info(
            "Comparing directory %s with candidate %s (%d files, %d deferred)",
            context.relative_path, candidate_dir, len(child_matches), len(deferred_items)
        )

        if logger.isEnabledFor(logging.DEBUG):
            if child_matches:
                sorted_child_names = sorted(child_matches.keys())
                logger.debug("Child files for %s vs %s: %s", context.relative_path, candidate_dir,
                             ', '.join(sorted_child_names))
            if deferred_items:
                sorted_deferred_names = sorted(deferred_items)
                logger.debug("Deferred items for %s vs %s: %s", context.relative_path, candidate_dir,
                             ', '.join(sorted_deferred_names))

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

        # Process all deferred items
        # Collect all results before determining if this is a valid match
        for base_name in deferred_items:
            # Construct paths for both analyzed and candidate items
            analyzed_item_path: Path = dir_path / base_name
            candidate_item_path: Path = candidate_full_path / base_name

            # Get file stats (don't follow symlinks)
            analyzed_stat: os.stat_result = analyzed_item_path.lstat()
            try:
                candidate_stat: os.stat_result = candidate_item_path.lstat()
            except FileNotFoundError:
                continue

            # Use common helper to compare the deferred item
            matching_comparison = self._compare_deferred_item(
                analyzed_item_path, candidate_item_path, analyzed_stat, candidate_stat
            )
            if matching_comparison is None:
                continue

            # Aggregate metadata matches from deferred item
            reducer.aggregate_from_match(matching_comparison)

        # Get items in candidate directory for comparison
        candidate_items: set[str] = set((i.name for i in candidate_full_path.iterdir()))

        # Aggregate directory-level metadata with child metadata
        reducer.aggregate_from_stat(context.stat, candidate_full_path.stat())

        # Create the DuplicateMatch with identity determined by set comparison
        # non_identical: True if the item sets differ (different structure)
        # non_superset: True if analyzed items are not a subset of candidate items
        return reducer.create_duplicate_match(
            candidate_dir,
            non_identical=all_items != candidate_items,
            non_superset=(not all_items.issubset(candidate_items))
        )

    async def _analyze_file(self, file_path: Path, context: FileContext) -> ImmediateResult:
        """Analyze a single file and write duplicate record to database if duplicates found.

        Args:
            file_path: Path to the file being analyzed

        Returns:
            ImmediateResult containing all duplicate information
        """
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

        # Get metadata for the analyzed file
        file_size: int = context.stat.st_size

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
                duplicated_size=file_size, duplicated_items=1,
                is_identical=is_identical, is_superset=is_superset,
                rule=self._comparison_rule
            ))

        # For files, both total_size and duplicated_size are the file size
        # total_size: size of this file
        # duplicated_size: size of this file (since it has duplicates)
        # total_items: 1 (the file itself)
        # duplicated_items: 1 (the file itself, since it has duplicates)

        # Create and write duplicate record
        record = DuplicateRecord(context.relative_path, metadata_comparisons, file_size, 1, file_size, 1)
        self._writer.write_duplicate_record(record)

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
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            if unit == 'B':
                return f"{size_bytes} {unit}"
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


@dataclass
class DescribeOptions:
    """Options for controlling duplicate display in describe command.

    Attributes:
        limit: Maximum number of duplicates to show. None means show all.
        sort_by: Sorting criterion for duplicates - 'size', 'items', 'identical', or 'path'
        sort_children: Sorting criterion for directory children - 'dup-size', 'dup-items', 'total-size', or 'name'
        use_bytes: If True, show sizes in bytes instead of human-readable format
        show_details: If True, show report metadata (Report, Analyzed, Archive, Timestamp)
    """
    limit: int | None = 1
    sort_by: str = 'size'
    sort_children: str = 'dup-size'
    use_bytes: bool = False
    show_details: bool = False


def do_describe(target_path: Path, options: DescribeOptions | None = None) -> None:
    """Describe duplicate information for a file or directory from analysis reports.

    Args:
        target_path: Path to the file or directory to describe
        options: Options for controlling duplicate display
    """
    if options is None:
        options = DescribeOptions()

    # Resolve target path
    target_path = target_path.resolve()

    # Check if target exists
    if not target_path.exists():
        print(f"Error: Path does not exist: {target_path}")
        return

    # Find report directory
    analyzed_path = find_report_for_path(target_path)
    if analyzed_path is None:
        print(f"No analysis report found for: {target_path}")
        print(f"Run 'arindexer analyze {target_path}' to generate a report.")
        return

    report_dir = get_report_directory_path(analyzed_path)

    # Read report
    try:
        with ReportReader(report_dir, analyzed_path) as reader:
            manifest = reader.read_manifest()

            # Show report metadata if requested
            if options.show_details:
                print(f"Report: {report_dir}")
                print(f"Analyzed: {analyzed_path}")
                print(f"Archive: {manifest.archive_path}")
                print(f"Timestamp: {manifest.timestamp}")
                print()

            # Calculate relative path within the analyzed directory
            if target_path == analyzed_path:
                # Target is the analyzed path itself
                relative_path = Path(analyzed_path.name)
            else:
                # Target is inside the analyzed path
                relative_path = Path(analyzed_path.name) / target_path.relative_to(analyzed_path)

            # Check if target is a directory and create appropriate formatter
            if target_path.is_dir():
                formatter = DirectoryDescribeFormatter(reader, relative_path, manifest, options)
            else:
                formatter = FileDescribeFormatter(reader, relative_path, manifest, options)

            # Execute the describe operation
            formatter.describe()

    except FileNotFoundError as e:
        print(f"Error reading report: {e}")
    except Exception as e:
        print(f"Error: {e}")


class DescribeFormatter(ABC):
    """Base class for formatting describe output using template pattern."""

    def __init__(self, reader: ReportReader, relative_path: Path, manifest: ReportManifest,
                 options: DescribeOptions | None = None):
        """Initialize formatter.

        Args:
            reader: ReportReader instance with open database
            relative_path: Path relative to the analyzed target
            manifest: Report manifest
            options: Options for controlling duplicate display
        """
        self.reader = reader
        self.relative_path = relative_path
        self.manifest = manifest
        self.options = options if options is not None else DescribeOptions()

    def describe(self) -> None:
        """Template method that describes the item."""
        record = self.reader.read_duplicate_record(self.relative_path)

        if record is None or not record.duplicates:
            print(f"No duplicates found for: {self.relative_path}")
            return

        # Print unified header
        self._print_header(record)

        # Print unified duplicates section
        self._print_duplicates(record)

        # Print additional details (only for directories)
        self._print_details(record)

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
            size_str = str(comparison.duplicated_size) if self.options.use_bytes else format_size(comparison.duplicated_size)
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
        directory_path: Path = self.reader.analyzed_path / Path(*self.relative_path.parts[1:])

        # List children from filesystem
        try:
            children = sorted(directory_path.iterdir(), key=lambda p: p.name)
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            print("  Could not list directory contents")
            return

        if not children:
            print("  Directory is empty")
            return

        # Prepare table data
        # Store both raw values (for sorting) and formatted strings (for display)
        # Format: (name, is_dir, total_size_raw, dup_size_raw, dups_raw, max_match_dup_size_raw, best_status_rank,
        #          total_items_raw, dup_items_raw, size_ratio, items_ratio, max_ratio, in_report,
        #          type_str, total_size_str, dup_size_str, dups_str, max_match_dup_size_str, best_status_str,
        #          total_items_str, dup_items_str, size_ratio_str, items_ratio_str, max_ratio_str, in_report_str)
        rows_data: list[tuple[str, bool, int, int, int, int, int, int, int, float, float, float, bool,
                              str, str, str, str, str, str, str, str, str, str, str, str]] = []
        for child_path in children:
            child_name = child_path.name
            is_dir = child_path.is_dir()
            type_str = "Dir" if is_dir else "File"

            # Construct the relative path for this child
            child_relative_path: Path = self.relative_path / child_name

            # Query the report for this child
            child_record = self.reader.read_duplicate_record(child_relative_path)

            if child_record is None:
                # Child exists on disk but not in report (no duplicates found)
                # Total size is not available in report, but dup size is known to be 0
                total_size_str = "-"
                dup_size_str = "0" if self.options.use_bytes else "0 B"
                max_match_dup_size_str = "0" if self.options.use_bytes else "0 B"
                best_status_str = "-"
                rows_data.append((child_name, is_dir, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, False,
                                type_str, total_size_str, dup_size_str, "0", max_match_dup_size_str, best_status_str,
                                "-", "-", "-", "-", "-", "No"))
            elif child_record.duplicates:
                total_size_str = str(child_record.total_size) if self.options.use_bytes else format_size(child_record.total_size)
                dup_size_str = str(child_record.duplicated_size) if self.options.use_bytes else format_size(child_record.duplicated_size)

                # Find the duplicate with highest duplicated_size and use its status
                max_match_dup = max(child_record.duplicates, key=lambda dup: dup.duplicated_size)
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
                size_ratio = child_record.duplicated_size / child_record.total_size if child_record.total_size > 0 else 0.0
                items_ratio = child_record.duplicated_items / child_record.total_items if child_record.total_items > 0 else 0.0
                max_ratio = max_match_dup_size / child_record.total_size if child_record.total_size > 0 else 0.0

                size_ratio_str = f"{size_ratio:.1%}"
                items_ratio_str = f"{items_ratio:.1%}"
                max_ratio_str = f"{max_ratio:.1%}"

                rows_data.append((
                    child_name,
                    is_dir,
                    child_record.total_size,
                    child_record.duplicated_size,
                    len(child_record.duplicates),
                    max_match_dup_size,
                    best_status_rank,
                    child_record.total_items,
                    child_record.duplicated_items,
                    size_ratio,
                    items_ratio,
                    max_ratio,
                    True,
                    type_str,
                    total_size_str,
                    dup_size_str,
                    str(len(child_record.duplicates)),
                    max_match_dup_size_str,
                    best_status_str,
                    str(child_record.total_items),
                    str(child_record.duplicated_items),
                    size_ratio_str,
                    items_ratio_str,
                    max_ratio_str,
                    "Yes"
                ))
            else:
                total_size_str = str(child_record.total_size) if self.options.use_bytes else format_size(child_record.total_size)
                dup_size_str = "0" if self.options.use_bytes else "0 B"
                max_match_dup_size_str = "0" if self.options.use_bytes else "0 B"
                best_status_str = "-"
                rows_data.append((
                    child_name,
                    is_dir,
                    child_record.total_size,
                    0,
                    0,
                    0,
                    0,
                    child_record.total_items,
                    0,
                    0.0,
                    0.0,
                    0.0,
                    True,
                    type_str,
                    total_size_str,
                    dup_size_str,
                    "0",
                    max_match_dup_size_str,
                    best_status_str,
                    str(child_record.total_items),
                    "0",
                    "0.0%",
                    "0.0%",
                    "0.0%",
                    "Yes"
                ))

        # Sort rows according to options
        def sort_key(row: tuple[str, bool, int, int, int, int, int, int, int, float, float, float, bool,
                                str, str, str, str, str, str, str, str, str, str, str, str]) -> tuple[Any, ...]:
            name, child_is_dir, total_size, dup_size, dups, match_dup_size, status_rank, total_items_val, dup_items_val, size_ratio_val, items_ratio_val, max_ratio_val, in_report, *_ = row

            if self.options.sort_children == 'dup-size':
                # Sort by dup_size descending, then dup_items descending, then total_size descending
                return -dup_size, -dups, -total_size
            elif self.options.sort_children == 'dup-items':
                # Sort by dups descending, then dup_size descending, then total_size descending
                return -dups, -dup_size, -total_size
            elif self.options.sort_children == 'total-size':
                # Sort by total_size descending
                return (-total_size,)
            elif self.options.sort_children == 'name':
                # Sort alphabetically by name
                return (name,)
            else:
                # Default to dup-size
                return -dup_size, -dups, -total_size

        rows_data = sorted(rows_data, key=sort_key)

        # Extract formatted strings for display
        # Order: name, type, total_size, dup_size, size_ratio, total_items, dup_items, items_ratio,
        #        dups, max_match_dup_size, max_ratio, status, in_report
        rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str]] = [
            (row[0], row[13], row[14], row[15], row[21], row[19], row[20], row[22], row[16], row[17], row[23], row[18], row[24]) for row in rows_data
        ]

        # Calculate column widths
        col1_width = max(len(row[0]) for row in rows)
        col2_width = max(max(len(row[1]) for row in rows), len('Type'))
        col3_width = max(max(len(row[2]) for row in rows), len('Total Size'))
        col4_width = max(max(len(row[3]) for row in rows), len('Dup Size'))
        col5_width = max(max(len(row[4]) for row in rows), len('Size %'))
        col6_width = max(max(len(row[5]) for row in rows), len('Total Items'))
        col7_width = max(max(len(row[6]) for row in rows), len('Dup Items'))
        col8_width = max(max(len(row[7]) for row in rows), len('Items %'))
        col9_width = max(max(len(row[8]) for row in rows), len('Dups'))
        col10_width = max(max(len(row[9]) for row in rows), len('Max Match'))
        col11_width = max(max(len(row[10]) for row in rows), len('Max %'))
        col12_width = max(max(len(row[11]) for row in rows), len('Status'))
        col13_width = max(len(row[12]) for row in rows)

        # Print header (numeric columns right-aligned)
        header = f"{'Name':<{col1_width}}  {'Type':<{col2_width}}  {'Total Size':>{col3_width}}  {'Dup Size':>{col4_width}}  {'Size %':>{col5_width}}  {'Total Items':>{col6_width}}  {'Dup Items':>{col7_width}}  {'Items %':>{col8_width}}  {'Dups':>{col9_width}}  {'Max Match':>{col10_width}}  {'Max %':>{col11_width}}  {'Status':<{col12_width}}  {'In Report':<{col13_width}}"
        print(header)
        print("-" * len(header))

        # Print rows (numeric columns right-aligned)
        for row in rows:
            line = f"{row[0]:<{col1_width}}  {row[1]:<{col2_width}}  {row[2]:>{col3_width}}  {row[3]:>{col4_width}}  {row[4]:>{col5_width}}  {row[5]:>{col6_width}}  {row[6]:>{col7_width}}  {row[7]:>{col8_width}}  {row[8]:>{col9_width}}  {row[9]:>{col10_width}}  {row[10]:>{col11_width}}  {row[11]:<{col12_width}}  {row[12]:<{col13_width}}"
            print(line)
