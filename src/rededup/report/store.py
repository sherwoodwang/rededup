"""Report storage for duplicate analysis results."""

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import mmh3
import msgpack
import plyvel

from .duplicate_match import DuplicateMatch
from ..utils.varint import encode_varint, decode_varint


class DuplicateRecord:
    """Record of a file and its duplicates in the repository with metadata comparisons.

    Attributes:
        path: Path relative to the analyzed target (including target's base name)
        duplicates: List of DuplicateMatch objects for each duplicate found in the repository.
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
                        has content-equivalent files in the repository. This is the deduplicated size - each
                        file is counted once regardless of how many content-equivalent files exist in the repository.
                        For files: the file size (if content-equivalent files exist in the repository).
                        For directories: sum of all descendant file sizes that have any content-equivalent files,
                                       aggregated recursively from child results across all repository paths
                                       (simple sum without requiring matching files to be at the same location
                                       in any hierarchy).

                        IMPORTANT: Semantic difference from DuplicateMatch.duplicated_size:
                        - DuplicateRecord.duplicated_size: Global deduplicated size. Each file in the analyzed
                          path is counted once, regardless of how many content-equivalent files exist across
                          all repository paths. This is a simple sum of duplicated_size from all child results,
                          without structural requirements.
                        - DuplicateMatch.duplicated_size: Localized size for a specific repository path.
                          When a file in the analyzed path has multiple content-equivalent files in different
                          repository directories, each DuplicateMatch counts that file's size independently.
                          For directories, only includes files that exist at the same relative location
                          within the specific repository directory's hierarchy (hierarchy must match).
        duplicated_items: Total count of items within this analyzed file/directory's content that
                         have content-equivalent files in the repository. This is the deduplicated count - each
                         item is counted once regardless of how many content-equivalent files exist in the repository.
                         For files: 1 (if content-equivalent files exist in the repository).
                         For directories: count of all descendant items that have any content-equivalent files,
                                        aggregated recursively from child results across all repository paths
                                        (simple sum without requiring matching items to be at the same location
                                        in any hierarchy).

                         IMPORTANT: Semantic difference from DuplicateMatch.duplicated_items:
                         - DuplicateRecord.duplicated_items: Global deduplicated count. Each item in the analyzed
                           path is counted once, regardless of how many content-equivalent files exist across
                           all repository paths. This is a simple sum of duplicated_items from all child results,
                           without structural requirements.
                         - DuplicateMatch.duplicated_items: Localized count for a specific repository path.
                           When an item in the analyzed path has multiple content-equivalent files in different
                           repository directories, each DuplicateMatch counts that item independently.
                           For directories, only includes items that exist at the same relative location
                           within the specific repository directory's hierarchy (hierarchy must match).
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
            Msgpack-encoded bytes containing [path_components, duplicate_data_list, total_size, total_items,
            duplicated_size, duplicated_items]
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

        result = msgpack.dumps(
            [path_components, duplicate_data, self.total_size, self.total_items, self.duplicated_size,
             self.duplicated_items])
        assert isinstance(result, bytes)
        return result

    @classmethod
    def from_msgpack(cls, data: bytes) -> "DuplicateRecord":
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


@dataclass
class ReportManifest:
    """Simple manifest structure for duplicate analysis reports.

    This is persisted as manifest.json in the .report directory.
    """
    version: str = "1.0"
    """Report format version"""

    repository_path: str = ""
    """Absolute path to the repository that was used for analysis"""

    repository_id: str = ""
    """Identifier of the repository, used to validate report is still valid"""

    timestamp: str = ""
    """ISO format timestamp when analysis was performed"""

    comparison_rule: dict[str, bool] | None = None
    """Rule used for determining identical matches (which metadata fields must match)"""

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReportManifest":
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

    def __enter__(self) -> "ReportStore":
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

    def read_manifest(self) -> "ReportManifest":
        """Read existing report manifest.

        Returns:
            The report manifest loaded from manifest.json

        Raises:
            FileNotFoundError: If manifest.json doesn't exist
        """
        with open(self.manifest_path, 'r') as f:
            data = json.load(f)
        return ReportManifest.from_dict(data)

    def validate_report(self, current_repository_id: str) -> bool:
        """Validate that report matches current repository state.

        Args:
            current_repository_id: Current repository identifier to check against

        Returns:
            True if report is valid for current repository, False otherwise
        """
        try:
            manifest = self.read_manifest()
            return manifest.repository_id == current_repository_id
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
