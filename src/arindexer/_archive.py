import asyncio
import os
from pathlib import Path
from typing import Iterator

from ._processor import Processor
from ._archive_store import ArchiveStore
from ._duplicate_finder import (
    do_find_duplicates,
    FileMetadataDifferencePattern,
    Output,
    StandardOutput
)
from ._rebuild_refresh import do_rebuild, do_refresh


class Archive:
    """High-level workflow orchestration layer for archive file indexing and deduplication.

    This class represents the archive from a business operations perspective, providing
    complete workflows for common archive management tasks:
    - rebuild(): Full index reconstruction from scratch
    - refresh(): Incremental updates based on file system changes
    - find_duplicates(): Content-aware duplicate detection with metadata comparison

    Archive operates at the workflow level, coordinating multiple ArchiveStore operations,
    managing concurrency with async/await patterns, handling error cases, and implementing
    business logic like content equivalent class assignment and duplicate reporting.

    Contrast with ArchiveStore class, which provides low-level data operations for direct
    storage access without workflow orchestration. Archive composes ArchiveStore primitives
    into meaningful, user-facing operations with proper sequencing and error handling.
    """
    __CONFIG_HASH_ALGORITHM = 'hash-algorithm'

    def __init__(self, processor: Processor, path: str | os.PathLike, create: bool = False,
                 output: Output | None = None):
        """Initialize archive with LevelDB index at path/.aridx/database.

        Args:
            processor: File processing backend for hashing and comparison
            path: Archive root directory path
            create: Create .aridx directory if missing
            output: Output handler for duplicate reporting

        Raises:
            FileNotFoundError: Archive directory does not exist
            NotADirectoryError: Archive path is not a directory
            ArchiveIndexNotFound: Index directory missing and create=False
        """
        archive_path = Path(path)

        if output is None:
            output = StandardOutput()

        self._store = ArchiveStore(archive_path, create)
        self._processor = processor
        self._output = output

        self._hash_algorithms = {
            'sha256': (32, self._processor.sha256)
        }
        self._default_hash_algorithm = 'sha256'

    def __del__(self):
        """Destructor ensures database is closed."""
        self.close()

    def __enter__(self):
        """Context manager entry, validates archive is still alive."""
        self._store.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, closes database connection."""
        self._store.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        """Close LevelDB database and mark archive as closed."""
        if hasattr(self, '_store'):
            self._store.close()

    @property
    def _archive_path(self) -> Path:
        """Get the archive root directory path."""
        return self._store.archive_path

    def rebuild(self):
        """Completely rebuild index by truncating database and re-scanning all files.

        Sets hash algorithm to SHA-256, removes all existing entries, and performs
        full archive traversal to generate new signatures and equivalent classes.
        """
        asyncio.run(do_rebuild(
            self._store,
            self._processor,
            self._archive_path,
            self._hash_algorithms,
            self._default_hash_algorithm,
            Archive.__CONFIG_HASH_ALGORITHM
        ))

    def refresh(self):
        """Incrementally update index by checking for file changes since last scan.

        Compares stored mtime against filesystem, removes deleted files,
        and processes new/modified files. More efficient than rebuild.
        """
        asyncio.run(do_refresh(
            self._store,
            self._processor,
            self._archive_path,
            self._hash_algorithms,
            Archive.__CONFIG_HASH_ALGORITHM
        ))

    def find_duplicates(self, input: Path, ignore: FileMetadataDifferencePattern | None = None):
        """Find files in input path that duplicate content in the archive.

        Args:
            input: Directory or file path to check for duplicates
            ignore: Metadata differences to ignore when matching (default: none)

        Outputs duplicate reports through configured Output handler.
        Groups files by content hash and compares within equivalent classes.
        """
        if ignore is None:
            ignore = FileMetadataDifferencePattern()

        asyncio.run(do_find_duplicates(
            self._store,
            self._processor,
            self._output,
            self._archive_path,
            self._hash_algorithms,
            self._default_hash_algorithm,
            Archive.__CONFIG_HASH_ALGORITHM,
            input,
            ignore
        ))

    def inspect(self) -> Iterator[str]:
        """Generate human-readable index entries for debugging and inspection.

        Yields:
            Formatted strings showing config, file-hash, and file-metadata entries
            with hex digests, timestamps, and URL-encoded paths
        """
        yield from self._store.inspect(self._hash_algorithms)
