import asyncio
import os
from pathlib import Path
from typing import Iterator, Callable, Awaitable

from ._processor import Processor
from ._archive_store import ArchiveStore
from ._duplicate_finder import (
    do_find_duplicates,
    FindDuplicatesArgs,
    FileMetadataDifferencePattern,
    Output,
    StandardOutput
)
from ._rebuild_refresh import do_rebuild, do_refresh, RebuildRefreshArgs


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

    def rebuild(self):
        """Completely rebuild index by truncating database and re-scanning all files.

        Sets hash algorithm to SHA-256, removes all existing entries, and performs
        full archive traversal to generate new signatures and equivalent classes.
        """
        asyncio.run(do_rebuild(
            self._store,
            RebuildRefreshArgs(
                self._processor,
                self._hash_algorithms[self._default_hash_algorithm]
            ),
            self._default_hash_algorithm
        ))

    def refresh(self):
        """Incrementally update index by checking for file changes since last scan.

        Compares stored mtime against filesystem, removes deleted files,
        and processes new/modified files. More efficient than rebuild.
        """
        asyncio.run(do_refresh(
            self._store,
            RebuildRefreshArgs(
                self._processor,
                self._get_hash_algorithm()
            )
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
            FindDuplicatesArgs(
                self._processor,
                self._output,
                self._get_hash_algorithm(),
                input,
                ignore
            )
        ))

    def inspect(self) -> Iterator[str]:
        """Generate human-readable index entries for debugging and inspection.

        Yields:
            Formatted strings showing manifest-property, file-hash, and file-metadata
            entries with hex digests, timestamps, and URL-encoded paths
        """
        yield from self._store.inspect(self._hash_algorithms)

    def _get_hash_algorithm(self, hash_algorithm: str | None = None) -> tuple[int, Callable[[Path], Awaitable[bytes]]]:
        """Get hash algorithm configuration.

        Args:
            hash_algorithm: Hash algorithm name, or None to use stored algorithm

        Returns:
            Tuple of (digest_size, calculator_function)

        Raises:
            RuntimeError: If no algorithm specified and index not built
            RuntimeError: If unknown hash algorithm specified
        """
        if hash_algorithm is None:
            hash_algorithm = self._store.read_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM)

            if hash_algorithm is None:
                raise RuntimeError("The index hasn't been build")

            if hash_algorithm not in self._hash_algorithms:
                raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        return self._hash_algorithms[hash_algorithm]
