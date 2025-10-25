import asyncio
import logging
import os
from pathlib import Path
from typing import Iterator, Callable, Awaitable

from .utils.processor import Processor
from .store.archive_store import ArchiveStore
from .store.archive_settings import ArchiveSettings
from .commands.duplicate_finder import (
    do_find_duplicates,
    FindDuplicatesArgs,
    FileMetadataDifferencePattern,
    Output,
    StandardOutput
)
from .commands.rebuild_refresh import do_rebuild, do_refresh, RebuildRefreshArgs
from .commands.archive_importer import do_import, ImportArgs
from .commands.analyzer import do_analyze, AnalyzeArgs


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

        settings = ArchiveSettings(archive_path)

        self._store = ArchiveStore(settings, archive_path, create)
        self._processor = processor
        self._output = output
        self._settings = settings

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

    def configure_logging_from_settings(self) -> bool:
        """Configure logging from archive settings if a log path is specified.

        Returns:
            True if logging was configured, False otherwise
        """
        log_path = self._settings.get('logging.path')
        if log_path:
            # Reset logging configuration
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

            logging.basicConfig(
                filename=log_path,
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            return True
        return False

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

    def import_index(self, source_archive_path: str | os.PathLike):
        """Import index entries from another archive with path transformation.

        If the source archive is a nested directory of the current archive,
        entries are imported with the relative path prepended as a prefix.
        If the source archive is an ancestor directory of the current archive,
        only entries within the current archive's scope are imported with
        their prefix removed.

        Args:
            source_archive_path: Path to source archive directory

        Raises:
            ValueError: If source archive is invalid, same as current, or
                       relationship is neither nested nor ancestor
        """
        asyncio.run(do_import(
            self._store,
            ImportArgs(Path(source_archive_path), self._processor)
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

    def analyze(self, input_paths: list[Path], comparison_rule: 'DuplicateMatchRule | None' = None):
        """Generate analysis reports for input paths against the archive.

        Creates a .report directory for each input path containing:
        - manifest.json: Report metadata including archive path, ID, and timestamp
        - .dup files: Lists of duplicate file paths for each analyzed file

        Args:
            input_paths: List of files or directories to analyze
            comparison_rule: Optional rule defining which metadata must match for identity.
                           If None, uses default rule (atime excluded, all else included).

        Each input path gets its own report directory (e.g., path/to/file.report)
        containing the analysis results and a manifest that references this archive
        with a validation identifier.

        Raises:
            RuntimeError: If archive ID is not set (archive needs refresh/rebuild)
            FileExistsError: If a file exists at the report directory path
        """
        archive_id = self._store.get_archive_id()
        if archive_id is None:
            raise RuntimeError("Archive ID not set. Please rebuild or refresh the archive first.")

        from .commands.analyzer import DuplicateMatchRule

        if comparison_rule is None:
            comparison_rule = DuplicateMatchRule()  # Use default rule (atime excluded)

        asyncio.run(do_analyze(
            self._store,
            AnalyzeArgs(
                self._processor,
                input_paths,
                self._get_hash_algorithm(),
                archive_id,
                self._store.archive_path,
                comparison_rule
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
