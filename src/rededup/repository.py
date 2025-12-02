import asyncio
import logging
import os
from pathlib import Path
from typing import Iterator, Callable, Awaitable

from .utils.processor import Processor
from .index.store import IndexStore
from .index.settings import IndexSettings
from .commands.rebuild_refresh import do_rebuild, do_refresh, RebuildRefreshArgs
from .commands.do_import import do_import, ImportArgs
from .commands.analyze import do_analyze, AnalyzeArgs
from .report.duplicate_match import DuplicateMatchRule


class Repository:
    """High-level workflow orchestration layer for repository file indexing and deduplication.

    This class represents the repository from a business operations perspective, providing
    complete workflows for common repository management tasks:
    - rebuild(): Full index reconstruction from scratch
    - refresh(): Incremental updates based on file system changes

    Repository operates at the workflow level, coordinating multiple IndexStore operations,
    managing concurrency with async/await patterns, handling error cases, and implementing
    business logic like content equivalent class assignment and duplicate reporting.

    Contrast with IndexStore class, which provides low-level data operations for direct
    storage access without workflow orchestration. Repository composes IndexStore primitives
    into meaningful, user-facing operations with proper sequencing and error handling.
    """

    def __init__(self, processor: Processor, path: str | os.PathLike, create: bool = False):
        """Initialize repository with LevelDB index at path/.rededup/index.

        Args:
            processor: File processing backend for hashing and comparison
            path: Repository root directory path
            create: Create .rededup directory if missing

        Raises:
            FileNotFoundError: Repository directory does not exist
            NotADirectoryError: Repository path is not a directory
            IndexNotFound: Index directory missing and create=False
        """
        repository_path = Path(path)

        settings = IndexSettings(repository_path)

        self._store = IndexStore(settings, repository_path, create)
        self._processor = processor
        self._settings = settings

        self._hash_algorithms = {
            'sha256': (32, self._processor.sha256)
        }
        self._default_hash_algorithm = 'sha256'

    def __del__(self):
        """Destructor ensures database is closed."""
        self.close()

    def __enter__(self):
        """Context manager entry, validates repository is still alive."""
        self._store.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, closes database connection."""
        self._store.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        """Close LevelDB database and mark repository as closed."""
        if hasattr(self, '_store'):
            self._store.close()

    def configure_logging_from_settings(self) -> bool:
        """Configure logging from repository settings if a log path is specified.

        Preserves the current logging level if already configured (e.g., from CLI arguments).
        Only changes the log file path.

        Returns:
            True if logging was configured, False otherwise
        """
        log_path_setting = self._settings.get('logging.path')
        if log_path_setting:
            log_path = str(log_path_setting)
            # Preserve current log level if already configured, otherwise default to INFO
            current_level = logging.root.level if logging.root.level != logging.NOTSET else logging.INFO

            # Reset logging configuration
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

            logging.basicConfig(
                filename=log_path,
                level=current_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            return True
        return False

    def rebuild(self):
        """Completely rebuild index by truncating database and re-scanning all files.

        Sets hash algorithm to SHA-256, removes all existing entries, and performs
        full repository traversal to generate new signatures and equivalent classes.
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

    def import_index(self, source_repository_path: str | os.PathLike):
        """Import index entries from another repository with path transformation.

        If the source repository is a nested directory of the current repository,
        entries are imported with the relative path prepended as a prefix.
        If the source repository is an ancestor directory of the current repository,
        only entries within the current repository's scope are imported with
        their prefix removed.

        Args:
            source_repository_path: Path to source repository directory

        Raises:
            ValueError: If source repository is invalid, same as current, or
                       relationship is neither nested nor ancestor
        """
        asyncio.run(do_import(
            self._store,
            ImportArgs(Path(source_repository_path), self._processor)
        ))

    def analyze(self, input_paths: list[Path], comparison_rule: DuplicateMatchRule | None = None):
        """Generate analysis reports for input paths against the repository.

        Creates a .report directory for each input path containing:
        - manifest.json: Report metadata including repository path, ID, and timestamp
        - .dup files: Lists of duplicate file paths for each analyzed file

        Args:
            input_paths: List of files or directories to analyze
            comparison_rule: Optional rule defining which metadata must match for identity.
                           If None, uses default rule (atime excluded, all else included).

        Each input path gets its own report directory (e.g., path/to/file.report)
        containing the analysis results and a manifest that references this repository
        with a validation identifier.

        Raises:
            RuntimeError: If repository ID is not set (repository needs refresh/rebuild)
            FileExistsError: If a file exists at the report directory path
        """
        repository_id = self._store.get_repository_id()
        if repository_id is None:
            raise RuntimeError("Repository ID not set. Please rebuild or refresh the repository first.")

        if comparison_rule is None:
            comparison_rule = DuplicateMatchRule()  # Use default rule (atime excluded)

        asyncio.run(do_analyze(
            self._store,
            AnalyzeArgs(
                self._processor,
                input_paths,
                self._get_hash_algorithm(),
                repository_id,
                self._store.repository_path,
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
            hash_algorithm = self._store.read_manifest(IndexStore.MANIFEST_HASH_ALGORITHM)

            if hash_algorithm is None:
                raise RuntimeError("The index hasn't been build")

            if hash_algorithm not in self._hash_algorithms:
                raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        return self._hash_algorithms[hash_algorithm]
