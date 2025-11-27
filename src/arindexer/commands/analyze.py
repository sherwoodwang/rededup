"""Analyzer module for duplicate detection and analysis.

This module contains the processor for analyze command operations, including:
- FileAnalysisResult, ImmediateResult, DeferredResult - Analysis result types
- AnalyzeArgs - Arguments for analyze operations
- AnalyzeProcessor - Main processor class for analysis
- do_analyze - Entry point for analysis
"""

import asyncio
import logging
import os
import stat
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

from ..report.duplicate_match import (
    DuplicateMatch,
    DuplicateMatchRule,
    MetadataMatchReducer,
)
from ..report.path import get_report_directory_path
from ..report.store import DuplicateRecord, ReportManifest, ReportStore
from ..utils.processor import Processor
from ..utils.directory_listener import DirectoryListenerCoordinator, DirectoryListener
from ..utils.walker import FileContext
from ..store.archive_store import ArchiveStore

logger = logging.getLogger(__name__)


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
    def from_duplicate_record(cls, duplicate_record: DuplicateRecord) -> "ImmediateResult":
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
                            f"Inconsistent comparison rules within directory {context.relative_path}: expected "
                            f"{self._comparison_rule} but found {matching_comparison.rule} for child {base_name}"
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
                            For single items (symlinks, devices, pipes, sockets): 1 if any candidate matched, 0
                            otherwise.
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
            has_match = False
            for idx, entry in enumerate(candidate_states):
                if entry is None:
                    continue
                candidate_path, _, reducer = entry
                if analyzed_target != candidate_path.readlink():
                    candidate_states[idx] = None
                else:
                    # Symlink matched - set duplicated_items for this candidate
                    reducer.duplicated_items = 1
                    has_match = True
            if has_match:
                total_matched += 1

        elif stat.S_ISBLK(analyzed_stat.st_mode) or stat.S_ISCHR(analyzed_stat.st_mode):
            # Device files: compare major/minor numbers
            analyzed_major = os.major(analyzed_stat.st_rdev)
            analyzed_minor = os.minor(analyzed_stat.st_rdev)
            has_match = False
            for idx, entry in enumerate(candidate_states):
                if entry is None:
                    continue
                _, candidate_stat, reducer = entry
                if (os.major(candidate_stat.st_rdev) != analyzed_major or
                        os.minor(candidate_stat.st_rdev) != analyzed_minor):
                    candidate_states[idx] = None
                else:
                    # Device matched - set duplicated_items for this candidate
                    reducer.duplicated_items = 1
                    has_match = True
            if has_match:
                total_matched += 1

        elif stat.S_ISFIFO(analyzed_stat.st_mode) or stat.S_ISSOCK(analyzed_stat.st_mode):
            # Pipes/sockets: existence check is sufficient
            # Set duplicated_items for all valid candidates
            has_match = False
            for entry in candidate_states:
                if entry is not None:
                    _, _, reducer = entry
                    reducer.duplicated_items = 1
                    has_match = True
            if has_match:
                total_matched += 1

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
        # Normalize archive_path to remove . and .. components without following symlinks
        # - Path() keeps .. components (e.g., Path("/a/b/../c") has .. in parts)
        # - Path.resolve() follows symlinks on filesystem (undesirable here)
        # - os.path.normpath() removes . and .. without following symlinks (what we want)
        archive_path_normalized = args.archive_path if args.archive_path.is_absolute() else Path.cwd() / args.archive_path
        archive_path_normalized = Path(os.path.normpath(str(archive_path_normalized)))

        manifest: ReportManifest = ReportManifest(
            archive_path=str(archive_path_normalized),
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
