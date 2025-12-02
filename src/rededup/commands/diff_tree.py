"""Diff-tree subcommand for comparing analyzed directories with repository duplicates."""

from collections.abc import Callable
from enum import Enum
from pathlib import Path

from ..report.path import find_report_for_path, get_report_directory_path
from ..report.store import ReportStore, DuplicateRecord
from ..index.path import find_repository_for_path
from ..index.store import IndexStore
from ..index.settings import IndexSettings


class NodeStatus(Enum):
    """Status of a node in the diff tree."""
    ANALYZED_ONLY = "analyzed"  # Only in analyzed directory
    REPOSITORY_ONLY = "repository"  # Only in repository directory
    DIFFERENT = "different"  # In both but content differs
    CONTENT_MATCH = "content_match"  # Same content but different metadata (files only)
    SUPERSET = "superset"  # Repository contains all analyzed content plus extras (dirs only)
    IDENTICAL = "identical"  # In both and identical (content + metadata, will be skipped)


# Tree drawing characters
BRANCH = "├── "
LAST_BRANCH = "└── "
VERTICAL = "│   "
SPACE = "    "

# Status markers for files
FILE_STATUS_MARKERS = {
    NodeStatus.ANALYZED_ONLY: " [A]",
    NodeStatus.REPOSITORY_ONLY: " [R]",
    NodeStatus.DIFFERENT: " [D]",
    NodeStatus.CONTENT_MATCH: " [M]",  # Metadata differs
}

# Status markers for directories (incorporating superset info)
DIR_STATUS_MARKERS = {
    NodeStatus.ANALYZED_ONLY: " [A]",
    NodeStatus.REPOSITORY_ONLY: " [R]",
    NodeStatus.DIFFERENT: " [D]",
    NodeStatus.SUPERSET: " [+]",  # Repository has all analyzed content plus extras
    NodeStatus.CONTENT_MATCH: " [M]",  # Same content but metadata differs
}


class TreeOutput:
    """Buffered output for tree rendering with cork/rewind/flush capabilities.

    This allows us to defer rendering decisions until we know which children
    are actually visible after filtering. We can cork (save position), rewind
    (discard back to position), and flush (output to stdout).
    """

    def __init__(self) -> None:
        """Initialize an empty tree output buffer."""
        self._lines: list[str] = []
        self._cork_stack: list[int] = []

    def print(self, line: str) -> None:
        """Add a line to the buffer or output immediately if not corked.

        Args:
            line: The line to add (without trailing newline)
        """
        if not self._cork_stack:
            # No cork active - output immediately
            print(line)
        else:
            # Cork active - buffer the line
            self._lines.append(line)

    def cork(self) -> None:
        """Save the current position for potential rewinding.

        Creates a checkpoint that can be returned to via rewind().
        Cork points are stacked to support nested corking.
        """
        self._cork_stack.append(len(self._lines))

    def rewind(self) -> None:
        """Discard all output back to the most recent cork point.

        Removes the cork point from the stack and truncates the buffer
        to that position.

        Raises:
            IndexError: If there are no cork points to rewind to
        """
        if not self._cork_stack:
            raise IndexError("Cannot rewind: no cork points exist")

        position = self._cork_stack.pop()
        self._lines = self._lines[:position]

    def uncork(self) -> None:
        """Remove the most recent cork point without rewinding.

        Use this when you've decided to keep the output that was produced
        after the cork point. If this was the last cork, output all buffered lines.

        Raises:
            IndexError: If there are no cork points to remove
        """
        if not self._cork_stack:
            raise IndexError("Cannot uncork: no cork points exist")

        self._cork_stack.pop()

        # If no more corks, output all buffered lines
        if not self._cork_stack:
            for line in self._lines:
                print(line)
            self._lines.clear()

    def collapse(self, n: int) -> None:
        """Collapse the youngest n cork points into a single cork.

        Removes the n-1 corks immediately before the youngest cork, effectively
        committing the output decisions of those removed corks.

        Example: collapse(3) on stack [0,1,2,3,4,5,6] removes corks at indices 4,5
        (the n-1 corks before the youngest at index 6), resulting in [0,1,2,3,6].

        If the oldest remaining cork has buffered lines before it (position > 0),
        those lines are output to stdout and all cork positions are adjusted.

        Args:
            n: Number of youngest corks to collapse (removes n-1, keeps 1).
               Must be >= 1 and <= len(cork_stack).

        Raises:
            ValueError: If n < 1 or n > number of corks
        """
        if n < 1:
            raise ValueError("collapse(n) requires n >= 1")
        if n > len(self._cork_stack):
            raise ValueError(f"collapse({n}): only {len(self._cork_stack)} corks available")

        # If n == 1, no corks need to be removed (we keep the youngest one)
        if n == 1:
            return

        # Reconstruct stack: keep all corks before the removed section plus the youngest
        self._cork_stack = self._cork_stack[:-n] + [self._cork_stack[-1]]

        # If the oldest remaining cork has buffered lines before it, output them
        first_position = self._cork_stack[0]
        if first_position > 0:
            # Output lines that came before the first cork
            for line in self._lines[:first_position]:
                print(line)
            # Remove output lines from buffer and adjust all cork positions
            self._lines = self._lines[first_position:]
            for i in range(len(self._cork_stack)):
                self._cork_stack[i] -= first_position


class DiffTreeProcessor:
    """Processor for diff-tree operations that encapsulates state and logic."""

    def __init__(
        self,
        report_store: ReportStore,
        analyzed_base: Path,
        repository_base: Path,
        hide_content_match: bool = False,
        max_depth: int | None = None,
        show_filter: str = "both"
    ):
        """Initialize the diff-tree processor.

        Args:
            report_store: Report store to look up duplicate information
            analyzed_base: Parent of the directory that has *.report next to it (absolute path)
            repository_base: Directory where .rededup is a direct child (absolute path)
            hide_content_match: If True, hide files that match content but differ in metadata
            max_depth: Maximum depth to display (None = unlimited)
            show_filter: Filter for which files to show: "both", "analyzed", or "repository"
        """
        self._report_store = report_store
        self._analyzed_base = analyzed_base
        self._repository_base = repository_base
        self._hide_content_match = hide_content_match
        self._max_depth = max_depth
        self._show_filter = show_filter

    def _get_node_status(
        self,
        analyzed_relative: Path | None,
        repository_relative: Path | None
    ) -> NodeStatus:
        """Determine the status of a node using report information.

        Args:
            analyzed_relative: Relative path from analyzed_base, or None if not present
            repository_relative: Relative path from repository_base, or None if not present

        Returns:
            NodeStatus indicating the comparison result
        """
        if analyzed_relative is None:
            return NodeStatus.REPOSITORY_ONLY
        if repository_relative is None:
            return NodeStatus.ANALYZED_ONLY

        # Both exist - look up in report
        # analyzed_relative is already relative to analyzed_base (parent of report target)
        # and includes the report target name, so it's ready to use as report_path
        report_path = analyzed_relative

        # Read duplicate record for this path
        record = self._report_store.read_duplicate_record(report_path)

        if record is None or not record.duplicates:
            # No duplicates found in analysis - they differ
            return NodeStatus.DIFFERENT

        # repository_relative is already relative to repository_base (the repository root)
        # so it's ready to use for lookup
        lookup_path = repository_relative

        # Find matching duplicate
        for dup in record.duplicates:
            if dup.path == lookup_path:
                # Found the matching duplicate
                if dup.is_identical:
                    return NodeStatus.IDENTICAL
                elif dup.is_superset:
                    # Repository contains all analyzed content plus possibly extras (directories only)
                    return NodeStatus.SUPERSET
                else:
                    # is_identical=False and is_superset=False
                    # For files: is_superset always equals is_identical, so this branch means
                    # content matches but metadata differs → CONTENT_MATCH
                    # For directories: could be either partial match or full content match with metadata diff
                    #
                    # The is_superset flag properly propagates from children, so if is_superset=False
                    # for a directory, it means either:
                    # 1. Some analyzed content is missing from repository, OR
                    # 2. Some descendant has differing content or metadata
                    #
                    # Only return CONTENT_MATCH for directories when ALL of:
                    # - All items are present (duplicated_items == total_items)
                    # - All content matches (check via duplicated_size or other means)
                    # - Only metadata differs
                    #
                    # Since is_superset=False for directories can indicate descendant differences,
                    # we should return DIFFERENT for directories unless we're certain it's only metadata.
                    # For now, always return DIFFERENT for directories when is_superset=False.
                    if dup.duplicated_items < record.total_items:
                        # Partial match - not all items are duplicated
                        return NodeStatus.DIFFERENT
                    elif record.total_items > 1:
                        # Directory (has multiple items or subdirectories)
                        # is_superset=False means descendant content/metadata differs
                        return NodeStatus.DIFFERENT
                    else:
                        # Single item (file): all content duplicated but metadata differs
                        return NodeStatus.CONTENT_MATCH

        # No matching duplicate found
        return NodeStatus.DIFFERENT

    @staticmethod
    def _print_tree_node(
        output: TreeOutput,
        name: str,
        status: NodeStatus,
        level: int,
        is_last: bool,
        parent_prefixes: list[bool],
        is_dir: bool = False
    ) -> None:
        """Print a single tree node with proper indentation and status marker.

        Args:
            output: TreeOutput object to write to
            name: Name of the file or directory
            status: Status of this node
            level: Depth level in the tree (0 = root)
            is_last: Whether this is the last child in its parent
            parent_prefixes: List of booleans for each ancestor level, True if ancestor was last child
            is_dir: Whether this node is a directory
        """
        if level == 0:
            # Root node - no prefix
            prefix = ""
        else:
            # Build prefix from ancestor information
            # parent_prefixes contains one entry per ancestor level indicating if that ancestor was last
            prefix_parts = []
            for was_last in parent_prefixes:
                prefix_parts.append(SPACE if was_last else VERTICAL)
            # Add connector for this node
            prefix_parts.append(LAST_BRANCH if is_last else BRANCH)
            prefix = "".join(prefix_parts)

        # Use appropriate marker set based on whether this is a directory
        markers = DIR_STATUS_MARKERS if is_dir else FILE_STATUS_MARKERS
        marker = markers.get(status, "")
        output.print(f"{prefix}{name}{marker}")

    def _build_and_print_tree(
        self,
        output: TreeOutput,
        analyzed_relative: Path | None,
        repository_relative: Path | None,
        name: str,
        level: int,
        is_last: bool,
        parent_prefixes: list[bool],
        ensure_ancestors_printed: Callable[[], None] | None = None,
        preserve_state: Callable[[], Callable[[], None]] | None = None
    ) -> bool:
        """Recursively build and print the diff tree.

        Args:
            output: TreeOutput object to write to
            analyzed_relative: Relative path from analyzed_base, or None
            repository_relative: Relative path from repository_base, or None
            name: Name to display for this node
            level: Current depth level (0 = root)
            is_last: Whether this node is the last child in its parent
            parent_prefixes: List tracking whether each ancestor was a last child
            ensure_ancestors_printed: Callback to ensure all ancestor nodes are printed

        Returns:
            True if this node or any descendant had differences
        """
        # Convert relative paths to absolute for directory operations
        analyzed_dir = None if analyzed_relative is None else self._analyzed_base / analyzed_relative
        repository_dir = None if repository_relative is None else self._repository_base / repository_relative

        # Get child names from both directories
        analyzed_children: set[str] = set()
        repository_children: set[str] = set()

        if analyzed_dir is not None and analyzed_dir.is_dir():
            try:
                analyzed_children = {child.name for child in analyzed_dir.iterdir()}
            except (OSError, PermissionError):
                pass

        if repository_dir is not None and repository_dir.is_dir():
            try:
                repository_children = {child.name for child in repository_dir.iterdir()}
            except (OSError, PermissionError):
                pass

        # Merge child names and sort
        all_names = sorted(analyzed_children | repository_children)

        # Process children
        children_with_diffs: list[tuple[str, Path | None, Path | None, NodeStatus, bool]] = []

        for child_name in all_names:
            # Determine which side has this child
            in_analyzed = child_name in analyzed_children
            in_repository = child_name in repository_children

            # Build relative paths for children (only if they exist and parent exists)
            analyzed_child_relative = \
                None if (not in_analyzed or analyzed_relative is None) else analyzed_relative / child_name
            repository_child_relative = \
                None if (not in_repository or repository_relative is None) else repository_relative / child_name

            status = self._get_node_status(analyzed_child_relative, repository_child_relative)

            # Check if this is a directory or regular file (reconstruct absolute path only when needed)
            is_dir = False
            is_regular_file = False
            if in_analyzed and analyzed_dir is not None:
                child_path = analyzed_dir / child_name
                if not child_path.is_symlink():
                    is_dir = child_path.is_dir()
                    is_regular_file = child_path.is_file()
            elif in_repository and repository_dir is not None:
                child_path = repository_dir / child_name
                if not child_path.is_symlink():
                    is_dir = child_path.is_dir()
                    is_regular_file = child_path.is_file()

            # Skip special files (symlinks, devices, etc.) - they don't have individual records
            # They are accounted for in the parent directory's comparison
            if not is_dir and not is_regular_file:
                continue

            # Skip identical items
            if status == NodeStatus.IDENTICAL:
                continue

            # Skip content-match items when hiding them
            if self._hide_content_match and status == NodeStatus.CONTENT_MATCH:
                continue

            # Apply show_filter (applies to both files and directories)
            # When filtering to one side, directories are essential for showing tree structure
            if self._show_filter == "analyzed" and status == NodeStatus.REPOSITORY_ONLY:
                continue
            elif self._show_filter == "repository" and status == NodeStatus.ANALYZED_ONLY:
                continue

            children_with_diffs.append((child_name, analyzed_child_relative, repository_child_relative, status, is_dir))

        if not children_with_diffs:
            return False

        # Check if we've reached max depth
        at_max_depth = self._max_depth is not None and level + 1 >= self._max_depth
        # Prefixes for this node's children include whether this node is the last child of its parent
        # Exception: level 0 is the invisible root and doesn't contribute to the prefix
        new_prefixes = parent_prefixes + [is_last] if level > 0 else parent_prefixes

        # Get this node's status (for conditional printing at level > 0)
        dir_status: NodeStatus | None = None
        if level > 0:
            dir_status = self._get_node_status(analyzed_relative, repository_relative)

        # Track if we actually printed this node
        node_printed = False

        # Create closure to ensure this node (and all ancestors) are printed
        def ensure_this_node_printed() -> None:
            nonlocal node_printed

            # First, ensure ancestors are printed
            if ensure_ancestors_printed is not None:
                ensure_ancestors_printed()

            # Then print this node if it's not the root and not already printed
            if not node_printed and level > 0 and dir_status is not None:
                self._print_tree_node(
                    output, name, dir_status, level, is_last, parent_prefixes, is_dir=True)
                node_printed = True

        # Create state preservation function that captures the entire hierarchy
        def preserve_this_state() -> Callable[[], None]:
            # Capture current state
            old_node_printed = node_printed

            # Get ancestor undo function if available
            ancestor_undo: Callable[[], None] = lambda: None
            if preserve_state is not None:
                ancestor_undo = preserve_state()

            # Return chained undo function
            def undo() -> None:
                nonlocal node_printed
                node_printed = old_node_printed
                ancestor_undo()
            return undo

        # Single-pass loop with backtracking capability for re-rendering the last visible child
        #
        # Strategy:
        # - Cork before processing each child to buffer its output
        # - When a second visible child appears, collapse(2) commits the first while keeping second corked
        # - At most 2 children are corked: the previous visible child and the current one
        # - If the last visible child was rendered with is_last=False, rewind and re-render with is_last=True
        #
        # Cork/State operation pairs (ensuring stack balance and state consistency):
        #
        # Setup (every child):
        #   cork() + preserve_state() → Creates checkpoint for both buffer and state (line 385-386)
        #
        # Resolution pairs (exactly one per child):
        #   1. collapse(2) → Commits previous child, keeps current corked (line 437)
        #      - Maintains stack balance: removes 1 cork (previous child's)
        #      - State handling: keeps state as-is (previous child committed, current preserved)
        #
        #   2. rewind() + undo() → Discards output and resets state for re-render (line 367-368)
        #      - Maintains stack balance: removes 1 cork
        #      - State handling: resets entire hierarchy via chained undo
        #
        #   3. uncork() (no output) → Removes cork, no state changes (line 449)
        #      - Maintains stack balance: removes 1 cork
        #      - State handling: no-op (ensure_this_node_printed was never called)
        #
        #   4. uncork() (finalization) → Flushes all buffered output (line 375)
        #      - Maintains stack balance: removes final cork
        #      - State handling: no changes (state already committed)
        #
        # Loop variables:
        idx = 0  # Current position in children_with_diffs list
        last_visible_idx: int | None = None  # Index of the most recent visible child (for backtracking)
        last_visible_was_last = False  # Whether last visible child was rendered with is_last=True
        last_undo: Callable[[], None] = lambda: None  # Undo function to reset state hierarchy when rewinding
        force_is_last = False  # Override is_last=True when re-rendering after backtrack
        has_visible_child = False  # Whether we've encountered at least one visible child

        while idx <= len(children_with_diffs):
            # Post-end check: if we've gone past the last child, handle finalization
            if idx == len(children_with_diffs):
                if last_visible_idx is not None and not last_visible_was_last:
                    # The last visible child was rendered with is_last=False
                    # Need to rewind and re-render it with is_last=True
                    output.rewind()
                    last_undo()
                    idx = last_visible_idx  # Move index back
                    force_is_last = True  # Force is_last=True on re-render
                    continue  # Re-render with corrected is_last
                else:
                    # Last child is correctly rendered, uncork if we had visible children
                    if has_visible_child:
                        output.uncork()
                    break

            # Processing a real child
            child_name, analyzed_child_rel, repository_child_rel, status, is_dir = children_with_diffs[idx]

            # Determine if this child is the last in the original list
            child_is_last = force_is_last or (idx == len(children_with_diffs) - 1)

            # Cork before processing this child and preserve state
            output.cork()
            saved_undo = preserve_this_state()

            # Render the child
            child_produced_output = False

            if is_dir:
                if at_max_depth:
                    # At max depth - always produces output
                    ensure_this_node_printed()

                    self._print_tree_node(
                        output, child_name, status, level + 1, child_is_last, new_prefixes, is_dir=True)
                    # Show "..." to indicate elided content
                    ellipsis_parent_prefixes = new_prefixes + [child_is_last]
                    prefix_parts = []
                    for was_last in ellipsis_parent_prefixes:
                        prefix_parts.append(SPACE if was_last else VERTICAL)
                    prefix_parts.append(LAST_BRANCH)
                    prefix = "".join(prefix_parts)
                    output.print(f"{prefix}...")

                    child_produced_output = True
                else:
                    # Recursively render directory
                    child_has_diffs = self._build_and_print_tree(
                        output,
                        analyzed_child_rel,
                        repository_child_rel,
                        child_name,
                        level + 1,
                        child_is_last,
                        new_prefixes,
                        ensure_this_node_printed,
                        preserve_this_state
                    )
                    if child_has_diffs:
                        # Node was already printed by recursive call if needed
                        # Just mark that this child produced output
                        child_produced_output = True
            else:
                # Render file
                ensure_this_node_printed()
                self._print_tree_node(
                    output, child_name, status, level + 1, child_is_last, new_prefixes, is_dir=False)
                child_produced_output = True

            # Decide what to do based on whether child produced output
            if child_produced_output:
                # This child is visible
                # If we had a previous visible child, collapse to commit it while keeping current corked
                if has_visible_child and not force_is_last:
                    output.collapse(2)

                has_visible_child = True
                last_visible_idx = idx
                last_visible_was_last = child_is_last
                last_undo = saved_undo
                force_is_last = False  # Reset flag
                idx += 1
            else:
                # No output - remove cork without modifying buffer or state
                # Since the child produced no output, ensure_this_node_printed() was never called,
                # so no state changes occurred that need to be undone.
                output.uncork()
                idx += 1

        return node_printed or has_visible_child

    def run(
        self,
        analyzed_start: Path,
        repository_start: Path,
        record: DuplicateRecord
    ) -> bool:
        """Run the diff-tree operation.

        Args:
            analyzed_start: Starting path for comparison, relative to analyzed_base (including report target name)
            repository_start: Starting path for comparison, relative to repository_base
            record: Duplicate record for the analyzed path

        Returns:
            True if differences were found, False if directories are identical
        """
        # Check if repository_start matches any duplicate
        matching_duplicate = None
        for dup in record.duplicates:
            if dup.path == repository_start:
                matching_duplicate = dup
                break

        if matching_duplicate is None:
            print(f"Error: {self._repository_base / repository_start} is not a known duplicate of "
                  f"{self._analyzed_base / analyzed_start}")
            print(f"Known duplicates:")
            for dup in record.duplicates:
                print(f"  {self._repository_base / dup.path}")
            return False

        # Print header
        print(f"Comparing:")
        print(f"  Analyzed: {self._analyzed_base / analyzed_start}")
        print(f"  Repository:  {self._repository_base / repository_start}")
        print()

        # Extract the name from the starting paths to use as the root display name
        name = analyzed_start.parts[-1] if analyzed_start.parts else "."

        # Create TreeOutput for buffered rendering
        output = TreeOutput()

        has_diffs = self._build_and_print_tree(
            output,
            analyzed_start,  # Start from the specified analyzed path
            repository_start,   # Start from the specified repository path
            name,
            level=0,
            is_last=True,
            parent_prefixes=[]
        )

        return has_diffs


def do_diff_tree(
        analyzed_path: Path,
        repository_path: Path,
        hide_content_match: bool = False,
        max_depth: int | None = None,
        show_filter: str = "both"
) -> None:
    """Compare directory trees between an analyzed path and its duplicate in the repository.

    Args:
        analyzed_path: Path to the analyzed directory (must have a report)
        repository_path: Path to the duplicate directory in the repository
        hide_content_match: If True, hide files that match content but differ in metadata
        max_depth: Maximum depth to display (None = unlimited)
        show_filter: Filter for which files to show: "both", "analyzed", or "repository"
    """
    # Find report for analyzed path
    # find_report_for_path() handles path normalization and traversal
    report_result = find_report_for_path(analyzed_path)
    if report_result is None:
        # Provide better error messages for analyzed_path validation issues
        analyzed_path_abs = analyzed_path if analyzed_path.is_absolute() else Path.cwd() / analyzed_path
        if not analyzed_path_abs.exists():
            print(f"Error: Analyzed path does not exist: {analyzed_path_abs}")
        elif not analyzed_path_abs.is_dir():
            print(f"Error: Analyzed path must be a directory: {analyzed_path_abs}")
        else:
            print(f"No analysis report found for: {analyzed_path_abs}")
            print(f"Run 'rededup analyze {analyzed_path_abs}' to generate a report.")
        return

    report_target, record_path = report_result

    # Find repository root containing .rededup for the repository_path
    # find_repository_for_path() handles path normalization and returns (repository_root, repository_record_path)
    # repository_record_path is relative to repository_root and can be concatenated:
    #   repository_root / repository_record_path == repository_path
    repository_result = find_repository_for_path(repository_path)
    if repository_result is None:
        # Provide better error messages for repository_path validation issues
        repository_path_abs = repository_path if repository_path.is_absolute() else Path.cwd() / repository_path
        if not repository_path_abs.exists():
            print(f"Error: Repository path does not exist: {repository_path_abs}")
        elif not repository_path_abs.is_dir():
            print(f"Error: Repository path must be a directory: {repository_path_abs}")
        else:
            print(f"Error: No repository found containing: {repository_path_abs}")
            print(f"Repository must be within a directory containing .rededup")
        return

    repository_root, repository_relative = repository_result
    report_dir = get_report_directory_path(report_target)

    try:
        with ReportStore(report_dir, report_target) as store:
            manifest = store.read_manifest()

            # record_path is already the correct path for database lookup,
            # including the analyzed directory name as its first component
            # (e.g., dir/subdir/file.txt for analyzed /parent/dir/subdir/file.txt)

            # Read duplicate record
            record = store.read_duplicate_record(record_path)

            if record is None:
                print(f"No duplicate record found for: {analyzed_path}")
                return

            if not record.duplicates:
                print(f"No duplicates found for: {analyzed_path}")
                return

            # Validate that repository matches the one used in the report
            # Use repository_id rather than path comparison, as paths can differ but represent the same repository
            repository_settings = IndexSettings(repository_root)
            repository_store = IndexStore(repository_settings, repository_root)
            current_repository_id = repository_store.get_repository_id()
            if current_repository_id != manifest.repository_id:
                print(f"Error: Repository mismatch")
                print(f"Provided repository has ID: {current_repository_id}")
                print(f"Report expects repository ID: {manifest.repository_id}")
                return

            # Compute the bases for path resolution
            # analyzed_base is the parent of the report target
            analyzed_base = report_target.parent
            # repository_base is the repository root (where .rededup is a direct child)
            repository_base = repository_root

            # Create processor and run diff-tree operation
            processor = DiffTreeProcessor(
                report_store=store,
                analyzed_base=analyzed_base,
                repository_base=repository_base,
                hide_content_match=hide_content_match,
                max_depth=max_depth,
                show_filter=show_filter
            )

            # Compute the starting path in analyzed_base for the processor
            # record_path is relative to analyzed_base and includes the report target name
            analyzed_start = record_path

            has_diffs = processor.run(
                analyzed_start=analyzed_start,
                repository_start=repository_relative,
                record=record
            )

            if not has_diffs:
                print("Directories are identical.")

    except FileNotFoundError as e:
        print(f"Error reading report: {e}")
    except Exception as e:
        print(f"Error: {e}")
