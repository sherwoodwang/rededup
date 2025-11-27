"""Diff-tree subcommand for comparing analyzed directories with archive duplicates."""

from collections.abc import Callable
from enum import Enum
from pathlib import Path

from ..report.path import find_report_for_path, get_report_directory_path
from ..report.store import ReportStore, DuplicateRecord
from ..store.path import find_archive_for_path
from ..store.archive_store import ArchiveStore
from ..store.archive_settings import ArchiveSettings


class NodeStatus(Enum):
    """Status of a node in the diff tree."""
    ANALYZED_ONLY = "analyzed"  # Only in analyzed directory
    ARCHIVE_ONLY = "archive"  # Only in archive directory
    DIFFERENT = "different"  # In both but content differs
    CONTENT_MATCH = "content_match"  # Same content but different metadata (files only)
    SUPERSET = "superset"  # Archive contains all analyzed content plus extras (dirs only)
    IDENTICAL = "identical"  # In both and identical (content + metadata, will be skipped)


# Tree drawing characters
BRANCH = "├── "
LAST_BRANCH = "└── "
VERTICAL = "│   "
SPACE = "    "

# Status markers for files
FILE_STATUS_MARKERS = {
    NodeStatus.ANALYZED_ONLY: " [A]",
    NodeStatus.ARCHIVE_ONLY: " [R]",
    NodeStatus.DIFFERENT: " [D]",
    NodeStatus.CONTENT_MATCH: " [M]",  # Metadata differs
}

# Status markers for directories (incorporating superset info)
DIR_STATUS_MARKERS = {
    NodeStatus.ANALYZED_ONLY: " [A]",
    NodeStatus.ARCHIVE_ONLY: " [R]",
    NodeStatus.DIFFERENT: " [D]",
    NodeStatus.SUPERSET: " [+]",  # Archive has all analyzed content plus extras
    NodeStatus.CONTENT_MATCH: " [M]",  # Same content but metadata differs
}


class DiffTreeProcessor:
    """Processor for diff-tree operations that encapsulates state and logic."""

    def __init__(
        self,
        report_store: ReportStore,
        analyzed_base: Path,
        archive_base: Path,
        hide_content_match: bool = False,
        max_depth: int | None = None,
        show_filter: str = "both"
    ):
        """Initialize the diff-tree processor.

        Args:
            report_store: Report store to look up duplicate information
            analyzed_base: Parent of the directory that has *.report next to it (absolute path)
            archive_base: Directory where .aridx is a direct child (absolute path)
            hide_content_match: If True, hide files that match content but differ in metadata
            max_depth: Maximum depth to display (None = unlimited)
            show_filter: Filter for which files to show: "both", "analyzed", or "archive"
        """
        self._report_store = report_store
        self._analyzed_base = analyzed_base
        self._archive_base = archive_base
        self._hide_content_match = hide_content_match
        self._max_depth = max_depth
        self._show_filter = show_filter

    def _get_node_status(
        self,
        analyzed_relative: Path | None,
        archive_relative: Path | None
    ) -> NodeStatus:
        """Determine the status of a node using report information.

        Args:
            analyzed_relative: Relative path from analyzed_base, or None if not present
            archive_relative: Relative path from archive_base, or None if not present

        Returns:
            NodeStatus indicating the comparison result
        """
        if analyzed_relative is None:
            return NodeStatus.ARCHIVE_ONLY
        if archive_relative is None:
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

        # archive_relative is already relative to archive_base (the archive root)
        # so it's ready to use for lookup
        lookup_path = archive_relative

        # Find matching duplicate
        for dup in record.duplicates:
            if dup.path == lookup_path:
                # Found the matching duplicate
                if dup.is_identical:
                    return NodeStatus.IDENTICAL
                elif dup.is_superset:
                    # Archive contains all analyzed content plus possibly extras (directories only)
                    return NodeStatus.SUPERSET
                else:
                    # For files: is_superset always equals is_identical, so this means
                    # content matches but metadata differs → CONTENT_MATCH
                    # For directories: this is a partial match (some content differs) → DIFFERENT
                    # We can distinguish by checking if duplicated_items < total_items
                    if dup.duplicated_items < record.total_items:
                        # Partial match - not all content is duplicated
                        return NodeStatus.DIFFERENT
                    else:
                        # All content duplicated but metadata differs
                        return NodeStatus.CONTENT_MATCH

        # No matching duplicate found
        return NodeStatus.DIFFERENT

    @staticmethod
    def _print_tree_node(
        name: str,
        status: NodeStatus,
        level: int,
        is_last: bool,
        parent_prefixes: list[bool],
        is_dir: bool = False
    ) -> None:
        """Print a single tree node with proper indentation and status marker.

        Args:
            name: Name of the file or directory
            status: Status of this node
            level: Depth level in the tree (0 = root)
            is_last: Whether this is the last child in its parent
            parent_prefixes: List of booleans for each parent level, True if parent was last child
            is_dir: Whether this node is a directory
        """
        if level == 0:
            # Root node - no prefix
            prefix = ""
        else:
            # Build prefix from parent information
            prefix_parts = []
            for was_last in parent_prefixes[:-1]:
                prefix_parts.append(SPACE if was_last else VERTICAL)
            prefix_parts.append(LAST_BRANCH if is_last else BRANCH)
            prefix = "".join(prefix_parts)

        # Use appropriate marker set based on whether this is a directory
        markers = DIR_STATUS_MARKERS if is_dir else FILE_STATUS_MARKERS
        marker = markers.get(status, "")
        print(f"{prefix}{name}{marker}")

    def _build_and_print_tree(
        self,
        analyzed_relative: Path | None,
        archive_relative: Path | None,
        name: str,
        level: int,
        is_last: bool,
        parent_prefixes: list[bool],
        ensure_ancestors_printed: Callable[[], None] | None = None
    ) -> bool:
        """Recursively build and print the diff tree.

        Args:
            analyzed_relative: Relative path from analyzed_base, or None
            archive_relative: Relative path from archive_base, or None
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
        archive_dir = None if archive_relative is None else self._archive_base / archive_relative

        # Get child names from both directories
        analyzed_children: set[str] = set()
        archive_children: set[str] = set()

        if analyzed_dir is not None and analyzed_dir.is_dir():
            try:
                analyzed_children = {child.name for child in analyzed_dir.iterdir()}
            except (OSError, PermissionError):
                pass

        if archive_dir is not None and archive_dir.is_dir():
            try:
                archive_children = {child.name for child in archive_dir.iterdir()}
            except (OSError, PermissionError):
                pass

        # Merge child names and sort
        all_names = sorted(analyzed_children | archive_children)

        # Process children
        children_with_diffs: list[tuple[str, Path | None, Path | None, NodeStatus, bool]] = []

        for child_name in all_names:
            # Determine which side has this child
            in_analyzed = child_name in analyzed_children
            in_archive = child_name in archive_children

            # Build relative paths for children (only if they exist and parent exists)
            analyzed_child_relative = \
                None if (not in_analyzed or analyzed_relative is None) else analyzed_relative / child_name
            archive_child_relative = \
                None if (not in_archive or archive_relative is None) else archive_relative / child_name

            status = self._get_node_status(analyzed_child_relative, archive_child_relative)

            # Check if this is a directory or regular file (reconstruct absolute path only when needed)
            is_dir = False
            is_regular_file = False
            if in_analyzed and analyzed_dir is not None:
                child_path = analyzed_dir / child_name
                is_dir = child_path.is_dir()
                is_regular_file = child_path.is_file()
            elif in_archive and archive_dir is not None:
                child_path = archive_dir / child_name
                is_dir = child_path.is_dir()
                is_regular_file = child_path.is_file()

            # Skip special files (symlinks, devices, etc.) - they don't have individual records
            # They are accounted for in the parent directory's comparison
            if not is_dir and not is_regular_file:
                continue

            # Skip identical files
            if status == NodeStatus.IDENTICAL and not is_dir:
                continue

            # Skip content-only matches when hiding them
            if self._hide_content_match and status == NodeStatus.CONTENT_MATCH and not is_dir:
                continue

            # Apply show_filter
            if self._show_filter == "analyzed" and status == NodeStatus.ARCHIVE_ONLY:
                continue
            elif self._show_filter == "archive" and status == NodeStatus.ANALYZED_ONLY:
                continue

            children_with_diffs.append((child_name, analyzed_child_relative, archive_child_relative, status, is_dir))

        if not children_with_diffs:
            return False

        # Check if we've reached max depth
        at_max_depth = self._max_depth is not None and level + 1 >= self._max_depth
        new_prefixes = parent_prefixes + [is_last] if level > 0 else []

        # Get this node's status (for conditional printing at level > 0)
        dir_status: NodeStatus | None = None
        if level > 0:
            dir_status = self._get_node_status(analyzed_relative, archive_relative)

        # Track if we actually printed this node
        node_printed = False

        # Create closure to ensure this node (and all ancestors) are printed
        def ensure_this_node_printed() -> None:
            nonlocal node_printed
            if node_printed:
                return

            # First, ensure ancestors are printed
            if ensure_ancestors_printed is not None:
                ensure_ancestors_printed()

            # Then print this node if it's not the root
            if level > 0 and dir_status is not None:
                self._print_tree_node(name, dir_status, level, is_last, parent_prefixes, is_dir=True)
                node_printed = True

        for idx, (child_name, analyzed_child_rel, archive_child_rel, status, is_dir) in enumerate(children_with_diffs):
            child_is_last = (idx == len(children_with_diffs) - 1)

            if is_dir:
                if at_max_depth:
                    # At max depth - show directory with "..."
                    # Ensure this node is printed before showing child
                    ensure_this_node_printed()

                    self._print_tree_node(
                        child_name, status, level + 1, child_is_last, new_prefixes + [child_is_last], is_dir=True)
                    # Show "..." to indicate elided content
                    elision_prefixes = new_prefixes + [child_is_last]
                    prefix_parts = []
                    for was_last in elision_prefixes[:-1]:
                        prefix_parts.append(SPACE if was_last else VERTICAL)
                    prefix_parts.append(SPACE if child_is_last else VERTICAL)
                    prefix = "".join(prefix_parts)
                    print(f"{prefix}...")
                else:
                    # Recursively handle directory
                    child_has_diffs = self._build_and_print_tree(
                        analyzed_child_rel,
                        archive_child_rel,
                        child_name,
                        level + 1,
                        child_is_last,
                        new_prefixes,
                        ensure_this_node_printed  # Pass callback to child
                    )
                    if child_has_diffs:
                        node_printed = True  # Child had diffs, so we must have been printed
            else:
                # Ensure this node is printed before showing file child
                ensure_this_node_printed()

                # Print file node
                self._print_tree_node(
                    child_name, status, level + 1, child_is_last, new_prefixes + [child_is_last], is_dir=False)

        return node_printed or (level == 0 and len(children_with_diffs) > 0)

    def run(
        self,
        analyzed_start: Path,
        archive_start: Path,
        record: DuplicateRecord
    ) -> bool:
        """Run the diff-tree operation.

        Args:
            analyzed_start: Starting path for comparison, relative to analyzed_base (including report target name)
            archive_start: Starting path for comparison, relative to archive_base
            record: Duplicate record for the analyzed path

        Returns:
            True if differences were found, False if directories are identical
        """
        # Check if archive_start matches any duplicate
        matching_duplicate = None
        for dup in record.duplicates:
            if dup.path == archive_start:
                matching_duplicate = dup
                break

        if matching_duplicate is None:
            print(f"Error: {self._archive_base / archive_start} is not a known duplicate of "
                  f"{self._analyzed_base / analyzed_start}")
            print(f"Known duplicates:")
            for dup in record.duplicates:
                print(f"  {self._archive_base / dup.path}")
            return False

        # Print header
        print(f"Comparing:")
        print(f"  Analyzed: {self._analyzed_base / analyzed_start}")
        print(f"  Archive:  {self._archive_base / archive_start}")
        print()

        # Extract the name from the starting paths to use as the root display name
        name = analyzed_start.parts[-1] if analyzed_start.parts else "."

        return self._build_and_print_tree(
            analyzed_start,  # Start from the specified analyzed path
            archive_start,   # Start from the specified archive path
            name,
            level=0,
            is_last=True,
            parent_prefixes=[]
        )


def do_diff_tree(
        analyzed_path: Path,
        archive_path: Path,
        hide_content_match: bool = False,
        max_depth: int | None = None,
        show_filter: str = "both"
) -> None:
    """Compare directory trees between an analyzed path and its duplicate in the archive.

    Args:
        analyzed_path: Path to the analyzed directory (must have a report)
        archive_path: Path to the duplicate directory in the archive
        hide_content_match: If True, hide files that match content but differ in metadata
        max_depth: Maximum depth to display (None = unlimited)
        show_filter: Filter for which files to show: "both", "analyzed", or "archive"
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
            print(f"Run 'arindexer analyze {analyzed_path_abs}' to generate a report.")
        return

    report_target, record_path = report_result

    # Find archive root containing .aridx for the archive_path
    # find_archive_for_path() handles path normalization and returns (archive_root, archive_record_path)
    # archive_record_path is relative to archive_root and can be concatenated:
    #   archive_root / archive_record_path == archive_path
    archive_result = find_archive_for_path(archive_path)
    if archive_result is None:
        # Provide better error messages for archive_path validation issues
        archive_path_abs = archive_path if archive_path.is_absolute() else Path.cwd() / archive_path
        if not archive_path_abs.exists():
            print(f"Error: Archive path does not exist: {archive_path_abs}")
        elif not archive_path_abs.is_dir():
            print(f"Error: Archive path must be a directory: {archive_path_abs}")
        else:
            print(f"Error: No archive found containing: {archive_path_abs}")
            print(f"Archive must be within a directory containing .aridx")
        return

    archive_root, archive_relative = archive_result
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

            # Validate that archive matches the one used in the report
            # Use archive_id rather than path comparison, as paths can differ but represent the same archive
            archive_settings = ArchiveSettings(archive_root)
            archive_store = ArchiveStore(archive_settings, archive_root)
            current_archive_id = archive_store.get_archive_id()
            if current_archive_id != manifest.archive_id:
                print(f"Error: Archive mismatch")
                print(f"Provided archive has ID: {current_archive_id}")
                print(f"Report expects archive ID: {manifest.archive_id}")
                return

            # Compute the bases for path resolution
            # analyzed_base is the parent of the report target
            analyzed_base = report_target.parent
            # archive_base is the archive root (where .aridx is a direct child)
            archive_base = archive_root

            # Create processor and run diff-tree operation
            processor = DiffTreeProcessor(
                report_store=store,
                analyzed_base=analyzed_base,
                archive_base=archive_base,
                hide_content_match=hide_content_match,
                max_depth=max_depth,
                show_filter=show_filter
            )

            # Compute the starting path in analyzed_base for the processor
            # record_path is relative to analyzed_base and includes the report target name
            analyzed_start = record_path

            has_diffs = processor.run(
                analyzed_start=analyzed_start,
                archive_start=archive_relative,
                record=record
            )

            if not has_diffs:
                print("Directories are identical.")

    except FileNotFoundError as e:
        print(f"Error reading report: {e}")
    except Exception as e:
        print(f"Error: {e}")
