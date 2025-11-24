"""Describe subcommand for displaying duplicate analysis results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple

from ..report.duplicate_match import DuplicateMatch
from ..report.path import find_report_for_path, get_report_directory_path
from ..report.store import DuplicateRecord, ReportManifest, ReportStore


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
                type_str=type_str, total_size_str="-", dup_size_str="0" if self.options.use_bytes else "0 B",
                dups_str="0", max_match_dup_size_str="0" if self.options.use_bytes else "0 B", best_status_str="-",
                total_items_str="-", dup_items_str="-", size_ratio_str="-", items_ratio_str="-",
                max_ratio_str="-", in_report_str="No")

        if not record.duplicates:
            total_size_str = str(record.total_size) if self.options.use_bytes else format_size(record.total_size)
            return SortableRowData(
                name=name, is_dir=is_dir,
                total_size=record.total_size, dup_size=0, dups=0, match_dup_size=0, status_rank=0,
                total_items=record.total_items, dup_items=0, size_ratio=0.0, items_ratio=0.0, max_ratio=0.0,
                in_report=True, type_str=type_str, total_size_str=total_size_str,
                dup_size_str="0" if self.options.use_bytes else "0 B", dups_str="0",
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
