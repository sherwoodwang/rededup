"""Report path utilities for finding and generating report directory paths."""

from pathlib import Path


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
