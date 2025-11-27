"""Report path utilities for finding and generating report directory paths."""

import os
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


def find_report_for_path(target_path: Path) -> tuple[Path, Path] | None:
    """Find the analyzed path for a given file or directory.

    Searches upward from the target path to find a .report directory that contains
    analysis data for the target.

    Args:
        target_path: The file or directory to find a report for

    Returns:
        A 2-tuple of (analyzed_path, record_path) if found, None otherwise.
        - analyzed_path: Path that was analyzed (e.g., /path/to/dir for /path/to/dir.report)
        - record_path: Path as it appears in the report database, starting with the analyzed
          directory name. For example, if analyzed_path is /root/dir and target_path is
          /root/dir/subdir/file.txt, returns (/root/dir, dir/subdir/file.txt)
    """
    # Normalize path to remove . and .. components without following symlinks
    # - Path() keeps .. components (e.g., Path("/a/b/../c") has .. in parts)
    # - Path.resolve() follows symlinks on filesystem (undesirable for finding reports)
    # - os.path.normpath() removes . and .. without following symlinks (what we want)
    target_path = target_path if target_path.is_absolute() else Path.cwd() / target_path
    target_path = Path(os.path.normpath(str(target_path)))

    # Start from the target path and traverse upward, collecting path components
    current = target_path
    path_components: list[str] = []

    while True:
        # Check if there's a .report directory for the current path
        report_dir = get_report_directory_path(current)
        if report_dir.exists() and report_dir.is_dir():
            # Found the analyzed path
            # Build record path by starting with the analyzed directory name and adding collected components
            record_path_parts = [current.name] + list(reversed(path_components))
            record_path = Path(*record_path_parts)
            return (current, record_path)

        # Move to parent directory and collect the component we're moving past
        parent = current.parent
        if parent == current:
            # Reached root without finding a report
            return None

        # Collect the current directory name as we move up
        path_components.append(current.name)
        current = parent
