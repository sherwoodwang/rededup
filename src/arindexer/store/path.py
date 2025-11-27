"""Path utilities for finding archive directories."""

import os
from pathlib import Path


def get_index_directory_path(archive_path: Path) -> Path:
    """Generate the .aridx directory path for a given archive path.

    Args:
        archive_path: The archive directory being indexed

    Returns:
        Path to the .aridx directory (e.g., /path/to/archive/.aridx)
    """
    # For /path/to/archive -> /path/to/archive/.aridx
    return archive_path / '.aridx'


def find_archive_for_path(target_path: Path) -> tuple[Path, Path] | None:
    """Find the ancestor archive directory containing a .aridx subdirectory.

    Searches upward from the target path to find an archive directory that contains
    a .aridx index directory.

    Args:
        target_path: The file or directory to find an archive for

    Returns:
        A 2-tuple of (archive_root, archive_record_path) if found, None otherwise.
        - archive_root: The archive directory (where .aridx is a direct child)
        - archive_record_path: Path relative to archive_root. Concatenating archive_root
          with archive_record_path gives the target path. For example, if archive_root
          is /archive and target_path is /archive/subdir/file.txt, returns
          (/archive, subdir/file.txt) such that /archive / subdir/file.txt points to
          /archive/subdir/file.txt.

    Examples:
        If target_path is /archive/subdir/file.txt and /archive/.aridx exists,
        returns (/archive, subdir/file.txt). If called on /archive itself and
        /archive/.aridx exists, returns (/archive, .).
    """
    # Normalize path to remove . and .. components without following symlinks
    # - Path() keeps .. components (e.g., Path("/a/b/../c") has .. in parts)
    # - Path.resolve() follows symlinks on filesystem (undesirable for finding archives)
    # - os.path.normpath() removes . and .. without following symlinks (what we want)
    target_path = target_path if target_path.is_absolute() else Path.cwd() / target_path
    target_path = Path(os.path.normpath(str(target_path)))

    # Start from the target path and traverse upward, collecting path components
    current = target_path
    path_components: list[str] = []

    while True:
        # Check if there's a .aridx directory in the current path
        index_dir = get_index_directory_path(current)
        if index_dir.exists() and index_dir.is_dir():
            # Found the archive directory
            # Build archive record path by joining collected components in reverse order
            if path_components:
                record_path = Path(*reversed(path_components))
            else:
                # target_path is the archive root itself
                record_path = Path('.')
            return (current, record_path)

        # Move to parent directory and collect the component we're moving past
        parent = current.parent
        if parent == current:
            # Reached root without finding an archive
            return None

        # Collect the current directory name as we move up
        path_components.append(current.name)
        current = parent
