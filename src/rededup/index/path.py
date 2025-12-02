"""Path utilities for finding repository directories."""

import os
from pathlib import Path


def get_index_directory_path(repository_path: Path) -> Path:
    """Generate the .rededup directory path for a given repository path.

    Args:
        repository_path: The repository directory being indexed

    Returns:
        Path to the .rededup directory (e.g., /path/to/repository/.rededup)
    """
    # For /path/to/repository -> /path/to/repository/.rededup
    return repository_path / '.rededup'


def find_repository_for_path(target_path: Path) -> tuple[Path, Path] | None:
    """Find the ancestor repository directory containing a .rededup subdirectory.

    Searches upward from the target path to find a repository directory that contains
    a .rededup index directory.

    Args:
        target_path: The file or directory to find a repository for

    Returns:
        A 2-tuple of (repository_root, repository_record_path) if found, None otherwise.
        - repository_root: The repository directory (where .rededup is a direct child)
        - repository_record_path: Path relative to repository_root. Concatenating repository_root
          with repository_record_path gives the target path. For example, if repository_root
          is /repository and target_path is /repository/subdir/file.txt, returns
          (/repository, subdir/file.txt) such that /repository / subdir/file.txt points to
          /repository/subdir/file.txt.

    Examples:
        If target_path is /repository/subdir/file.txt and /repository/.rededup exists,
        returns (/repository, subdir/file.txt). If called on /repository itself and
        /repository/.rededup exists, returns (/repository, .).
    """
    # Normalize path to remove . and .. components without following symlinks
    # - Path() keeps .. components (e.g., Path("/a/b/../c") has .. in parts)
    # - Path.resolve() follows symlinks on filesystem (undesirable for finding repositories)
    # - os.path.normpath() removes . and .. without following symlinks (what we want)
    target_path = target_path if target_path.is_absolute() else Path.cwd() / target_path
    target_path = Path(os.path.normpath(str(target_path)))

    # Start from the target path and traverse upward, collecting path components
    current = target_path
    path_components: list[str] = []

    while True:
        # Check if there's a .rededup directory in the current path
        index_dir = get_index_directory_path(current)
        if index_dir.exists() and index_dir.is_dir():
            # Found the repository directory
            # Build repository record path by joining collected components in reverse order
            if path_components:
                record_path = Path(*reversed(path_components))
            else:
                # target_path is the repository root itself
                record_path = Path('.')
            return (current, record_path)

        # Move to parent directory and collect the component we're moving past
        parent = current.parent
        if parent == current:
            # Reached root without finding a repository
            return None

        # Collect the current directory name as we move up
        path_components.append(current.name)
        current = parent
