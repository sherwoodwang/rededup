"""Profiling support for rededup using cProfile.

When the REDEDUP_PROFILE environment variable is set to a directory path,
profiling data will be collected and saved to that directory with unique
filenames containing timestamp, controlling process PID, and actual PID.
"""
import cProfile
import functools
import itertools
import os
import time
from pathlib import Path
from typing import Callable, TypeVar, ParamSpec

P = ParamSpec('P')
T = TypeVar('T')

# Global counter for generating unique sequence numbers within the same process
_profile_counter = itertools.count()


def get_profile_dir() -> Path | None:
    """Get the profile directory from environment variable.

    Returns:
        Path to profile directory if REDEDUP_PROFILE is set, None otherwise.
        The path will include a subdirectory for the main process with format:
        {timestamp}_{pid} (e.g., "1730332456789_54321")
    """
    profile_path = os.environ.get('REDEDUP_PROFILE')
    if profile_path:
        # Get the session directory name (timestamp_pid)
        session_dir = _get_session_dir_name()
        return Path(profile_path) / session_dir
    return None


def _get_session_dir_name() -> str:
    """Get the session directory name for organizing profile data.

    The directory name format is: {timestamp_ms}_{main_pid}
    This ensures all profiling data from a single run is stored in the same
    subdirectory and makes it easy to identify when the session started.

    Returns:
        Directory name string like "1730332456789_54321"
    """
    # Use environment variable to track the session directory across processes
    session_dir = os.environ.get('_REDEDUP_PROFILE_SESSION_DIR')
    if session_dir:
        return session_dir
    # If not set, we are the main process - create new session directory name
    timestamp_ms = int(time.time() * 1000)
    main_pid = os.getpid()
    return f"{timestamp_ms}_{main_pid}"


def generate_profile_filename(prefix: str = "profile") -> str:
    """Generate a unique profile filename.

    The filename includes:
    - prefix (e.g., "main", "worker")
    - actual process PID (current process)
    - sequence number (to ensure uniqueness within same process)

    The timestamp and main PID are already in the directory name, so they're
    not needed in the filename.

    Args:
        prefix: Prefix for the filename (default: "profile")

    Returns:
        Filename string like "worker_54398_0.prof"
    """
    current_pid = os.getpid()
    seq = next(_profile_counter)

    return f"{prefix}_{current_pid}_{seq}.prof"


def profile_function(func: Callable[P, T], prefix: str = "profile") -> Callable[P, T]:
    """Decorator/wrapper to profile a function if REDEDUP_PROFILE is set.

    Args:
        func: Function to profile
        prefix: Prefix for the profile filename

    Returns:
        Wrapped function that profiles if environment variable is set
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        profile_dir = get_profile_dir()

        if profile_dir is None:
            # Profiling not enabled, just run the function
            return func(*args, **kwargs)

        # Ensure profile directory exists
        profile_dir.mkdir(parents=True, exist_ok=True)

        # Generate unique filename
        profile_file = profile_dir / generate_profile_filename(prefix)

        # Profile the function
        profiler = cProfile.Profile()
        try:
            profiler.enable()
            result = func(*args, **kwargs)
            return result
        finally:
            profiler.disable()
            profiler.dump_stats(str(profile_file))

    return wrapper


def profile_main(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator for the main entry point function.

    This is a convenience wrapper around profile_function with prefix="main".
    It also sets an environment variable so worker processes can store their
    profiling data in the same subdirectory.

    Args:
        func: Main function to profile

    Returns:
        Wrapped function that profiles with "main" prefix
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        # Set the session directory environment variable so workers know where to store profiles
        if os.environ.get('REDEDUP_PROFILE'):
            # Generate the session directory name (timestamp_pid) once for the whole session
            session_dir = _get_session_dir_name()
            os.environ['_REDEDUP_PROFILE_SESSION_DIR'] = session_dir

        # Use the standard profile_function wrapper
        profiled_func = profile_function(func, prefix="main")
        return profiled_func(*args, **kwargs)

    return wrapper


def profile_worker(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator for worker process functions.

    This is a convenience wrapper around profile_function with prefix="worker".

    Args:
        func: Worker function to profile

    Returns:
        Wrapped function that profiles with "worker" prefix
    """
    return profile_function(func, prefix="worker")