import functools
import os
import stat
from pathlib import Path
from typing import Any, Generator, Iterator, NamedTuple, Callable


class FileContext:
    """Context object for a file or directory during traversal.

    IMPORTANT: The _path attribute is protected and should not be accessed directly
    outside of this class. To get the full path of a file, concatenate the repository/root
    path with the relative_path property instead:
        full_path = repository_path / context.relative_path

    The _path attribute is used internally for stat() operations when stat info
    is not pre-computed.
    """
    def __init__(self, parent, name: str | None, path: Path | None = None, st: os.stat_result | None = None):
        self._parent: FileContext | None = parent
        self._name: str | None = name
        self._stat: os.stat_result | None = st
        self._path: Path | None = path
        self._associated: dict[Any, Any] = {}

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def parent(self) -> 'FileContext':
        if self._parent is None:
            raise LookupError("no parent")

        return self._parent

    @property
    def stat(self) -> os.stat_result:
        if self._stat is None:
            if self._path is None:
                raise LookupError("stat not available and path not provided")
            self._stat = self._path.stat(follow_symlinks=False)
        return self._stat

    @functools.cached_property
    def relative_path(self) -> Path | None:
        """Get the relative path from the root context.

        This method recursively builds the path by reusing parent results,
        and caches the result for performance.
        """
        if self._name is None:
            # Root context with no name
            return None

        if self._parent is None:
            # This is a top-level entry
            return Path(self._name)

        # Recursively get parent's path and append this name
        parent_path = self._parent.relative_path
        if parent_path is None:
            return Path(self._name)

        return parent_path / self._name

    def complete(self):
        # Call complete() on any associated objects that have a complete method
        for obj in self._associated.values():
            if hasattr(obj, 'complete') and callable(obj.complete):
                obj.complete()

    def is_file(self):
        return stat.S_ISREG(self.stat.st_mode)

    # Dictionary-like interface for associated objects
    def __getitem__(self, key: Any) -> Any:
        return self._associated[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._associated[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._associated[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._associated

    def get(self, key: Any, default: Any = None) -> Any:
        return self._associated.get(key, default)


def walk(path: Path, parent: FileContext) -> Generator[tuple[Path, FileContext], None | bool | FileContext, None]:
    """Recursively traverse directory, respecting exclusion patterns."""
    child: Path
    for child in path.iterdir():
        context = FileContext(parent, child.name, path=child)
        substitute_context = yield child, context

        if substitute_context is False:
            continue
        elif substitute_context is not True and substitute_context is not None:
            context = substitute_context

        if stat.S_ISDIR(context.stat.st_mode):
            yield from walk(child, context)
            context.complete()


class WalkPolicy(NamedTuple):
    """Policy controlling filesystem traversal behavior.

    Attributes:
        excluded_paths: Set of relative paths to exclude from traversal
        should_follow_symlink: Function that takes (absolute_path, file_context) and returns
                              a substitute FileContext if symlink should be followed, or None
        yield_root: Whether to yield the root directory itself before walking its children
    """
    excluded_paths: set[Path]
    should_follow_symlink: Callable[[Path, FileContext], FileContext | None]
    yield_root: bool = False


def walk_with_policy(path: Path, policy: WalkPolicy) -> Iterator[tuple[Path, FileContext]]:
    """Walk filesystem tree using provided policy for exclusions and symlink handling.

    This function coordinates the low-level walk() generator with policy-based decisions
    about which paths to exclude and how to handle symlinks. It uses the generator .send()
    protocol to communicate decisions back to the walker.

    Args:
        path: Root directory to walk
        policy: WalkPolicy instance controlling walk behavior

    Yields:
        Tuples of (absolute_path, file_context) for each file/directory encountered

    Example:
        policy = WalkPolicy(
            excluded_paths={Path('.rededup')},
            should_follow_symlink=my_symlink_handler,
            yield_root=True
        )
        for file_path, context in walk_with_policy(repository_path, policy):
            process_file(file_path, context)
    """
    if policy.yield_root:
        # When yielding root, create proper hierarchy with pseudo_parent
        pseudo_parent = FileContext(None, None, path)
        context = FileContext(pseudo_parent, path.name, path)
        yield path, context
        gen = walk(path, context)
    else:
        # When not yielding root, use context with no name for proper relative paths
        context = FileContext(None, None, path)
        pseudo_parent = None
        gen = walk(path, context)
    pending = None

    try:
        while True:
            file_path, file_context = gen.send(pending)
            pending = None

            # Check if this path should be excluded
            if file_context.relative_path in policy.excluded_paths:
                pending = False
                continue

            # Check if this is a symlink that should be followed
            substitute = policy.should_follow_symlink(file_path, file_context)
            if substitute is not None:
                pending = file_context = substitute

            yield file_path, file_context
    except StopIteration:
        pass
    finally:
        context.complete()
        if pseudo_parent is not None:
            pseudo_parent.complete()


def resolve_symlink_target(
    file_path: Path,
    boundary_paths: set[Path]
) -> Path | None:
    """Follow symlink chain and return final target path if outside boundaries.

    Follows symlinks one jump at a time, checking each target in the chain. If any target
    in the chain is under any of the boundary_paths, returns None to indicate the symlink
    should not be followed (to avoid duplicate indexing or crossing boundaries).

    Args:
        file_path: Path to the symlink (will be converted to absolute)
        boundary_paths: Set of boundary paths to check against (e.g., {repository_root})

    Returns:
        Path to the final symlink target if it should be followed (outside all boundaries),
        None if symlink should not be followed (target is within a boundary, broken link, or loop)
    """
    current_path = file_path.absolute()
    visited = set()

    while current_path.is_symlink():
        # Detect symlink loops by tracking the symlink paths themselves
        try:
            # Use absolute path to normalize, but don't resolve symlinks yet
            normalized = current_path.absolute()
            if normalized in visited:
                # Symlink loop detected, don't follow
                return None
            visited.add(normalized)
        except (OSError, RuntimeError):
            # Can't process path, don't follow
            return None

        try:
            target = current_path.readlink()
        except OSError:
            # Broken symlink, don't follow
            return None

        # Convert relative symlinks to absolute paths (without resolving)
        if not target.is_absolute():
            target = (current_path.parent / target).absolute()

        # Check if this target (after one jump) is under any boundary path
        try:
            target_absolute = target.absolute()

            # Check if target is under any boundary by path prefix comparison
            for boundary_path in boundary_paths:
                boundary_absolute = boundary_path.absolute()
                try:
                    # This will succeed if target is under boundary_absolute
                    target_absolute.relative_to(boundary_absolute)
                    # Target is under a boundary, stop following
                    return None
                except ValueError:
                    # Target is not under this boundary, continue checking others
                    pass

            # Target is outside all boundaries, continue following this chain
            current_path = target
        except (OSError, RuntimeError):
            # Can't process paths, don't follow
            return None

    # Successfully followed entire chain without hitting any boundary, return final target
    try:
        # Verify the final target exists and is accessible
        current_path.stat()
        return current_path
    except OSError:
        # Final target doesn't exist or is inaccessible
        return None

