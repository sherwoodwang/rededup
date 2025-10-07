import os
import stat
from pathlib import Path
from typing import Any, Iterator


class FileContext:
    def __init__(self, parent, name: str | None, st: os.stat_result):
        self._parent: FileContext | None = parent
        self.name: str = name
        self.stat: os.stat_result = st
        self.exclusion: set[str] = set()
        self._associated: dict[Any, Any] = {}

    @property
    def parent(self) -> 'FileContext':
        if self._parent is None:
            raise LookupError("no parent")

        return self._parent

    def exclude(self, filename):
        self.exclusion.add(filename)

    def is_excluded(self, filename):
        return filename in self.exclusion

    def relative_path(self) -> Path:
        path = None

        context = self
        while context is not None:
            if context.name is not None:
                if path is None:
                    path = Path(context.name)
                else:
                    path = Path(context.name) / path

            context = context._parent

        return path

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


def walk_recursively(path: Path, parent: FileContext) -> Iterator[tuple[Path, FileContext]]:
    """Recursively traverse directory, respecting exclusion patterns."""
    child: Path
    for child in path.iterdir():
        if parent.is_excluded(child.name):
            continue

        st = child.stat(follow_symlinks=False)
        context = FileContext(parent, child.name, st)
        if stat.S_ISDIR(st.st_mode):
            yield child, context
            yield from walk_recursively(child, context)
            context.complete()
        else:
            yield child, context