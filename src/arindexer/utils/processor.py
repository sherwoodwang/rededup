import asyncio
import datetime
import filecmp
import hashlib
import multiprocessing
import pathlib
import stat
from enum import StrEnum
from typing import Awaitable


def compute_sha256_for_path(path: pathlib.Path):
    with open(path, "rb") as f:
        # noinspection PyTypeChecker
        return hashlib.file_digest(f, hashlib.sha256).digest()


def compare_file_content(a: pathlib.Path, b: pathlib.Path):
    return filecmp.cmp(a, b, shallow=False)


def compare_file_metadata(a: pathlib.Path, b: pathlib.Path):
    sta = a.stat(follow_symlinks=False)
    stb = b.stat(follow_symlinks=False)

    if not stat.S_ISREG(sta.st_mode):
        raise ValueError(f"{a} is not a regular file")

    if not stat.S_ISREG(stb.st_mode):
        raise ValueError(f"{b} is not a regular file")

    diffs = []

    if sta.st_atime != stb.st_atime or sta.st_atime_ns != stb.st_atime_ns:
        diffs.append(('atime', sta.st_atime_ns, stb.st_atime_ns))

    if sta.st_ctime != stb.st_ctime or sta.st_ctime_ns != stb.st_ctime_ns:
        diffs.append(('ctime', sta.st_ctime_ns, stb.st_ctime_ns))

    if sta.st_mtime != stb.st_mtime or sta.st_mtime_ns != stb.st_mtime_ns:
        diffs.append(('mtime', sta.st_mtime_ns, stb.st_mtime_ns))

    if hasattr(sta, "st_birthtime") and hasattr(stb, "st_birthtime"):
        if sta.st_birthtime != stb.st_birthtime:
            diffs.append(('birthtime', sta.st_birthtime, stb.st_birthtime))

    return diffs


class FileMetadataDifferenceType(StrEnum):
    ATIME = 'atime'
    CTIME = 'ctime'
    MTIME = 'mtime'
    BIRTHTIME = 'birthtime'


class FileMetadataDifference:
    def __init__(self, type: str, a, b):
        self.type = FileMetadataDifferenceType(type)
        self.a = a
        self.b = b

    def description(self, tag_a: str | None = None, tag_b: str | None = None, *, tz=None):
        label_a = "" if tag_a is None else f" ({tag_a})"
        label_b = "" if tag_a is None else f" ({tag_b})"
        if self.type in ["atime", "ctime", "mtime", "birthtime"]:
            if tz is None:
                tz = datetime.UTC
            ts_a = datetime.datetime.fromtimestamp(self.a // 1000000000, tz=tz)\
                .strftime("%Y-%m-%dT%H:%M:%S.{:09}Z").format(self.a % 1000000000)
            ts_b = datetime.datetime.fromtimestamp(self.b // 1000000000, tz=tz)\
                .strftime("%Y-%m-%dT%H:%M:%S.{:09}Z").format(self.b % 1000000000)
            return f"{self.type}: {ts_a}{label_a} != {ts_b}{label_b}"
        else:
            return f"{self.type}: {self.a}{label_a} != {self.b}{label_b}"


class Processor:
    def __init__(self, concurrency: int | None = None):
        if concurrency is None:
            concurrency = multiprocessing.cpu_count()

        self._concurrency = concurrency
        self._pool: multiprocessing.Pool = multiprocessing.Pool(self._concurrency)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._pool.close()

    @property
    def concurrency(self):
        return self._concurrency

    def sha256(self, path: pathlib.Path) -> Awaitable[bytes]:
        return self._evaluate(compute_sha256_for_path, path)

    def compare_content(self, a: pathlib.Path, b: pathlib.Path) -> Awaitable[bool]:
        """Compare content of two files.

        :return: True if two files are equal, False otherwise."""
        return self._evaluate(compare_file_content, a, b)

    def compare_metadata(self, a: pathlib.Path, b: pathlib.Path) -> Awaitable[list[FileMetadataDifference]]:
        async def evaluate_and_convert():
            return [FileMetadataDifference(*t) for t in await self._evaluate(compare_file_metadata, a, b)]

        return evaluate_and_convert()

    def _evaluate(self, func, *args):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self._pool.apply_async(func, args=args,
                               callback=lambda v: loop.call_soon_threadsafe(future.set_result, v),
                               error_callback=lambda e: loop.call_soon_threadsafe(future.set_exception, e))

        return future
