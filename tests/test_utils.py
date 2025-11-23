"""Shared test utilities for archive-indexer tests."""
import os
from pathlib import Path


async def compute_xor(path: Path):
    """Compute XOR-based hash for testing hash collisions."""
    data = path.read_bytes()
    value = 0
    while data:
        if len(data) > 4:
            seg = data[:4]
            data = data[4:]
        else:
            seg = (data + b'\0\0\0\0')[:4]
            data = b''

        value = value ^ int.from_bytes(seg)

    value = int.to_bytes(value, length=4)

    return value


def copy_times(src: Path, dest: Path):
    """Copy timestamps from src to dest."""
    st = src.lstat()
    os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns), follow_symlinks=False)


def tweak_times(path: Path, shift: int):
    """Adjust timestamps by the given shift amount."""
    st = path.lstat()
    os.utime(path, ns=(st.st_atime_ns + shift, st.st_mtime_ns + shift), follow_symlinks=False)
