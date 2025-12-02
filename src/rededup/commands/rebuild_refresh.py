from asyncio import TaskGroup
from pathlib import Path
from typing import NamedTuple, Callable, Awaitable

from ..utils.async_keyed_lock import AsyncKeyedLock
from ..utils.walker import FileContext
from ..utils.throttler import Throttler
from ..store.archive_store import ArchiveStore, FileSignature
from ..utils.processor import Processor


class RebuildRefreshArgs(NamedTuple):
    """Arguments for rebuild and refresh operations."""
    processor: Processor  # File processing backend for hashing and comparison
    # Hash algorithm configuration (digest_size, calculator)
    hash_algorithm: tuple[int, Callable[[Path], Awaitable[bytes]]]


async def do_rebuild(store: ArchiveStore, args: RebuildRefreshArgs, hash_algorithm_name: str):
    """Async implementation of rebuild operation."""
    store.truncate()
    await do_refresh(store, args)
    store.write_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM, hash_algorithm_name)


class RefreshProcessor:
    """Processor for refresh operations that encapsulates state and logic."""

    def __init__(self, store: ArchiveStore, args: RebuildRefreshArgs):
        self._store = store
        self._processor = args.processor
        self._archive_path = store.archive_path
        _, self._calculate_digest = args.hash_algorithm
        self._keyed_lock = AsyncKeyedLock()

    async def run(self):
        """Execute the refresh operation."""
        async with TaskGroup() as tg:
            throttler = Throttler(tg, self._processor.concurrency * 2)

            for file_path, signature in self._store.list_registered_files():
                await throttler.schedule(self._refresh_entry(file_path, signature))

            for file_path, context in self._store.walk_archive():
                if context.is_file():
                    await throttler.schedule(self._handle_file(file_path, context))

    async def _handle_file(self, file_path: Path, file_context: FileContext):
        """Handle a file found during archive walk."""
        relative_path = file_context.relative_path
        assert relative_path is not None, "File context must have a relative path"
        # Generate signature only for new files. Existing files are handled in _refresh_entry().
        if self._store.lookup_file(relative_path) is None:
            return await self._generate_signature(
                file_path, relative_path, file_context.stat.st_mtime_ns)
        return None

    async def _refresh_entry(self, relative_path: Path, entry_signature: FileSignature):
        """Refresh an existing index entry."""
        file_path = (self._archive_path / relative_path)

        try:
            stat = file_path.stat()
        except FileNotFoundError:
            await self._clean_up(relative_path, entry_signature)
        else:
            if entry_signature.mtime_ns is None or entry_signature.mtime_ns < stat.st_mtime_ns or \
                    entry_signature.ec_id is None:
                await self._clean_up(relative_path, entry_signature)
                return await self._generate_signature(file_path, relative_path, stat.st_mtime_ns)

    async def _clean_up(self, relative_path: Path, entry_signature: FileSignature):
        """Clean up an entry that needs to be refreshed or removed."""
        self._store.register_file(relative_path, FileSignature(relative_path, entry_signature.digest, entry_signature.mtime_ns, None))

        # Acquire async-level lock for this digest to coordinate concurrent async tasks.
        # While ArchiveStore methods are thread-safe, this lock prevents async task-level
        # races (e.g., EC removal happening concurrently with EC assignment in _generate_signature).
        async with self._keyed_lock.lock(entry_signature.digest):
            if entry_signature.ec_id is not None:
                self._store.remove_paths_from_equivalent_class(entry_signature.digest, entry_signature.ec_id, [relative_path])

        self._store.deregister_file(relative_path)

    async def _generate_signature(self, file_path: Path, relative_path: Path, mtime: int):
        """Generate signature for a file and assign equivalence class."""
        digest = await self._calculate_digest(file_path)

        # Acquire async-level lock for this digest to ensure the entire EC assignment logic
        # runs atomically across concurrent async tasks. Without this, two tasks processing
        # the same digest could interleave: both read the same EC list, both determine the
        # next EC ID is 0, both compare content, and both try to add to the same EC ID,
        # leading to incorrect EC assignments.
        async with self._keyed_lock.lock(digest):
            next_ec_id = 0
            for ec_id, paths in self._store.list_content_equivalent_classes(digest):
                if next_ec_id <= ec_id:
                    next_ec_id = ec_id + 1

                if await self._processor.compare_content(file_path, self._archive_path / paths[0]):
                    break
            else:
                ec_id = next_ec_id

            self._store.register_file(relative_path, FileSignature(relative_path, digest, mtime, None))
            self._store.add_paths_to_equivalent_class(digest, ec_id, [relative_path])
            self._store.register_file(relative_path, FileSignature(relative_path, digest, mtime, ec_id))


async def do_refresh(store: ArchiveStore, args: RebuildRefreshArgs):
    """Async implementation of refresh operation with optional hash algorithm override."""
    # Ensure archive ID exists (generate if needed, before refresh)
    store.ensure_archive_id()
    processor = RefreshProcessor(store, args)
    await processor.run()
