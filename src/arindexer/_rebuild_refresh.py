from asyncio import TaskGroup
from pathlib import Path

from ._keyed_lock import KeyedLock
from ._file_context import FileContext
from ._throttler import Throttler
from ._archive_store import ArchiveStore, FileSignature
from ._processor import Processor


async def do_rebuild(
        store: ArchiveStore,
        processor: Processor,
        archive_path: Path,
        hash_algorithms: dict,
        default_hash_algorithm: str,
        config_hash_algorithm_key: str):
    """Async implementation of rebuild operation."""
    store.truncate()
    await do_refresh(
        store,
        processor,
        archive_path,
        hash_algorithms,
        config_hash_algorithm_key,
        hash_algorithm=default_hash_algorithm
    )
    store.write_config(config_hash_algorithm_key, default_hash_algorithm)


async def do_refresh(
        store: ArchiveStore,
        processor: Processor,
        archive_path: Path,
        hash_algorithms: dict,
        config_hash_algorithm_key: str,
        hash_algorithm: str | None = None):
    """Async implementation of refresh operation with optional hash algorithm override."""
    async with TaskGroup() as tg:
        throttler = Throttler(tg, processor.concurrency * 2)
        keyed_lock = KeyedLock()

        if hash_algorithm is None:
            hash_algorithm = store.read_config(config_hash_algorithm_key)

            if hash_algorithm is None:
                raise RuntimeError("The index hasn't been build")

            if hash_algorithm not in hash_algorithms:
                raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        _, calculate_digest = hash_algorithms[hash_algorithm]

        async def handle_file(path: Path, context: FileContext):
            if store.lookup_file(context.relative_path()) is None:
                return await generate_signature(path, context.relative_path(), context.stat.st_mtime_ns)
            return None

        async def refresh_entry(relative_path: Path, signature: FileSignature):
            path = (archive_path / relative_path)

            async def clean_up():
                store.register_file(relative_path, FileSignature(signature.digest, signature.mtime_ns, None))

                async with keyed_lock.lock(signature.digest):
                    for ec_id, paths in store.list_content_equivalent_classes(signature.digest):
                        if relative_path in paths:
                            paths.remove(relative_path)
                            break
                    else:
                        ec_id = None

                    if ec_id is not None:
                        store.store_content_equivalent_class(signature.digest, ec_id, paths)

                store.deregister_file(relative_path)

            try:
                stat = path.stat()
            except FileNotFoundError:
                await clean_up()
            else:
                if signature.mtime_ns is None or signature.mtime_ns < stat.st_mtime_ns:
                    await clean_up()
                    return await generate_signature(path, relative_path, stat.st_mtime_ns)

        async def generate_signature(path: Path, relative_path: Path, mtime: int):
            digest = await calculate_digest(path)

            async with keyed_lock.lock(digest):
                next_ec_id = 0
                for ec_id, paths in store.list_content_equivalent_classes(digest):
                    if next_ec_id <= ec_id:
                        next_ec_id = ec_id + 1

                    if await processor.compare_content(path, archive_path / paths[0]):
                        paths.append(relative_path)
                        break
                else:
                    ec_id = next_ec_id
                    paths = [relative_path]

                store.register_file(relative_path, FileSignature(digest, mtime, None))
                store.store_content_equivalent_class(digest, ec_id, paths)
                store.register_file(relative_path, FileSignature(digest, mtime, ec_id))

        for path, signature in store.list_registered_files():
            await throttler.schedule(refresh_entry(path, signature))

        for path, context in store.walk_archive():
            if context.is_file():
                await throttler.schedule(handle_file(path, context))