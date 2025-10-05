import abc
import asyncio
import contextvars
import os
import stat
import threading
import urllib.parse
from asyncio import TaskGroup, Semaphore, Future
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Iterator, Iterable, Callable, Awaitable, TypeAlias

import msgpack
import plyvel

from ._processor import Processor, FileMetadataDifference, FileMetadataDifferenceType
from ._keyed_lock import KeyedLock


class Throttler:
    def __init__(self, task_group: TaskGroup, concurrency: int):
        self._task_group = task_group
        self._semaphore = Semaphore(concurrency)

    async def schedule(self, coro, name=None, context=None) -> asyncio.Task:
        await self._semaphore.acquire()

        async def wrapper():
            tenure = Throttler.__Tenure(lambda: self._semaphore.release())
            token = Throttler.__tenure.set(tenure)
            try:
                return await coro
            finally:
                Throttler.__tenure.reset(token)
                tenure.release()

        try:
            return self._task_group.create_task(wrapper(), name=name, context=context)
        except:
            self._semaphore.release()
            raise

    @staticmethod
    def terminate_current_tenure():
        Throttler.__tenure.get().terminate()

    __tenure = contextvars.ContextVar('Throttler.__tenure')

    class __Tenure:
        def __init__(self, release):
            self.lock = threading.Lock()
            self.released = False
            self.release = lambda: release()

        def terminate(self):
            with self.lock:
                self.release()
                self.release = lambda: None
                self.released = True


class FileMetadataDifferencePattern:
    ALL: 'FileMetadataDifferencePattern'
    TRIVIAL: 'FileMetadataDifferencePattern'

    def __init__(self):
        self._value: set[FileMetadataDifferenceType] = set()

    def add_trivial_attributes(self):
        self.add(FileMetadataDifferenceType.ATIME)
        self.add(FileMetadataDifferenceType.CTIME)

    def add_all(self):
        for kind in FileMetadataDifferenceType:
            kind: FileMetadataDifferenceType
            self.add(kind)

    def add(self, kind: FileMetadataDifferenceType):
        self._value.add(kind)

    def match(self, diff_desc: FileMetadataDifference) -> bool:
        return diff_desc.type in self._value


FileMetadataDifferencePattern.ALL = FileMetadataDifferencePattern()
FileMetadataDifferencePattern.ALL.add_all()

FileMetadataDifferencePattern.TRIVIAL = FileMetadataDifferencePattern()
FileMetadataDifferencePattern.TRIVIAL.add_trivial_attributes()


class Message(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def deliver(self, value) -> Future[Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def deliver_nowait(self, value) -> None:
        raise NotImplementedError()


class ConcreteMessage(Message):
    def __init__(self, notify_delivery: Callable[[], None]):
        self._notify_delivery = notify_delivery
        self._delivered = False
        self.value = None
        self._reply = None

    def deliver(self, value) -> Future[Any]:
        if self._delivered:
            raise RuntimeError("already delivered")

        loop = asyncio.get_running_loop()
        future_reply = loop.create_future()
        self._reply = lambda r: loop.call_soon_threadsafe(future_reply.set_result, r)

        self.value = value

        self._notify_delivery()
        self._delivered = True

        return future_reply

    def deliver_nowait(self, value) -> None:
        self._reply = lambda r: None
        self.value = value
        self._notify_delivery()
        self._delivered = True

    def reply(self, reply):
        self._reply(reply)


class DiscardedMessage(Message):
    def __init__(self, reply: Any):
        self._reply = reply

    def deliver(self, value) -> Future[Any]:
        future = asyncio.get_event_loop().create_future()
        future.set_result(self._reply)
        return future

    def deliver_nowait(self, value) -> None:
        pass


TaskGroupLike: TypeAlias = TaskGroup | asyncio.AbstractEventLoop
MessageProcessor: TypeAlias = Callable[[Callable[[Any], None], list[Any]], Awaitable[None]]


class MessageGatherer:
    def __init__(self, task_group: TaskGroupLike, processor: MessageProcessor):
        self._task_group = task_group
        self._processor = processor
        self._messages: list[ConcreteMessage] = []
        self._delivered = 0
        self._completed = False

    def send(self) -> Message:
        message = ConcreteMessage(self._notify_delivery)
        self._messages.append(message)
        return message

    def _notify_delivery(self):
        self._delivered += 1
        self._trigger_if_possible()

    def complete(self):
        self._completed = True
        self._trigger_if_possible()

    def _trigger_if_possible(self):
        if not (self._completed and self._delivered == len(self._messages)):
            return

        replied = False
        lock = threading.Lock()

        def reply(value):
            nonlocal replied

            with lock:
                if replied:
                    raise RuntimeError("already replied")
                replied = True

            for message in self._messages:
                message.reply(value)

        async def trigger():
            await self._processor(reply, [msg.value for msg in self._messages])

            if not replied:
                reply(None)

        self._task_group.create_task(trigger())


class FileContext:
    def __init__(self, parent, name: str | None, st: os.stat_result):
        self._parent: FileContext | None = parent
        self.name: str = name
        self.stat: os.stat_result = st
        self.exclusion: set[str] = set()

        self._completed = False
        self._message_gatherer: dict[Any, MessageGatherer] = {}

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
        for message_gatherer in self._message_gatherer.values():
            message_gatherer.complete()

        self._completed = True

    def is_file(self):
        return stat.S_ISREG(self.stat.st_mode)

    def send_message(self, key: Any, fallback: Callable[[], Message] | None = None) -> Message:
        """
        Send a message to the parent file context.

        This method only creates a stub at the parent file context. The actual content is delivered later with
        Message.deliver().

        :param key: the key of the message processor.
        :param fallback: the function to be called when the message processor hasn't been registered; None produces
        KeyError in that case.
        :return: the message object.
        """

        try:
            message_gatherer = self._message_gatherer[key]
        except KeyError:
            if fallback is None:
                raise
            else:
                return fallback()

        return message_gatherer.send()

    def register_message_processor(
            self, task_group: TaskGroupLike, key: Any, processor: MessageProcessor):
        if self._completed:
            raise RuntimeError("the file context has already completed")

        if key in self._message_gatherer:
            raise RuntimeError('the message processor has already been initialized')

        self._message_gatherer[key] = MessageGatherer(task_group, processor)


@dataclass
class FileSignature:
    digest: bytes
    mtime_ns: int
    ec_id: int | None


class Output(metaclass=abc.ABCMeta):
    def __init__(self):
        self.verbosity = 0
        self.showing_content_wise_duplicates = False

    @abc.abstractmethod
    def _offer(self, record: list[str]):
        raise NotImplementedError()

    def describe_duplicate(
            self, path: Path, is_directory: bool, duplicates: list[tuple[Path, list[FileMetadataDifference]]]):
        suffix = os.sep if is_directory else ''
        record = [str(path) + suffix]

        if self.verbosity >= 1:
            for duplicate, diffs in duplicates:
                record.append(f"## identical {'directory' if is_directory else 'file'}: {duplicate}{suffix}")
                for diff in diffs:
                    record.append(f"## ignored difference - {diff.description('indexed', 'target')}")

        self._offer(record)

    def describe_content_wise_duplicate(self, path, is_directory: bool, duplicates):
        if self.showing_content_wise_duplicates:
            suffix = os.sep if is_directory else ''
            record = [f'# content-wise duplicate: {str(path)}{suffix}']

            if self.verbosity >= 1:
                for candidate, major_diffs, diffs in duplicates:
                    record.append(
                        f"## {'directory' if is_directory else 'file'} with identical content: {candidate}{suffix}")

                    for diff in major_diffs:
                        record.append(f"## difference - {diff.description('indexed', 'target')}")

                    for diff in diffs:
                        if diff in major_diffs:
                            continue

                        record.append(f"## ignored difference - {diff.description('indexed', 'target')}")

            self._offer(record)


class StandardOutput(Output):
    def __init__(self):
        super().__init__()

    def _offer(self, record):
        for part in record:
            print(part)


class ArchiveIndexNotFound(FileNotFoundError):
    pass


class Archive:
    """LevelDB-based file archive indexer for deduplication and content comparison.
    
    Creates and maintains an index of files using SHA-256 hashes and content equivalent
    classes. Stores metadata in .aridx/database using prefixed keys for configuration,
    file hashes, and file signatures.
    """
    __CONFIG_PREFIX = b'c:'
    __FILE_HASH_PREFIX = b'h:'
    __FILE_SIGNATURE_PREFIX = b'm:'

    __CONFIG_HASH_ALGORITHM = 'hash-algorithm'
    __CONFIG_PENDING_ACTION = 'truncating'

    def __init__(self, processor: Processor, path: str | os.PathLike, create: bool = False,
                 output: Output | None = None):
        """Initialize archive with LevelDB index at path/.aridx/database.
        
        Args:
            processor: File processing backend for hashing and comparison
            path: Archive root directory path
            create: Create .aridx directory if missing
            output: Output handler for duplicate reporting
            
        Raises:
            FileNotFoundError: Archive directory does not exist
            NotADirectoryError: Archive path is not a directory
            ArchiveIndexNotFound: Index directory missing and create=False
        """
        archive_path = Path(path)

        if output is None:
            output = StandardOutput()

        if not archive_path.exists():
            raise FileNotFoundError(f"Archive {archive_path} does not exist")

        if not archive_path.is_dir():
            raise NotADirectoryError(f"Archive {archive_path} is not a directory")

        index_path = archive_path / '.aridx'

        if create:
            index_path.mkdir(exist_ok=True)

        if not index_path.exists():
            raise ArchiveIndexNotFound(f"The index for archive {archive_path} has not been created")

        if not index_path.is_dir():
            raise NotADirectoryError(f"The index for archive {archive_path} is not a directory")

        database_path = index_path / 'database'

        database = None
        try:
            database = plyvel.DB(str(database_path), create_if_missing=True)
            config_database: plyvel.DB = database.prefixed_db(Archive.__CONFIG_PREFIX)
            file_hash_database: plyvel.DB = database.prefixed_db(Archive.__FILE_HASH_PREFIX)
            file_signature_database: plyvel.DB = database.prefixed_db(Archive.__FILE_SIGNATURE_PREFIX)
        except:
            if database is not None:
                database.close()
            raise

        self._processor = processor
        self._archive_path = archive_path
        self._output = output
        self._alive = True
        self._database = database
        self._config_database = config_database
        self._file_hash_database = file_hash_database
        self._file_signature_database = file_signature_database

        self._hash_algorithms = {
            'sha256': (32, self._processor.sha256)
        }
        self._default_hash_algorithm = 'sha256'

    def __del__(self):
        """Destructor ensures database is closed."""
        self.close()

    def __enter__(self):
        """Context manager entry, validates archive is still alive."""
        if not self._alive:
            raise BrokenPipeError(f"Archive was closed")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, closes database connection."""
        self.close()

    def close(self):
        """Close LevelDB database and mark archive as closed."""
        if not getattr(self, '_alive', False):
            return

        self._alive = False
        self._file_hash_database = None
        self._database.close()
        self._database = None

    def rebuild(self):
        """Completely rebuild index by truncating database and re-scanning all files.
        
        Sets hash algorithm to SHA-256, removes all existing entries, and performs
        full archive traversal to generate new signatures and equivalent classes.
        """
        asyncio.run(self._do_rebuild())

    async def _do_rebuild(self):
        """Async implementation of rebuild operation."""
        self._truncate()
        await self._do_refresh(hash_algorithm=self._default_hash_algorithm)
        self._write_config(Archive.__CONFIG_HASH_ALGORITHM, self._default_hash_algorithm)

    def refresh(self):
        """Incrementally update index by checking for file changes since last scan.
        
        Compares stored mtime against filesystem, removes deleted files,
        and processes new/modified files. More efficient than rebuild.
        """
        asyncio.run(self._do_refresh())

    async def _do_refresh(self, hash_algorithm: str | None = None):
        """Async implementation of refresh operation with optional hash algorithm override."""
        async with TaskGroup() as tg:
            throttler = Throttler(tg, self._processor.concurrency * 2)
            keyed_lock = KeyedLock()

            if hash_algorithm is None:
                hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)

                if hash_algorithm is None:
                    raise RuntimeError("The index hasn't been build")

                if hash_algorithm not in self._hash_algorithms:
                    raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

            _, calculate_digest = self._hash_algorithms[hash_algorithm]

            async def handle_file(path: Path, context: FileContext):
                if self._lookup_file(context.relative_path()) is None:
                    return await generate_signature(path, context.relative_path(), context.stat.st_mtime_ns)
                return None

            async def refresh_entry(relative_path: Path, signature: FileSignature):
                path = (self._archive_path / relative_path)

                async def clean_up():
                    self._register_file(relative_path, FileSignature(signature.digest, signature.mtime_ns, None))

                    async with keyed_lock.lock(signature.digest):
                        for ec_id, paths in self._list_content_equivalent_classes(signature.digest):
                            if relative_path in paths:
                                paths.remove(relative_path)
                                break
                        else:
                            ec_id = None

                        if ec_id is not None:
                            self._store_content_equivalent_class(signature.digest, ec_id, paths)

                    self._deregister_file(relative_path)

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
                    for ec_id, paths in self._list_content_equivalent_classes(digest):
                        if next_ec_id <= ec_id:
                            next_ec_id = ec_id + 1

                        if await self._processor.compare_content(path, self._archive_path / paths[0]):
                            paths.append(relative_path)
                            break
                    else:
                        ec_id = next_ec_id
                        paths = [relative_path]

                    self._register_file(relative_path, FileSignature(digest, mtime, None))
                    self._store_content_equivalent_class(digest, ec_id, paths)
                    self._register_file(relative_path, FileSignature(digest, mtime, ec_id))

            for path, signature in self._list_registered_files():
                await throttler.schedule(refresh_entry(path, signature))

            for path, context in self._walk_archive():
                if context.is_file():
                    await throttler.schedule(handle_file(path, context))

    def find_duplicates(self, input: Path, ignore: FileMetadataDifferencePattern | None = None):
        """Find files in input path that duplicate content in the archive.
        
        Args:
            input: Directory or file path to check for duplicates
            ignore: Metadata differences to ignore when matching (default: none)
            
        Outputs duplicate reports through configured Output handler.
        Groups files by content hash and compares within equivalent classes.
        """
        asyncio.run(self._do_find_duplicates(input, ignore=ignore))

    async def _do_find_duplicates(self, input: Path, ignore: FileMetadataDifferencePattern | None):
        """Async implementation of duplicate finding with configurable metadata ignore patterns."""
        if ignore is None:
            ignore = FileMetadataDifferencePattern()

        hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)

        if hash_algorithm is None:
            raise RuntimeError("The index hasn't been build")

        if hash_algorithm not in self._hash_algorithms:
            raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        async with (TaskGroup() as tg):
            throttler = Throttler(tg, self._processor.concurrency * 2)
            _, calculate_digest = self._hash_algorithms[self._default_hash_algorithm]

            @dataclass
            class DiscoveredDuplicate:
                path_in_archive: Path
                major_diffs: list[FileMetadataDifference]
                minor_diffs: list[FileMetadataDifference]

            @dataclass
            class DirectoryEntryResult:
                name: str
                file_size: int
                deferred_comparison: bool
                extensions: list[DiscoveredDuplicate]
                duplicates: list[DiscoveredDuplicate]
                content_wise_duplicates: list[DiscoveredDuplicate]

            @dataclass
            class DirectoryResult:
                inhibit_file_report: bool

            async def handle_file(path: Path, context: FileContext, message_to_parent: Message):
                digest = await calculate_digest(path)

                # Find an equivalent class where the contents of files match the file at 'path'.
                for ec_id, paths in self._list_content_equivalent_classes(digest):
                    if await self._processor.compare_content(self._archive_path / paths[0], path):
                        # Only one match will suffice as all files in an equivalent class share the same content
                        break
                else:
                    message_to_parent.deliver_nowait(DirectoryEntryResult(
                        path.name, context.stat.st_size, False, [], [], []))
                    return

                duplicates = []
                content_wise_duplicates = []
                # Find a file in the equivalent class which matches the file at 'path' with regard to their metadata
                for candidate in paths:
                    diffs = await self._processor.compare_metadata(self._archive_path / candidate, path)
                    major_diffs = [diff for diff in diffs if not ignore.match(diff)]
                    duplicate = DiscoveredDuplicate(candidate, major_diffs, diffs)
                    if not major_diffs:
                        duplicates.append(duplicate)
                    else:
                        content_wise_duplicates.append(duplicate)

                # duplicates in directory view
                dup_in_dir_view = [d for d in duplicates if d.path_in_archive.name == path.name]
                # content-wise duplicates in directory view
                cw_dup_in_dir_view = [d for d in content_wise_duplicates if d.path_in_archive.name == path.name]

                Throttler.terminate_current_tenure()

                if dup_in_dir_view:
                    directory_result: DirectoryResult = await message_to_parent.deliver(DirectoryEntryResult(
                        path.name, context.stat.st_size, False, [], dup_in_dir_view, cw_dup_in_dir_view))
                    inhibit_file_report = directory_result.inhibit_file_report
                else:
                    message_to_parent.deliver_nowait(DirectoryEntryResult(
                        path.name, context.stat.st_size, False, [], [], cw_dup_in_dir_view))
                    inhibit_file_report = False

                if not inhibit_file_report:
                    if duplicates:
                        self._output.describe_duplicate(
                            path, False, [(d.path_in_archive, d.minor_diffs) for d in duplicates])
                    else:
                        self._output.describe_content_wise_duplicate(
                            path, False,
                            [(d.path_in_archive, d.major_diffs, d.minor_diffs) for d in content_wise_duplicates])

            async def handle_directory_entries(
                    path: Path, context: FileContext, message_to_parent: Message,
                    reply: Callable[[DirectoryResult], None], entry_results: list[DirectoryEntryResult]):
                child_count = 0
                """The number of children of `path`"""
                total_size = 0
                """The total size of the content of `path`, where the sizes of special files are count as 0"""

                children_deferred_comparison: list[str] = []
                """The children of `path` that have not been compared by the task sending `message_to_parent`"""

                def compare_non_regular_file(reference: Path, target: Path) -> bool:
                    try:
                        rst = reference.lstat()
                        tst = target.lstat()
                    except FileNotFoundError:
                        return False

                    if stat.S_ISREG(rst.st_mode) or stat.S_ISREG(tst.st_mode):
                        return False

                    if rst.st_mode != tst.st_mode:
                        return False

                    if stat.S_ISDIR(rst.st_mode):
                        visited = set()

                        for filename in (fn for it in [reference.iterdir(), target.iterdir()] for fn in it):
                            if filename in visited:
                                continue

                            if not compare_non_regular_file(reference / filename, target / filename):
                                return False

                            visited.add(filename)
                    elif stat.S_ISLNK(rst.st_mode):
                        if os.readlink(reference) != os.readlink(target):
                            return False
                    else:
                        return False

                    return True

                @dataclass
                class CandidateInfo:
                    """A directory in the archive that is possibly a duplicate of the directory at `path`."""
                    duplicates: set[str]
                    """The filenames of the duplicate children"""
                    duplicate_size: int
                    """The total size of the duplicate children in bytes"""
                    content_wise_duplicates: set[str]
                    """The filenames of children with duplicate content but different metadata"""
                    content_wise_duplicate_size: int
                    """The total size of children with duplicate content"""

                candidates: dict[Path, CandidateInfo] = dict()
                """The directories in the archive that is possibly a duplicate of the directory at `path`.
                
                The key is a relative path from the root of the archive. The value is a CandidateInfo object.
                
                An entry is not added unless a candidate has at least one child that is duplicate to a child of `path`
                that has the same filename. It implies, if all the children of `path` are deferred for comparison, this 
                dictionary will be empty."""

                for entry_result in entry_results:
                    child_count += 1
                    total_size += entry_result.file_size

                    if entry_result.deferred_comparison:
                        children_deferred_comparison.append(entry_result.name)
                    else:
                        for full, duplicate in \
                                [(True, d) for d in entry_result.duplicates] + \
                                [(False, d) for d in entry_result.content_wise_duplicates]:
                            # skip if `duplicate.path_in_archive` is the root
                            if duplicate.path_in_archive.parent == duplicate.path_in_archive:
                                continue

                            # it's implied that `entry_result.name` == `duplicate.path_in_archive.name` because it's
                            # checked before the message is sent from the child

                            candidate_path = duplicate.path_in_archive.parent

                            if candidate_path not in candidates:
                                candidates[candidate_path] = CandidateInfo(set(), 0, set(), 0)

                            candidate = candidates[candidate_path]

                            if full:
                                candidate.duplicates.add(entry_result.name)
                                candidate.duplicate_size += entry_result.file_size

                            candidate.content_wise_duplicates.add(entry_result.name)
                            candidate.content_wise_duplicate_size += entry_result.file_size

                extensions: list[DiscoveredDuplicate] = []
                """The directories that are proper supersets to `path`"""
                duplicates: list[DiscoveredDuplicate] = []
                """The directories that are exact duplicates to `path`"""
                content_wise_duplicates: list[DiscoveredDuplicate] = []
                """The directories all files of which share the same content to `path`"""

                for candidate_path, candidate in candidates.items():
                    candidate_children: set[str] = set()
                    """All the children this candidate has"""

                    child: Path
                    for child in (self._archive_path / candidate_path).iterdir():
                        candidate_children.add(child.name)

                        if child.name in children_deferred_comparison:
                            if compare_non_regular_file(
                                    self._archive_path / candidate_path / child.name, path / child.name):
                                candidate.duplicates.add(child.name)
                                candidate.content_wise_duplicates.add(child.name)

                    # TODO generate minor differences for DiscoveredDuplicate;
                    #      handle the metadata of the directory.
                    if len(candidate.duplicates) >= child_count:
                        if not candidate_children.difference(candidate.duplicates):
                            duplicates.append(DiscoveredDuplicate(candidate_path, [], []))
                            content_wise_duplicates.append(DiscoveredDuplicate(candidate_path, [], []))
                        elif len(candidate.duplicates) > 1:
                            extensions.append(DiscoveredDuplicate(candidate_path, [], []))
                    elif len(candidate.content_wise_duplicates) >= child_count:
                        if not candidate_children.difference(candidate.content_wise_duplicates):
                            content_wise_duplicates.append(DiscoveredDuplicate(candidate_path, [], []))

                if extensions or duplicates or content_wise_duplicates:
                    # TODO inhibit reports of content-wise duplicates
                    reply(DirectoryResult(inhibit_file_report=len(duplicates) > 0))

                    directory_result: DirectoryResult = await message_to_parent.deliver(DirectoryEntryResult(
                        path.name, total_size, False, extensions, duplicates, content_wise_duplicates))

                    if not directory_result.inhibit_file_report:
                        if duplicates:
                            self._output.describe_duplicate(
                                path, True, [(d.path_in_archive, d.minor_diffs) for d in duplicates])
                        elif content_wise_duplicates:
                            self._output.describe_content_wise_duplicate(
                                path, True,
                                [(d.path_in_archive, d.major_diffs, d.minor_diffs) for d in content_wise_duplicates])

                        # TODO
                        if extensions:
                            pass
                else:
                    reply(DirectoryResult(inhibit_file_report=False))

                    deferred_comparison = \
                        len(children_deferred_comparison) > 0 and len(children_deferred_comparison) >= child_count
                    message_to_parent.deliver_nowait(DirectoryEntryResult(
                        path.name, total_size, deferred_comparison, [], [], []))

            def make_default_directory_result():
                return DiscardedMessage(DirectoryResult(inhibit_file_report=False))

            for path, context in self._walk(input):
                message_to_parent = context.parent.send_message(handle_directory_entries, make_default_directory_result)

                if stat.S_ISREG(context.stat.st_mode):
                    await throttler.schedule(handle_file(path, context, message_to_parent))
                elif stat.S_ISDIR(context.stat.st_mode):
                    context.register_message_processor(
                        tg, handle_directory_entries,
                        partial(handle_directory_entries, path, context, message_to_parent))
                else:
                    message_to_parent.deliver_nowait(DirectoryEntryResult(
                        path.name, context.stat.st_size, True, [], [], []))

    def inspect(self) -> Iterator[str]:
        """Generate human-readable index entries for debugging and inspection.
        
        Yields:
            Formatted strings showing config, file-hash, and file-metadata entries
            with hex digests, timestamps, and URL-encoded paths
        """
        hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)
        if hash_algorithm in self._hash_algorithms:
            hash_length, _ = self._hash_algorithms[hash_algorithm]
        else:
            hash_length = None

        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(Archive.__CONFIG_PREFIX):
                entry = key[len(Archive.__CONFIG_PREFIX):].decode()
                yield f'config {entry} {value.decode()}'
            elif key.startswith(Archive.__FILE_HASH_PREFIX):
                digest_and_ec_id = key[len(Archive.__FILE_HASH_PREFIX):]
                paths = ' '.join((
                    '/'.join((urllib.parse.quote_plus(part) for part in path))
                    for path in msgpack.loads(value)))
                if hash_length is not None:
                    hex_digest = digest_and_ec_id[:hash_length].hex()
                    ec_id = int.from_bytes(digest_and_ec_id[hash_length:])
                    yield f'file-hash {hex_digest} {ec_id} {paths}'
                else:
                    hex_digest_and_ec_id = digest_and_ec_id.hex()
                    yield f'file-hash *{hex_digest_and_ec_id} {paths}'
            elif key.startswith(Archive.__FILE_SIGNATURE_PREFIX):
                from datetime import datetime, timezone
                path = Path(*[part.decode() for part in key[len(Archive.__FILE_SIGNATURE_PREFIX):].split(b'\0')])
                [digest, mtime, ec_id] = msgpack.loads(value)
                quoted_path = '/'.join((urllib.parse.quote_plus(part) for part in path.parts))
                hex_digest = digest.hex()
                mtime_string = \
                    datetime.fromtimestamp(mtime / 1000000000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                yield f'file-metadata {quoted_path} digest:{hex_digest} mtime:{mtime_string} ec_id:{ec_id}'
            else:
                yield f'OTHER {key} {value}'

    def _truncate(self):
        """Clear all file hash and signature entries, reset configuration."""
        self._write_config(Archive.__CONFIG_PENDING_ACTION, 'truncate')
        self._write_config(Archive.__CONFIG_HASH_ALGORITHM, None)

        batch = self._file_signature_database.write_batch()
        for key, _ in self._file_signature_database.iterator():
            batch.delete(key)
        batch.write()

        batch = self._file_hash_database.write_batch()
        for key, _ in self._file_hash_database.iterator():
            batch.delete(key)
        batch.write()

        self._write_config(Archive.__CONFIG_PENDING_ACTION, None)

    def _write_config(self, entry: str, value: str | None) -> None:
        """Write or delete configuration entry. None value deletes the key."""
        if value is None:
            self._config_database.delete(entry.encode())
        else:
            self._config_database.put(entry.encode(), value.encode())

    def _read_config(self, entry: str) -> str | None:
        """Read configuration value from c: prefixed database entry."""
        value = self._config_database.get(entry.encode())

        if value is not None:
            value = value.decode()

        return value

    def _register_file(self, path, signature: FileSignature) -> None:
        """Store file signature in m: prefixed database entry.
        
        Args:
            path: Relative path from archive root
            signature: File metadata including digest, mtime_ns, and ec_id
        """
        self._file_signature_database.put(
            b'\0'.join((str(part).encode() for part in path.parts)),
            msgpack.dumps([signature.digest, signature.mtime_ns, signature.ec_id])
        )

    def _deregister_file(self, path):
        """Remove file signature entry from database."""
        self._file_signature_database.delete(b'\0'.join((str(part).encode() for part in path.parts)))

    def _lookup_file(self, path) -> FileSignature | None:
        """Retrieve stored file signature by path.
        
        Args:
            path: Relative path from archive root
            
        Returns:
            FileSignature if found, None otherwise
        """
        value = self._file_signature_database.get(b'\0'.join((str(part).encode() for part in path.parts)))

        if value is None:
            return None

        return FileSignature(*msgpack.loads(value))

    def _list_registered_files(self) -> Iterator[tuple[Path, FileSignature]]:
        """Iterate all file signature entries, yielding (path, signature) pairs."""
        for key, value in self._file_signature_database.iterator():
            path = Path(*[part.decode() for part in key.split(b'\0')])
            signature = FileSignature(*msgpack.loads(value))
            yield path, signature

    def _store_content_equivalent_class(self, digest: bytes, ec_id: int, paths: list[Path]) -> None:
        """
        Store an equivalent class in which all the files share the same content exactly.

        :param digest: the digest of the content of files
        :param ec_id: the id of this equivalent class, local to this particular digest
        :param paths: the paths of files in the equivalent class, relative to the archive root
        """
        key = digest + ec_id.to_bytes(length=4).lstrip(b'\0')

        if not paths:
            self._file_hash_database.delete(key)
        else:
            data = [[str(part) for part in path.parts] for path in paths]
            data.sort()
            data = msgpack.dumps(data)
            self._file_hash_database.put(key, data)

    def _list_content_equivalent_classes(self, digest: bytes) -> Iterable[tuple[int, list[Path]]]:
        """
        List all the equivalent classes where the digest of content the files of each equivalent class matches the
        specified argument.

        :param digest: the digest of the content of files
        """
        ec_db: plyvel.DB = self._file_hash_database.prefixed_db(digest)
        for key, data in ec_db.iterator():
            ec_id = int.from_bytes(key)
            data: list[list[str]] = msgpack.loads(data)
            yield ec_id, [Path(*parts) for parts in data]

    def _walk_archive(self) -> Iterator[tuple[Path, FileContext]]:
        """Traverse archive directory excluding .aridx, yielding (path, context) pairs."""
        context = FileContext(None, None, self._archive_path.stat())
        context.exclude('.aridx')
        yield from self.__walk_recursively(self._archive_path, context)
        context.complete()

    def _walk(self, path: Path) -> Iterator[tuple[Path, FileContext]]:
        """Traverse arbitrary path, yielding (path, context) pairs for duplicate detection."""
        st = path.stat(follow_symlinks=False)
        pseudo_parent = FileContext(None, None, st)
        context = FileContext(pseudo_parent, path.name, st)
        yield path, context
        yield from self.__walk_recursively(path, context)
        context.complete()
        pseudo_parent.complete()

    def __walk_recursively(self, path: Path, parent: FileContext) -> Iterator[tuple[Path, FileContext]]:
        """Recursively traverse directory, respecting exclusion patterns."""
        child: Path
        for child in path.iterdir():
            if parent.is_excluded(child.name):
                continue

            st = child.stat(follow_symlinks=False)
            context = FileContext(parent, child.name, st)
            if stat.S_ISDIR(st.st_mode):
                yield child, context
                yield from self.__walk_recursively(child, context)
                context.complete()
            else:
                yield child, context
