import abc
import asyncio
import os
import stat
import threading
from asyncio import TaskGroup, Future
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable, Awaitable, TypeAlias, NamedTuple

from ._processor import Processor, FileMetadataDifference, FileMetadataDifferenceType
from ._throttler import Throttler
from ._archive_store import ArchiveStore


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


class FindDuplicatesArgs(NamedTuple):
    """Arguments for duplicate finding operations."""
    processor: Processor  # File processing backend for content comparison and metadata analysis
    output: Output  # Output handler for reporting duplicate findings
    hash_algorithms: dict  # Available hash algorithms mapping name to (digest_size, calculator)
    default_hash_algorithm: str  # Default hash algorithm name to use for digest calculation
    config_hash_algorithm_key: str  # Configuration key for retrieving stored hash algorithm
    input: Path  # Directory or file path to search for duplicates
    ignore: FileMetadataDifferencePattern  # Metadata differences to ignore when matching


async def do_find_duplicates(
        store: ArchiveStore,
        args: FindDuplicatesArgs):
    """Async implementation of duplicate finding with configurable metadata ignore patterns."""
    archive_path = store.archive_path
    hash_algorithm = store.read_config(args.config_hash_algorithm_key)

    if hash_algorithm is None:
        raise RuntimeError("The index hasn't been build")

    if hash_algorithm not in args.hash_algorithms:
        raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

    async with (TaskGroup() as tg):
        throttler = Throttler(tg, args.processor.concurrency * 2)
        _, calculate_digest = args.hash_algorithms[args.default_hash_algorithm]

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

        async def handle_file(path: Path, context, message_to_parent: Message):  # context is FileContext
            digest = await calculate_digest(path)

            # Find an equivalent class where the contents of files match the file at 'path'.
            for ec_id, paths in store.list_content_equivalent_classes(digest):
                if await args.processor.compare_content(archive_path / paths[0], path):
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
                diffs = await args.processor.compare_metadata(archive_path / candidate, path)
                major_diffs = [diff for diff in diffs if not args.ignore.match(diff)]
                duplicate = DiscoveredDuplicate(candidate, major_diffs, diffs)
                if not major_diffs:
                    duplicates.append(duplicate)
                else:
                    content_wise_duplicates.append(duplicate)

            # duplicates in directory view
            dup_in_dir_view = [d for d in duplicates if d.path_in_archive.name == path.name]
            # content-wise duplicates in directory view
            cw_dup_in_dir_view = [d for d in content_wise_duplicates if d.path_in_archive.name == path.name]

            Throttler.yield_slot()

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
                    args.output.describe_duplicate(
                        path, False, [(d.path_in_archive, d.minor_diffs) for d in duplicates])
                else:
                    args.output.describe_content_wise_duplicate(
                        path, False,
                        [(d.path_in_archive, d.major_diffs, d.minor_diffs) for d in content_wise_duplicates])

        async def handle_directory_entries(
                path: Path, context, message_to_parent: Message,  # context is FileContext
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
                for child in (archive_path / candidate_path).iterdir():
                    candidate_children.add(child.name)

                    if child.name in children_deferred_comparison:
                        if compare_non_regular_file(
                                archive_path / candidate_path / child.name, path / child.name):
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
                        args.output.describe_duplicate(
                            path, True, [(d.path_in_archive, d.minor_diffs) for d in duplicates])
                    elif content_wise_duplicates:
                        args.output.describe_content_wise_duplicate(
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

        for path, context in store.walk(args.input):
            # Inline send_message logic using the key directly in the dictionary
            try:
                message_gatherer = context.parent[handle_directory_entries]
            except KeyError:
                message_to_parent = make_default_directory_result()
            else:
                message_to_parent = message_gatherer.send()

            if stat.S_ISREG(context.stat.st_mode):
                await throttler.schedule(handle_file(path, context, message_to_parent))
            elif stat.S_ISDIR(context.stat.st_mode):
                # Inline register_message_processor logic
                if handle_directory_entries in context:
                    raise RuntimeError('the message processor has already been initialized')
                context[handle_directory_entries] = MessageGatherer(
                    tg, partial(handle_directory_entries, path, context, message_to_parent))
            else:
                message_to_parent.deliver_nowait(DirectoryEntryResult(
                    path.name, context.stat.st_size, True, [], [], []))