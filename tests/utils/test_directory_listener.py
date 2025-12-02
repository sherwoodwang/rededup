"""Tests for directory listener coordination utilities."""

import asyncio
import tempfile
import unittest
from asyncio import TaskGroup
from pathlib import Path

from rededup.utils.directory_listener import (
    DirectoryListenerCoordinator,
    DirectoryListener,
    ChildTaskException,
    _DirectoryListenerKey
)
from rededup.utils.walker import walk_with_policy, WalkPolicy


class MockFileContext:
    """Mock FileContext for testing."""

    def __init__(self):
        self._storage = {}

    def __setitem__(self, key, value):
        self._storage[key] = value

    def __getitem__(self, key):
        return self._storage[key]

    def __contains__(self, key):
        return key in self._storage


class DirectoryListenerKeyTest(unittest.TestCase):
    """Tests for _DirectoryListenerKey singleton."""

    def test_singleton_pattern(self):
        """Test that _DirectoryListenerKey is a singleton."""
        key1 = _DirectoryListenerKey()
        key2 = _DirectoryListenerKey()
        self.assertIs(key1, key2)

    def test_repr(self):
        """Test that __repr__ returns expected string."""
        key = _DirectoryListenerKey()
        self.assertEqual(repr(key), "DirectoryListenerKey")


class ChildTaskExceptionTest(unittest.TestCase):
    """Tests for ChildTaskException wrapper."""

    def test_stores_exception_and_future(self):
        """Test that ChildTaskException stores exception and future."""
        async def test_impl():
            original_exception = ValueError("test error")
            mock_future = asyncio.Future()

            wrapped = ChildTaskException(original_exception, mock_future)

            self.assertIs(wrapped.exception, original_exception)
            self.assertIs(wrapped.future, mock_future)

        asyncio.run(test_impl())


class DirectoryListenerCoordinatorTest(unittest.TestCase):
    """Tests for DirectoryListenerCoordinator."""

    def test_context_key_is_singleton(self):
        """Test that context_key returns the same singleton key."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator1 = DirectoryListenerCoordinator(tg)
                coordinator2 = DirectoryListenerCoordinator(tg)

                self.assertIs(coordinator1.context_key, coordinator2.context_key)

        asyncio.run(test_impl())

    def test_register_directory_creates_and_stores_listener(self):
        """Test that register_directory creates listener and stores in context."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()

                listener = coordinator.register_directory(context)

                self.assertIsInstance(listener, DirectoryListener)
                self.assertIs(context[coordinator.context_key], listener)

        asyncio.run(test_impl())


class DirectoryListenerTest(unittest.TestCase):
    """Tests for DirectoryListener."""

    def test_add_child_before_completion(self):
        """Test that add_child works before completion."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()
                listener = coordinator.register_directory(context)

                future = asyncio.Future()
                listener.add_child(future)

                # Should not raise
                self.assertEqual(len(listener._child_futures), 1)

        asyncio.run(test_impl())

    def test_add_child_after_completion_raises_error(self):
        """Test that add_child after completion raises RuntimeError."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()
                listener = coordinator.register_directory(context)

                listener.complete()

                future = asyncio.Future()
                with self.assertRaises(RuntimeError) as cm:
                    listener.add_child(future)

                self.assertIn("after directory is completed", str(cm.exception))

        asyncio.run(test_impl())

    def test_callback_executes_with_successful_results(self):
        """Test that callback receives successful results from children."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()
                listener = coordinator.register_directory(context)

                # Create some child tasks that return values
                async def child_task(value):
                    await asyncio.sleep(0.01)
                    return value

                future1 = tg.create_task(child_task("result1"))
                future2 = tg.create_task(child_task("result2"))
                future3 = tg.create_task(child_task(None))  # Should be filtered out

                listener.add_child(future1)
                listener.add_child(future2)
                listener.add_child(future3)

                # Track callback invocation
                callback_results = []
                async def callback(results):
                    callback_results.extend(results)

                listener.schedule_callback(callback)
                listener.complete()

            # After TaskGroup exits, all tasks should be complete
            self.assertEqual(sorted(callback_results), ["result1", "result2"])

        asyncio.run(test_impl())

    def test_callback_receives_exceptions(self):
        """Test that callback receives ChildTaskException for failed tasks."""
        async def test_impl():
            callback_results = []

            try:
                async with TaskGroup() as tg:
                    coordinator = DirectoryListenerCoordinator(tg)
                    context = MockFileContext()
                    listener = coordinator.register_directory(context)

                    # Create tasks that succeed and fail
                    async def success_task():
                        return "success"

                    async def failing_task():
                        raise ValueError("test error")

                    future1 = tg.create_task(success_task())
                    future2 = tg.create_task(failing_task())

                    listener.add_child(future1)
                    listener.add_child(future2)

                    # Track callback invocation
                    async def callback(results):
                        callback_results.extend(results)

                    listener.schedule_callback(callback)
                    listener.complete()
            except* ValueError:
                # TaskGroup will raise an ExceptionGroup, but our callback
                # should have already captured the exception
                pass

            # Should have one success and one exception
            self.assertEqual(len(callback_results), 2)

            # Find the successful result
            successes = [r for r in callback_results if isinstance(r, str)]
            self.assertEqual(successes, ["success"])

            # Find the exception
            exceptions = [r for r in callback_results if isinstance(r, ChildTaskException)]
            self.assertEqual(len(exceptions), 1)
            self.assertIsInstance(exceptions[0].exception, ValueError)
            self.assertEqual(str(exceptions[0].exception), "test error")

        asyncio.run(test_impl())

    def test_callback_waits_for_completion_event(self):
        """Test that callback waits for complete() before executing."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()
                listener = coordinator.register_directory(context)

                callback_executed = False

                async def callback(results):
                    nonlocal callback_executed
                    callback_executed = True

                # Schedule callback but don't complete yet
                listener.schedule_callback(callback)

                # Give it a moment
                await asyncio.sleep(0.05)

                # Callback should not have executed yet
                self.assertFalse(callback_executed)

                # Now complete
                listener.complete()

                # Give callbacks time to execute
                await asyncio.sleep(0.05)

            # After TaskGroup exits, callback should have executed
            self.assertTrue(callback_executed)

        asyncio.run(test_impl())

    def test_multiple_callbacks_execute_serially(self):
        """Test that multiple callbacks execute one at a time (semaphore control)."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)

                # Register two directories
                context1 = MockFileContext()
                context2 = MockFileContext()
                listener1 = coordinator.register_directory(context1)
                listener2 = coordinator.register_directory(context2)

                execution_order = []
                lock = asyncio.Lock()

                async def callback1(results):
                    async with lock:
                        execution_order.append("start1")
                    await asyncio.sleep(0.05)
                    async with lock:
                        execution_order.append("end1")

                async def callback2(results):
                    async with lock:
                        execution_order.append("start2")
                    await asyncio.sleep(0.05)
                    async with lock:
                        execution_order.append("end2")

                listener1.schedule_callback(callback1)
                listener2.schedule_callback(callback2)

                # Complete both
                listener1.complete()
                listener2.complete()

            # Callbacks should not interleave due to semaphore
            # Either callback1 completes fully before callback2 starts,
            # or vice versa
            self.assertEqual(len(execution_order), 4)

            # Check that one callback completes before the other starts
            if execution_order[0] == "start1":
                self.assertEqual(execution_order[:2], ["start1", "end1"])
                self.assertEqual(execution_order[2:], ["start2", "end2"])
            else:
                self.assertEqual(execution_order[:2], ["start2", "end2"])
                self.assertEqual(execution_order[2:], ["start1", "end1"])

        asyncio.run(test_impl())

    def test_callback_with_no_children(self):
        """Test that callback executes even when there are no children."""
        async def test_impl():
            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                context = MockFileContext()
                listener = coordinator.register_directory(context)

                callback_results = []
                async def callback(results):
                    callback_results.extend(results)

                listener.schedule_callback(callback)
                listener.complete()

            # Callback should have been called with empty results
            self.assertEqual(callback_results, [])

        asyncio.run(test_impl())


class WalkerIntegrationTest(unittest.TestCase):
    """Tests for DirectoryListener integration with walker module."""

    def setUp(self):
        """Create temporary directory structure for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.root = Path(self.temp_dir)

        # Create directory structure:
        # root/
        #   dir1/
        #     file1.txt
        #     file2.txt
        #   dir2/
        #     subdir/
        #       file3.txt
        #   file4.txt
        (self.root / "dir1").mkdir()
        (self.root / "dir1" / "file1.txt").write_text("content1")
        (self.root / "dir1" / "file2.txt").write_text("content2")
        (self.root / "dir2").mkdir()
        (self.root / "dir2" / "subdir").mkdir()
        (self.root / "dir2" / "subdir" / "file3.txt").write_text("content3")
        (self.root / "file4.txt").write_text("content4")

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_listeners_called_for_each_directory(self):
        """Test that listeners are registered and called for each directory."""
        async def test_impl():
            directories_completed = []

            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                policy = WalkPolicy(
                    excluded_paths=set(),
                    should_follow_symlink=lambda p, c: None,
                    yield_root=True
                )

                for file_path, context in walk_with_policy(self.root, policy):
                    # Only register listeners for directories
                    if file_path.is_dir():
                        dir_name = file_path.name

                        async def make_callback(name):
                            async def callback(results):
                                directories_completed.append(name)
                            return callback

                        listener = coordinator.register_directory(context)
                        listener.schedule_callback(await make_callback(dir_name))

            # All directories should have been completed
            # Note: The root dir name is the temp dir name
            self.assertIn("dir1", directories_completed)
            self.assertIn("dir2", directories_completed)
            self.assertIn("subdir", directories_completed)
            # Root directory callback also executes
            self.assertEqual(len(directories_completed), 4)

        asyncio.run(test_impl())

    def test_file_tasks_added_to_parent_listener(self):
        """Test that file processing tasks are added to parent directory listeners."""
        async def test_impl():
            directory_results = {}

            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                policy = WalkPolicy(
                    excluded_paths=set(),
                    should_follow_symlink=lambda p, c: None,
                    yield_root=True
                )

                # Process files
                async def process_file(path: Path):
                    await asyncio.sleep(0.01)
                    return f"processed:{path.name}"

                for file_path, context in walk_with_policy(self.root, policy):
                    if file_path.is_dir():
                        # Register listener for directory
                        dir_name = file_path.name

                        async def make_callback(name):
                            async def callback(results):
                                directory_results[name] = results
                            return callback

                        listener = coordinator.register_directory(context)
                        listener.schedule_callback(await make_callback(dir_name))
                    elif file_path.is_file():
                        # Process file and add to parent listener
                        file_task = tg.create_task(process_file(file_path))

                        # Add to parent directory's listener
                        try:
                            parent_context = context.parent
                            listener = parent_context[coordinator.context_key]
                            listener.add_child(file_task)
                        except (LookupError, KeyError):
                            pass

            # dir1 should have results for file1.txt and file2.txt
            self.assertIn("dir1", directory_results)
            dir1_results = sorted(directory_results["dir1"])
            self.assertEqual(dir1_results, ["processed:file1.txt", "processed:file2.txt"])

            # subdir should have results for file3.txt
            self.assertIn("subdir", directory_results)
            subdir_results = directory_results["subdir"]
            self.assertEqual(subdir_results, ["processed:file3.txt"])

        asyncio.run(test_impl())

    def test_nested_directory_completion_order(self):
        """Test that all directory callbacks are executed."""
        async def test_impl():
            completion_order = []
            lock = asyncio.Lock()

            async with TaskGroup() as tg:
                coordinator = DirectoryListenerCoordinator(tg)
                policy = WalkPolicy(
                    excluded_paths=set(),
                    should_follow_symlink=lambda p, c: None,
                    yield_root=True
                )

                for file_path, context in walk_with_policy(self.root, policy):
                    if file_path.is_dir():
                        dir_name = file_path.name

                        async def make_callback(name):
                            async def callback(results):
                                async with lock:
                                    completion_order.append(name)
                            return callback

                        listener = coordinator.register_directory(context)
                        listener.schedule_callback(await make_callback(dir_name))

            # All directories should have callbacks executed
            self.assertIn("subdir", completion_order)
            self.assertIn("dir2", completion_order)
            self.assertIn("dir1", completion_order)
            # Root directory also gets a callback
            self.assertEqual(len(completion_order), 4)

        asyncio.run(test_impl())

    def test_exception_in_file_task_captured(self):
        """Test that exceptions in file processing tasks are captured by listeners."""
        async def test_impl():
            directory_results = {}

            try:
                async with TaskGroup() as tg:
                    coordinator = DirectoryListenerCoordinator(tg)
                    policy = WalkPolicy(
                        excluded_paths=set(),
                        should_follow_symlink=lambda p, c: None,
                        yield_root=True
                    )

                    # Process files, some fail
                    async def process_file(path: Path):
                        if "file1" in path.name:
                            raise ValueError(f"Failed to process {path.name}")
                        return f"processed:{path.name}"

                    for file_path, context in walk_with_policy(self.root, policy):
                        if file_path.is_dir():
                            dir_name = file_path.name

                            async def make_callback(name):
                                async def callback(results):
                                    directory_results[name] = results
                                return callback

                            listener = coordinator.register_directory(context)
                            listener.schedule_callback(await make_callback(dir_name))
                        elif file_path.is_file():
                            file_task = tg.create_task(process_file(file_path))

                            try:
                                parent_context = context.parent
                                listener = parent_context[coordinator.context_key]
                                listener.add_child(file_task)
                            except (LookupError, KeyError):
                                pass
            except* ValueError:
                # TaskGroup will raise exception group, but we continue
                pass

            # dir1 results should contain both success and exception
            self.assertIn("dir1", directory_results)
            dir1_results = directory_results["dir1"]

            # Should have 2 results: 1 success and 1 exception
            self.assertEqual(len(dir1_results), 2)

            # Find the exception
            exceptions = [r for r in dir1_results if isinstance(r, ChildTaskException)]
            self.assertEqual(len(exceptions), 1)
            self.assertIsInstance(exceptions[0].exception, ValueError)
            self.assertIn("file1", str(exceptions[0].exception))

            # Find the success
            successes = [r for r in dir1_results if isinstance(r, str)]
            self.assertEqual(len(successes), 1)
            self.assertIn("file2", successes[0])

        asyncio.run(test_impl())


if __name__ == '__main__':
    unittest.main()
