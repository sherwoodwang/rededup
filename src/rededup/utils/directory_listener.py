"""Directory completion listener utilities for coordinating async callbacks.

This module provides DirectoryListener and DirectoryListenerCoordinator classes
that enable callbacks to be executed after all child tasks in a directory have
been processed.

These classes are designed to work with the walker module's FileContext system.
The typical usage pattern is:

1. Create a DirectoryListenerCoordinator bound to a TaskGroup
2. When walking directories with walk_with_policy(), register a DirectoryListener
   for each directory using coordinator.register_directory(context)
3. Schedule a callback on the listener using listener.schedule_callback(callback)
4. Add child task futures to the listener using listener.add_child(future)
5. Call listener.complete() (typically via FileContext.complete()) when the
   directory is fully walked
6. The callback executes automatically after all children complete

The FileContext's complete() method is called automatically by the walker when
a directory is finished, which triggers the listener's completion event.
"""

import asyncio
from typing import Any, Callable, Awaitable, TYPE_CHECKING

if TYPE_CHECKING:
    from .walker import FileContext


class ChildTaskException:
    """Wrapper for exceptions that occurred in child tasks.

    Attributes:
        exception: The original exception that was raised
        future: The future that raised the exception
    """

    def __init__(self, exception: Exception, future: asyncio.Future):
        self.exception = exception
        self.future = future


class _DirectoryListenerKey:
    """Singleton key object for storing DirectoryListener in FileContext.

    Using a unique object instead of a string prevents key collisions and
    makes the association more explicit and type-safe.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "DirectoryListenerKey"


class DirectoryListenerCoordinator:
    """Coordinates directory listeners and their callback execution.

    This class manages the shared state needed for directory listener operations,
    including the context key for storing listeners, the semaphore for controlling
    callback concurrency, task group for scheduling, and tracking pending callback tasks.
    """

    def __init__(self, task_group):
        """Initialize the coordinator with a unique key and task group binding.

        Args:
            task_group: TaskGroup to use for scheduling callback tasks
        """
        # Unique key for storing DirectoryListener in FileContext
        self._key = _DirectoryListenerKey()

        # Task group for scheduling callbacks
        self._task_group = task_group

        # Semaphore to control directory callback concurrency (only one at a time)
        self._callback_semaphore = asyncio.Semaphore(1)

        # Track pending directory callbacks
        self._pending_callbacks: list[asyncio.Task] = []

    @property
    def context_key(self):
        """Get the key for storing DirectoryListener in FileContext."""
        return self._key

    def register_directory(self, context) -> 'DirectoryListener':
        """Create and register a DirectoryListener for the given context.

        Args:
            context: File context for the directory

        Returns:
            A new DirectoryListener instance registered to the context
        """
        listener = DirectoryListener(
            task_group=self._task_group,
            callback_semaphore=self._callback_semaphore,
            pending_callbacks=self._pending_callbacks
        )
        context[self._key] = listener
        return listener

    def register_child_with_parent(
            self,
            child_context: 'FileContext',
            child_task: asyncio.Future[Any]
    ) -> None:
        """Register a child's task with its parent directory's listener.

        Args:
            child_context: File context for the child file/directory
            child_task: Task or Future representing the child's analysis result
        """
        try:
            parent_context = child_context.parent
        except LookupError:
            # No parent context (root level file)
            return

        if self._key in parent_context:
            parent_listener: DirectoryListener = parent_context[self._key]
            parent_listener.add_child(child_task)


class DirectoryListener:
    """Listens for completion of directory processing and executes callbacks.

    This class collects results from child files and executes a callback when
    all children have been processed. Callbacks are executed with concurrency
    control managed by the coordinator.
    """

    def __init__(self, task_group, callback_semaphore: asyncio.Semaphore, pending_callbacks: list[asyncio.Task]):
        """Initialize directory listener.

        Args:
            task_group: TaskGroup to use for scheduling callback tasks
            callback_semaphore: Semaphore to control callback concurrency
            pending_callbacks: List to track pending callback tasks
        """
        self._task_group = task_group
        self._callback_semaphore = callback_semaphore
        self._pending_callbacks = pending_callbacks
        self._child_futures: list[asyncio.Future] = []
        self._completion_event = asyncio.Event()

    def add_child(self, future: asyncio.Future) -> None:
        """Add a child task result future.

        Args:
            future: Future that will contain the result of processing a child file
        """
        if self._completion_event.is_set():
            raise RuntimeError("Cannot add children after directory is completed")
        self._child_futures.append(future)

    def complete(self) -> None:
        """Mark directory as complete - no more children will be added."""
        self._completion_event.set()

    def schedule_callback(self, callback: Callable[[list[Any]], Awaitable[Any]]) -> asyncio.Task[Any]:
        """Schedule callback execution in the task group and return a future for its result.

        Args:
            callback: Async function to call when directory processing completes.
                     Receives list of results from child files as argument.
                     Results may include ChildTaskException instances for failed tasks.
                     The callback's return value will be captured in the returned task.

        Returns:
            A Task that will contain the callback's return value once execution completes.
        """
        async def await_completion_and_execute():
            return await self._await_completion_and_execute(callback)

        callback_task = self._task_group.create_task(await_completion_and_execute())
        self._pending_callbacks.append(callback_task)
        return callback_task

    async def _await_completion_and_execute(self, callback: Callable[[list[Any]], Awaitable[Any]]) -> Any:
        """Wait for directory completion, then execute callback with collected child results.

        Args:
            callback: Async function to call with the results

        Returns:
            The return value of the callback
        """
        # Wait for directory to be marked as complete
        await self._completion_event.wait()

        # Wait for all child futures to complete, capturing both results and exceptions
        results = []
        for future in self._child_futures:
            try:
                result = await future
                if result is not None:
                    results.append(result)
            except Exception as e:
                # Wrap exception and include it in results
                results.append(ChildTaskException(e, future))

        # Execute callback with controlled concurrency and return its result
        async with self._callback_semaphore:
            return await callback(results)
