import asyncio
import contextvars
import threading
from asyncio import TaskGroup, Semaphore


class Throttler:
    """Concurrency throttler that limits the number of simultaneously running tasks.

    Uses a semaphore to control how many tasks can execute concurrently. Each task is
    assigned a slot that tracks ownership of the semaphore permit and allows early release.
    """

    def __init__(self, task_group: TaskGroup, concurrency: int):
        """Initialize the throttler.

        Args:
            task_group: The TaskGroup to which tasks will be added
            concurrency: Maximum number of tasks that can run concurrently
        """
        self._task_group = task_group
        self._semaphore = Semaphore(concurrency)

    async def schedule(self, coro, name=None, context=None) -> asyncio.Task:
        """Schedule a coroutine to run with concurrency control.

        Acquires a semaphore permit before starting the task. A slot is created to track
        ownership of this permit. The slot is automatically released when the task completes,
        or can be released early using yield_slot().

        Args:
            coro: The coroutine to execute
            name: Optional name for the task
            context: Optional context for the task

        Returns:
            The created asyncio.Task

        Raises:
            Any exception raised during task creation
        """
        await self._semaphore.acquire()

        async def wrapper():
            slot = Throttler.__Slot(lambda: self._semaphore.release())
            context_token = Throttler.__current_slot.set(slot)
            try:
                return await coro
            finally:
                Throttler.__current_slot.reset(context_token)
                slot.release()

        try:
            return self._task_group.create_task(wrapper(), name=name, context=context)
        except:
            self._semaphore.release()
            raise

    @staticmethod
    def yield_slot():
        """Release the current task's concurrency slot early.

        This releases the semaphore permit, allowing another task to start while the
        current task continues executing. Useful when a task has completed its
        resource-intensive work but still has non-blocking operations to perform.

        Must be called from within a task scheduled by this Throttler.

        Raises:
            LookupError: If called outside a task scheduled by this Throttler
        """
        Throttler.__current_slot.get().release()

    __current_slot = contextvars.ContextVar('Throttler.__current_slot')

    class __Slot:
        """Tracks ownership of a semaphore permit for a single task.

        A slot represents one task's ownership of a semaphore permit. It ensures the
        permit is released exactly once, even if multiple release attempts occur
        (e.g., from yield_slot() followed by automatic release at task completion).
        """

        def __init__(self, release_callback):
            """Initialize a slot with a semaphore release callback.

            Args:
                release_callback: Callable that releases the underlying semaphore permit
            """
            self._lock = threading.Lock()
            self._released = False
            self._release_callback = release_callback

        def release(self):
            """Release the semaphore permit if not already released.

            This method is thread-safe and idempotent - it can be called multiple
            times from different contexts without side effects after the first call.
            Only the first call will invoke the release callback.
            """
            with self._lock:
                if not self._released:
                    self._release_callback()
                    self._released = True