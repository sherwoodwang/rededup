"""Per-key locking mechanism for coordinating concurrent access to shared resources.

This module provides KeyedLock, a data structure that allows multiple async tasks
to acquire exclusive locks on individual keys. Tasks attempting to lock the same
key will wait in a queue until the lock becomes available.

Typical usage example:

    keyed_lock = KeyedLock()

    async with keyed_lock.lock(digest):
        # Perform operations on resources identified by 'digest'
        # Other tasks trying to lock the same digest will wait
        await process_content(digest)
"""

from asyncio import Lock, Condition


class KeyedLock:
    """A lock manager indexed by arbitrary hashable keys.

    KeyedLock manages per-key locks that allow multiple concurrent tasks to
    coordinate access to resources identified by keys. Each unique key has
    its own lock queue, and tasks are granted access in FIFO order.

    This is particularly useful when processing files by content hash, where
    multiple tasks may need to update the same hash bucket but different hashes
    can be processed in parallel.

    Example:
        >>> keyed_lock = KeyedLock()
        >>> async with keyed_lock.lock("resource_a"):
        ...     # Exclusive access to "resource_a"
        ...     await modify_resource("resource_a")
        >>> async with keyed_lock.lock("resource_b"):
        ...     # Can run in parallel with resource_a operations
        ...     await modify_resource("resource_b")

    Thread-safety:
        This class is designed for use with asyncio and is not thread-safe.
        All operations should be performed within the same event loop.
    """

    class _Lock:
        """Internal async context manager representing a lock on a specific key.

        This class implements the actual locking logic using a queue mechanism.
        Tasks wait until they are at the front of the queue for their key.

        Args:
            parent: The parent KeyedLock instance
            key: The hashable key identifying the resource to lock
        """

        def __init__(self, parent, key):
            self._parent: KeyedLock = parent
            self._key = key

        async def __aenter__(self):
            """Acquire the lock for this key.

            Adds this lock request to the key's queue and waits until it
            reaches the front of the queue. If this is the first lock request
            for the key, it acquires the lock immediately.
            """
            async with self._parent._lock:
                if self._key in self._parent._keys:
                    self._parent._keys[self._key].append(self)
                else:
                    self._parent._keys[self._key] = [self]

                while self._parent._keys[self._key][0] is not self:
                    await self._parent._key_releasing.wait()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Release the lock for this key.

            Removes this lock from the front of the key's queue and notifies
            waiting tasks. If the queue becomes empty, removes the key from
            the lock manager entirely.

            Args:
                exc_type: Exception type if an exception occurred
                exc_val: Exception value if an exception occurred
                exc_tb: Exception traceback if an exception occurred
            """
            async with self._parent._lock:
                backlog = self._parent._keys[self._key]
                if len(backlog) > 1:
                    self._parent._keys[self._key] = backlog[1:]
                    self._parent._key_releasing.notify_all()
                else:
                    del self._parent._keys[self._key]

    def __init__(self):
        """Initialize a new empty KeyedLock.

        Creates the internal lock and condition variable used to coordinate
        access to the key queues.
        """
        self._lock = Lock()
        self._key_releasing = Condition(self._lock)
        self._keys = dict()

    def lock(self, key):
        """Create a lock for the specified key.

        Returns an async context manager that, when entered, will acquire
        exclusive access to the specified key. Multiple calls with the same
        key will queue up and execute serially. Calls with different keys
        can execute in parallel.

        Args:
            key: A hashable key identifying the resource to lock. Common
                choices include file paths, content digests, or database keys.

        Returns:
            An async context manager (_Lock) that acquires the lock on enter
            and releases it on exit.

        Example:
            >>> async with keyed_lock.lock(file_digest):
            ...     # Exclusive access guaranteed for this digest
            ...     await update_digest_metadata(file_digest)
        """
        return KeyedLock._Lock(self, key)