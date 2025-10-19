"""Tests for equivalence class storage methods with controlled hash collisions.

This module tests the hash-based path storage in ArchiveStore, specifically:
- list_content_equivalent_classes
- add_paths_to_equivalent_class
- remove_paths_from_equivalent_class

Tests use a derived class that overrides _compute_short_path_hash to create
controlled hash collisions for testing collision handling and compaction logic.
"""
import tempfile
import unittest
from pathlib import Path

from arindexer._archive_settings import ArchiveSettings
from arindexer._archive_store import ArchiveStore


class InterceptedArchiveStore(ArchiveStore):
    """ArchiveStore subclass with controllable hash function for testing."""

    # Class-level hash mapping for deterministic testing
    # Maps path string to hash value
    hash_mapping: dict[str, int] = {}

    @staticmethod
    def _compute_short_path_hash(path: Path) -> int:
        """Override to use controlled hash values for testing collisions."""
        path_str = '/'.join(str(part) for part in path.parts)

        # Use mapping if available, otherwise fall back to sequential assignment
        if path_str in InterceptedArchiveStore.hash_mapping:
            return InterceptedArchiveStore.hash_mapping[path_str]

        # Default: return a simple hash based on path length for predictability
        return len(path_str) % 100


class TestEquivalenceClassStorage(unittest.TestCase):
    """Test equivalence class storage methods with controlled hash collisions."""

    def setUp(self):
        """Create temporary archive for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.archive_path = Path(self.temp_dir.name) / 'test_archive'
        self.archive_path.mkdir()

        # Create archive store with testable subclass
        settings = ArchiveSettings(self.archive_path)
        self.store = InterceptedArchiveStore(settings, self.archive_path, create=True)

        # Reset hash mapping for each test
        InterceptedArchiveStore.hash_mapping = {}

        # Test digest for equivalence classes
        self.digest = b'test_digest_123456'

    def tearDown(self):
        """Clean up temporary files."""
        self.store.close()
        self.temp_dir.cleanup()

    def test_add_single_path_no_collision(self):
        """Test adding a single path with no hash collision."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100
        }

        path = Path('file1.txt')
        self.store.add_paths_to_equivalent_class(self.digest, 0, [path])

        # Verify it was stored
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        self.assertEqual(ec_classes[0], (0, [path]))

    def test_add_multiple_paths_no_collision(self):
        """Test adding multiple paths with different hashes (no collisions)."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 200,
            'file3.txt': 300
        }

        paths = [Path('file1.txt'), Path('file2.txt'), Path('file3.txt')]
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)

        # Verify all were stored
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        self.assertEqual(ec_classes[0][0], 0)
        self.assertEqual(sorted(ec_classes[0][1]), sorted(paths))

    def test_add_paths_with_collision(self):
        """Test adding multiple paths with same hash (collision)."""
        # Create collision: all paths hash to 100
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 100,
            'file3.txt': 100
        }

        paths = [Path('file1.txt'), Path('file2.txt'), Path('file3.txt')]
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)

        # Verify all were stored despite collision
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        self.assertEqual(ec_classes[0][0], 0)
        self.assertEqual(sorted(ec_classes[0][1]), sorted(paths))

    def test_add_duplicate_path_skipped(self):
        """Test that adding a path that already exists is skipped."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100
        }

        path = Path('file1.txt')

        # Add once
        self.store.add_paths_to_equivalent_class(self.digest, 0, [path])

        # Add again (should be skipped)
        self.store.add_paths_to_equivalent_class(self.digest, 0, [path])

        # Verify only one copy
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        self.assertEqual(ec_classes[0], (0, [path]))

    def test_add_paths_mixed_collision(self):
        """Test adding paths with partial collisions."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 100,  # Collides with file1
            'file3.txt': 200,
            'file4.txt': 200   # Collides with file3
        }

        paths = [Path('file1.txt'), Path('file2.txt'),
                 Path('file3.txt'), Path('file4.txt')]
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)

        # Verify all were stored
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        self.assertEqual(sorted(ec_classes[0][1]), sorted(paths))

    def test_remove_single_path_no_collision(self):
        """Test removing a single path with no collision."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100
        }

        path = Path('file1.txt')

        # Add then remove
        self.store.add_paths_to_equivalent_class(self.digest, 0, [path])
        self.store.remove_paths_from_equivalent_class(self.digest, 0, [path])

        # Verify it was removed
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 0)

    def test_remove_path_from_multiple(self):
        """Test removing one path when multiple exist."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 200,
            'file3.txt': 300
        }

        paths = [Path('file1.txt'), Path('file2.txt'), Path('file3.txt')]

        # Add all, then remove middle one
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)
        self.store.remove_paths_from_equivalent_class(self.digest, 0, [Path('file2.txt')])

        # Verify only file1 and file3 remain
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        remaining = sorted(ec_classes[0][1])
        self.assertEqual(remaining, [Path('file1.txt'), Path('file3.txt')])

    def test_remove_path_with_collision(self):
        """Test removing a path when collisions exist."""
        # Create collision: all hash to 100
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 100,
            'file3.txt': 100
        }

        paths = [Path('file1.txt'), Path('file2.txt'), Path('file3.txt')]

        # Add all, then remove middle one
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)
        self.store.remove_paths_from_equivalent_class(self.digest, 0, [Path('file2.txt')])

        # Verify file1 and file3 remain, properly compacted
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        remaining = sorted(ec_classes[0][1])
        self.assertEqual(remaining, [Path('file1.txt'), Path('file3.txt')])

    def test_remove_nonexistent_path_ignored(self):
        """Test that removing a path that doesn't exist is silently ignored."""
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100
        }

        # Add file1
        self.store.add_paths_to_equivalent_class(self.digest, 0, [Path('file1.txt')])

        # Try to remove file2 (doesn't exist)
        self.store.remove_paths_from_equivalent_class(self.digest, 0, [Path('file2.txt')])

        # Verify file1 still exists
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(ec_classes[0], (0, [Path('file1.txt')]))

    def test_remove_multiple_paths_with_collision(self):
        """Test removing multiple paths when collisions exist."""
        # All paths hash to 100
        InterceptedArchiveStore.hash_mapping = {
            'file1.txt': 100,
            'file2.txt': 100,
            'file3.txt': 100,
            'file4.txt': 100,
            'file5.txt': 100
        }

        paths = [Path(f'file{i}.txt') for i in range(1, 6)]

        # Add all
        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)

        # Remove file2 and file4
        self.store.remove_paths_from_equivalent_class(
            self.digest, 0, [Path('file2.txt'), Path('file4.txt')]
        )

        # Verify file1, file3, file5 remain
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 1)
        remaining = sorted(ec_classes[0][1])
        expected = [Path('file1.txt'), Path('file3.txt'), Path('file5.txt')]
        self.assertEqual(remaining, expected)

    def test_complex_collision_scenario(self):
        """Test complex scenario with multiple collision groups."""
        # Three collision groups: hash 100, 200, 300
        InterceptedArchiveStore.hash_mapping = {
            'a1.txt': 100,
            'a2.txt': 100,
            'a3.txt': 100,
            'b1.txt': 200,
            'b2.txt': 200,
            'c1.txt': 300,
            'c2.txt': 300,
            'c3.txt': 300,
            'c4.txt': 300
        }

        all_paths = [
            Path('a1.txt'), Path('a2.txt'), Path('a3.txt'),
            Path('b1.txt'), Path('b2.txt'),
            Path('c1.txt'), Path('c2.txt'), Path('c3.txt'), Path('c4.txt')
        ]

        # Add all
        self.store.add_paths_to_equivalent_class(self.digest, 0, all_paths)

        # Remove some from each group
        to_remove = [Path('a2.txt'), Path('b1.txt'), Path('c2.txt'), Path('c4.txt')]
        self.store.remove_paths_from_equivalent_class(self.digest, 0, to_remove)

        # Verify remaining paths
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        remaining = sorted(ec_classes[0][1])
        expected = [
            Path('a1.txt'), Path('a3.txt'),
            Path('b2.txt'),
            Path('c1.txt'), Path('c3.txt')
        ]
        self.assertEqual(remaining, expected)

    def test_list_empty_ec_class(self):
        """Test listing EC classes when none exist."""
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(len(ec_classes), 0)

    def test_nested_paths(self):
        """Test with nested directory paths."""
        InterceptedArchiveStore.hash_mapping = {
            'dir1/file1.txt': 100,
            'dir1/subdir/file2.txt': 100,  # Collision
            'dir2/file3.txt': 200
        }

        paths = [
            Path('dir1/file1.txt'),
            Path('dir1/subdir/file2.txt'),
            Path('dir2/file3.txt')
        ]

        self.store.add_paths_to_equivalent_class(self.digest, 0, paths)

        # Verify all stored
        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        self.assertEqual(sorted(ec_classes[0][1]), sorted(paths))

        # Remove one and verify
        self.store.remove_paths_from_equivalent_class(
            self.digest, 0, [Path('dir1/subdir/file2.txt')]
        )

        ec_classes = list(self.store.list_content_equivalent_classes(self.digest))
        remaining = sorted(ec_classes[0][1])
        expected = [Path('dir1/file1.txt'), Path('dir2/file3.txt')]
        self.assertEqual(remaining, expected)


if __name__ == '__main__':
    unittest.main()
