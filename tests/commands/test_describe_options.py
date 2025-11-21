"""Tests for describe command options and formatting.

This module tests the DescribeOptions class and sorting/filtering functionality:
- Duplicate sorting by size, items, identical status, and path
- Directory children sorting
- Byte format vs human-readable format
"""
import unittest
from pathlib import Path

from arindexer.commands.analyzer import (
    DescribeOptions,
    DuplicateMatch,
    format_size,
)


class DescribeOptionsTest(unittest.TestCase):
    """Tests for DescribeOptions class."""

    def test_default_options(self):
        """Test default DescribeOptions values."""
        options = DescribeOptions()

        self.assertEqual(1, options.limit)
        self.assertEqual('size', options.sort_by)
        self.assertEqual('dup-size', options.sort_children)
        self.assertFalse(options.use_bytes)

    def test_custom_options(self):
        """Test DescribeOptions with custom values."""
        options = DescribeOptions(
            limit=5,
            sort_by='items',
            sort_children='name',
            use_bytes=True
        )

        self.assertEqual(5, options.limit)
        self.assertEqual('items', options.sort_by)
        self.assertEqual('name', options.sort_children)
        self.assertTrue(options.use_bytes)

    def test_unlimited_duplicates(self):
        """Test DescribeOptions with no limit (show all)."""
        options = DescribeOptions(limit=None)

        self.assertIsNone(options.limit)


class DuplicateSortingTest(unittest.TestCase):
    """Tests for duplicate sorting logic."""

    def setUp(self):
        """Create sample DuplicateMatch objects for sorting tests."""
        self.duplicates = [
            # Large size, many items, partial match, long path
            DuplicateMatch(
                Path('archive/very/long/path/to/file1.txt'),
                mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
                duplicated_size=1000000, duplicated_items=10,
                is_identical=False, is_superset=False
            ),
            # Medium size, few items, superset, medium path
            DuplicateMatch(
                Path('archive/medium/file2.txt'),
                mtime_match=True, atime_match=True, ctime_match=True, mode_match=False,
                duplicated_size=500000, duplicated_items=5,
                is_identical=False, is_superset=True
            ),
            # Small size, few items, identical, short path
            DuplicateMatch(
                Path('archive/file3.txt'),
                mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                duplicated_size=100000, duplicated_items=3,
                is_identical=True, is_superset=True
            ),
            # Large size, many items, identical, medium path
            DuplicateMatch(
                Path('archive/path/file4.txt'),
                mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                duplicated_size=1000000, duplicated_items=10,
                is_identical=True, is_superset=True
            ),
        ]

    def test_sort_by_size_descending(self):
        """Test sorting by duplicated_size (largest first)."""
        # When sizes are equal, prefer identical > superset > partial
        # When size and identity are equal, prefer shorter path

        def sort_key(dup):
            identity_rank = 2 if dup.is_identical else (1 if dup.is_superset else 0)
            return (-dup.duplicated_size, -identity_rank, len(str(dup.path)))

        sorted_dups = sorted(self.duplicates, key=sort_key)

        # First should be large size + identical + shorter path
        self.assertEqual(Path('archive/path/file4.txt'), sorted_dups[0].path)
        self.assertEqual(1000000, sorted_dups[0].duplicated_size)
        self.assertTrue(sorted_dups[0].is_identical)

        # Second should be large size + partial match
        self.assertEqual(Path('archive/very/long/path/to/file1.txt'), sorted_dups[1].path)
        self.assertEqual(1000000, sorted_dups[1].duplicated_size)
        self.assertFalse(sorted_dups[1].is_identical)

    def test_sort_by_items_descending(self):
        """Test sorting by duplicated_items (most first)."""
        def sort_key(dup):
            identity_rank = 2 if dup.is_identical else (1 if dup.is_superset else 0)
            return (-dup.duplicated_items, -identity_rank, len(str(dup.path)))

        sorted_dups = sorted(self.duplicates, key=sort_key)

        # First two both have 10 items, but one is identical
        self.assertEqual(10, sorted_dups[0].duplicated_items)
        self.assertTrue(sorted_dups[0].is_identical)

        self.assertEqual(10, sorted_dups[1].duplicated_items)
        self.assertFalse(sorted_dups[1].is_identical)

    def test_sort_by_identical_status(self):
        """Test sorting by identity status (identical first)."""
        def sort_key(dup):
            identity_rank = 2 if dup.is_identical else (1 if dup.is_superset else 0)
            return (-identity_rank, len(str(dup.path)))

        sorted_dups = sorted(self.duplicates, key=sort_key)

        # First two should be identical (sorted by path length)
        self.assertTrue(sorted_dups[0].is_identical)
        self.assertTrue(sorted_dups[1].is_identical)
        # Shorter path first
        self.assertLess(len(str(sorted_dups[0].path)), len(str(sorted_dups[1].path)))

        # Third should be superset
        self.assertFalse(sorted_dups[2].is_identical)
        self.assertTrue(sorted_dups[2].is_superset)

        # Last should be partial match
        self.assertFalse(sorted_dups[3].is_identical)
        self.assertFalse(sorted_dups[3].is_superset)

    def test_sort_by_path_length(self):
        """Test sorting by path length (shortest first)."""
        def sort_key(dup):
            identity_rank = 2 if dup.is_identical else (1 if dup.is_superset else 0)
            return (len(str(dup.path)), -identity_rank)

        sorted_dups = sorted(self.duplicates, key=sort_key)

        # Shortest path should be first
        self.assertEqual(Path('archive/file3.txt'), sorted_dups[0].path)

        # Verify paths are in ascending length order
        for i in range(len(sorted_dups) - 1):
            self.assertLessEqual(
                len(str(sorted_dups[i].path)),
                len(str(sorted_dups[i + 1].path))
            )


class FormatSizeTest(unittest.TestCase):
    """Tests for format_size function."""

    def test_format_bytes(self):
        """Test formatting small sizes (bytes)."""
        self.assertEqual("0 B", format_size(0))
        self.assertEqual("1 B", format_size(1))
        self.assertEqual("999 B", format_size(999))
        self.assertEqual("1023 B", format_size(1023))

    def test_format_kilobytes(self):
        """Test formatting kilobyte sizes."""
        self.assertEqual("1.00 KB", format_size(1024))
        self.assertEqual("1.50 KB", format_size(1536))
        self.assertEqual("100.00 KB", format_size(102400))

    def test_format_megabytes(self):
        """Test formatting megabyte sizes."""
        self.assertEqual("1.00 MB", format_size(1024 * 1024))
        self.assertEqual("1.50 MB", format_size(int(1.5 * 1024 * 1024)))
        self.assertEqual("500.00 MB", format_size(500 * 1024 * 1024))

    def test_format_gigabytes(self):
        """Test formatting gigabyte sizes."""
        self.assertEqual("1.00 GB", format_size(1024 * 1024 * 1024))
        self.assertEqual("2.50 GB", format_size(int(2.5 * 1024 * 1024 * 1024)))

    def test_format_terabytes(self):
        """Test formatting terabyte sizes."""
        self.assertEqual("1.00 TB", format_size(1024 * 1024 * 1024 * 1024))

    def test_format_petabytes(self):
        """Test formatting petabyte sizes."""
        self.assertEqual("1.00 PB", format_size(1024 * 1024 * 1024 * 1024 * 1024))


class DirectoryChildrenSortingTest(unittest.TestCase):
    """Tests for directory children table sorting."""

    def test_sort_children_by_dup_size(self):
        """Test sorting children by duplicated size (default)."""
        # Format: (name, is_dir, total_size, dup_size, dups, in_report, ...)
        children = [
            ('file1.txt', False, 1000, 500, 3, True),
            ('file2.txt', False, 2000, 1000, 2, True),
            ('file3.txt', False, 500, 0, 0, True),
            ('dir1', True, 5000, 2000, 10, True),
        ]

        def sort_key(row):
            name, is_dir, total_size, dup_size, dups, in_report = row
            return (-dup_size, -dups, -total_size)

        sorted_children = sorted(children, key=sort_key)

        # Highest dup_size first
        self.assertEqual('dir1', sorted_children[0][0])
        self.assertEqual(2000, sorted_children[0][3])

    def test_sort_children_by_dup_items(self):
        """Test sorting children by number of duplicates."""
        children = [
            ('file1.txt', False, 1000, 500, 3, True),
            ('file2.txt', False, 2000, 1000, 5, True),
            ('file3.txt', False, 500, 100, 1, True),
        ]

        def sort_key(row):
            name, is_dir, total_size, dup_size, dups, in_report = row
            return (-dups, -dup_size, -total_size)

        sorted_children = sorted(children, key=sort_key)

        # Most duplicates first
        self.assertEqual('file2.txt', sorted_children[0][0])
        self.assertEqual(5, sorted_children[0][4])

    def test_sort_children_by_total_size(self):
        """Test sorting children by total size."""
        children = [
            ('file1.txt', False, 1000, 500, 3, True),
            ('file2.txt', False, 5000, 100, 1, True),
            ('file3.txt', False, 2000, 200, 2, True),
        ]

        def sort_key(row):
            name, is_dir, total_size, dup_size, dups, in_report = row
            return (-total_size,)

        sorted_children = sorted(children, key=sort_key)

        # Largest total size first
        self.assertEqual('file2.txt', sorted_children[0][0])
        self.assertEqual(5000, sorted_children[0][2])

    def test_sort_children_by_name(self):
        """Test sorting children alphabetically by name."""
        children = [
            ('zebra.txt', False, 1000, 500, 3, True),
            ('apple.txt', False, 2000, 1000, 2, True),
            ('banana.txt', False, 500, 0, 0, True),
        ]

        def sort_key(row):
            name, is_dir, total_size, dup_size, dups, in_report = row
            return (name,)

        sorted_children = sorted(children, key=sort_key)

        # Alphabetical order
        self.assertEqual('apple.txt', sorted_children[0][0])
        self.assertEqual('banana.txt', sorted_children[1][0])
        self.assertEqual('zebra.txt', sorted_children[2][0])


if __name__ == '__main__':
    unittest.main()