"""Tests for DuplicateMatch class."""
import unittest
from pathlib import Path

from rededup.report.duplicate_match import DuplicateMatch, DuplicateMatchRule


class DuplicateMatchTest(unittest.TestCase):
    """Tests for DuplicateMatch class."""

    def test_create_with_all_fields(self):
        """Create DuplicateMatch with all fields."""
        rule = DuplicateMatchRule(include_atime=True)
        comparison = DuplicateMatch(
            Path('repository/file.txt'),
            mtime_match=True,
            atime_match=False,
            ctime_match=True,
            mode_match=True,
            owner_match=False,
            group_match=True,
            duplicated_size=1024,
            duplicated_items=5,
            is_identical=False,
            is_superset=True,
            rule=rule
        )

        self.assertEqual(Path('repository/file.txt'), comparison.path)
        self.assertTrue(comparison.mtime_match)
        self.assertFalse(comparison.atime_match)
        self.assertTrue(comparison.ctime_match)
        self.assertTrue(comparison.mode_match)
        self.assertFalse(comparison.owner_match)
        self.assertTrue(comparison.group_match)
        self.assertEqual(1024, comparison.duplicated_size)
        self.assertEqual(5, comparison.duplicated_items)
        self.assertFalse(comparison.is_identical)
        self.assertTrue(comparison.is_superset)
        self.assertEqual(rule, comparison.rule)

    def test_create_with_defaults(self):
        """Create DuplicateMatch with default values."""
        comparison = DuplicateMatch(
            Path('file.txt'),
            mtime_match=True,
            atime_match=True,
            ctime_match=True,
            mode_match=True
        )

        self.assertEqual(Path('file.txt'), comparison.path)
        self.assertTrue(comparison.mtime_match)
        self.assertTrue(comparison.atime_match)
        self.assertTrue(comparison.ctime_match)
        self.assertTrue(comparison.mode_match)
        self.assertFalse(comparison.owner_match)  # Default
        self.assertFalse(comparison.group_match)  # Default
        self.assertEqual(0, comparison.duplicated_size)  # Default
        self.assertEqual(0, comparison.duplicated_items)  # Default
        self.assertFalse(comparison.is_identical)  # Default
        self.assertFalse(comparison.is_superset)  # Default
        self.assertIsNone(comparison.rule)  # Default

    def test_is_identical_for_files(self):
        """For files, is_identical means all metadata matches."""
        # All metadata matches
        comparison1 = DuplicateMatch(
            Path('file.txt'),
            mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
            duplicated_size=100, duplicated_items=1,
            is_identical=True, is_superset=True
        )
        self.assertTrue(comparison1.is_identical)
        self.assertTrue(comparison1.is_superset)
        self.assertEqual(1, comparison1.duplicated_items)

        # Metadata doesn't match
        comparison2 = DuplicateMatch(
            Path('file.txt'),
            mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
            duplicated_size=100, duplicated_items=1,
            is_identical=False, is_superset=False
        )
        self.assertFalse(comparison2.is_identical)
        self.assertFalse(comparison2.is_superset)
        self.assertEqual(1, comparison2.duplicated_items)


if __name__ == '__main__':
    unittest.main()
