"""Tests for find_report_for_path function."""
import tempfile
import unittest
from pathlib import Path

from arindexer.commands.analyzer import find_report_for_path


class FindReportTest(unittest.TestCase):
    """Tests for find_report_for_path function."""

    def test_find_report_for_exact_path(self):
        """Test finding report for the exact analyzed path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            report_dir = Path(str(target_path) + '.report')
            report_dir.mkdir()

            analyzed_path = find_report_for_path(target_path)

            self.assertIsNotNone(analyzed_path)
            self.assertEqual(target_path.resolve(), analyzed_path)

    def test_find_report_for_child_file(self):
        """Test finding report for a file inside analyzed directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            child_file = target_path / 'file.txt'
            child_file.write_text('test')

            report_dir = Path(str(target_path) + '.report')
            report_dir.mkdir()

            analyzed_path = find_report_for_path(child_file)

            self.assertIsNotNone(analyzed_path)
            self.assertEqual(target_path.resolve(), analyzed_path)

    def test_find_report_for_nested_file(self):
        """Test finding report for a deeply nested file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            subdir = target_path / 'subdir' / 'deep'
            subdir.mkdir(parents=True)
            nested_file = subdir / 'file.txt'
            nested_file.write_text('test')

            report_dir = Path(str(target_path) + '.report')
            report_dir.mkdir()

            analyzed_path = find_report_for_path(nested_file)

            self.assertIsNotNone(analyzed_path)
            self.assertEqual(target_path.resolve(), analyzed_path)

    def test_find_report_not_found(self):
        """Test when no report exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()

            analyzed_path = find_report_for_path(target_path)

            self.assertIsNone(analyzed_path)

    def test_find_report_closest_parent(self):
        """Test that find_report_for_path finds the closest parent report."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create outer directory with report
            outer = Path(tmpdir) / 'outer'
            outer.mkdir()
            outer_report = Path(str(outer) + '.report')
            outer_report.mkdir()

            # Create nested directory without report
            inner = outer / 'inner'
            inner.mkdir()
            file_in_inner = inner / 'file.txt'
            file_in_inner.write_text('test')

            analyzed_path = find_report_for_path(file_in_inner)

            self.assertIsNotNone(analyzed_path)
            # Should find the outer analyzed path
            self.assertEqual(outer.resolve(), analyzed_path)


if __name__ == '__main__':
    unittest.main()
