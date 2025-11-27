"""Tests for find_archive_for_path function."""
import tempfile
import unittest
from pathlib import Path

from arindexer.store.path import find_archive_for_path


class FindArchiveTest(unittest.TestCase):
    """Tests for find_archive_for_path function."""

    def test_find_archive_for_exact_path(self):
        """Test finding archive when the exact archive path is provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            index_dir = archive_path / '.aridx'
            index_dir.mkdir()

            result = find_archive_for_path(archive_path)

            self.assertIsNotNone(result)
            archive_root, archive_record_path = result
            self.assertEqual(archive_path.resolve(), archive_root)
            # record_path should be . when target is the archive root itself
            self.assertEqual(Path('.'), archive_record_path)
            # Verify concatenation: archive_root / archive_record_path should equal archive_path
            self.assertEqual(archive_path.resolve(), archive_root / archive_record_path)

    def test_find_archive_for_child_file(self):
        """Test finding archive for a file inside the archive directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            child_file = archive_path / 'file.txt'
            child_file.write_text('test')

            index_dir = archive_path / '.aridx'
            index_dir.mkdir()

            result = find_archive_for_path(child_file)

            self.assertIsNotNone(result)
            archive_root, archive_record_path = result
            self.assertEqual(archive_path.resolve(), archive_root)
            # record_path should be relative to archive root
            self.assertEqual(Path('file.txt'), archive_record_path)
            # Verify concatenation: archive_root / archive_record_path should equal child_file
            self.assertEqual(child_file.resolve(), archive_root / archive_record_path)

    def test_find_archive_for_nested_file(self):
        """Test finding archive for a deeply nested file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            subdir = archive_path / 'subdir' / 'deep'
            subdir.mkdir(parents=True)
            nested_file = subdir / 'file.txt'
            nested_file.write_text('test')

            index_dir = archive_path / '.aridx'
            index_dir.mkdir()

            result = find_archive_for_path(nested_file)

            self.assertIsNotNone(result)
            archive_root, archive_record_path = result
            self.assertEqual(archive_path.resolve(), archive_root)
            # record_path should be relative to archive root
            self.assertEqual(Path('subdir') / 'deep' / 'file.txt', archive_record_path)
            # Verify concatenation: archive_root / archive_record_path should equal nested_file
            self.assertEqual(nested_file.resolve(), archive_root / archive_record_path)

    def test_find_archive_not_found(self):
        """Test when no archive exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()

            result = find_archive_for_path(target_path)

            self.assertIsNone(result)

    def test_find_archive_closest_parent(self):
        """Test that find_archive_for_path finds the closest parent archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create outer archive with .aridx
            outer = Path(tmpdir) / 'outer_archive'
            outer.mkdir()
            outer_index = outer / '.aridx'
            outer_index.mkdir()

            # Create nested directory without .aridx
            inner = outer / 'inner'
            inner.mkdir()
            file_in_inner = inner / 'file.txt'
            file_in_inner.write_text('test')

            result = find_archive_for_path(file_in_inner)

            self.assertIsNotNone(result)
            archive_root, archive_record_path = result
            # Should find the outer archive
            self.assertEqual(outer.resolve(), archive_root)
            # record_path should be relative to archive root
            self.assertEqual(Path('inner') / 'file.txt', archive_record_path)
            # Verify concatenation
            self.assertEqual(file_in_inner.resolve(), archive_root / archive_record_path)

    def test_find_archive_ignores_nested_archives(self):
        """Test that find_archive_for_path finds the closest archive when nested."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create outer archive
            outer = Path(tmpdir) / 'outer_archive'
            outer.mkdir()
            outer_index = outer / '.aridx'
            outer_index.mkdir()

            # Create nested archive
            inner_archive = outer / 'inner_archive'
            inner_archive.mkdir()
            inner_index = inner_archive / '.aridx'
            inner_index.mkdir()

            # Create file in nested archive
            file_in_inner = inner_archive / 'file.txt'
            file_in_inner.write_text('test')

            result = find_archive_for_path(file_in_inner)

            self.assertIsNotNone(result)
            archive_root, archive_record_path = result
            # Should find the closest (inner) archive
            self.assertEqual(inner_archive.resolve(), archive_root)
            # record_path should be relative to archive root
            self.assertEqual(Path('file.txt'), archive_record_path)
            # Verify concatenation
            self.assertEqual(file_in_inner.resolve(), archive_root / archive_record_path)


if __name__ == '__main__':
    unittest.main()
