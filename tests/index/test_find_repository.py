"""Tests for find_repository_for_path function."""
import tempfile
import unittest
from pathlib import Path

from rededup.index.path import find_repository_for_path


class FindRepositoryTest(unittest.TestCase):
    """Tests for find_repository_for_path function."""

    def test_find_repository_for_exact_path(self):
        """Test finding repository when the exact repository path is provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            index_dir = repository_path / '.rededup'
            index_dir.mkdir()

            result = find_repository_for_path(repository_path)

            self.assertIsNotNone(result)
            repository_root, repository_record_path = result
            self.assertEqual(repository_path.resolve(), repository_root)
            # record_path should be . when target is the repository root itself
            self.assertEqual(Path('.'), repository_record_path)
            # Verify concatenation: repository_root / repository_record_path should equal repository_path
            self.assertEqual(repository_path.resolve(), repository_root / repository_record_path)

    def test_find_repository_for_child_file(self):
        """Test finding repository for a file inside the repository directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            child_file = repository_path / 'file.txt'
            child_file.write_text('test')

            index_dir = repository_path / '.rededup'
            index_dir.mkdir()

            result = find_repository_for_path(child_file)

            self.assertIsNotNone(result)
            repository_root, repository_record_path = result
            self.assertEqual(repository_path.resolve(), repository_root)
            # record_path should be relative to repository root
            self.assertEqual(Path('file.txt'), repository_record_path)
            # Verify concatenation: repository_root / repository_record_path should equal child_file
            self.assertEqual(child_file.resolve(), repository_root / repository_record_path)

    def test_find_repository_for_nested_file(self):
        """Test finding repository for a deeply nested file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            subdir = repository_path / 'subdir' / 'deep'
            subdir.mkdir(parents=True)
            nested_file = subdir / 'file.txt'
            nested_file.write_text('test')

            index_dir = repository_path / '.rededup'
            index_dir.mkdir()

            result = find_repository_for_path(nested_file)

            self.assertIsNotNone(result)
            repository_root, repository_record_path = result
            self.assertEqual(repository_path.resolve(), repository_root)
            # record_path should be relative to repository root
            self.assertEqual(Path('subdir') / 'deep' / 'file.txt', repository_record_path)
            # Verify concatenation: repository_root / repository_record_path should equal nested_file
            self.assertEqual(nested_file.resolve(), repository_root / repository_record_path)

    def test_find_repository_not_found(self):
        """Test when no repository exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()

            result = find_repository_for_path(target_path)

            self.assertIsNone(result)

    def test_find_repository_closest_parent(self):
        """Test that find_repository_for_path finds the closest parent repository."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create outer repository with .rededup
            outer = Path(tmpdir) / 'outer_repository'
            outer.mkdir()
            outer_index = outer / '.rededup'
            outer_index.mkdir()

            # Create nested directory without .rededup
            inner = outer / 'inner'
            inner.mkdir()
            file_in_inner = inner / 'file.txt'
            file_in_inner.write_text('test')

            result = find_repository_for_path(file_in_inner)

            self.assertIsNotNone(result)
            repository_root, repository_record_path = result
            # Should find the outer repository
            self.assertEqual(outer.resolve(), repository_root)
            # record_path should be relative to repository root
            self.assertEqual(Path('inner') / 'file.txt', repository_record_path)
            # Verify concatenation
            self.assertEqual(file_in_inner.resolve(), repository_root / repository_record_path)

    def test_find_repository_ignores_nested_repositories(self):
        """Test that find_repository_for_path finds the closest repository when nested."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create outer repository
            outer = Path(tmpdir) / 'outer_repository'
            outer.mkdir()
            outer_index = outer / '.rededup'
            outer_index.mkdir()

            # Create nested repository
            inner_repository = outer / 'inner_repository'
            inner_repository.mkdir()
            inner_index = inner_repository / '.rededup'
            inner_index.mkdir()

            # Create file in nested repository
            file_in_inner = inner_repository / 'file.txt'
            file_in_inner.write_text('test')

            result = find_repository_for_path(file_in_inner)

            self.assertIsNotNone(result)
            repository_root, repository_record_path = result
            # Should find the closest (inner) repository
            self.assertEqual(inner_repository.resolve(), repository_root)
            # record_path should be relative to repository root
            self.assertEqual(Path('file.txt'), repository_record_path)
            # Verify concatenation
            self.assertEqual(file_in_inner.resolve(), repository_root / repository_record_path)


if __name__ == '__main__':
    unittest.main()
