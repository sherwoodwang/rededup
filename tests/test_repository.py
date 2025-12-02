"""Tests for core Repository operations.

This module contains focused unit tests for Repository class functionality:
- Repository creation and basic operations
- Rebuild and refresh operations
"""
import tempfile
import unittest
from pathlib import Path

from rededup import Repository
from rededup.utils.processor import Processor


class RepositoryCoreOperationsTest(unittest.TestCase):
    """Tests for basic repository creation and core operations."""

    def test_create_repository(self):
        """Repository can be created in an empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    # Repository should be created successfully
                    self.assertIsNotNone(repository)
                    # Index directory should exist
                    self.assertTrue((repository_path / '.rededup').exists())

    def test_rebuild_empty_repository(self):
        """Rebuild on a repository with no files completes successfully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    # Should have manifest properties (hash-algorithm and repository-id), no file entries
                    entries = list(repository.inspect())
                    self.assertEqual(2, len(entries))
                    self.assertTrue(any('manifest-property' in e for e in entries))
                    # Verify both manifest properties are present
                    manifest_entries = [e for e in entries if 'manifest-property' in e]
                    self.assertEqual(2, len(manifest_entries))

    def test_rebuild_single_file(self):
        """Rebuild correctly indexes a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Create a single file
            (repository_path / 'test.txt').write_bytes(b'test content')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    entries = list(repository.inspect())
                    # Should have file-metadata and file-hash entries
                    self.assertGreater(len(entries), 0)
                    # Should contain the filename
                    self.assertTrue(any('test.txt' in entry for entry in entries))

    def test_rebuild_idempotent(self):
        """Multiple rebuilds produce identical results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            (repository_path / 'file1.txt').write_bytes(b'content1')
            (repository_path / 'file2.txt').write_bytes(b'content2')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    first_result = set(repository.inspect())

                    repository.rebuild()
                    second_result = set(repository.inspect())

                    # Results should be identical (ignoring timestamp variations)
                    self.assertEqual(len(first_result), len(second_result))

    def test_refresh_no_changes(self):
        """Refresh with no filesystem changes maintains the index."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            (repository_path / 'test.txt').write_bytes(b'test content')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    before = set(repository.inspect())

                    repository.refresh()
                    after = set(repository.inspect())

                    # Filter out repository-id which changes on every refresh
                    before_filtered = {e for e in before if 'repository-id' not in e}
                    after_filtered = {e for e in after if 'repository-id' not in e}
                    self.assertEqual(before_filtered, after_filtered)

    def test_refresh_after_file_added(self):
        """Refresh detects newly added file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            (repository_path / 'existing.txt').write_bytes(b'existing')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    before = list(repository.inspect())

                    # Add new file
                    (repository_path / 'new.txt').write_bytes(b'new content')

                    repository.refresh()
                    after = list(repository.inspect())

                    # Should have more entries now
                    self.assertGreater(len(after), len(before))
                    # New file should be in the index
                    self.assertTrue(any('new.txt' in entry for entry in after))

    def test_refresh_after_file_deleted(self):
        """Refresh detects deleted file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            (repository_path / 'file1.txt').write_bytes(b'content1')
            (repository_path / 'file2.txt').write_bytes(b'content2')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    before = list(repository.inspect())

                    # Delete a file
                    (repository_path / 'file1.txt').unlink()

                    repository.refresh()
                    after = list(repository.inspect())

                    # Should have fewer entries
                    self.assertLess(len(after), len(before))
                    # Deleted file should not be in index
                    self.assertFalse(any('file1.txt' in entry for entry in after))
                    # Other file should still be there
                    self.assertTrue(any('file2.txt' in entry for entry in after))

    def test_refresh_after_file_modified(self):
        """Refresh detects modified file content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            test_file = repository_path / 'test.txt'
            test_file.write_bytes(b'original content')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    before = list(repository.inspect())

                    # Modify the file
                    import time
                    time.sleep(0.01)  # Ensure mtime changes
                    test_file.write_bytes(b'modified content')

                    repository.refresh()
                    after = list(repository.inspect())

                    # Index should be updated (different hash)
                    # Extract digests from entries
                    def get_digest(entries):
                        for entry in entries:
                            if 'digest:' in entry:
                                return entry.split('digest:')[1].split()[0]
                        return None

                    before_digest = get_digest(before)
                    after_digest = get_digest(after)

                    self.assertIsNotNone(before_digest)
                    self.assertIsNotNone(after_digest)
                    self.assertNotEqual(before_digest, after_digest)


if __name__ == '__main__':
    unittest.main()
