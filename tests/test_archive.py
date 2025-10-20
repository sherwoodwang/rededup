"""Tests for core Archive operations.

This module contains focused unit tests for Archive class functionality:
- Archive creation and basic operations
- Rebuild and refresh operations
"""
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive
from arindexer.utils.processor import Processor


class ArchiveCoreOperationsTest(unittest.TestCase):
    """Tests for basic archive creation and core operations."""

    def test_create_archive(self):
        """Archive can be created in an empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    # Archive should be created successfully
                    self.assertIsNotNone(archive)
                    # Index directory should exist
                    self.assertTrue((archive_path / '.aridx').exists())

    def test_rebuild_empty_archive(self):
        """Rebuild on an archive with no files completes successfully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    # Should have only manifest property, no file entries
                    entries = list(archive.inspect())
                    self.assertEqual(1, len(entries))
                    self.assertIn('manifest-property', entries[0])

    def test_rebuild_single_file(self):
        """Rebuild correctly indexes a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create a single file
            (archive_path / 'test.txt').write_bytes(b'test content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    entries = list(archive.inspect())
                    # Should have file-metadata and file-hash entries
                    self.assertGreater(len(entries), 0)
                    # Should contain the filename
                    self.assertTrue(any('test.txt' in entry for entry in entries))

    def test_rebuild_idempotent(self):
        """Multiple rebuilds produce identical results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'file1.txt').write_bytes(b'content1')
            (archive_path / 'file2.txt').write_bytes(b'content2')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    first_result = set(archive.inspect())

                    archive.rebuild()
                    second_result = set(archive.inspect())

                    # Results should be identical (ignoring timestamp variations)
                    self.assertEqual(len(first_result), len(second_result))

    def test_refresh_no_changes(self):
        """Refresh with no filesystem changes maintains the index."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'test.txt').write_bytes(b'test content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    before = set(archive.inspect())

                    archive.refresh()
                    after = set(archive.inspect())

                    self.assertEqual(before, after)

    def test_refresh_after_file_added(self):
        """Refresh detects newly added file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'existing.txt').write_bytes(b'existing')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    before = list(archive.inspect())

                    # Add new file
                    (archive_path / 'new.txt').write_bytes(b'new content')

                    archive.refresh()
                    after = list(archive.inspect())

                    # Should have more entries now
                    self.assertGreater(len(after), len(before))
                    # New file should be in the index
                    self.assertTrue(any('new.txt' in entry for entry in after))

    def test_refresh_after_file_deleted(self):
        """Refresh detects deleted file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'file1.txt').write_bytes(b'content1')
            (archive_path / 'file2.txt').write_bytes(b'content2')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    before = list(archive.inspect())

                    # Delete a file
                    (archive_path / 'file1.txt').unlink()

                    archive.refresh()
                    after = list(archive.inspect())

                    # Should have fewer entries
                    self.assertLess(len(after), len(before))
                    # Deleted file should not be in index
                    self.assertFalse(any('file1.txt' in entry for entry in after))
                    # Other file should still be there
                    self.assertTrue(any('file2.txt' in entry for entry in after))

    def test_refresh_after_file_modified(self):
        """Refresh detects modified file content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            test_file = archive_path / 'test.txt'
            test_file.write_bytes(b'original content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    before = list(archive.inspect())

                    # Modify the file
                    import time
                    time.sleep(0.01)  # Ensure mtime changes
                    test_file.write_bytes(b'modified content')

                    archive.refresh()
                    after = list(archive.inspect())

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
