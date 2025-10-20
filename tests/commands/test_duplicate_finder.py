"""Tests for duplicate file and directory detection.

This module contains tests for the duplicate detection functionality:
- Exact duplicate file detection
- Content-wise duplicate detection
- Duplicate detection with metadata differences
- Directory-level duplicate detection
"""
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive, FileMetadataDifferencePattern, FileMetadataDifferenceType
from arindexer.utils.processor import Processor

from ..test_utils import CollectingOutput, copy_times, tweak_times


class DuplicateDetectionTest(unittest.TestCase):
    """Tests for duplicate file detection functionality."""

    def test_find_duplicates_exact_match(self):
        """Find exact duplicate with identical content and metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create file in archive
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'duplicate content')

            # Create identical file in target
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'duplicate.txt'
            target_file.write_bytes(b'duplicate content')
            copy_times(archive_file, target_file)

            with Processor() as processor:
                output = CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path, ignore=FileMetadataDifferencePattern.TRIVIAL)

                    # Should find one duplicate
                    self.assertEqual(1, len(output.data))
                    self.assertTrue(any('duplicate.txt' in str(record) for record in output.data))

    def test_find_duplicates_no_match(self):
        """No duplicates found when files have different content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            (archive_path / 'file1.txt').write_bytes(b'content one')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'file2.txt').write_bytes(b'content two')

            with Processor() as processor:
                output = CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path)

                    # Should find no duplicates
                    self.assertEqual(0, len(output.data))

    def test_find_duplicates_ignore_mtime(self):
        """Find duplicates when ignoring mtime differences."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'same content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'newer.txt'
            target_file.write_bytes(b'same content')
            # Different timestamp
            tweak_times(target_file, 5000000000)

            with Processor() as processor:
                output = CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()

                    # Without ignoring differences, should not match
                    archive.find_duplicates(target_path)
                    self.assertEqual(0, len(output.data))

                    # With ignoring mtime, should match
                    output.data.clear()
                    diffptn = FileMetadataDifferencePattern()
                    diffptn.add(FileMetadataDifferenceType.MTIME)
                    diffptn.add(FileMetadataDifferenceType.ATIME)
                    diffptn.add(FileMetadataDifferenceType.CTIME)
                    archive.find_duplicates(target_path, ignore=diffptn)
                    self.assertEqual(1, len(output.data))

    def test_find_duplicates_content_wise(self):
        """Find content-wise duplicates with different metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'shared content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'copy.txt'
            target_file.write_bytes(b'shared content')
            tweak_times(target_file, 2000000000)

            with Processor() as processor:
                output = CollectingOutput()
                output.showing_content_wise_duplicates = True
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path)

                    # Should find content-wise duplicate
                    self.assertGreater(len(output.data), 0)
                    # Output should mention content-wise duplicate
                    found_content_wise = any(
                        any('content-wise' in item for item in record if isinstance(item, str))
                        for record in output.data
                    )
                    self.assertTrue(found_content_wise)

    def test_find_duplicates_multiple_matches(self):
        """One target file matches multiple files in archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create multiple files with same content in archive
            (archive_path / 'dup1.txt').write_bytes(b'duplicate')
            (archive_path / 'dup2.txt').write_bytes(b'duplicate')
            (archive_path / 'dup3.txt').write_bytes(b'duplicate')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'test.txt'
            target_file.write_bytes(b'duplicate')

            with Processor() as processor:
                output = CollectingOutput()
                output.verbosity = 1
                output.showing_content_wise_duplicates = True
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path)

                    # Should find one record for the target file
                    self.assertGreater(len(output.data), 0)
                    # That record should mention multiple archive files
                    if output.data:
                        record = output.data[0]
                        record_text = ' '.join(str(item) for item in record)
                        # Should mention at least 2 of the duplicate files
                        matches = sum(1 for f in ['dup1.txt', 'dup2.txt', 'dup3.txt'] if f in record_text)
                        self.assertGreaterEqual(matches, 2)


class DirectoryDuplicateTest(unittest.TestCase):
    """Tests for directory-level duplicate detection."""

    def test_directory_exact_duplicate(self):
        """Detect exact duplicate directory with matching content and metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create directory in archive
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'file1.txt').write_bytes(b'content1')
            (archive_dir / 'file2.txt').write_bytes(b'content2')

            # Create identical directory in target
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'duplicate_dir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(archive_dir / 'file2.txt', target_file2)

            with Processor() as processor:
                output = CollectingOutput()
                output.verbosity = 1
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path, ignore=FileMetadataDifferencePattern.TRIVIAL)

                    # Should find directory duplicate
                    found_dir = any(
                        any('duplicate_dir/' in str(item) for item in record)
                        for record in output.data
                    )
                    self.assertTrue(found_dir)

    def test_directory_content_wise_duplicate(self):
        """Detect content-wise duplicate directory with different timestamps."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            archive_dir = archive_path / 'original'
            archive_dir.mkdir()
            (archive_dir / 'data.txt').write_bytes(b'data')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'copy'
            target_dir.mkdir()
            target_file = target_dir / 'data.txt'
            target_file.write_bytes(b'data')
            tweak_times(target_file, 3000000000)

            with Processor() as processor:
                output = CollectingOutput()
                output.verbosity = 1
                output.showing_content_wise_duplicates = True
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path)

                    # Should find content-wise directory duplicate
                    found = any(
                        any('copy/' in str(item) and 'content' in str(item) for item in record)
                        for record in output.data
                    )
                    self.assertTrue(found)

    def test_directory_with_nested_subdirs(self):
        """Detect duplicate directories containing nested subdirectories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create nested directory structure
            archive_dir = archive_path / 'parent'
            archive_dir.mkdir()
            nested = archive_dir / 'nested'
            nested.mkdir()
            (nested / 'deep.txt').write_bytes(b'deep content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'parent_copy'
            target_dir.mkdir()
            target_nested = target_dir / 'nested'
            target_nested.mkdir()
            target_file = target_nested / 'deep.txt'
            target_file.write_bytes(b'deep content')
            copy_times(nested / 'deep.txt', target_file)

            with Processor() as processor:
                output = CollectingOutput()
                output.verbosity = 1
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path, ignore=FileMetadataDifferencePattern.TRIVIAL)

                    # Should detect the duplicate directory structure
                    self.assertGreater(len(output.data), 0)

    def test_directory_with_symlinks(self):
        """Detect duplicate directories containing symbolic links."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            archive_dir = archive_path / 'linkdir'
            archive_dir.mkdir()
            (archive_dir / 'file.txt').write_bytes(b'content')
            (archive_dir / 'link').symlink_to('file.txt')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'linkdir_copy'
            target_dir.mkdir()
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', target_file)
            (target_dir / 'link').symlink_to('file.txt')

            with Processor() as processor:
                output = CollectingOutput()
                output.verbosity = 1
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    archive.find_duplicates(target_path, ignore=FileMetadataDifferencePattern.TRIVIAL)

                    # Should find the duplicate directory including symlinks
                    found = any(
                        any('linkdir_copy/' in str(item) for item in record)
                        for record in output.data
                    )
                    self.assertTrue(found)


if __name__ == '__main__':
    unittest.main()
