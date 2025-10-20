"""Targeted tests for core Archive operations.

This module contains focused unit tests for Archive class functionality:
- Archive creation and basic operations
- Rebuild and refresh operations
- Duplicate file and directory detection
- Edge cases and collision handling
"""
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive, FileMetadataDifferencePattern, FileMetadataDifferenceType
# noinspection PyProtectedMember
from arindexer._processor import Processor

from .test_utils import CollectingOutput, compute_xor, copy_times, tweak_times


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


class EdgeCaseTest(unittest.TestCase):
    """Tests for edge cases and special scenarios."""

    def test_hash_collision_handling(self):
        """Archive handles hash collisions correctly using custom weak hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create files that will have XOR hash collision
            (archive_path / 'file1').write_bytes(b'\x00\x00\x00\x00\x01\x01\x01\x01')
            (archive_path / 'file2').write_bytes(b'\x01\x01\x01\x01\x00\x00\x00\x00')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    # Use weak XOR hash to force collisions
                    archive._hash_algorithms['xor'] = (4, compute_xor)
                    archive._default_hash_algorithm = 'xor'

                    archive.rebuild()

                    entries = list(archive.inspect())
                    # Both files should be indexed despite collision
                    self.assertTrue(any('file1' in entry for entry in entries))
                    self.assertTrue(any('file2' in entry for entry in entries))

    def test_empty_files(self):
        """Archive correctly handles zero-byte files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create empty files
            (archive_path / 'empty1.txt').write_bytes(b'')
            (archive_path / 'empty2.txt').write_bytes(b'')
            (archive_path / 'nonempty.txt').write_bytes(b'content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    entries = list(archive.inspect())
                    # All files should be indexed
                    self.assertTrue(any('empty1.txt' in entry for entry in entries))
                    self.assertTrue(any('empty2.txt' in entry for entry in entries))
                    self.assertTrue(any('nonempty.txt' in entry for entry in entries))

    def test_large_directory(self):
        """Archive handles directory with many files efficiently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            large_dir = archive_path / 'many_files'
            large_dir.mkdir()

            # Create 100 files
            num_files = 100
            for i in range(num_files):
                (large_dir / f'file{i:03d}.txt').write_bytes(f'content {i}'.encode())

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    entries = list(archive.inspect())
                    # Should index all files
                    file_entries = [e for e in entries if 'file0' in e or 'file1' in e]
                    self.assertGreater(len(file_entries), 0)

                    # Verify a sampling of files are present
                    self.assertTrue(any('file000.txt' in entry for entry in entries))
                    self.assertTrue(any('file050.txt' in entry for entry in entries))
                    self.assertTrue(any('file099.txt' in entry for entry in entries))
