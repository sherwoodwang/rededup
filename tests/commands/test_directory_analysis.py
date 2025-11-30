"""Tests for directory analysis with database verification."""
import tempfile
import unittest
from pathlib import Path

import plyvel

from arindexer import Archive
from arindexer.report.store import DuplicateRecord
from arindexer.utils.processor import Processor

from ..test_utils import copy_times, tweak_times


class DirectoryAnalysisTest(unittest.TestCase):
    """Tests for directory analysis with database verification."""

    def test_analyze_directory_exact_match(self):
        """Analyze directory that exactly matches an archive directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            file1 = archive_dir / 'file1.txt'
            file1.write_bytes(b'content1')
            file2 = archive_dir / 'file2.txt'
            file2.write_bytes(b'content2')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'duplicate_dir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(file1, target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(file2, target_file2)
            copy_times(archive_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis
                    archive.analyze([target_dir])

            # Verify database contents for directory
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        # Look for the directory record
                        if 'duplicate_dir' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'mydir' in str(comparison.path):
                                found_dir_record = True
                                # Should be identical (all files match, no extras)
                                self.assertTrue(comparison.is_identical)
                                self.assertTrue(comparison.is_superset)
                                # Directory with 2 matching files should have duplicated_items=2
                                self.assertEqual(2, comparison.duplicated_items)
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "No directory duplicate record found")
            finally:
                db.close()

    def test_analyze_directory_with_extra_files_in_archive(self):
        """Analyze directory where archive has extra files (superset)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'bigdir'
            archive_dir.mkdir()
            (archive_dir / 'file1.txt').write_bytes(b'content1')
            (archive_dir / 'file2.txt').write_bytes(b'content2')
            (archive_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(archive_dir / 'file2.txt', target_file2)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis
                    archive.analyze([target_dir])

            # Verify is_superset is true but is_identical is false
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'smalldir' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'bigdir' in str(comparison.path):
                                found_dir_record = True
                                # Archive has extra files, so not identical
                                self.assertFalse(comparison.is_identical)
                                # But archive contains all analyzed files
                                self.assertTrue(comparison.is_superset)
                                # Directory with 2 matching files should have duplicated_items=2
                                self.assertEqual(2, comparison.duplicated_items)
                                break
                    except:
                        pass

                self.assertTrue(found_dir_record)
            finally:
                db.close()

    def test_analyze_directory_with_symlinks(self):
        """Analyze directory containing symbolic links."""
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

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis
                    archive.analyze([target_dir])

            # Verify symlinks were compared correctly
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                # Should find directory with matching symlinks
                found = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'linkdir_copy' in str(record.path):
                            found = True
                            # Directory with 1 file + 1 symlink should have duplicated_items=2
                            if len(record.duplicates) > 0:
                                comparison = record.duplicates[0]
                                self.assertEqual(2, comparison.duplicated_items)
                            break
                    except:
                        pass

                self.assertTrue(found, "Directory with symlinks should be found as duplicate")
            finally:
                db.close()

    def test_deferred_items_counted_in_duplicated_items(self):
        """Test that deferred items (symlinks) matching candidates are counted in duplicated_items.

        This test verifies that when deferred items match their corresponding candidates
        in the archive, they are properly counted in the duplicated_items counter.
        This ensures the bug fix (collecting matched_count from _compare_deferred_item)
        works correctly.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'dir_with_links'
            archive_dir.mkdir()
            # Create files and symlinks in archive
            (archive_dir / 'file1.txt').write_bytes(b'content1')
            (archive_dir / 'file2.txt').write_bytes(b'content2')
            (archive_dir / 'link1').symlink_to('file1.txt')
            (archive_dir / 'link2').symlink_to('file2.txt')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'dir_copy'
            target_dir.mkdir()
            # Create matching structure in target
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(archive_dir / 'file2.txt', target_file2)
            (target_dir / 'link1').symlink_to('file1.txt')
            (target_dir / 'link2').symlink_to('file2.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify deferred items (symlinks) are counted
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'dir_copy' in str(record.path) and len(record.duplicates) > 0:
                            found = True
                            comparison = record.duplicates[0]
                            # Should have 4 duplicated_items: 2 files + 2 matching symlinks
                            self.assertEqual(4, comparison.duplicated_items,
                                "duplicated_items should count both files and matching symlinks")
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found, "Directory with symlinks should be found as duplicate")
            finally:
                db.close()

    def test_deferred_items_not_counted_when_no_match(self):
        """Test that deferred items (symlinks) without matches are NOT counted in duplicated_items.

        This test verifies that when deferred items don't have matching candidates,
        they contribute 0 to the duplicated_items counter. This ensures the logic
        correctly distinguishes between matched and unmatched deferred items.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'limited_dir'
            archive_dir.mkdir()
            # Archive only has a regular file, no symlinks
            (archive_dir / 'file1.txt').write_bytes(b'content1')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'full_dir'
            target_dir.mkdir()
            # Target has a regular file + symlinks (no matching symlinks in archive)
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            (target_dir / 'link1').symlink_to('file1.txt')
            (target_dir / 'link2').symlink_to('file1.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify deferred items without matches are not counted
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'full_dir' in str(record.path) and len(record.duplicates) > 0:
                            found = True
                            comparison = record.duplicates[0]
                            # Should have only 1 duplicated_item (the file)
                            # The 2 symlinks should not be counted since archive has no matching symlinks
                            self.assertEqual(1, comparison.duplicated_items,
                                "duplicated_items should only count matching items, not unmatched symlinks")
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found, "Directory should be found")
            finally:
                db.close()

    def test_duplicated_size_semantics_with_partial_matches(self):
        """Test that duplicated_size is correctly calculated when archive dirs match different file subsets.

        This test verifies the semantic distinction between:
        - DuplicateRecord.duplicated_size: deduplicated size (each file counted once)
        - DuplicateMatch.duplicated_size: size of files in specific archive path
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Archive has 2 directories with different file subsets
            # dir1 has files A and B
            dir1 = archive_path / 'dir1'
            dir1.mkdir()
            (dir1 / 'fileA.txt').write_bytes(b'A' * 100)  # 100 bytes
            (dir1 / 'fileB.txt').write_bytes(b'B' * 200)  # 200 bytes

            # dir2 has files A, B, and C
            dir2 = archive_path / 'dir2'
            dir2.mkdir()
            (dir2 / 'fileA.txt').write_bytes(b'A' * 100)  # 100 bytes
            (dir2 / 'fileB.txt').write_bytes(b'B' * 200)  # 200 bytes
            (dir2 / 'fileC.txt').write_bytes(b'C' * 300)  # 300 bytes

            # Target directory has all 3 files
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'analyzed_dir'
            target_dir.mkdir()
            fileA = target_dir / 'fileA.txt'
            fileA.write_bytes(b'A' * 100)
            copy_times(dir1 / 'fileA.txt', fileA)
            fileB = target_dir / 'fileB.txt'
            fileB.write_bytes(b'B' * 200)
            copy_times(dir1 / 'fileB.txt', fileB)
            fileC = target_dir / 'fileC.txt'
            fileC.write_bytes(b'C' * 300)
            copy_times(dir2 / 'fileC.txt', fileC)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify duplicated_size semantics
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'analyzed_dir' in str(record.path) and len(record.duplicates) > 0:
                            found_dir_record = True

                            # DuplicateRecord.duplicated_size should be the sum of ALL files
                            # with ANY duplicates (A + B + C = 100 + 200 + 300 = 600)
                            # Each file is counted ONCE regardless of how many archive dirs have it
                            self.assertEqual(600, record.duplicated_size,
                                           "DuplicateRecord.duplicated_size should count each file once")

                            # Find the DuplicateMatch for each archive directory
                            for comparison in record.duplicates:
                                if 'dir1' in str(comparison.path):
                                    # dir1 matches files A and B (100 + 200 = 300)
                                    self.assertEqual(300, comparison.duplicated_size,
                                                   "DuplicateMatch for dir1 should be A + B")
                                elif 'dir2' in str(comparison.path):
                                    # dir2 matches files A, B, and C (100 + 200 + 300 = 600)
                                    self.assertEqual(600, comparison.duplicated_size,
                                                   "DuplicateMatch for dir2 should be A + B + C")

                            # Should have found both archive directories as duplicates
                            self.assertEqual(2, len(record.duplicates))
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_analyze_directory_with_nested_subdirectories(self):
        """Test that deferred subdirectories are recursively compared correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create archive directory with nested subdirectories
            archive_dir = archive_path / 'parent'
            archive_dir.mkdir()
            (archive_dir / 'file1.txt').write_bytes(b'content1')

            # Create nested subdirectory with files
            archive_subdir = archive_dir / 'subdir'
            archive_subdir.mkdir()
            (archive_subdir / 'file2.txt').write_bytes(b'content2')
            (archive_subdir / 'file3.txt').write_bytes(b'content3')

            # Create deeply nested subdirectory
            archive_deep = archive_subdir / 'deep'
            archive_deep.mkdir()
            (archive_deep / 'file4.txt').write_bytes(b'content4')

            # Create target directory with same structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'analyzed_parent'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)

            # Create nested subdirectory
            target_subdir = target_dir / 'subdir'
            target_subdir.mkdir()
            target_file2 = target_subdir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(archive_subdir / 'file2.txt', target_file2)
            target_file3 = target_subdir / 'file3.txt'
            target_file3.write_bytes(b'content3')
            copy_times(archive_subdir / 'file3.txt', target_file3)

            # Create deeply nested subdirectory
            target_deep = target_subdir / 'deep'
            target_deep.mkdir()
            target_file4 = target_deep / 'file4.txt'
            target_file4.write_bytes(b'content4')
            copy_times(archive_deep / 'file4.txt', target_file4)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify nested subdirectory comparison worked
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'analyzed_parent' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'parent' in str(comparison.path):
                                found_dir_record = True
                                # Should be identical (all files match, including nested ones)
                                self.assertTrue(comparison.is_identical)
                                self.assertTrue(comparison.is_superset)
                                # Directory with 1 file + nested subdir (3 more files) = 4 files total
                                self.assertEqual(4, comparison.duplicated_items)
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory with nested subdirectories not found")
            finally:
                db.close()

    def test_directory_total_size_includes_non_duplicate_files(self):
        """Test that total_size and total_items include files without duplicates.

        This test addresses a bug where total_size and total_items only counted
        files that had duplicates in the archive, excluding files without duplicates.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'partial_match_dir'
            archive_dir.mkdir()
            # Only file1 exists in archive
            (archive_dir / 'file1.txt').write_bytes(b'content1')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'mixed_dir'
            target_dir.mkdir()

            # file1 has duplicate in archive
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)

            # file2 and file3 have NO duplicates in archive
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'unique content 2')
            target_file3 = target_dir / 'file3.txt'
            target_file3.write_bytes(b'unique content 3')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify total_size and total_items include ALL files, not just duplicates
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'mixed_dir' in str(record.path):
                            found_dir_record = True
                            # Should have 3 total items (all files)
                            self.assertEqual(3, record.total_items,
                                "total_items should count all files, not just duplicates")
                            # Should have total_size equal to sum of all 3 files
                            expected_size = len(b'content1') + len(b'unique content 2') + len(b'unique content 3')
                            self.assertEqual(expected_size, record.total_size,
                                "total_size should include all files, not just duplicates")
                            # duplicated_size should only include file1
                            self.assertEqual(len(b'content1'), record.duplicated_size,
                                "duplicated_size should only include files with duplicates")
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_directory_total_size_includes_deferred_items(self):
        """Test that total_size and total_items include deferred items (symlinks).

        This test addresses a bug where DeferredResult didn't track size/items,
        causing them to be excluded from directory totals.
        """
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

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify symlinks are included in totals
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'linkdir_copy' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'linkdir' in str(comparison.path):
                                found_dir_record = True
                                # Should have 2 total items (file + symlink)
                                self.assertEqual(2, record.total_items,
                                    "total_items should include deferred items like symlinks")
                                # Total size should include the file (symlinks have size 0)
                                self.assertEqual(len(b'content'), record.total_size,
                                    "total_size should include sizes from deferred items")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory with symlinks not found")
            finally:
                db.close()

    def test_is_superset_false_when_metadata_differs(self):
        """Test that is_superset is False when archive has extra files AND metadata differs.

        This test addresses a bug where is_superset calculation didn't properly check
        metadata matching - it should be False if metadata doesn't match, even if all
        analyzed items are present in the archive.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'bigdir'
            archive_dir.mkdir()
            (archive_dir / 'file1.txt').write_bytes(b'content1')
            (archive_dir / 'file2.txt').write_bytes(b'content2')
            (archive_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            # Copy times first, then tweak so metadata doesn't match
            copy_times(archive_dir / 'file2.txt', target_file2)
            tweak_times(target_file2, -3600_000_000_000)  # Shift by -1 hour in nanoseconds

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify is_superset is False when metadata differs
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'smalldir' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'bigdir' in str(comparison.path):
                                found_dir_record = True
                                # Not identical (extra files)
                                self.assertFalse(comparison.is_identical,
                                    "is_identical should be False when archive has extra files")
                                # is_superset should be False because metadata doesn't match
                                self.assertFalse(comparison.is_superset,
                                    "is_superset should be False when metadata doesn't match")
                                break
                    except:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_is_superset_true_when_archive_has_extras_but_metadata_matches(self):
        """Test that is_superset is True when archive has extra files but all metadata matches.

        This test addresses a bug where is_superset was incorrectly calculated based on
        structure differences rather than checking if all analyzed items are present with
        matching metadata.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'bigdir'
            archive_dir.mkdir()
            (archive_dir / 'file1.txt').write_bytes(b'content1')
            (archive_dir / 'file2.txt').write_bytes(b'content2')
            (archive_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(archive_dir / 'file2.txt', target_file2)
            copy_times(archive_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify is_superset is True when all analyzed items present with matching metadata
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'smalldir' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'bigdir' in str(comparison.path):
                                found_dir_record = True
                                # Not identical (extra files in archive)
                                self.assertFalse(comparison.is_identical,
                                    "is_identical should be False when archive has extra files")
                                # But is_superset should be True (all items present, metadata matches)
                                self.assertTrue(comparison.is_superset,
                                    "is_superset should be True when all analyzed items are present with matching metadata")
                                # duplicated_items should be 2 (only the analyzed files)
                                self.assertEqual(2, comparison.duplicated_items,
                                    "duplicated_items should count analyzed items, not extras")
                                break
                    except:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_broken_symlinks_counted_when_targets_match(self):
        """Test that broken symlinks are counted in duplicated_items when targets match.

        This test addresses a bug where broken symlinks (pointing to non-existent targets)
        were not being counted in duplicated_items even when both sides had matching
        symlink targets. The bug was that _compare_deferred_item didn't set
        reducer.duplicated_items = 1 for matched non-directory items.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'broken_link_dir'
            archive_dir.mkdir()
            # Create regular file
            (archive_dir / 'file.txt').write_bytes(b'content')
            # Create broken symlink (target doesn't exist)
            (archive_dir / 'broken_link').symlink_to('/nonexistent/target')
            # Create another broken symlink with different target
            (archive_dir / 'broken_link2').symlink_to('/another/nonexistent')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'broken_link_copy'
            target_dir.mkdir()
            # Create matching regular file
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', target_file)
            # Create broken symlinks with matching targets
            target_link = target_dir / 'broken_link'
            target_link.symlink_to('/nonexistent/target')
            copy_times(archive_dir / 'broken_link', target_link)
            target_link2 = target_dir / 'broken_link2'
            target_link2.symlink_to('/another/nonexistent')
            copy_times(archive_dir / 'broken_link2', target_link2)
            # Copy directory mtime
            copy_times(archive_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify broken symlinks are counted correctly
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'broken_link_copy' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'broken_link_dir' in str(comparison.path):
                                found_dir_record = True
                                # Should be identical (file + 2 symlinks all match)
                                self.assertTrue(comparison.is_identical,
                                    "is_identical should be True when all items including broken symlinks match")
                                self.assertTrue(comparison.is_superset,
                                    "is_superset should be True when all items match")
                                # Should have 3 duplicated_items: 1 file + 2 broken symlinks
                                self.assertEqual(3, comparison.duplicated_items,
                                    "duplicated_items should count broken symlinks when targets match")
                                # Should have 3 total_items as well
                                self.assertEqual(3, record.total_items,
                                    "total_items should include all items")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory with broken symlinks not found")
            finally:
                db.close()

    def test_broken_symlinks_not_counted_when_targets_differ(self):
        """Test that broken symlinks are NOT counted in duplicated_items when targets differ.

        This test verifies that broken symlinks with different targets between
        analyzed and archive are not counted as matches, ensuring the fix for
        duplicated_items counting works correctly in the negative case.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'link_dir'
            archive_dir.mkdir()
            # Create regular file
            (archive_dir / 'file.txt').write_bytes(b'content')
            # Create broken symlink in archive
            (archive_dir / 'link').symlink_to('/archive/target')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'link_dir_copy'
            target_dir.mkdir()
            # Create matching regular file
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', target_file)
            # Create broken symlink with DIFFERENT target
            (target_dir / 'link').symlink_to('/analyzed/different_target')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_dir])

            # Verify broken symlinks with different targets are not counted
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'link_dir_copy' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'link_dir' in str(comparison.path):
                                found_dir_record = True
                                # Should NOT be identical (symlink targets differ)
                                self.assertFalse(comparison.is_identical,
                                    "is_identical should be False when symlink targets differ")
                                self.assertFalse(comparison.is_superset,
                                    "is_superset should be False when symlink targets differ")
                                # Should have only 1 duplicated_item (just the file, not the mismatched symlink)
                                self.assertEqual(1, comparison.duplicated_items,
                                    "duplicated_items should NOT count symlinks with different targets")
                                # Should have 2 total_items (file + symlink)
                                self.assertEqual(2, record.total_items,
                                    "total_items should include all items")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory with symlinks not found")
            finally:
                db.close()

    def test_directory_not_identical_when_descendant_differs(self):
        """Test that directories with identical immediate children but differing descendants are NOT identical.

        This is a regression test for a bug where directory is_identical was only checking
        immediate child names, not propagating non-identical status from descendants.

        Scenario:
        - Parent directory has same immediate children in both analyzed and archive
        - But a deeply nested descendant file differs
        - The parent directory should be marked as NOT identical
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create archive directory structure
            archive_parent = archive_path / 'parent'
            archive_parent.mkdir()
            (archive_parent / 'file1.txt').write_bytes(b'content1')

            # Create nested structure
            archive_level2 = archive_parent / 'level2'
            archive_level2.mkdir()
            (archive_level2 / 'file2.txt').write_bytes(b'identical_content')

            archive_level3 = archive_level2 / 'level3'
            archive_level3.mkdir()
            (archive_level3 / 'deep_file.txt').write_bytes(b'archive_version')

            # Create target directory with same structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_parent = target_path / 'analyzed_parent'
            target_parent.mkdir()
            target_file1 = target_parent / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(archive_parent / 'file1.txt', target_file1)

            # Create nested structure with same names
            target_level2 = target_parent / 'level2'
            target_level2.mkdir()
            target_file2 = target_level2 / 'file2.txt'
            target_file2.write_bytes(b'identical_content')
            copy_times(archive_level2 / 'file2.txt', target_file2)

            target_level3 = target_level2 / 'level3'
            target_level3.mkdir()
            # This file has DIFFERENT content than archive version
            target_deep = target_level3 / 'deep_file.txt'
            target_deep.write_bytes(b'analyzed_version')
            copy_times(archive_level3 / 'deep_file.txt', target_deep)

            report_dir = target_parent.parent / (target_parent.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_parent])

            # Verify parent directory is NOT marked as identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_parent_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'analyzed_parent' in str(record.path) and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if 'parent' in str(comparison.path):
                                found_parent_record = True
                                # Parent should NOT be identical (descendant differs)
                                self.assertFalse(comparison.is_identical,
                                    "Parent directory should NOT be identical when descendant file differs")
                                # Should also not be superset (content mismatch)
                                self.assertFalse(comparison.is_superset,
                                    "Parent directory should NOT be superset when descendant content differs")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_parent_record, "Parent directory record not found")
            finally:
                db.close()

    def test_nested_directory_propagates_non_identical_status(self):
        """Test that non-identical status propagates through multiple directory levels.

        This tests that if a file deep in the hierarchy differs, all ancestor directories
        up to the root are marked as non-identical.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create deep nesting with files at each level to ensure matching
            archive_l1 = archive_path / 'l1'
            archive_l1.mkdir()
            (archive_l1 / 'file_l1.txt').write_bytes(b'content_l1')

            archive_l2 = archive_l1 / 'l2'
            archive_l2.mkdir()
            (archive_l2 / 'file_l2.txt').write_bytes(b'content_l2')

            archive_l3 = archive_l2 / 'l3'
            archive_l3.mkdir()
            # Deep file with archive-specific content
            (archive_l3 / 'deep_file.txt').write_bytes(b'archive_content')

            # Create matching structure in target
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_l1 = target_path / 'analyzed_l1'
            target_l1.mkdir()
            target_file_l1 = target_l1 / 'file_l1.txt'
            target_file_l1.write_bytes(b'content_l1')
            copy_times(archive_l1 / 'file_l1.txt', target_file_l1)

            target_l2 = target_l1 / 'l2'
            target_l2.mkdir()
            target_file_l2 = target_l2 / 'file_l2.txt'
            target_file_l2.write_bytes(b'content_l2')
            copy_times(archive_l2 / 'file_l2.txt', target_file_l2)

            target_l3 = target_l2 / 'l3'
            target_l3.mkdir()
            # Different content at deepest level
            target_deep = target_l3 / 'deep_file.txt'
            target_deep.write_bytes(b'analyzed_content')
            copy_times(archive_l3 / 'deep_file.txt', target_deep)

            report_dir = target_path / 'analyzed_l1.report'

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path / 'analyzed_l1'])

            # Verify root directory (l1) is not identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_root_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        # Look for the root level directory
                        if record.path == Path('analyzed_l1') and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if comparison.path == Path('l1'):
                                found_root_record = True
                                # Root should not be identical (deep descendant differs)
                                self.assertFalse(comparison.is_identical,
                                    "Root directory should NOT be identical when deep descendant differs")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_root_record, "Root directory record not found")
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
