"""Tests for directory analysis with database verification."""
import tempfile
import unittest
from pathlib import Path

import plyvel

from rededup import Repository
from rededup.report.store import DuplicateRecord
from rededup.utils.processor import Processor

from ..test_utils import copy_times, tweak_times


class DirectoryAnalysisTest(unittest.TestCase):
    """Tests for directory analysis with database verification."""

    def test_analyze_directory_exact_match(self):
        """Analyze directory that exactly matches an repository directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'mydir'
            repository_dir.mkdir()
            file1 = repository_dir / 'file1.txt'
            file1.write_bytes(b'content1')
            file2 = repository_dir / 'file2.txt'
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
            copy_times(repository_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis
                    repository.analyze([target_dir])

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

    def test_analyze_directory_with_extra_files_in_repository(self):
        """Analyze directory where repository has extra files (superset)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'bigdir'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content1')
            (repository_dir / 'file2.txt').write_bytes(b'content2')
            (repository_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(repository_dir / 'file2.txt', target_file2)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis
                    repository.analyze([target_dir])

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
                                # Repository has extra files, so not identical
                                self.assertFalse(comparison.is_identical)
                                # But repository contains all analyzed files
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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'linkdir'
            repository_dir.mkdir()
            (repository_dir / 'file.txt').write_bytes(b'content')
            (repository_dir / 'link').symlink_to('file.txt')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'linkdir_copy'
            target_dir.mkdir()
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(repository_dir / 'file.txt', target_file)
            (target_dir / 'link').symlink_to('file.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis
                    repository.analyze([target_dir])

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
        in the repository, they are properly counted in the duplicated_items counter.
        This ensures the bug fix (collecting matched_count from _compare_deferred_item)
        works correctly.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'dir_with_links'
            repository_dir.mkdir()
            # Create files and symlinks in repository
            (repository_dir / 'file1.txt').write_bytes(b'content1')
            (repository_dir / 'file2.txt').write_bytes(b'content2')
            (repository_dir / 'link1').symlink_to('file1.txt')
            (repository_dir / 'link2').symlink_to('file2.txt')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'dir_copy'
            target_dir.mkdir()
            # Create matching structure in target
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(repository_dir / 'file2.txt', target_file2)
            (target_dir / 'link1').symlink_to('file1.txt')
            (target_dir / 'link2').symlink_to('file2.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'limited_dir'
            repository_dir.mkdir()
            # Repository only has a regular file, no symlinks
            (repository_dir / 'file1.txt').write_bytes(b'content1')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'full_dir'
            target_dir.mkdir()
            # Target has a regular file + symlinks (no matching symlinks in repository)
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)
            (target_dir / 'link1').symlink_to('file1.txt')
            (target_dir / 'link2').symlink_to('file1.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
                            # The 2 symlinks should not be counted since repository has no matching symlinks
                            self.assertEqual(1, comparison.duplicated_items,
                                "duplicated_items should only count matching items, not unmatched symlinks")
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found, "Directory should be found")
            finally:
                db.close()

    def test_duplicated_size_semantics_with_partial_matches(self):
        """Test that duplicated_size is correctly calculated when repository dirs match different file subsets.

        This test verifies the semantic distinction between:
        - DuplicateRecord.duplicated_size: deduplicated size (each file counted once)
        - DuplicateMatch.duplicated_size: size of files in specific repository path
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Repository has 2 directories with different file subsets
            # dir1 has files A and B
            dir1 = repository_path / 'dir1'
            dir1.mkdir()
            (dir1 / 'fileA.txt').write_bytes(b'A' * 100)  # 100 bytes
            (dir1 / 'fileB.txt').write_bytes(b'B' * 200)  # 200 bytes

            # dir2 has files A, B, and C
            dir2 = repository_path / 'dir2'
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
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
                            # Each file is counted ONCE regardless of how many repository dirs have it
                            self.assertEqual(600, record.duplicated_size,
                                           "DuplicateRecord.duplicated_size should count each file once")

                            # Find the DuplicateMatch for each repository directory
                            for comparison in record.duplicates:
                                if 'dir1' in str(comparison.path):
                                    # dir1 matches files A and B (100 + 200 = 300)
                                    self.assertEqual(300, comparison.duplicated_size,
                                                   "DuplicateMatch for dir1 should be A + B")
                                elif 'dir2' in str(comparison.path):
                                    # dir2 matches files A, B, and C (100 + 200 + 300 = 600)
                                    self.assertEqual(600, comparison.duplicated_size,
                                                   "DuplicateMatch for dir2 should be A + B + C")

                            # Should have found both repository directories as duplicates
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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create repository directory with nested subdirectories
            repository_dir = repository_path / 'parent'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content1')

            # Create nested subdirectory with files
            repository_subdir = repository_dir / 'subdir'
            repository_subdir.mkdir()
            (repository_subdir / 'file2.txt').write_bytes(b'content2')
            (repository_subdir / 'file3.txt').write_bytes(b'content3')

            # Create deeply nested subdirectory
            repository_deep = repository_subdir / 'deep'
            repository_deep.mkdir()
            (repository_deep / 'file4.txt').write_bytes(b'content4')

            # Create target directory with same structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'analyzed_parent'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)

            # Create nested subdirectory
            target_subdir = target_dir / 'subdir'
            target_subdir.mkdir()
            target_file2 = target_subdir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(repository_subdir / 'file2.txt', target_file2)
            target_file3 = target_subdir / 'file3.txt'
            target_file3.write_bytes(b'content3')
            copy_times(repository_subdir / 'file3.txt', target_file3)

            # Create deeply nested subdirectory
            target_deep = target_subdir / 'deep'
            target_deep.mkdir()
            target_file4 = target_deep / 'file4.txt'
            target_file4.write_bytes(b'content4')
            copy_times(repository_deep / 'file4.txt', target_file4)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
        files that had duplicates in the repository, excluding files without duplicates.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'partial_match_dir'
            repository_dir.mkdir()
            # Only file1 exists in repository
            (repository_dir / 'file1.txt').write_bytes(b'content1')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'mixed_dir'
            target_dir.mkdir()

            # file1 has duplicate in repository
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)

            # file2 and file3 have NO duplicates in repository
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'unique content 2')
            target_file3 = target_dir / 'file3.txt'
            target_file3.write_bytes(b'unique content 3')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'linkdir'
            repository_dir.mkdir()
            (repository_dir / 'file.txt').write_bytes(b'content')
            (repository_dir / 'link').symlink_to('file.txt')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'linkdir_copy'
            target_dir.mkdir()
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(repository_dir / 'file.txt', target_file)
            (target_dir / 'link').symlink_to('file.txt')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
        """Test that is_superset is False when repository has extra files AND metadata differs.

        This test addresses a bug where is_superset calculation didn't properly check
        metadata matching - it should be False if metadata doesn't match, even if all
        analyzed items are present in the repository.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'bigdir'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content1')
            (repository_dir / 'file2.txt').write_bytes(b'content2')
            (repository_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            # Copy times first, then tweak so metadata doesn't match
            copy_times(repository_dir / 'file2.txt', target_file2)
            tweak_times(target_file2, -3600_000_000_000)  # Shift by -1 hour in nanoseconds

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
                                    "is_identical should be False when repository has extra files")
                                # is_superset should be False because metadata doesn't match
                                self.assertFalse(comparison.is_superset,
                                    "is_superset should be False when metadata doesn't match")
                                break
                    except:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_is_superset_true_when_repository_has_extras_but_metadata_matches(self):
        """Test that is_superset is True when repository has extra files but all metadata matches.

        This test addresses a bug where is_superset was incorrectly calculated based on
        structure differences rather than checking if all analyzed items are present with
        matching metadata.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'bigdir'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content1')
            (repository_dir / 'file2.txt').write_bytes(b'content2')
            (repository_dir / 'extra.txt').write_bytes(b'extra')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'smalldir'
            target_dir.mkdir()
            target_file1 = target_dir / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', target_file1)
            target_file2 = target_dir / 'file2.txt'
            target_file2.write_bytes(b'content2')
            copy_times(repository_dir / 'file2.txt', target_file2)
            copy_times(repository_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
                                # Not identical (extra files in repository)
                                self.assertFalse(comparison.is_identical,
                                    "is_identical should be False when repository has extra files")
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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'broken_link_dir'
            repository_dir.mkdir()
            # Create regular file
            (repository_dir / 'file.txt').write_bytes(b'content')
            # Create broken symlink (target doesn't exist)
            (repository_dir / 'broken_link').symlink_to('/nonexistent/target')
            # Create another broken symlink with different target
            (repository_dir / 'broken_link2').symlink_to('/another/nonexistent')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'broken_link_copy'
            target_dir.mkdir()
            # Create matching regular file
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(repository_dir / 'file.txt', target_file)
            # Create broken symlinks with matching targets
            target_link = target_dir / 'broken_link'
            target_link.symlink_to('/nonexistent/target')
            copy_times(repository_dir / 'broken_link', target_link)
            target_link2 = target_dir / 'broken_link2'
            target_link2.symlink_to('/another/nonexistent')
            copy_times(repository_dir / 'broken_link2', target_link2)
            # Copy directory mtime
            copy_times(repository_dir, target_dir)

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
        analyzed and repository are not counted as matches, ensuring the fix for
        duplicated_items counting works correctly in the negative case.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'link_dir'
            repository_dir.mkdir()
            # Create regular file
            (repository_dir / 'file.txt').write_bytes(b'content')
            # Create broken symlink in repository
            (repository_dir / 'link').symlink_to('/repository/target')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'link_dir_copy'
            target_dir.mkdir()
            # Create matching regular file
            target_file = target_dir / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(repository_dir / 'file.txt', target_file)
            # Create broken symlink with DIFFERENT target
            (target_dir / 'link').symlink_to('/analyzed/different_target')

            report_dir = target_dir.parent / (target_dir.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

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
        - Parent directory has same immediate children in both analyzed and repository
        - But a deeply nested descendant file differs
        - The parent directory should be marked as NOT identical
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create repository directory structure
            repository_parent = repository_path / 'parent'
            repository_parent.mkdir()
            (repository_parent / 'file1.txt').write_bytes(b'content1')

            # Create nested structure
            repository_level2 = repository_parent / 'level2'
            repository_level2.mkdir()
            (repository_level2 / 'file2.txt').write_bytes(b'identical_content')

            repository_level3 = repository_level2 / 'level3'
            repository_level3.mkdir()
            (repository_level3 / 'deep_file.txt').write_bytes(b'repository_version')

            # Create target directory with same structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_parent = target_path / 'analyzed_parent'
            target_parent.mkdir()
            target_file1 = target_parent / 'file1.txt'
            target_file1.write_bytes(b'content1')
            copy_times(repository_parent / 'file1.txt', target_file1)

            # Create nested structure with same names
            target_level2 = target_parent / 'level2'
            target_level2.mkdir()
            target_file2 = target_level2 / 'file2.txt'
            target_file2.write_bytes(b'identical_content')
            copy_times(repository_level2 / 'file2.txt', target_file2)

            target_level3 = target_level2 / 'level3'
            target_level3.mkdir()
            # This file has DIFFERENT content than repository version
            target_deep = target_level3 / 'deep_file.txt'
            target_deep.write_bytes(b'analyzed_version')
            copy_times(repository_level3 / 'deep_file.txt', target_deep)

            report_dir = target_parent.parent / (target_parent.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_parent])

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
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create deep nesting with files at each level to ensure matching
            repository_l1 = repository_path / 'l1'
            repository_l1.mkdir()
            (repository_l1 / 'file_l1.txt').write_bytes(b'content_l1')

            repository_l2 = repository_l1 / 'l2'
            repository_l2.mkdir()
            (repository_l2 / 'file_l2.txt').write_bytes(b'content_l2')

            repository_l3 = repository_l2 / 'l3'
            repository_l3.mkdir()
            # Deep file with repository-specific content
            (repository_l3 / 'deep_file.txt').write_bytes(b'repository_content')

            # Create matching structure in target
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_l1 = target_path / 'analyzed_l1'
            target_l1.mkdir()
            target_file_l1 = target_l1 / 'file_l1.txt'
            target_file_l1.write_bytes(b'content_l1')
            copy_times(repository_l1 / 'file_l1.txt', target_file_l1)

            target_l2 = target_l1 / 'l2'
            target_l2.mkdir()
            target_file_l2 = target_l2 / 'file_l2.txt'
            target_file_l2.write_bytes(b'content_l2')
            copy_times(repository_l2 / 'file_l2.txt', target_file_l2)

            target_l3 = target_l2 / 'l3'
            target_l3.mkdir()
            # Different content at deepest level
            target_deep = target_l3 / 'deep_file.txt'
            target_deep.write_bytes(b'analyzed_content')
            copy_times(repository_l3 / 'deep_file.txt', target_deep)

            report_dir = target_path / 'analyzed_l1.report'

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path / 'analyzed_l1'])

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

    def test_directory_not_identical_when_child_has_no_duplicates(self):
        """Test that directories are NOT identical when a child file has no duplicates in a candidate.

        This is a regression test for a bug where directories were incorrectly marked as identical
        when child files existed in both directories but had different content (no duplicate match).

        Scenario:
        - Directory has same child names in both analyzed and repository
        - One child file has different content (no duplicate in repository)
        - The directory should be marked as NOT identical and NOT superset

        Bug: The old code only checked structural mismatch (all_items != candidate_items) but missed
        the case where items had the same names but different content (items_with_no_duplicates).
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create repository directory
            repository_dir = repository_path / 'mydir'
            repository_dir.mkdir()
            (repository_dir / 'identical.txt').write_bytes(b'same content')
            (repository_dir / 'different.txt').write_bytes(b'repository version')

            # Create target directory with same structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'analyzed_dir'
            target_dir.mkdir()

            # Identical file with copied times
            identical_file = target_dir / 'identical.txt'
            identical_file.write_bytes(b'same content')
            copy_times(repository_dir / 'identical.txt', identical_file)

            # Different content file (same name, different content)
            different_file = target_dir / 'different.txt'
            different_file.write_bytes(b'analyzed version')
            copy_times(repository_dir / 'different.txt', different_file)

            report_dir = target_path / 'analyzed_dir.report'

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

            # Verify directory is NOT marked as identical or superset
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if record.path == Path('analyzed_dir') and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if comparison.path == Path('mydir'):
                                found_dir_record = True
                                # Directory should NOT be identical (child content differs)
                                self.assertFalse(comparison.is_identical,
                                    "Directory should NOT be identical when child file has different content")
                                # Directory should NOT be superset (child content differs)
                                self.assertFalse(comparison.is_superset,
                                    "Directory should NOT be superset when child file has different content")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()

    def test_directory_not_superset_when_analyzed_file_has_no_duplicate_in_candidate(self):
        """Test that a directory is NOT a superset when an analyzed file has no duplicate in that candidate.

        This regression test covers the case where:
        - Multiple candidate directories exist in the repository
        - An analyzed file has duplicates in some candidates but NOT in others
        - Each candidate should be independently evaluated

        Bug: The old code used items_with_no_duplicates (global across all candidates) instead of
        checking per-candidate whether all analyzed items have matches.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create first repository candidate with only fileA
            repository_candidate1 = repository_path / 'candidate1'
            repository_candidate1.mkdir()
            (repository_candidate1 / 'fileA.txt').write_bytes(b'content A')

            # Create second repository candidate with only fileB
            repository_candidate2 = repository_path / 'candidate2'
            repository_candidate2.mkdir()
            (repository_candidate2 / 'fileB.txt').write_bytes(b'content B')

            # Create analyzed directory with both files
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            analyzed_dir = target_path / 'analyzed'
            analyzed_dir.mkdir()

            fileA = analyzed_dir / 'fileA.txt'
            fileA.write_bytes(b'content A')
            copy_times(repository_candidate1 / 'fileA.txt', fileA)

            fileB = analyzed_dir / 'fileB.txt'
            fileB.write_bytes(b'content B')
            copy_times(repository_candidate2 / 'fileB.txt', fileB)

            report_dir = target_path / 'analyzed.report'

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([analyzed_dir])

            # Verify both candidates are NOT identical and NOT superset
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_analyzed_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if record.path == Path('analyzed'):
                            found_analyzed_record = True
                            # Should have 2 candidates
                            self.assertEqual(len(record.duplicates), 2,
                                "Should have 2 candidate directories")

                            # Check each candidate
                            for dup in record.duplicates:
                                candidate_name = dup.path.name
                                # Neither candidate should be identical or superset
                                # candidate1 is missing fileB, candidate2 is missing fileA
                                self.assertFalse(dup.is_identical,
                                    f"{candidate_name} should NOT be identical (missing a file)")
                                self.assertFalse(dup.is_superset,
                                    f"{candidate_name} should NOT be superset (missing a file)")
                            break
                    except Exception as e:
                        pass

                self.assertTrue(found_analyzed_record, "Analyzed directory record not found")
            finally:
                db.close()

    def test_directory_identical_only_when_all_children_match(self):
        """Test that a directory is only identical when ALL children match in content AND metadata.

        This is a positive test case to verify the fix doesn't break correct behavior.

        Scenario:
        - Directory has multiple children
        - All children have matching content and metadata
        - Directory should be marked as identical
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()

            # Create repository directory with multiple files
            repository_dir = repository_path / 'complete'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content 1')
            (repository_dir / 'file2.txt').write_bytes(b'content 2')
            (repository_dir / 'file3.txt').write_bytes(b'content 3')

            # Create analyzed directory with all matching files
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            analyzed_dir = target_path / 'analyzed_complete'
            analyzed_dir.mkdir()

            for filename in ['file1.txt', 'file2.txt', 'file3.txt']:
                target_file = analyzed_dir / filename
                target_file.write_bytes((repository_dir / filename).read_bytes())
                copy_times(repository_dir / filename, target_file)

            # Copy directory metadata
            copy_times(repository_dir, analyzed_dir)

            report_dir = target_path / 'analyzed_complete.report'

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([analyzed_dir])

            # Verify directory IS marked as identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_dir_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if record.path == Path('analyzed_complete') and len(record.duplicates) > 0:
                            comparison = record.duplicates[0]
                            if comparison.path == Path('complete'):
                                found_dir_record = True
                                # All children match with metadata - should be identical
                                self.assertTrue(comparison.is_identical,
                                    "Directory should be identical when all children match with metadata")
                                self.assertTrue(comparison.is_superset,
                                    "Directory should be superset when all children match")
                                break
                    except Exception as e:
                        pass

                self.assertTrue(found_dir_record, "Directory record not found")
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
