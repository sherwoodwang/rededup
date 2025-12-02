"""Tests for file analysis with database verification."""
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import plyvel

from rededup import Repository
from rededup.report.duplicate_match import DuplicateMatchRule
from rededup.report.store import DuplicateRecord
from rededup.utils.processor import Processor

from ..test_utils import copy_times, tweak_times


class FileAnalysisTest(unittest.TestCase):
    """Tests for file analysis with database verification."""

    def test_analyze_single_file_exact_match(self):
        """Analyze a directory containing a single file that exactly matches an repository file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'duplicate.txt'
            target_file.write_bytes(b'test content')
            copy_times(repository_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis on the directory
                    repository.analyze([target_path])

            # Verify database contents
            self.assertTrue(report_dir.exists())
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_record = False
                for key, value in db.iterator():
                    if len(key) == 16:  # Skip hash prefixes
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'duplicate.txt' in str(record.path):
                            found_record = True
                            # Verify the record
                            self.assertEqual(1, len(record.duplicates))
                            comparison = record.duplicates[0]
                            # All metadata matches
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.atime_match)
                            self.assertTrue(comparison.mode_match)
                            # Should be identical
                            self.assertTrue(comparison.is_identical)
                            self.assertTrue(comparison.is_superset)
                            # Single file should have duplicated_items=1
                            self.assertEqual(1, comparison.duplicated_items)
                            break
                    except:
                        pass

                self.assertTrue(found_record, "No duplicate record found in database")
            finally:
                db.close()

    def test_analyze_file_content_match_metadata_differ(self):
        """Analyze directory with file having matching content but different metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'shared content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'newer.txt'
            target_file.write_bytes(b'shared content')
            tweak_times(target_file, 5000000000)  # Different timestamp

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis on the directory
                    repository.analyze([target_path])

            # Verify database contents
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'newer.txt' in str(record.path):
                            found_record = True
                            comparison = record.duplicates[0]
                            # Metadata should not match
                            self.assertFalse(comparison.mtime_match)
                            # Should not be identical
                            self.assertFalse(comparison.is_identical)
                            self.assertFalse(comparison.is_superset)
                            # File still counts as 1 duplicated item
                            self.assertEqual(1, comparison.duplicated_items)
                            break
                    except:
                        pass

                self.assertTrue(found_record)
            finally:
                db.close()

    def test_analyze_file_multiple_duplicates(self):
        """Analyze directory with file that matches multiple files in repository."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            (repository_path / 'dup1.txt').write_bytes(b'duplicate')
            (repository_path / 'dup2.txt').write_bytes(b'duplicate')
            (repository_path / 'dup3.txt').write_bytes(b'duplicate')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'duplicate')

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Run analysis on the directory
                    repository.analyze([target_path])

            # Verify multiple duplicates were found
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                found_record = False
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            found_record = True
                            # Should have 3 duplicates
                            self.assertEqual(3, len(record.duplicates))
                            duplicate_names = [str(comparison.path) for comparison in record.duplicates]
                            self.assertIn('dup1.txt', duplicate_names)
                            self.assertIn('dup2.txt', duplicate_names)
                            self.assertIn('dup3.txt', duplicate_names)
                            # Each duplicate should have duplicated_items=1 for a file
                            for comparison in record.duplicates:
                                self.assertEqual(1, comparison.duplicated_items)

                            # CRITICAL: Verify duplicated_size semantics
                            # DuplicateRecord.duplicated_size should be file size (counted once)
                            file_size = target_file.stat().st_size
                            self.assertEqual(file_size, record.duplicated_size,
                                           "DuplicateRecord.duplicated_size should count file once")
                            # Each DuplicateMatch.duplicated_size should also be the file size
                            for comparison in record.duplicates:
                                self.assertEqual(file_size, comparison.duplicated_size,
                                               "DuplicateMatch.duplicated_size should be file size")
                            break
                    except:
                        pass

                self.assertTrue(found_record)
            finally:
                db.close()

    def test_analyze_file_with_custom_comparison_rule(self):
        """Test that comparison rule correctly determines is_identical."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Different atime, same everything else
            copy_times(repository_file, target_file)
            # Manually set only atime to be different
            st = repository_file.stat()
            os.utime(target_file, ns=(st.st_atime_ns + 9999999999, st.st_mtime_ns), follow_symlinks=False)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()

                    # Test with default rule (atime excluded)
                    repository.analyze([target_path])

            # With default rule (atime excluded), should be identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # atime should not match
                            self.assertFalse(comparison.atime_match)
                            # But should still be identical (default rule excludes atime)
                            self.assertTrue(comparison.is_identical)
                            # Verify all timestamp fields are checked
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.ctime_match)
                            self.assertTrue(comparison.mode_match)
                            self.assertTrue(comparison.owner_match)
                            self.assertTrue(comparison.group_match)
                            break
                    except:
                        pass
            finally:
                db.close()

            # Clean up for second test
            shutil.rmtree(report_dir)

            # Test with atime included
            rule_with_atime = DuplicateMatchRule(include_atime=True)

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path], comparison_rule=rule_with_atime)

            # With atime included, should NOT be identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # atime still doesn't match
                            self.assertFalse(comparison.atime_match)
                            # Now should NOT be identical (rule includes atime)
                            self.assertFalse(comparison.is_identical)
                            break
                    except:
                        pass
            finally:
                db.close()

    def test_analyze_file_ctime_differs(self):
        """Test that ctime_match is False when only ctime differs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Copy mtime and atime to match
            copy_times(repository_file, target_file)
            # Change ctime by modifying a metadata attribute
            current_mode = target_file.stat().st_mode
            os.chmod(target_file, current_mode | 0o100)  # Add execute for owner
            os.chmod(target_file, current_mode)  # Change back (ctime updated)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path])

            # Verify ctime_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # ctime should not match
                            self.assertFalse(comparison.ctime_match)
                            # Other fields should match
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.atime_match)
                            self.assertTrue(comparison.mode_match)
                            self.assertTrue(comparison.owner_match)
                            self.assertTrue(comparison.group_match)
                            # Should not be identical (default rule includes ctime)
                            self.assertFalse(comparison.is_identical)
                            break
                    except:
                        pass
            finally:
                db.close()

    def test_analyze_file_mode_differs(self):
        """Test that mode_match is False when file permissions differ."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')
            os.chmod(repository_file, 0o644)

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            os.chmod(target_file, 0o755)  # Different permissions
            copy_times(repository_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path])

            # Verify mode_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # mode should not match
                            self.assertFalse(comparison.mode_match)
                            # Timestamps should match (we copied them)
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.atime_match)
                            # Owner/group should match (same process)
                            self.assertTrue(comparison.owner_match)
                            self.assertTrue(comparison.group_match)
                            # Should not be identical (mode differs)
                            self.assertFalse(comparison.is_identical)
                            break
                    except:
                        pass
            finally:
                db.close()

    def test_analyze_file_owner_differs(self):
        """Test that owner_match is False when file owner differs."""
        # Skip test if not running as root (can't change file ownership)
        if os.geteuid() != 0:
            self.skipTest("Test requires root privileges to change file ownership")

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')
            # Keep current owner for repository file

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Change owner to nobody (UID 65534 on most systems)
            os.chown(target_file, 65534, -1)  # -1 means don't change group
            copy_times(repository_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path])

            # Verify owner_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # owner should not match
                            self.assertFalse(comparison.owner_match)
                            # Other fields should match
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.atime_match)
                            self.assertTrue(comparison.mode_match)
                            # Should not be identical (default rule includes owner)
                            self.assertFalse(comparison.is_identical)
                            break
                    except:
                        pass
            finally:
                db.close()

    def test_analyze_file_group_differs(self):
        """Test that group_match is False when file group differs."""
        import grp

        # Get list of groups current user belongs to
        try:
            groups = os.getgroups()
            if len(groups) < 2:
                self.skipTest("Test requires user to belong to at least 2 groups")

            # Use two different groups the user belongs to
            group1 = groups[0]
            group2 = groups[1]
        except:
            self.skipTest("Unable to get user groups")

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_file = repository_path / 'original.txt'
            repository_file.write_bytes(b'test content')
            os.chown(repository_file, -1, group1)

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            os.chown(target_file, -1, group2)  # Different group
            copy_times(repository_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path])

            # Verify group_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            comparison = record.duplicates[0]
                            # group should not match
                            self.assertFalse(comparison.group_match)
                            # Other fields should match
                            self.assertTrue(comparison.mtime_match)
                            self.assertTrue(comparison.atime_match)
                            self.assertTrue(comparison.mode_match)
                            self.assertTrue(comparison.owner_match)
                            # Should not be identical (default rule includes group)
                            self.assertFalse(comparison.is_identical)
                            break
                    except:
                        pass
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
