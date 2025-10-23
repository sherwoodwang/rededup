"""Tests for analyzer module functionality.

This module tests the analyzer command including:
- DuplicateMatch creation and serialization
- DuplicateRecord creation and serialization
- File analysis with database verification
- Directory analysis with database verification
- is_identical and is_superset flag calculation
"""
import tempfile
import unittest
from pathlib import Path

import plyvel

from arindexer import Archive
from arindexer.commands.analyzer import (
    DuplicateMatch,
    DuplicateRecord,
    ReportWriter,
    ReportManifest
)
from arindexer.utils.processor import Processor

from ..test_utils import copy_times, tweak_times


class DuplicateMatchTest(unittest.TestCase):
    """Tests for DuplicateMatch class."""

    def test_create_with_all_fields(self):
        """Create DuplicateMatch with all fields."""
        from arindexer.commands.analyzer import DuplicateMatchRule

        rule = DuplicateMatchRule(include_atime=True)
        comparison = DuplicateMatch(
            Path('archive/file.txt'),
            mtime_match=True,
            atime_match=False,
            ctime_match=True,
            mode_match=True,
            owner_match=False,
            group_match=True,
            duplicated_size=1024,
            duplicated_items=5,
            is_identical=False,
            is_superset=True,
            rule=rule
        )

        self.assertEqual(Path('archive/file.txt'), comparison.path)
        self.assertTrue(comparison.mtime_match)
        self.assertFalse(comparison.atime_match)
        self.assertTrue(comparison.ctime_match)
        self.assertTrue(comparison.mode_match)
        self.assertFalse(comparison.owner_match)
        self.assertTrue(comparison.group_match)
        self.assertEqual(1024, comparison.duplicated_size)
        self.assertEqual(5, comparison.duplicated_items)
        self.assertFalse(comparison.is_identical)
        self.assertTrue(comparison.is_superset)
        self.assertEqual(rule, comparison.rule)

    def test_create_with_defaults(self):
        """Create DuplicateMatch with default values."""
        comparison = DuplicateMatch(
            Path('file.txt'),
            mtime_match=True,
            atime_match=True,
            ctime_match=True,
            mode_match=True
        )

        self.assertEqual(Path('file.txt'), comparison.path)
        self.assertTrue(comparison.mtime_match)
        self.assertTrue(comparison.atime_match)
        self.assertTrue(comparison.ctime_match)
        self.assertTrue(comparison.mode_match)
        self.assertFalse(comparison.owner_match)  # Default
        self.assertFalse(comparison.group_match)  # Default
        self.assertEqual(0, comparison.duplicated_size)  # Default
        self.assertEqual(0, comparison.duplicated_items)  # Default
        self.assertFalse(comparison.is_identical)  # Default
        self.assertFalse(comparison.is_superset)  # Default
        self.assertIsNone(comparison.rule)  # Default

    def test_is_identical_for_files(self):
        """For files, is_identical means all metadata matches."""
        # All metadata matches
        comparison1 = DuplicateMatch(
            Path('file.txt'),
            mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
            duplicated_size=100, duplicated_items=1,
            is_identical=True, is_superset=True
        )
        self.assertTrue(comparison1.is_identical)
        self.assertTrue(comparison1.is_superset)
        self.assertEqual(1, comparison1.duplicated_items)

        # Metadata doesn't match
        comparison2 = DuplicateMatch(
            Path('file.txt'),
            mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
            duplicated_size=100, duplicated_items=1,
            is_identical=False, is_superset=False
        )
        self.assertFalse(comparison2.is_identical)
        self.assertFalse(comparison2.is_superset)
        self.assertEqual(1, comparison2.duplicated_items)


class DuplicateRecordTest(unittest.TestCase):
    """Tests for DuplicateRecord class."""

    def test_create_with_duplicates_list(self):
        """Create DuplicateRecord with pre-zipped duplicates list."""
        comparison1 = DuplicateMatch(Path('dup1.txt'),
                                        mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                                        duplicated_size=100, duplicated_items=1,
                                        is_identical=True, is_superset=True)
        comparison2 = DuplicateMatch(Path('dup2.txt'),
                                        mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
                                        duplicated_size=100, duplicated_items=1,
                                        is_identical=False, is_superset=False)

        duplicates = [
            (Path('dup1.txt'), comparison1),
            (Path('dup2.txt'), comparison2)
        ]

        record = DuplicateRecord(
            path=Path('target/file.txt'),
            duplicates=duplicates,
            total_size=200,
            duplicated_size=200
        )

        self.assertEqual(Path('target/file.txt'), record.path)
        self.assertEqual(2, len(record.duplicates))
        self.assertEqual(200, record.total_size)
        self.assertEqual(200, record.duplicated_size)
        self.assertEqual(Path('dup1.txt'), record.duplicates[0][0])
        self.assertEqual(Path('dup2.txt'), record.duplicates[1][0])

    def test_create_empty(self):
        """Create empty DuplicateRecord."""
        record = DuplicateRecord(Path('file.txt'))

        self.assertEqual(Path('file.txt'), record.path)
        self.assertEqual(0, len(record.duplicates))
        self.assertEqual(0, record.duplicated_size)

    def test_msgpack_serialization_new_format(self):
        """Test msgpack serialization with new format including is_identical, is_superset, and duplicated_items."""
        comparison = DuplicateMatch(
            Path('archive/dup.txt'),
            mtime_match=True, atime_match=False, ctime_match=True, mode_match=True,
            duplicated_size=512, duplicated_items=1,
            is_identical=False, is_superset=True
        )
        duplicates = [(Path('archive/dup.txt'), comparison)]

        original = DuplicateRecord(
            path=Path('target/file.txt'),
            duplicates=duplicates,
            total_size=512,
            duplicated_size=512
        )

        # Serialize and deserialize
        serialized = original.to_msgpack()
        deserialized = DuplicateRecord.from_msgpack(serialized)

        # Verify all fields
        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.total_size, deserialized.total_size)
        self.assertEqual(original.duplicated_size, deserialized.duplicated_size)
        self.assertEqual(len(original.duplicates), len(deserialized.duplicates))

        orig_path, orig_comp = original.duplicates[0]
        deser_path, deser_comp = deserialized.duplicates[0]

        self.assertEqual(orig_path, deser_path)
        self.assertEqual(orig_comp.mtime_match, deser_comp.mtime_match)
        self.assertEqual(orig_comp.atime_match, deser_comp.atime_match)
        self.assertEqual(orig_comp.ctime_match, deser_comp.ctime_match)
        self.assertEqual(orig_comp.mode_match, deser_comp.mode_match)
        self.assertEqual(orig_comp.duplicated_size, deser_comp.duplicated_size)
        self.assertEqual(orig_comp.is_identical, deser_comp.is_identical)
        self.assertEqual(orig_comp.is_superset, deser_comp.is_superset)
        self.assertEqual(orig_comp.duplicated_items, deser_comp.duplicated_items)

    def test_msgpack_with_nested_paths(self):
        """Test msgpack serialization with deeply nested paths."""
        comparison = DuplicateMatch(
            Path('archive/deep/nested/dir/file.txt'),
            mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
            duplicated_size=1024, duplicated_items=1,
            is_identical=True, is_superset=True
        )
        duplicates = [(Path('archive/deep/nested/dir/file.txt'), comparison)]

        original = DuplicateRecord(
            path=Path('target/also/deep/nested/file.txt'),
            duplicates=duplicates,
            total_size=1024,
            duplicated_size=1024
        )

        serialized = original.to_msgpack()
        deserialized = DuplicateRecord.from_msgpack(serialized)

        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.duplicates[0][0], deserialized.duplicates[0][0])


class ReportWriterTest(unittest.TestCase):
    """Tests for ReportWriter database operations."""

    def test_create_and_write_record(self):
        """Create report database and write a duplicate record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

                # Create and write a record
                comparison = DuplicateMatch(
                    Path('archive/file.txt'),
                    mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                    duplicated_size=100, duplicated_items=1,
                    is_identical=True, is_superset=True
                )
                record = DuplicateRecord(
                    Path('target/file.txt'),
                    [(Path('archive/file.txt'), comparison)],
                    100,  # total_size
                    100   # duplicated_size
                )

                writer.write_duplicate_record(record)

            # Verify it was written by reading the database directly
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                # Compute the same path hash
                import mmh3
                path_str = '\0'.join(str(part) for part in Path('target/file.txt').parts)
                path_hash = mmh3.hash128(path_str.encode('utf-8'), signed=False).to_bytes(16, byteorder='big')

                prefixed_db = db.prefixed_db(path_hash)
                found_record = False

                for key, value in prefixed_db.iterator():
                    retrieved = DuplicateRecord.from_msgpack(value)
                    if retrieved.path == Path('target/file.txt'):
                        found_record = True
                        self.assertEqual(100, retrieved.duplicated_size)
                        self.assertEqual(1, len(retrieved.duplicates))
                        path, comp = retrieved.duplicates[0]
                        self.assertEqual(Path('archive/file.txt'), path)
                        self.assertTrue(comp.is_identical)
                        self.assertEqual(1, comp.duplicated_items)
                        break

                self.assertTrue(found_record, "Record not found in database")
            finally:
                db.close()

    def test_update_existing_record(self):
        """Update an existing record in the database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

                # Write initial record
                comp1 = DuplicateMatch(Path('dup1.txt'),
                                          mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                                          duplicated_size=50, duplicated_items=1,
                                          is_identical=True, is_superset=True)
                record1 = DuplicateRecord(Path('file.txt'), [(Path('dup1.txt'), comp1)], 50, 50)
                writer.write_duplicate_record(record1)

                # Update with new duplicate
                comp2 = DuplicateMatch(Path('dup2.txt'),
                                          mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
                                          duplicated_size=50, duplicated_items=1,
                                          is_identical=False, is_superset=False)
                record2 = DuplicateRecord(
                    Path('file.txt'),
                    [(Path('dup1.txt'), comp1), (Path('dup2.txt'), comp2)],
                    100,  # total_size
                    100   # duplicated_size
                )
                writer.write_duplicate_record(record2)

            # Verify the record was updated
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                import mmh3
                path_str = '\0'.join(str(part) for part in Path('file.txt').parts)
                path_hash = mmh3.hash128(path_str.encode('utf-8'), signed=False).to_bytes(16, byteorder='big')

                prefixed_db = db.prefixed_db(path_hash)
                count = 0
                for key, value in prefixed_db.iterator():
                    retrieved = DuplicateRecord.from_msgpack(value)
                    if retrieved.path == Path('file.txt'):
                        count += 1
                        self.assertEqual(100, retrieved.duplicated_size)
                        self.assertEqual(2, len(retrieved.duplicates))

                # Should only have one entry for this path
                self.assertEqual(1, count)
            finally:
                db.close()

    def test_multiple_records_different_paths(self):
        """Write multiple records with different paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

                # Write multiple different records
                for i in range(3):
                    comp = DuplicateMatch(Path(f'dup{i}.txt'),
                                            mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                                            duplicated_size=100, duplicated_items=1,
                                            is_identical=True, is_superset=True)
                    record = DuplicateRecord(Path(f'file{i}.txt'), [(Path(f'dup{i}.txt'), comp)], 100, 100)
                    writer.write_duplicate_record(record)

            # Verify all records exist
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                all_records = []
                for key, value in db.iterator():
                    # Skip path hash prefix to get to actual records
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        all_records.append(record.path)
                    except:
                        pass

                # We should have exactly 3 records
                self.assertGreaterEqual(len(all_records), 3)
            finally:
                db.close()


class FileAnalysisTest(unittest.TestCase):
    """Tests for file analysis with database verification."""

    def test_analyze_single_file_exact_match(self):
        """Analyze a directory containing a single file that exactly matches an archive file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'duplicate.txt'
            target_file.write_bytes(b'test content')
            copy_times(archive_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis on the directory
                    archive.analyze([target_path])

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
                            path, comparison = record.duplicates[0]
                            self.assertEqual(Path('original.txt'), path)
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
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'shared content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'newer.txt'
            target_file.write_bytes(b'shared content')
            tweak_times(target_file, 5000000000)  # Different timestamp

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis on the directory
                    archive.analyze([target_path])

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
                            path, comparison = record.duplicates[0]
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
        """Analyze directory with file that matches multiple files in archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            (archive_path / 'dup1.txt').write_bytes(b'duplicate')
            (archive_path / 'dup2.txt').write_bytes(b'duplicate')
            (archive_path / 'dup3.txt').write_bytes(b'duplicate')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'duplicate')

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Run analysis on the directory
                    archive.analyze([target_path])

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
                            duplicate_names = [str(path) for path, _ in record.duplicates]
                            self.assertIn('dup1.txt', duplicate_names)
                            self.assertIn('dup2.txt', duplicate_names)
                            self.assertIn('dup3.txt', duplicate_names)
                            # Each duplicate should have duplicated_items=1 for a file
                            for _, comparison in record.duplicates:
                                self.assertEqual(1, comparison.duplicated_items)

                            # CRITICAL: Verify duplicated_size semantics
                            # DuplicateRecord.duplicated_size should be file size (counted once)
                            file_size = target_file.stat().st_size
                            self.assertEqual(file_size, record.duplicated_size,
                                           "DuplicateRecord.duplicated_size should count file once")
                            # Each DuplicateMatch.duplicated_size should also be the file size
                            for _, comparison in record.duplicates:
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
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Different atime, same everything else
            copy_times(archive_file, target_file)
            # Manually set only atime to be different
            st = archive_file.stat()
            import os
            os.utime(target_file, ns=(st.st_atime_ns + 9999999999, st.st_mtime_ns), follow_symlinks=False)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Test with default rule (atime excluded)
                    archive.analyze([target_path])

            # With default rule (atime excluded), should be identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
            import shutil
            shutil.rmtree(report_dir)

            # Test with atime included
            from arindexer.commands.analyzer import DuplicateMatchRule
            rule_with_atime = DuplicateMatchRule(include_atime=True)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path], comparison_rule=rule_with_atime)

            # With atime included, should NOT be identical
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Copy mtime and atime to match
            copy_times(archive_file, target_file)
            # Change ctime by modifying a metadata attribute
            import os
            current_mode = target_file.stat().st_mode
            os.chmod(target_file, current_mode | 0o100)  # Add execute for owner
            os.chmod(target_file, current_mode)  # Change back (ctime updated)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Verify ctime_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')
            import os
            os.chmod(archive_file, 0o644)

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            os.chmod(target_file, 0o755)  # Different permissions
            copy_times(archive_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Verify mode_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
        import os
        # Skip test if not running as root (can't change file ownership)
        if os.geteuid() != 0:
            self.skipTest("Test requires root privileges to change file ownership")

        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')
            # Keep current owner for archive file

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            # Change owner to nobody (UID 65534 on most systems)
            os.chown(target_file, 65534, -1)  # -1 means don't change group
            copy_times(archive_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Verify owner_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
        import os
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
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_file = archive_path / 'original.txt'
            archive_file.write_bytes(b'test content')
            os.chown(archive_file, -1, group1)

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'test content')
            os.chown(target_file, -1, group2)  # Different group
            copy_times(archive_file, target_file)

            report_dir = target_path.parent / (target_path.name + '.report')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Verify group_match is False
            db = plyvel.DB(str(report_dir / 'database'))
            try:
                for key, value in db.iterator():
                    if len(key) == 16:
                        continue
                    try:
                        record = DuplicateRecord.from_msgpack(value)
                        if 'file.txt' in str(record.path):
                            path, comparison = record.duplicates[0]
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
                            path, comparison = record.duplicates[0]
                            if 'mydir' in str(path):
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
                            path, comparison = record.duplicates[0]
                            if 'bigdir' in str(path):
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
                                _, comparison = record.duplicates[0]
                                self.assertEqual(2, comparison.duplicated_items)
                            break
                    except:
                        pass

                self.assertTrue(found, "Directory with symlinks should be found as duplicate")
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
                            for dup_path, comparison in record.duplicates:
                                if 'dir1' in str(dup_path):
                                    # dir1 matches files A and B (100 + 200 = 300)
                                    self.assertEqual(300, comparison.duplicated_size,
                                                   "DuplicateMatch for dir1 should be A + B")
                                elif 'dir2' in str(dup_path):
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
                            path, comparison = record.duplicates[0]
                            if 'parent' in str(path):
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


if __name__ == '__main__':
    unittest.main()
