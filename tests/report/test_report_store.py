"""Tests for ReportStore class."""
import tempfile
import unittest
from pathlib import Path

from arindexer.report.duplicate_match import DuplicateMatch
from arindexer.report.store import (
    DuplicateRecord,
    ReportManifest,
    ReportStore,
)


class ReportStoreTest(unittest.TestCase):
    """Tests for ReportStore class."""

    def test_create_and_write_record(self):
        """Create report database and write a duplicate record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.open_database(create_if_missing=True)
            try:
                # Create and write a record
                comparison = DuplicateMatch(
                    Path('archive/file.txt'),
                    mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                    duplicated_size=100, duplicated_items=1,
                    is_identical=True, is_superset=True
                )
                record = DuplicateRecord(
                    Path('target/file.txt'),
                    [comparison],
                    total_size=100,
                    total_items=1,
                    duplicated_size=100,
                    duplicated_items=1
                )

                store.write_duplicate_record(record)
            finally:
                store.close_database()

            # Verify it was written by reading it back
            with ReportStore(report_dir, Path('.')) as store:
                retrieved = store.read_duplicate_record(Path('target/file.txt'))

                self.assertIsNotNone(retrieved, "Record not found in database")
                self.assertEqual(100, retrieved.duplicated_size)
                self.assertEqual(1, len(retrieved.duplicates))
                comp = retrieved.duplicates[0]
                self.assertEqual(Path('archive/file.txt'), comp.path)
                self.assertTrue(comp.is_identical)
                self.assertEqual(1, comp.duplicated_items)

    def test_read_duplicate_record(self):
        """Test reading a duplicate record from database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            # Write a record
            store = ReportStore(report_dir)
            store.create_report_directory()
            store.open_database(create_if_missing=True)
            try:
                comparison = DuplicateMatch(
                    Path('archive/file.txt'),
                    mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                    duplicated_size=100, duplicated_items=1,
                    is_identical=True, is_superset=True
                )
                record = DuplicateRecord(
                    Path('target/file.txt'),
                    [comparison],
                    total_size=100,
                    total_items=1,
                    duplicated_size=100,
                    duplicated_items=1
                )
                store.write_duplicate_record(record)
            finally:
                store.close_database()

            # Read it back
            with ReportStore(report_dir, Path('.')) as store:
                retrieved = store.read_duplicate_record(Path('target/file.txt'))

                self.assertIsNotNone(retrieved)
                self.assertEqual(Path('target/file.txt'), retrieved.path)
                self.assertEqual(100, retrieved.duplicated_size)
                self.assertEqual(1, len(retrieved.duplicates))

                comp = retrieved.duplicates[0]
                self.assertEqual(Path('archive/file.txt'), comp.path)
                self.assertTrue(comp.is_identical)
                self.assertEqual(1, comp.duplicated_items)

    def test_read_nonexistent_record(self):
        """Test reading a record that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.open_database(create_if_missing=True)
            store.close_database()

            with ReportStore(report_dir, Path('.')) as store:
                retrieved = store.read_duplicate_record(Path('nonexistent/file.txt'))
                self.assertIsNone(retrieved)

    def test_read_manifest(self):
        """Test reading manifest from report."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            # Write manifest
            manifest = ReportManifest(
                archive_path='/path/to/archive',
                archive_id='test-archive-id',
                timestamp='2024-01-01T00:00:00'
            )

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.write_manifest(manifest)

            # Read it back
            store2 = ReportStore(report_dir, Path('.'))
            retrieved = store2.read_manifest()

            self.assertEqual('/path/to/archive', retrieved.archive_path)
            self.assertEqual('test-archive-id', retrieved.archive_id)
            self.assertEqual('2024-01-01T00:00:00', retrieved.timestamp)

    def test_update_existing_record(self):
        """Update an existing record in the database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.open_database(create_if_missing=True)
            try:
                # Write initial record
                comp1 = DuplicateMatch(Path('dup1.txt'),
                                          mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                                          duplicated_size=50, duplicated_items=1,
                                          is_identical=True, is_superset=True)
                record1 = DuplicateRecord(Path('file.txt'), [comp1], total_size=50, total_items=1, duplicated_size=50, duplicated_items=1)
                store.write_duplicate_record(record1)

                # Update with new duplicate
                comp2 = DuplicateMatch(Path('dup2.txt'),
                                          mtime_match=False, atime_match=True, ctime_match=True, mode_match=True,
                                          duplicated_size=50, duplicated_items=1,
                                          is_identical=False, is_superset=False)
                record2 = DuplicateRecord(
                    Path('file.txt'),
                    [comp1, comp2],
                    total_size=100,
                    total_items=1,
                    duplicated_size=100,
                    duplicated_items=1
                )
                store.write_duplicate_record(record2)
            finally:
                store.close_database()

            # Verify the record was updated
            with ReportStore(report_dir, Path('.')) as store:
                retrieved = store.read_duplicate_record(Path('file.txt'))

                self.assertIsNotNone(retrieved)
                self.assertEqual(100, retrieved.duplicated_size)
                self.assertEqual(2, len(retrieved.duplicates))

    def test_multiple_records_different_paths(self):
        """Write multiple records with different paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.open_database(create_if_missing=True)
            try:
                # Write multiple different records
                for i in range(3):
                    comp = DuplicateMatch(Path(f'dup{i}.txt'),
                                            mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                                            duplicated_size=100, duplicated_items=1,
                                            is_identical=True, is_superset=True)
                    record = DuplicateRecord(Path(f'file{i}.txt'), [comp], total_size=100, total_items=1, duplicated_size=100, duplicated_items=1)
                    store.write_duplicate_record(record)
            finally:
                store.close_database()

            # Verify all records exist by reading each one
            with ReportStore(report_dir, Path('.')) as store:
                for i in range(3):
                    record = store.read_duplicate_record(Path(f'file{i}.txt'))
                    self.assertIsNotNone(record, f"Record for file{i}.txt should exist")
                    self.assertEqual(Path(f'file{i}.txt'), record.path)

    def test_validate_report(self):
        """Test validating report against archive ID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            # Write manifest
            manifest = ReportManifest(
                archive_path='/path/to/archive',
                archive_id='test-archive-id',
                timestamp='2024-01-01T00:00:00'
            )

            store = ReportStore(report_dir)
            store.create_report_directory()
            store.write_manifest(manifest)

            # Validate with correct ID
            self.assertTrue(store.validate_report('test-archive-id'))

            # Validate with incorrect ID
            self.assertFalse(store.validate_report('wrong-archive-id'))


if __name__ == '__main__':
    unittest.main()
