"""Tests for ReportWriter database operations."""
import tempfile
import unittest
from pathlib import Path

from arindexer.commands.analyzer import (
    DuplicateMatch,
    DuplicateRecord,
    ReportWriter,
    ReportReader
)


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
                    [comparison],
                    total_size=100,
                    total_items=1,
                    duplicated_size=100,
                    duplicated_items=1
                )

                writer.write_duplicate_record(record)

            # Verify it was written using ReportReader
            with ReportReader(report_dir, Path('.')) as reader:
                retrieved = reader.read_duplicate_record(Path('target/file.txt'))

                self.assertIsNotNone(retrieved, "Record not found in database")
                self.assertEqual(100, retrieved.duplicated_size)
                self.assertEqual(1, len(retrieved.duplicates))
                comp = retrieved.duplicates[0]
                self.assertEqual(Path('archive/file.txt'), comp.path)
                self.assertTrue(comp.is_identical)
                self.assertEqual(1, comp.duplicated_items)

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
                record1 = DuplicateRecord(Path('file.txt'), [comp1], total_size=50, total_items=1, duplicated_size=50, duplicated_items=1)
                writer.write_duplicate_record(record1)

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
                writer.write_duplicate_record(record2)

            # Verify the record was updated using ReportReader
            with ReportReader(report_dir, Path('.')) as reader:
                retrieved = reader.read_duplicate_record(Path('file.txt'))

                self.assertIsNotNone(retrieved)
                self.assertEqual(100, retrieved.duplicated_size)
                self.assertEqual(2, len(retrieved.duplicates))

                # Verify we can iterate and find only one record for this path
                count = sum(1 for r in reader.iterate_all_records() if r.path == Path('file.txt'))
                self.assertEqual(1, count, "Should only have one entry for this path")

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
                    record = DuplicateRecord(Path(f'file{i}.txt'), [comp], total_size=100, total_items=1, duplicated_size=100, duplicated_items=1)
                    writer.write_duplicate_record(record)

            # Verify all records exist using ReportReader
            with ReportReader(report_dir, Path('.')) as reader:
                all_records = list(reader.iterate_all_records())

                # We should have exactly 3 records
                self.assertEqual(3, len(all_records))

                # Verify each expected path is present
                paths = {str(r.path) for r in all_records}
                self.assertIn('file0.txt', paths)
                self.assertIn('file1.txt', paths)
                self.assertIn('file2.txt', paths)


if __name__ == '__main__':
    unittest.main()
