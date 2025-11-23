"""Tests for ReportReader class."""
import tempfile
import unittest
from pathlib import Path

from arindexer.commands.analyzer import (
    DuplicateMatch,
    DuplicateRecord,
    ReportWriter,
    ReportReader,
    ReportManifest
)


class ReportReaderTest(unittest.TestCase):
    """Tests for ReportReader class."""

    def test_read_duplicate_record(self):
        """Test reading a duplicate record from database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            # Write a record using ReportWriter
            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

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

            # Read it back using ReportReader
            with ReportReader(report_dir, Path('.')) as reader:
                retrieved = reader.read_duplicate_record(Path('target/file.txt'))

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

            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

            with ReportReader(report_dir, Path('.')) as reader:
                retrieved = reader.read_duplicate_record(Path('nonexistent/file.txt'))
                self.assertIsNone(retrieved)

    def test_iterate_all_records(self):
        """Test iterating through all records in database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / '.report'

            # Write multiple records
            with ReportWriter(report_dir) as writer:
                writer.create_report_directory()

                for i in range(3):
                    comp = DuplicateMatch(
                        Path(f'dup{i}.txt'),
                        mtime_match=True, atime_match=True, ctime_match=True, mode_match=True,
                        duplicated_size=100, duplicated_items=1,
                        is_identical=True, is_superset=True
                    )
                    record = DuplicateRecord(
                        Path(f'file{i}.txt'),
                        [comp],
                        total_size=100,
                        total_items=1,
                        duplicated_size=100,
                        duplicated_items=1
                    )
                    writer.write_duplicate_record(record)

            # Iterate and verify
            with ReportReader(report_dir, Path('.')) as reader:
                records = list(reader.iterate_all_records())
                self.assertEqual(3, len(records))

                paths = {str(r.path) for r in records}
                self.assertIn('file0.txt', paths)
                self.assertIn('file1.txt', paths)
                self.assertIn('file2.txt', paths)

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

            writer = ReportWriter(report_dir)
            writer.create_report_directory()
            writer.write_manifest(manifest)

            # Read it back
            reader = ReportReader(report_dir, Path('.'))
            retrieved = reader.read_manifest()

            self.assertEqual('/path/to/archive', retrieved.archive_path)
            self.assertEqual('test-archive-id', retrieved.archive_id)
            self.assertEqual('2024-01-01T00:00:00', retrieved.timestamp)


if __name__ == '__main__':
    unittest.main()
