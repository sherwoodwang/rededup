"""Tests for describe functionality."""
import io
import sys
import tempfile
import unittest
from pathlib import Path
from typing import NamedTuple

from rededup import Repository
from rededup.commands.describe import DescribeFormatter, SortableRowData
from rededup.report.path import get_report_directory_path
from rededup.report.store import ReportStore
from rededup.utils.processor import Processor

from ..test_utils import copy_times


class TestRowData(NamedTuple):
    """Test-only NamedTuple for testing _print_formatted_table with arbitrary field names.

    Since we can't use dynamic fields in NamedTuple, we support common test field names.
    Tests can use a subset of these fields as needed.
    """
    Col0: str = ''
    Col1: str = ''
    Col2: str = ''
    Col3: str = ''
    Col4: str = ''
    Info: str = ''
    Size: str = ''
    Count: str = ''
    Name: str = ''
    Type: str = ''


class DescribeIntegrationTest(unittest.TestCase):
    """Integration tests for describe functionality."""

    def test_describe_file_with_duplicates(self):
        """Test describing a file that has duplicates in repository."""
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

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_path])

            # Use ReportStore to verify the report
            report_dir = get_report_directory_path(target_path)
            with ReportStore(report_dir, Path('.')) as store:
                # Read the record for the duplicate file
                record = store.read_duplicate_record(Path('target') / 'duplicate.txt')

                self.assertIsNotNone(record)
                self.assertEqual(1, len(record.duplicates))
                comparison = record.duplicates[0]
                self.assertEqual(Path('original.txt'), comparison.path)
                self.assertTrue(comparison.is_identical)

    def test_describe_directory_with_children(self):
        """Test describing a directory with child files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'repository'
            repository_path.mkdir()
            repository_dir = repository_path / 'mydir'
            repository_dir.mkdir()
            (repository_dir / 'file1.txt').write_bytes(b'content1')
            (repository_dir / 'file2.txt').write_bytes(b'content2')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_dir = target_path / 'duplicate_dir'
            target_dir.mkdir()
            file1 = target_dir / 'file1.txt'
            file1.write_bytes(b'content1')
            copy_times(repository_dir / 'file1.txt', file1)
            file2 = target_dir / 'file2.txt'
            file2.write_bytes(b'content2')
            copy_times(repository_dir / 'file2.txt', file2)

            with Processor() as processor:
                with Repository(processor, str(repository_path), create=True) as repository:
                    repository.rebuild()
                    repository.analyze([target_dir])

            # Use ReportStore to verify directory report
            report_dir = get_report_directory_path(target_dir)
            with ReportStore(report_dir, Path('.')) as store:
                # Read directory record
                dir_record = store.read_duplicate_record(Path('duplicate_dir'))

                self.assertIsNotNone(dir_record)
                self.assertEqual(1, len(dir_record.duplicates))
                self.assertTrue(dir_record.duplicates[0].is_identical)

                # Read child file records directly
                child1_record = store.read_duplicate_record(Path('duplicate_dir') / 'file1.txt')
                child2_record = store.read_duplicate_record(Path('duplicate_dir') / 'file2.txt')

                self.assertIsNotNone(child1_record)
                self.assertIsNotNone(child2_record)


class PrintFormattedTableTest(unittest.TestCase):
    """Tests for DescribeFormatter._print_formatted_table static method."""

    @staticmethod
    def make_row(name='test', type_str='', total_size_str='', dup_size_str='',
                 dups_str='', best_status_str='None', in_report_str='No',
                 is_dir=False, total_size=0, dup_size=0, dups=0, **kwargs):
        """Helper to create SortableRowData with default values.

        Fields are ordered to match SortableRowData (display fields first, then raw fields).
        """
        return SortableRowData(
            # Display fields (in COLUMNS order)
            name=name,
            type_str=type_str,
            total_size_str=total_size_str,
            dup_size_str=dup_size_str,
            size_ratio_str=kwargs.get('size_ratio_str', '0%'),
            total_items_str=kwargs.get('total_items_str', '1'),
            dup_items_str=kwargs.get('dup_items_str', '0'),
            items_ratio_str=kwargs.get('items_ratio_str', '0%'),
            dups_str=dups_str,
            max_match_dup_size_str=kwargs.get('max_match_dup_size_str', '0 B'),
            max_ratio_str=kwargs.get('max_ratio_str', '0%'),
            best_status_str=best_status_str,
            in_report_str=in_report_str,
            # Raw values (for sorting)
            is_dir=is_dir,
            total_size=total_size,
            dup_size=dup_size,
            dups=dups,
            match_dup_size=kwargs.get('match_dup_size', 0),
            status_rank=kwargs.get('status_rank', 0),
            total_items=kwargs.get('total_items', 1),
            dup_items=kwargs.get('dup_items', 0),
            size_ratio=kwargs.get('size_ratio', 0.0),
            items_ratio=kwargs.get('items_ratio', 0.0),
            max_ratio=kwargs.get('max_ratio', 0.0),
            in_report=kwargs.get('in_report', False)
        )

    def capture_output(self, func, *args, **kwargs):
        """Capture stdout from a function call."""
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            func(*args, **kwargs)
        finally:
            sys.stdout = old_stdout
        return captured_output.getvalue()

    def test_empty_rows_produces_no_output(self):
        """Empty rows list should produce no output."""
        columns = [('name', 'Name', False), ('total_size_str', 'Size', True)]
        rows = []
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)
        self.assertEqual('', output)

    def test_single_row_single_column(self):
        """Test with single row and single column."""
        columns = [('name', 'Name', False)]
        row = self.make_row(name='file.txt', type_str='File', total_size_str='1 KB', dup_size_str='0 B', dups_str='0')
        rows = [row]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        self.assertEqual(3, len(lines))  # Header, separator, row
        self.assertIn('Name', lines[0])
        self.assertTrue(lines[1].startswith('-'))
        self.assertIn('file.txt', lines[2])

    def test_multiple_columns_with_alignment(self):
        """Test multiple columns with left and right alignment."""
        # Use first two columns (Name and Type) - matching sequential positions 0 and 1
        columns = [('Name', 'Name', False), ('Type', 'Type', False)]
        rows = [TestRowData(Name='doc.pdf', Type='File')]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        self.assertEqual(3, len(lines))
        # Check header contains both column names
        self.assertIn('Name', lines[0])
        self.assertIn('Type', lines[0])
        # Check row data
        self.assertIn('doc.pdf', lines[2])
        self.assertIn('File', lines[2])

    def test_column_width_calculation(self):
        """Test that column widths accommodate both headers and data."""
        columns = [('Name', 'Name', False), ('Info', 'Info', False)]
        rows = [
            TestRowData(Name='very_long_filename_that_exceeds_header.txt', Info='F'),
            TestRowData(Name='short.txt', Info='File')
        ]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        self.assertEqual(4, len(lines))  # Header, separator, 2 rows

        # First data row should be fully visible despite being longer than header
        self.assertIn('very_long_filename_that_exceeds_header.txt', lines[2])
        # Check alignment: first column should be left-aligned
        self.assertTrue(lines[2].startswith('very_long_filename'))

    def test_right_alignment(self):
        """Test right-aligned columns."""
        # Use columns 0 (left-aligned) and 1 (right-aligned to show the difference)
        columns = [('Name', 'Name', False), ('Type', 'Type', True)]
        rows = [TestRowData(Name='file1.txt', Type='F')]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        # Check that both columns are present
        self.assertIn('Name', lines[0])
        self.assertIn('Type', lines[0])
        self.assertIn('file1.txt', lines[2])
        self.assertIn('F', lines[2])

    def test_multiple_rows_formatting(self):
        """Test formatting with multiple rows."""
        columns = [('Name', 'Name', False), ('Type', 'Type', False)]
        rows = [
            TestRowData(Name='directory', Type='Dir'),
            TestRowData(Name='file.txt', Type='File'),
            TestRowData(Name='archive.zip', Type='File')
        ]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        self.assertEqual(5, len(lines))  # Header, separator, 3 rows

        # Check all rows are present
        self.assertIn('directory', lines[2])
        self.assertIn('file.txt', lines[3])
        self.assertIn('archive.zip', lines[4])

        # Check type information is present
        self.assertIn('Dir', lines[2])
        self.assertIn('File', lines[3])
        self.assertIn('File', lines[4])

    def test_header_separator_format(self):
        """Test that separator line has correct format and length."""
        columns = [('Name', 'Name', False), ('Size', 'Size', True), ('Count', 'Count', True)]
        rows = [TestRowData(Name='test.txt', Size='1 KB', Count='1')]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        header_line = lines[0]
        separator_line = lines[1]

        # Separator should be dashes only
        self.assertTrue(all(c == '-' for c in separator_line))
        # Separator length should match header length
        self.assertEqual(len(header_line), len(separator_line))

    def test_column_separation(self):
        """Test that columns are separated by two spaces."""
        columns = [('Col1', 'Col1', False), ('Col2', 'Col2', False)]
        rows = [TestRowData(Col1='A', Col2='B')]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        header = lines[0]

        # Check that columns are separated by two spaces
        # "Col1" should be followed by "  " and then "Col2"
        col1_pos = header.find('Col1')
        col2_pos = header.find('Col2')
        # There should be some spacing between them
        self.assertGreater(col2_pos - col1_pos, 4)  # At least "Col1" + "  "

    def test_special_characters_in_data(self):
        """Test handling of special characters in row data."""
        columns = [('Name', 'Name', False)]
        rows = [TestRowData(Name='file-with-dashes_and_underscores.txt')]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        # Special characters should be preserved
        self.assertIn('file-with-dashes_and_underscores.txt', lines[2])

    def test_numeric_strings_right_alignment(self):
        """Test that numeric string columns are properly right-aligned."""
        # Use columns 0 (Name, left), 1 (Type, right), 2 (Total Size, right)
        columns = [('Name', 'Name', False), ('Type', 'Type', True), ('Size', 'Total Size', True)]
        rows = [
            TestRowData(Name='a', Type='X', Size='100 B'),
            TestRowData(Name='b', Type='Y', Size='1 MB')
        ]
        output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)

        lines = output.strip().split('\n')
        self.assertEqual(4, len(lines))  # Header, separator, 2 rows

        # All columns should be present in the output
        self.assertIn('100 B', lines[2])
        self.assertIn('X', lines[2])
        self.assertIn('1 MB', lines[3])
        self.assertIn('Y', lines[3])

    def test_varying_column_count(self):
        """Test with different numbers of columns."""
        for col_count in [1, 2, 3, 5]:
            columns = [(f'Col{i}', f'Col{i}', i % 2 == 0) for i in range(col_count)]

            # Create TestRowData with the required fields
            row_kwargs = {f'Col{i}': f'Value{i}' for i in range(col_count)}
            rows = [TestRowData(**row_kwargs)]

            output = self.capture_output(DescribeFormatter._print_formatted_table, columns, rows)
            lines = output.strip().split('\n')

            # Should have header, separator, and data row
            self.assertEqual(3, len(lines), f"Failed for {col_count} columns")

            # All column headers should be in the header line
            for i in range(col_count):
                self.assertIn(f'Col{i}', lines[0])


if __name__ == '__main__':
    unittest.main()
