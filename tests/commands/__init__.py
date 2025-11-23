"""Tests for command implementation modules.

Test Files and Coverage:
========================

| Test File                  | Test Classes                 | Tested Constructs              | Tested Functionalities              |
|----------------------------|------------------------------|--------------------------------|-------------------------------------|
| test_duplicate_match.py    | DuplicateMatchTest           | DuplicateMatch                 | Construction, default values, flags |
| test_duplicate_record.py   | DuplicateRecordTest          | DuplicateRecord                | Creation, msgpack serialization     |
| test_report_writer.py      | ReportWriterTest             | ReportWriter                   | DB write, record updates, multiple  |
| test_file_analysis.py      | FileAnalysisTest             | Archive.analyze()              | Exact match, metadata differ, rules |
| test_directory_analysis.py | DirectoryAnalysisTest        | Archive.analyze()              | Dir match, symlinks, nested dirs    |
| test_report_reader.py      | ReportReaderTest             | ReportReader, ReportManifest   | Read records, iteration, manifest   |
| test_find_report.py        | FindReportTest               | find_report_for_path()         | Exact path, children, nesting      |
| test_describe.py           | DescribeIntegrationTest      | describe command               | File/dir reporting, integration     |
|                            | PrintFormattedTableTest      | DescribeFormatter._print_formatted_table() | Table formatting, alignment |
| test_describe_options.py   | DescribeOptionsTest          | describe command options       | Flags, limit, sorting              |
| test_archive_importer.py   | ImportTest                   | Archive.import_from()          | Import from nested archives         |
"""