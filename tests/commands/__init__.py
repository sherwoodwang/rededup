"""Tests for command implementation modules.

Test Files and Coverage:
========================

| Test File                  | Test Classes                 | Tested Constructs                          | Tested Functionalities                        |
|----------------------------|------------------------------|--------------------------------------------|-----------------------------------------------|
| test_file_analysis.py      | FileAnalysisTest             | Archive.analyze()                          | Exact match, metadata differ, rules           |
| test_directory_analysis.py | DirectoryAnalysisTest        | Archive.analyze()                          | Dir match, symlinks, nested dirs, regressions |
| test_find_report.py        | FindReportTest               | find_report_for_path()                     | Exact path, children, nesting                 |
| test_describe.py           | DescribeIntegrationTest      | describe command                           | File/dir reporting, integration               |
|                            | PrintFormattedTableTest      | DescribeFormatter._print_formatted_table() | Table formatting, alignment                   |
| test_describe_options.py   | DescribeOptionsTest          | describe command options                   | Flags, limit, sorting                         |
| test_archive_importer.py   | ImportTest                   | Archive.import_from()                      | Import from nested archives                   |
| test_diff_tree.py          | DiffTreeIntegrationTest      | diff_tree command                          | Tree display, filtering, max depth            |

Note: Tests for DuplicateMatch, DuplicateRecord, and ReportStore have been moved to tests/analyzer/
"""
