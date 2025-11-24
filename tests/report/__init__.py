"""Tests for report module.

Test Files and Coverage:
========================

| Test File                  | Test Classes                 | Tested Constructs                          | Tested Functionalities              |
|----------------------------|------------------------------|--------------------------------------------|-------------------------------------|
| test_duplicate_match.py    | DuplicateMatchTest           | DuplicateMatch, DuplicateMatchRule         | Construction, default values, flags |
| test_duplicate_record.py   | DuplicateRecordTest          | DuplicateRecord                            | Creation, msgpack serialization     |
| test_report_store.py       | ReportStoreTest              | ReportStore, ReportManifest                | DB write, record updates, validation|
"""
