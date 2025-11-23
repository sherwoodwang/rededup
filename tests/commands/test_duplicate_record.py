"""Tests for DuplicateRecord class."""
import unittest
from pathlib import Path

from arindexer.commands.analyzer import DuplicateMatch, DuplicateRecord


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
            duplicates=[comparison1, comparison2],
            total_size=200,
            total_items=1,
            duplicated_size=200,
            duplicated_items=1
        )

        self.assertEqual(Path('target/file.txt'), record.path)
        self.assertEqual(2, len(record.duplicates))
        self.assertEqual(200, record.total_size)
        self.assertEqual(1, record.total_items)
        self.assertEqual(200, record.duplicated_size)
        self.assertEqual(1, record.duplicated_items)
        self.assertEqual(Path('dup1.txt'), record.duplicates[0].path)
        self.assertEqual(Path('dup2.txt'), record.duplicates[1].path)

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
            duplicates=[comparison],
            total_size=512,
            total_items=1,
            duplicated_size=512,
            duplicated_items=1
        )

        # Serialize and deserialize
        serialized = original.to_msgpack()
        deserialized = DuplicateRecord.from_msgpack(serialized)

        # Verify all fields
        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.total_size, deserialized.total_size)
        self.assertEqual(original.total_items, deserialized.total_items)
        self.assertEqual(original.duplicated_size, deserialized.duplicated_size)
        self.assertEqual(original.duplicated_items, deserialized.duplicated_items)
        self.assertEqual(len(original.duplicates), len(deserialized.duplicates))

        orig_comp = original.duplicates[0]
        deser_comp = deserialized.duplicates[0]

        self.assertEqual(orig_comp.path, deser_comp.path)
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
            duplicates=[comparison],
            total_size=1024,
            total_items=1,
            duplicated_size=1024,
            duplicated_items=1
        )

        serialized = original.to_msgpack()
        deserialized = DuplicateRecord.from_msgpack(serialized)

        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.duplicates[0].path, deserialized.duplicates[0].path)


if __name__ == '__main__':
    unittest.main()
