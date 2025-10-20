"""Tests for edge cases and special scenarios.

This module contains tests for edge cases:
- Hash collision handling
- Empty files
- Large directories
"""
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive
from arindexer.utils.processor import Processor

from .test_utils import compute_xor


class EdgeCaseTest(unittest.TestCase):
    """Tests for edge cases and special scenarios."""

    def test_hash_collision_handling(self):
        """Archive handles hash collisions correctly using custom weak hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create files that will have XOR hash collision
            (archive_path / 'file1').write_bytes(b'\x00\x00\x00\x00\x01\x01\x01\x01')
            (archive_path / 'file2').write_bytes(b'\x01\x01\x01\x01\x00\x00\x00\x00')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    # Use weak XOR hash to force collisions
                    archive._hash_algorithms['xor'] = (4, compute_xor)
                    archive._default_hash_algorithm = 'xor'

                    archive.rebuild()

                    entries = list(archive.inspect())
                    # Both files should be indexed despite collision
                    self.assertTrue(any('file1' in entry for entry in entries))
                    self.assertTrue(any('file2' in entry for entry in entries))

    def test_empty_files(self):
        """Archive correctly handles zero-byte files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            # Create empty files
            (archive_path / 'empty1.txt').write_bytes(b'')
            (archive_path / 'empty2.txt').write_bytes(b'')
            (archive_path / 'nonempty.txt').write_bytes(b'content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    entries = list(archive.inspect())
                    # All files should be indexed
                    self.assertTrue(any('empty1.txt' in entry for entry in entries))
                    self.assertTrue(any('empty2.txt' in entry for entry in entries))
                    self.assertTrue(any('nonempty.txt' in entry for entry in entries))

    def test_large_directory(self):
        """Archive handles directory with many files efficiently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            large_dir = archive_path / 'many_files'
            large_dir.mkdir()

            # Create 100 files
            num_files = 100
            for i in range(num_files):
                (large_dir / f'file{i:03d}.txt').write_bytes(f'content {i}'.encode())

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    entries = list(archive.inspect())
                    # Should index all files
                    file_entries = [e for e in entries if 'file0' in e or 'file1' in e]
                    self.assertGreater(len(file_entries), 0)

                    # Verify a sampling of files are present
                    self.assertTrue(any('file000.txt' in entry for entry in entries))
                    self.assertTrue(any('file050.txt' in entry for entry in entries))
                    self.assertTrue(any('file099.txt' in entry for entry in entries))


if __name__ == '__main__':
    unittest.main()
