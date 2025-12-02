import tempfile
import unittest
from pathlib import Path

from rededup.index.store import IndexStore, FileSignature
from rededup.index.settings import IndexSettings


class InterceptedIndexStore(IndexStore):
    """IndexStore subclass with controllable hash function for testing."""

    # Class-level hash mapping for deterministic testing
    # Maps path string to 16-byte hash value
    long_hash_mapping: dict[str, bytes] = {}

    @staticmethod
    def _compute_long_path_hash(path: Path) -> bytes:
        """Override to use controlled hash values for testing collisions."""
        path_str = '/'.join(str(part) for part in path.parts)

        # Use mapping if available, otherwise fall back to parent implementation
        if path_str in InterceptedIndexStore.long_hash_mapping:
            return InterceptedIndexStore.long_hash_mapping[path_str]

        # Default: use parent implementation
        return IndexStore._compute_long_path_hash(path)


class FileSignatureCollisionTest(unittest.TestCase):
    """Test collision handling in file signature storage."""

    def setUp(self):
        """Clear hash mapping before each test."""
        InterceptedIndexStore.long_hash_mapping = {}

    def test_register_file_no_collision(self):
        """Test registering files without hash collision."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                # Register two files with different paths (no collision setup)
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)

                store.register_file(path1, sig1)
                store.register_file(path2, sig2)

                # Both should be retrievable
                result1 = store.lookup_file(path1)
                result2 = store.lookup_file(path2)

                self.assertIsNotNone(result1)
                self.assertIsNotNone(result2)
                self.assertEqual(result1.path, path1)
                self.assertEqual(result2.path, path2)
                self.assertEqual(result1.digest, b'digest1')
                self.assertEqual(result2.digest, b'digest2')

    def test_register_file_with_collision(self):
        """Test registering files that have the same path hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision: both paths hash to same value
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)

                store.register_file(path1, sig1)
                store.register_file(path2, sig2)

                # Both should be retrievable despite hash collision
                result1 = store.lookup_file(path1)
                result2 = store.lookup_file(path2)

                self.assertIsNotNone(result1)
                self.assertIsNotNone(result2)
                self.assertEqual(result1.path, path1)
                self.assertEqual(result2.path, path2)
                self.assertEqual(result1.digest, b'digest1')
                self.assertEqual(result2.digest, b'digest2')

    def test_update_file_with_collision(self):
        """Test updating a file when there are hash collisions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')

                # Register both files
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)
                store.register_file(path1, sig1)
                store.register_file(path2, sig2)

                # Update file1
                sig1_updated = FileSignature(path1, b'digest1_new', 3000, 2)
                store.register_file(path1, sig1_updated)

                # Check that file1 was updated and file2 is unchanged
                result1 = store.lookup_file(path1)
                result2 = store.lookup_file(path2)

                self.assertEqual(result1.digest, b'digest1_new')
                self.assertEqual(result1.mtime_ns, 3000)
                self.assertEqual(result1.ec_id, 2)

                self.assertEqual(result2.digest, b'digest2')
                self.assertEqual(result2.mtime_ns, 2000)
                self.assertEqual(result2.ec_id, 1)

    def test_deregister_file_with_collision(self):
        """Test deregistering a file when there are hash collisions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision for three files
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
                'file3.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')
                path3 = Path('file3.txt')

                # Register three files with same hash
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)
                sig3 = FileSignature(path3, b'digest3', 3000, 2)
                store.register_file(path1, sig1)
                store.register_file(path2, sig2)
                store.register_file(path3, sig3)

                # Deregister file2 (middle one)
                store.deregister_file(path2)

                # Check that file2 is gone but file1 and file3 remain
                result1 = store.lookup_file(path1)
                result2 = store.lookup_file(path2)
                result3 = store.lookup_file(path3)

                self.assertIsNotNone(result1)
                self.assertIsNone(result2)
                self.assertIsNotNone(result3)

    def test_list_files_with_collision(self):
        """Test listing files when there are hash collisions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision for path1 and path2, but not path3
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')
                path3 = Path('dir/file3.txt')

                # Register files
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)
                sig3 = FileSignature(path3, b'digest3', 3000, 2)
                store.register_file(path1, sig1)
                store.register_file(path2, sig2)
                store.register_file(path3, sig3)

                # List all files
                files = list(store.list_registered_files())

                # Should get all three files
                paths = {path for path, _ in files}
                self.assertEqual(len(paths), 3)
                self.assertIn(path1, paths)
                self.assertIn(path2, paths)
                self.assertIn(path3, paths)

    def test_collision_with_many_files(self):
        """Test handling many files with the same hash collision."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Create 10 files that all collide
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                f'file{i}.txt': collision_hash for i in range(10)
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                paths = [Path(f'file{i}.txt') for i in range(10)]

                # Register all files
                for i, path in enumerate(paths):
                    sig = FileSignature(path, f'digest{i}'.encode(), 1000 + i, i)
                    store.register_file(path, sig)

                # Verify all files are retrievable
                for i, path in enumerate(paths):
                    result = store.lookup_file(path)
                    self.assertIsNotNone(result, f"Failed to retrieve {path}")
                    self.assertEqual(result.path, path)
                    self.assertEqual(result.digest, f'digest{i}'.encode())
                    self.assertEqual(result.mtime_ns, 1000 + i)

    def test_deregister_nonexistent_file_with_collision(self):
        """Test deregistering a file that doesn't exist when there are collisions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
                'file_nonexistent.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')
                path_nonexistent = Path('file_nonexistent.txt')

                # Register two files
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)
                store.register_file(path1, sig1)
                store.register_file(path2, sig2)

                # Try to deregister a file that doesn't exist but has same hash
                # This should not raise an error
                store.deregister_file(path_nonexistent)

                # Verify existing files are still there
                result1 = store.lookup_file(path1)
                result2 = store.lookup_file(path2)
                self.assertIsNotNone(result1)
                self.assertIsNotNone(result2)

    def test_lookup_nonexistent_file_with_collision(self):
        """Test looking up a file that doesn't exist when there are collisions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file_nonexistent.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path_nonexistent = Path('file_nonexistent.txt')

                # Register one file
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                store.register_file(path1, sig1)

                # Try to lookup a file that doesn't exist but has same hash
                result = store.lookup_file(path_nonexistent)
                self.assertIsNone(result)

    def test_inspect_output_with_collision(self):
        """Test that inspect output correctly shows sequence numbers for colliding paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_path = Path(tmpdir) / 'test_repository'
            repository_path.mkdir()

            # Set up collision
            collision_hash = b'\x00' * 16
            InterceptedIndexStore.long_hash_mapping = {
                'file1.txt': collision_hash,
                'file2.txt': collision_hash,
            }

            settings = IndexSettings(repository_path)
            with InterceptedIndexStore(settings, repository_path, create=True) as store:
                path1 = Path('file1.txt')
                path2 = Path('file2.txt')

                # Register files
                sig1 = FileSignature(path1, b'digest1', 1000, 0)
                sig2 = FileSignature(path2, b'digest2', 2000, 1)
                store.register_file(path1, sig1)
                store.register_file(path2, sig2)

                # Check inspect output
                hash_algorithms = {'sha256': (32, lambda x: b'')}
                inspect_lines = list(store.inspect(hash_algorithms))

                # Filter file-metadata lines
                metadata_lines = [line for line in inspect_lines if line.startswith('file-metadata')]

                # Should have two entries with different sequence numbers
                self.assertEqual(len(metadata_lines), 2)

                # Check that both have sequence numbers
                seq_numbers = []
                for line in metadata_lines:
                    if 'seq:0' in line:
                        seq_numbers.append(0)
                    elif 'seq:1' in line:
                        seq_numbers.append(1)

                self.assertEqual(sorted(seq_numbers), [0, 1])
