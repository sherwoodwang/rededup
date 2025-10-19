import re
import tempfile
import unittest
import urllib.parse
from pathlib import Path

from arindexer import Archive
# noinspection PyProtectedMember
from arindexer._processor import Processor

from .test_utils import compute_xor


class ImportTest(unittest.TestCase):
    """Test import functionality for importing index entries from other archives."""

    def test_import_from_nested_archive(self):
        """Test importing from a nested subdirectory archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'subdir' / 'nested_archive'
            nested_path.mkdir(parents=True)

            # Add files to nested archive
            (nested_path / 'file1.txt').write_text('content1')
            (nested_path / 'file2.txt').write_text('content2')
            (nested_path / 'dir1').mkdir()
            (nested_path / 'dir1' / 'file3.txt').write_text('content3')

            with Processor() as processor:
                # Build nested archive index
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import into parent archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that files are imported with correct prefix
                    indexed_files = []
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            # URL decode the path
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    self.assertIn('subdir/nested_archive/file1.txt', indexed_files)
                    self.assertIn('subdir/nested_archive/file2.txt', indexed_files)
                    self.assertIn('subdir/nested_archive/dir1/file3.txt', indexed_files)

    def test_import_from_ancestor_archive(self):
        """Test importing from an ancestor archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested directory
            nested_path = parent_path / 'subdir' / 'nested_archive'
            nested_path.mkdir(parents=True)

            # Add files to both parent and nested locations
            (parent_path / 'root_file.txt').write_text('root content')
            (nested_path / 'nested_file.txt').write_text('nested content')
            (parent_path / 'other_dir').mkdir()
            (parent_path / 'other_dir' / 'other_file.txt').write_text('other content')

            with Processor() as processor:
                # Build parent archive index
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Import into nested archive (should only get files under nested path)
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.import_index(str(parent_path))

                    # Check that only nested files are imported with prefix removed
                    indexed_files = []
                    for line in nested_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            # URL decode the path
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    # Should have the nested file with prefix removed
                    self.assertIn('nested_file.txt', indexed_files)
                    # Should NOT have files outside the nested scope
                    self.assertNotIn('root_file.txt', indexed_files)
                    self.assertNotIn('other_dir/other_file.txt', indexed_files)

    def test_import_rejects_same_archive(self):
        """Test that importing from the same archive is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    with self.assertRaises(ValueError) as context:
                        archive.import_index(str(archive_path))
                    self.assertIn('cannot be the same', str(context.exception))

    def test_import_rejects_aridx_directory(self):
        """Test that importing from .aridx directory is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    with self.assertRaises(ValueError) as context:
                        archive.import_index(str(aridx_path))
                    self.assertIn('.aridx', str(context.exception))

    def test_import_rejects_invalid_relationship(self):
        """Test that importing from unrelated archive is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive1_path = Path(tmpdir) / 'archive1'
            archive1_path.mkdir()
            archive2_path = Path(tmpdir) / 'archive2'
            archive2_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive1_path), create=True) as archive1:
                    with Archive(processor, str(archive2_path), create=True) as archive2:
                        with self.assertRaises(ValueError) as context:
                            archive1.import_index(str(archive2_path))
                        self.assertIn('nested', str(context.exception))
                        self.assertIn('ancestor', str(context.exception))

    def test_import_with_ec_id_collision(self):
        """Test that EC ID collisions are handled by remapping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files with identical content that will have EC ID 0 in both archives
            content = 'identical content'
            (parent_path / 'parent_file1.txt').write_text(content)
            (parent_path / 'parent_file2.txt').write_text(content)
            (nested_path / 'nested_file1.txt').write_text(content)
            (nested_path / 'nested_file2.txt').write_text(content)

            with Processor() as processor:
                # Build parent archive - files will get EC ID 0
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Build nested archive - files will also get EC ID 0
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Now import nested into parent - should remap EC IDs to avoid collision
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present
                    indexed_files = {}
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            # Extract EC ID from the line
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if ec_id_match:
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = ec_id

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # Files with identical content should be in the same EC class
                    # (They all have the same digest and content)
                    # After import, all should be in same EC class since content is identical
                    ec_ids = set(indexed_files.values())
                    # All files have identical content, so should be in same EC class
                    self.assertEqual(len(ec_ids), 1, f"Expected all files in same EC class, got: {indexed_files}")

    def test_import_with_ec_id_collision_different_content(self):
        """Test that files with different content maintain separate EC classes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files with same content in each archive (so they get EC ID 0)
            # But different content between archives
            (parent_path / 'parent_file1.txt').write_text('parent content A')
            (parent_path / 'parent_file2.txt').write_text('parent content A')
            (nested_path / 'nested_file1.txt').write_text('nested content B')
            (nested_path / 'nested_file2.txt').write_text('nested content B')

            with Processor() as processor:
                # Build parent archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Build nested archive
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import nested into parent
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present with correct EC assignments
                    indexed_files = {}  # path -> (digest, ec_id)
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            digest_match = re.search(r'digest:([0-9a-f]+)', line)
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if digest_match and ec_id_match:
                                digest = digest_match.group(1)
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = (digest, ec_id)

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # Files with same content should have same digest and EC ID
                    self.assertEqual(indexed_files['parent_file1.txt'], indexed_files['parent_file2.txt'])
                    self.assertEqual(indexed_files['nested/nested_file1.txt'], indexed_files['nested/nested_file2.txt'])

                    # Files with different content should have different digests
                    parent_digest = indexed_files['parent_file1.txt'][0]
                    nested_digest = indexed_files['nested/nested_file1.txt'][0]
                    self.assertNotEqual(parent_digest, nested_digest)

    def test_import_with_hash_collision(self):
        """Test that hash collisions are properly handled during import."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files that will have XOR hash collision but different content
            # XOR hash works by XORing 4-byte chunks
            # b'\0\0\0\1' XOR b'\0\0\0\1' = b'\0\0\0\0' (hash: 00000000)
            # b'\0\0\0\2' XOR b'\0\0\0\2' = b'\0\0\0\0' (hash: 00000000)
            # Both will have same hash 00000000 but different content
            (parent_path / 'parent_file1.txt').write_bytes(b'\0\0\0\1\0\0\0\1')
            (parent_path / 'parent_file2.txt').write_bytes(b'\0\0\0\1\0\0\0\1')  # Same as parent_file1
            (nested_path / 'nested_file1.txt').write_bytes(b'\0\0\0\2\0\0\0\2')
            (nested_path / 'nested_file2.txt').write_bytes(b'\0\0\0\2\0\0\0\2')  # Same as nested_file1

            with Processor() as processor:
                # Build parent archive with XOR hash
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive._hash_algorithms['xor'] = (4, compute_xor)
                    parent_archive._default_hash_algorithm = 'xor'
                    parent_archive.rebuild()

                # Build nested archive with XOR hash
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive._hash_algorithms['xor'] = (4, compute_xor)
                    nested_archive._default_hash_algorithm = 'xor'
                    nested_archive.rebuild()

                # Import nested into parent
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present with correct EC assignments
                    indexed_files = {}  # path -> (digest, ec_id)
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            digest_match = re.search(r'digest:([0-9a-f]+)', line)
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if digest_match and ec_id_match:
                                digest = digest_match.group(1)
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = (digest, ec_id)

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # All files should have the same digest (hash collision)
                    all_digests = {digest for digest, ec_id in indexed_files.values()}
                    self.assertEqual(len(all_digests), 1, "All files should have same digest due to hash collision")

                    # Files with same content should be in same EC class
                    self.assertEqual(indexed_files['parent_file1.txt'][1], indexed_files['parent_file2.txt'][1])
                    self.assertEqual(indexed_files['nested/nested_file1.txt'][1],
                                     indexed_files['nested/nested_file2.txt'][1])

                    # Files with different content should be in different EC classes
                    parent_ec_id = indexed_files['parent_file1.txt'][1]
                    nested_ec_id = indexed_files['nested/nested_file1.txt'][1]
                    self.assertNotEqual(parent_ec_id, nested_ec_id,
                                        "Files with different content should have different EC IDs even with same digest")

    def test_import_rejects_path_through_unfollowed_symlink(self):
        """Test that importing through an unfollowed symlink is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create a real directory
            real_dir = parent_path / 'real_dir'
            real_dir.mkdir()

            # Create nested archive inside the real directory
            nested_path = real_dir / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create a symlink pointing to the real directory
            symlink_path = parent_path / 'symlink_dir'
            symlink_path.symlink_to(real_dir)

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Try to import using the symlink path - should fail
                # The import path goes through: parent_path -> symlink_dir -> nested_archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    # Use symlink path to reference the nested archive
                    symlink_nested_path = symlink_path / 'nested_archive'
                    with self.assertRaises(ValueError) as context:
                        parent_archive.import_index(str(symlink_nested_path))
                    self.assertIn('symlink', str(context.exception).lower())
                    self.assertIn('not configured to be followed', str(context.exception))

    def test_import_accepts_path_through_followed_symlink(self):
        """Test that importing through a followed symlink is accepted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create a real directory
            real_dir = parent_path / 'real_dir'
            real_dir.mkdir()

            # Create nested archive inside the real directory
            nested_path = real_dir / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create a symlink pointing to the real directory
            symlink_path = parent_path / 'symlink_dir'
            symlink_path.symlink_to(real_dir)

            # Configure parent archive to follow the symlink
            aridx_path = parent_path / '.aridx'
            aridx_path.mkdir()
            settings_path = aridx_path / 'settings.toml'
            settings_path.write_text('followed_symlinks = ["symlink_dir"]\n')

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import using the symlink path - should succeed
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    symlink_nested_path = symlink_path / 'nested_archive'
                    parent_archive.import_index(str(symlink_nested_path))

                    # Verify the file was imported
                    indexed_files = []
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    self.assertIn('symlink_dir/nested_archive/file.txt', indexed_files)

    def test_import_rejects_nested_path_through_unfollowed_symlink(self):
        """Test that importing rejects paths with symlinks deep in the hierarchy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create directory structure: dir1/symlink_dir/dir2/nested_archive
            dir1 = parent_path / 'dir1'
            dir1.mkdir()

            # Create the real target
            real_target = parent_path / 'real_target'
            real_target.mkdir()
            dir2 = real_target / 'dir2'
            dir2.mkdir()

            # Create nested archive
            nested_path = dir2 / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create symlink in the middle of the path
            symlink_path = dir1 / 'symlink_dir'
            symlink_path.symlink_to(real_target)

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Try to import - should fail because path goes through unfollowed symlink
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    symlink_nested_path = dir1 / 'symlink_dir' / 'dir2' / 'nested_archive'
                    with self.assertRaises(ValueError) as context:
                        parent_archive.import_index(str(symlink_nested_path))
                    self.assertIn('symlink', str(context.exception).lower())
                    self.assertIn('dir1/symlink_dir', str(context.exception))

    def test_import_ancestor_validates_symlinks_in_containing_path(self):
        """Test that importing from ancestor validates symlinks in the contained archive's path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create ancestor archive
            ancestor_path = Path(tmpdir) / 'ancestor'
            ancestor_path.mkdir()

            # Create a real directory in ancestor
            real_dir = ancestor_path / 'real_subdir'
            real_dir.mkdir()

            # Create a symlink in ancestor pointing to real_dir
            symlink = ancestor_path / 'symlink_subdir'
            symlink.symlink_to(real_dir)

            # Create contained archive through symlink path
            # Physical path: ancestor/real_subdir/contained
            # Symlink path: ancestor/symlink_subdir/contained
            contained_path = real_dir / 'contained'
            contained_path.mkdir()
            (ancestor_path / 'file.txt').write_text('ancestor file')
            (contained_path / 'contained_file.txt').write_text('contained file')

            # Build ancestor archive
            with Processor() as processor:
                with Archive(processor, str(ancestor_path), create=True) as ancestor_archive:
                    ancestor_archive.rebuild()

                # Try to import into contained archive using symlink path
                # The contained archive is at ancestor/symlink_subdir/contained
                # When validating, it needs to check if symlink_subdir is followed
                symlink_contained_path = symlink / 'contained'
                with Archive(processor, str(symlink_contained_path), create=True) as contained_archive:
                    with self.assertRaises(ValueError) as context:
                        contained_archive.import_index(str(ancestor_path))
                    self.assertIn('symlink', str(context.exception).lower())
