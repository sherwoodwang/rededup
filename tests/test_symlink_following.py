import tempfile
import unittest
from pathlib import Path

from arindexer import Archive
# noinspection PyProtectedMember
from arindexer._processor import Processor


class SymlinkFollowingTest(unittest.TestCase):
    def test_symlink_not_followed_without_settings(self):
        """Test that symlinks are not followed when not configured in settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create a regular directory with a file
            (archive_path / 'real_dir').mkdir()
            (archive_path / 'real_dir' / 'file1.txt').write_text('content1')

            # Create a symlink to the directory
            (archive_path / 'linked_dir').symlink_to('real_dir')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Without settings, symlink should not be followed
                    # Only real_dir/file1.txt should be indexed
                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            # Extract path from line: file-metadata path_hash:{hash} {path} ...
                            parts = line.split()
                            indexed_files.append(parts[2])

                    self.assertIn('real_dir/file1.txt', indexed_files)
                    # linked_dir should appear as a file (symlink), not as a directory
                    self.assertNotIn('linked_dir/file1.txt', indexed_files)

    def test_symlink_followed_with_settings(self):
        """Test that symlinks are followed when configured in settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create .aridx directory and settings file
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            settings_content = """
followed_symlinks = ["parent/linked_dir"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create an external directory with files (outside the archive)
            external_dir = Path(tmpdir) / 'external_data'
            external_dir.mkdir()
            (external_dir / 'file1.txt').write_text('content1')
            (external_dir / 'file2.txt').write_text('content2')

            # Create a parent directory and a symlink inside it pointing outside the archive
            (archive_path / 'parent').mkdir()
            (archive_path / 'parent' / 'linked_dir').symlink_to(external_dir)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=False) as archive:
                    archive.rebuild()

                    # With settings, symlink should be followed (points outside archive)
                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            # Extract path from line: file-metadata path_hash:{hash} {path} ...
                            parts = line.split()
                            indexed_files.append(parts[2])

                    # Files should be indexed through the symlink path
                    self.assertIn('parent/linked_dir/file1.txt', indexed_files)
                    self.assertIn('parent/linked_dir/file2.txt', indexed_files)

    def test_symlink_selective_following(self):
        """Test that only configured symlinks are followed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create .aridx directory and settings file
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            # Only follow link1, not link2
            settings_content = """
followed_symlinks = ["link1"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create two external directories (outside the archive)
            external_dir1 = Path(tmpdir) / 'external_dir1'
            external_dir1.mkdir()
            (external_dir1 / 'file1.txt').write_text('content1')

            external_dir2 = Path(tmpdir) / 'external_dir2'
            external_dir2.mkdir()
            (external_dir2 / 'file2.txt').write_text('content2')

            # Create two symlinks pointing outside the archive
            (archive_path / 'link1').symlink_to(external_dir1)
            (archive_path / 'link2').symlink_to(external_dir2)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=False) as archive:
                    archive.rebuild()

                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            # Extract path from line: file-metadata path_hash:{hash} {path} ...
                            parts = line.split()
                            indexed_files.append(parts[2])

                    # link1 should be followed (configured in settings, points outside archive)
                    self.assertIn('link1/file1.txt', indexed_files)

                    # link2 should NOT be followed (not configured in settings)
                    self.assertNotIn('link2/file2.txt', indexed_files)

    def test_archive_path_is_symlink(self):
        """Test that symlinks can be followed when archive path itself is a symlink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the real archive directory
            real_archive_path = Path(tmpdir) / 'real_archive'
            real_archive_path.mkdir()

            # Create a symlink to the archive
            symlink_archive_path = Path(tmpdir) / 'symlink_archive'
            symlink_archive_path.symlink_to(real_archive_path)

            # Create .aridx directory and settings file using symlink path
            aridx_path = symlink_archive_path / '.aridx'
            aridx_path.mkdir()

            settings_content = """
followed_symlinks = ["link1"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create an external directory (outside the archive)
            external_dir = Path(tmpdir) / 'external_data'
            external_dir.mkdir()
            (external_dir / 'file1.txt').write_text('content1')
            (external_dir / 'file2.txt').write_text('content2')

            # Create a symlink inside the archive pointing outside
            (symlink_archive_path / 'link1').symlink_to(external_dir)

            # Also create a regular file in the archive
            (symlink_archive_path / 'regular_file.txt').write_text('regular content')

            with Processor() as processor:
                # Open archive using the symlink path
                with Archive(processor, str(symlink_archive_path), create=False) as archive:
                    archive.rebuild()

                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            # Extract path from line: file-metadata path_hash:{hash} {path} ...
                            parts = line.split()
                            indexed_files.append(parts[2])

                    # Regular file should be indexed
                    self.assertIn('regular_file.txt', indexed_files)

                    # External files through followed symlink should be indexed
                    self.assertIn('link1/file1.txt', indexed_files)
                    self.assertIn('link1/file2.txt', indexed_files)
