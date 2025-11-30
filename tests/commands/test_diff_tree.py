"""Tests for diff-tree functionality.

Marker Legend:
--------------
Common markers (files and directories):
  [A] - Analyzed only (exists only in analyzed directory)
  [R] - Archive only (exists only in archive directory)
  [D] - Different content (or partial match for directories)
  [M] - Metadata differs (same content, different metadata - typically files only)

Directory-specific marker:
  [+] - Superset (archive contains all analyzed content plus extras)

Notes:
- For files: [M] indicates content matches but metadata (mtime, mode, etc.) differs
- For directories:
  * [D] indicates partial match (some analyzed content missing from archive)
  * [+] indicates archive has all analyzed content plus extra files
  * [M] rare for directories (would mean all content present but metadata differs)
"""
import io
import sys
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive
from arindexer.commands.diff_tree import (
    NodeStatus,
    do_diff_tree,
)
from arindexer.utils.processor import Processor

from ..test_utils import copy_times, tweak_times


class DiffTreeIntegrationTest(unittest.TestCase):
    """Integration tests for do_diff_tree function."""

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

    def test_identical_directories(self):
        """Identical directories should show 'identical' message.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            Directories are identical.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'file.txt').write_bytes(b'content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', target_file)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('identical', output.lower())

    def test_file_only_in_analyzed(self):
        """File only in analyzed should show [A] marker.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── analyzed_only.txt [A]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            (target_path / 'analyzed_only.txt').write_bytes(b'extra content')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('analyzed_only.txt', output)
            self.assertIn('[A]', output)

    def test_file_only_in_archive(self):
        """File only in archive should show [R] marker.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── archive_only.txt [R]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'archive_only.txt').write_bytes(b'archive content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('archive_only.txt', output)
            self.assertIn('[R]', output)

    def test_file_different_content(self):
        """Files with different content should show [D] marker.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── different.txt [D]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'different.txt').write_bytes(b'archive version')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            (target_path / 'different.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('different.txt', output)
            self.assertIn('[D]', output)

    def test_file_content_match(self):
        """Files with same content but different metadata should show [M] marker.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── content_match.txt [M]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'content_match.txt').write_bytes(b'same content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            # Same content but tweak times to ensure metadata differs
            content_match_file = target_path / 'content_match.txt'
            content_match_file.write_bytes(b'same content')
            tweak_times(content_match_file, 1000000000)  # 1 second difference

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('content_match.txt', output)
            self.assertIn('[M]', output)

    def test_hide_content_match(self):
        """With hide_content_match=True, content-only matches should be hidden.

        Expected output (without flag):
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            ├── content_match.txt [M]
            └── different.txt [D]

        Expected output (with hide_content_match=True):
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── different.txt [D]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'content_match.txt').write_bytes(b'same content')
            (archive_dir / 'different.txt').write_bytes(b'archive version')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            # Same content but tweak times to ensure metadata differs
            content_match_file = target_path / 'content_match.txt'
            content_match_file.write_bytes(b'same content')
            tweak_times(content_match_file, 1000000000)
            # Different content
            (target_path / 'different.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Without hide_content_match - should show content_match.txt
            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('content_match.txt', output)
            self.assertIn('[M]', output)

            # With hide_content_match - should NOT show content_match.txt
            output = self.capture_output(do_diff_tree, target_path, archive_dir, hide_content_match=True)
            self.assertNotIn('content_match.txt', output)
            self.assertNotIn('[M]', output)
            # But should still show different.txt
            self.assertIn('different.txt', output)
            self.assertIn('[D]', output)

    def test_nested_directory_differences(self):
        """Directory with extra analyzed file shows [D] marker (partial match).

        The analyzed directory has an extra file not in archive, so archive
        doesn't contain all analyzed content. This is a partial match.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── subdir [D]
                └── extra.txt [A]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'subdir').mkdir()
            (archive_dir / 'subdir' / 'file.txt').write_bytes(b'content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'subdir').mkdir()
            nested_file = target_path / 'subdir' / 'file.txt'
            nested_file.write_bytes(b'content')
            copy_times(archive_dir / 'subdir' / 'file.txt', nested_file)
            (target_path / 'subdir' / 'extra.txt').write_bytes(b'extra')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('subdir', output)
            self.assertIn('extra.txt', output)
            self.assertIn('[A]', output)
            # Check tree characters are present
            self.assertTrue('├' in output or '└' in output)

    def test_mixed_differences(self):
        """Multiple types of differences including directories with superset/partial matches.

        Tests complex scenario with:
        - Files: identical (hidden), analyzed-only, archive-only, different content, metadata-only diff
        - Symlinks: identical broken symlink (hidden, same target path)
        - Directories: superset (archive has extras), partial (analyzed has extras),
          identical nested directory with symlink (completely hidden), deep nested directory
          (5 levels) with identical files at levels 2, 3, 4 but different file at level 5

        The identical directory structure is created bottom-up with mtime copying to ensure
        all metadata matches, including the broken symlink. This tests that identical files
        are properly hidden at any depth while differences bubble up through the tree.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            ├── analyzed_only.txt [A]
            ├── archive_only.txt [R]
            ├── different.txt [D]
            ├── metadata_diff.txt [M]
            ├── mixed_depth_dir [D]
            │   └── level3 [D]
            │       └── level4 [D]
            │           └── level5 [D]
            │               └── different_deep.txt [D]
            ├── partial_dir [D]
            │   └── analyzed_extra.txt [A]
            └── superset_dir [+]
                └── archive_extra.txt [R]

            Note: identical_level2.txt, identical_level3.txt, and identical_level4.txt
            are hidden at their respective levels since they're identical.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()

            # Files with various differences
            (archive_dir / 'identical.txt').write_bytes(b'same')
            (archive_dir / 'different.txt').write_bytes(b'archive')
            (archive_dir / 'archive_only.txt').write_bytes(b'only here')
            (archive_dir / 'metadata_diff.txt').write_bytes(b'content')

            # Directory that will be superset (archive has extras)
            (archive_dir / 'superset_dir').mkdir()
            (archive_dir / 'superset_dir' / 'common.txt').write_bytes(b'shared')
            (archive_dir / 'superset_dir' / 'archive_extra.txt').write_bytes(b'extra in archive')

            # Directory that will be partial match (analyzed has extras)
            (archive_dir / 'partial_dir').mkdir()
            (archive_dir / 'partial_dir' / 'common.txt').write_bytes(b'shared')

            # Completely identical nested directory (should be hidden)
            (archive_dir / 'identical_dir').mkdir()
            (archive_dir / 'identical_dir' / 'nested').mkdir()
            (archive_dir / 'identical_dir' / 'nested' / 'deep.txt').write_bytes(b'deep file')
            (archive_dir / 'identical_dir' / 'file.txt').write_bytes(b'file')
            # Add broken symlink (with identical target, should also be considered identical)
            (archive_dir / 'identical_dir' / 'broken_link').symlink_to('/nonexistent/target')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()

            # Copy identical file
            identical = target_path / 'identical.txt'
            identical.write_bytes(b'same')
            copy_times(archive_dir / 'identical.txt', identical)

            # Different content file
            (target_path / 'different.txt').write_bytes(b'target')

            # Analyzed-only file
            (target_path / 'analyzed_only.txt').write_bytes(b'only here')

            # Metadata difference file (same content, different mtime)
            metadata_file = target_path / 'metadata_diff.txt'
            metadata_file.write_bytes(b'content')
            tweak_times(metadata_file, 1000000000)

            # Superset directory (archive has extras)
            (target_path / 'superset_dir').mkdir()
            superset_common = target_path / 'superset_dir' / 'common.txt'
            superset_common.write_bytes(b'shared')
            copy_times(archive_dir / 'superset_dir' / 'common.txt', superset_common)
            # Copy directory times so metadata matches (mode/owner/group inherited from tmpdir)
            copy_times(archive_dir / 'superset_dir', target_path / 'superset_dir')

            # Partial directory (analyzed has extras)
            (target_path / 'partial_dir').mkdir()
            partial_common = target_path / 'partial_dir' / 'common.txt'
            partial_common.write_bytes(b'shared')
            copy_times(archive_dir / 'partial_dir' / 'common.txt', partial_common)
            (target_path / 'partial_dir' / 'analyzed_extra.txt').write_bytes(b'extra in analyzed')

            # Identical nested directory - copy bottom-up to preserve mtime
            (target_path / 'identical_dir').mkdir()
            (target_path / 'identical_dir' / 'nested').mkdir()

            # Copy deepest file first
            identical_deep = target_path / 'identical_dir' / 'nested' / 'deep.txt'
            identical_deep.write_bytes(b'deep file')
            copy_times(archive_dir / 'identical_dir' / 'nested' / 'deep.txt', identical_deep)

            # Copy nested directory mtime
            copy_times(archive_dir / 'identical_dir' / 'nested', target_path / 'identical_dir' / 'nested')

            # Copy file in parent directory
            identical_file = target_path / 'identical_dir' / 'file.txt'
            identical_file.write_bytes(b'file')
            copy_times(archive_dir / 'identical_dir' / 'file.txt', identical_file)

            # Copy broken symlink
            identical_link = target_path / 'identical_dir' / 'broken_link'
            identical_link.symlink_to('/nonexistent/target')
            copy_times(archive_dir / 'identical_dir' / 'broken_link', identical_link)

            # Finally copy parent directory mtime
            copy_times(archive_dir / 'identical_dir', target_path / 'identical_dir')

            # Directory with identical files at levels 2, 3, 4 and different file at level 5
            (archive_dir / 'mixed_depth_dir').mkdir()
            (archive_dir / 'mixed_depth_dir' / 'identical_level2.txt').write_bytes(b'level 2')
            (archive_dir / 'mixed_depth_dir' / 'level3').mkdir()
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'identical_level3.txt').write_bytes(b'level 3')
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'level4').mkdir()
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'identical_level4.txt').write_bytes(b'level 4')
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5').mkdir()
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5' / 'different_deep.txt').write_bytes(b'archive version')

            (target_path / 'mixed_depth_dir').mkdir()
            mixed_identical_2 = target_path / 'mixed_depth_dir' / 'identical_level2.txt'
            mixed_identical_2.write_bytes(b'level 2')
            copy_times(archive_dir / 'mixed_depth_dir' / 'identical_level2.txt', mixed_identical_2)

            (target_path / 'mixed_depth_dir' / 'level3').mkdir()
            mixed_identical_3 = target_path / 'mixed_depth_dir' / 'level3' / 'identical_level3.txt'
            mixed_identical_3.write_bytes(b'level 3')
            copy_times(archive_dir / 'mixed_depth_dir' / 'level3' / 'identical_level3.txt', mixed_identical_3)

            (target_path / 'mixed_depth_dir' / 'level3' / 'level4').mkdir()
            mixed_identical_4 = target_path / 'mixed_depth_dir' / 'level3' / 'level4' / 'identical_level4.txt'
            mixed_identical_4.write_bytes(b'level 4')
            copy_times(archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'identical_level4.txt', mixed_identical_4)

            (target_path / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5').mkdir()
            (target_path / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5' / 'different_deep.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)

            # Identical file should not appear in output
            self.assertNotIn('identical.txt', output)

            # Files with differences should appear with correct markers
            self.assertIn('different.txt', output)
            self.assertIn('archive_only.txt', output)
            self.assertIn('analyzed_only.txt', output)
            self.assertIn('metadata_diff.txt', output)

            # Check for marker types that should appear
            self.assertIn('[D]', output)  # Different content
            self.assertIn('[R]', output)  # Archive only
            self.assertIn('[A]', output)  # Analyzed only
            self.assertIn('[M]', output)  # Metadata differs
            self.assertIn('[+]', output)  # Superset marker

            # Directories should appear with correct markers
            self.assertIn('superset_dir', output)
            self.assertIn('partial_dir', output)

            # Nested files in directories should appear
            self.assertIn('archive_extra.txt', output)
            self.assertIn('analyzed_extra.txt', output)

            # Identical directory should be completely hidden (no diffs inside)
            self.assertNotIn('identical_dir', output)

            # Items within identical_dir should not appear
            # Note: Use word boundaries to avoid matching substrings like "different_deep.txt"
            import re
            self.assertIsNone(re.search(r'\bdeep\.txt\b', output))  # Nested file is identical
            self.assertIsNone(re.search(r'\bfile\.txt\b', output))  # File is identical

            # Mixed depth directory: should show parent and nested dirs with different file
            # but NOT the identical files at each level
            self.assertIn('mixed_depth_dir', output)
            self.assertIn('level3', output)
            self.assertIn('level4', output)
            self.assertIn('level5', output)
            self.assertNotIn('identical_level2.txt', output)  # Identical file at level 2 should be hidden
            self.assertNotIn('identical_level3.txt', output)  # Identical file at level 3 should be hidden
            self.assertNotIn('identical_level4.txt', output)  # Identical file at level 4 should be hidden
            self.assertIn('different_deep.txt', output)  # Different file at level 5 should appear


class DiffTreeMaxDepthTest(unittest.TestCase):
    """Tests for max_depth functionality."""

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

    def test_max_depth_1(self):
        """Max depth 1 shows directory marker and elides deeper content.

        Directory has differences (analyzed has extra file), shown as [D].
        The "..." indicates content is elided due to max depth.

        Expected output:
            Comparing:
              Analyzed: /tmp/tmpXXX/target
              Archive:  /tmp/tmpXXX/archive/mydir

            └── subdir [D]
                ...
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'file.txt').write_bytes(b'content')
            (archive_dir / 'subdir').mkdir()
            (archive_dir / 'subdir' / 'nested.txt').write_bytes(b'nested')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            file1 = target_path / 'file.txt'
            file1.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', file1)
            (target_path / 'subdir').mkdir()
            nested = target_path / 'subdir' / 'nested.txt'
            nested.write_bytes(b'nested')
            copy_times(archive_dir / 'subdir' / 'nested.txt', nested)
            (target_path / 'subdir' / 'extra.txt').write_bytes(b'extra')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir, max_depth=1)
            # Should show subdir
            self.assertIn('subdir', output)
            # Should NOT show nested files
            self.assertNotIn('nested.txt', output)
            self.assertNotIn('extra.txt', output)
            # Should show "..." to indicate elision
            self.assertIn('...', output)

    def test_max_depth_2(self):
        """Max depth 2 should show nested files but not deeper."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'subdir').mkdir()
            (archive_dir / 'subdir' / 'nested.txt').write_bytes(b'nested')
            (archive_dir / 'subdir' / 'deep').mkdir()
            (archive_dir / 'subdir' / 'deep' / 'file.txt').write_bytes(b'deep')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'subdir').mkdir()
            nested = target_path / 'subdir' / 'nested.txt'
            nested.write_bytes(b'nested')
            copy_times(archive_dir / 'subdir' / 'nested.txt', nested)
            (target_path / 'subdir' / 'deep').mkdir()
            deep_file = target_path / 'subdir' / 'deep' / 'file.txt'
            deep_file.write_bytes(b'deep')
            copy_times(archive_dir / 'subdir' / 'deep' / 'file.txt', deep_file)
            (target_path / 'subdir' / 'deep' / 'extra.txt').write_bytes(b'extra')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir, max_depth=2)
            # Should show subdir and nested.txt
            self.assertIn('subdir', output)
            # Should show deep directory but not its files
            self.assertIn('deep', output)
            # Should NOT show files in deep/
            self.assertNotIn('file.txt', output)
            self.assertNotIn('extra.txt', output)
            # Should show "..." for elided content
            self.assertIn('...', output)

    def test_max_depth_unlimited(self):
        """Max depth None should show all levels."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'subdir').mkdir()
            (archive_dir / 'subdir' / 'deep').mkdir()
            (archive_dir / 'subdir' / 'deep' / 'file.txt').write_bytes(b'content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'subdir').mkdir()
            (target_path / 'subdir' / 'deep').mkdir()
            deep_file = target_path / 'subdir' / 'deep' / 'file.txt'
            deep_file.write_bytes(b'content')
            copy_times(archive_dir / 'subdir' / 'deep' / 'file.txt', deep_file)
            (target_path / 'subdir' / 'deep' / 'extra.txt').write_bytes(b'extra')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Explicitly pass max_depth=None for unlimited
            output = self.capture_output(do_diff_tree, target_path, archive_dir, max_depth=None)
            # Should show all levels
            self.assertIn('subdir', output)
            self.assertIn('deep', output)
            self.assertIn('extra.txt', output)

    def test_max_depth_default(self):
        """Default max depth (3) should limit deep structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            # Create structure deeper than default depth (3)
            (archive_dir / 'l1').mkdir()
            (archive_dir / 'l1' / 'l2').mkdir()
            (archive_dir / 'l1' / 'l2' / 'l3').mkdir()
            (archive_dir / 'l1' / 'l2' / 'l3' / 'l4').mkdir()
            (archive_dir / 'l1' / 'l2' / 'l3' / 'l4' / 'deep.txt').write_bytes(b'deep')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'l1').mkdir()
            (target_path / 'l1' / 'l2').mkdir()
            (target_path / 'l1' / 'l2' / 'l3').mkdir()
            (target_path / 'l1' / 'l2' / 'l3' / 'l4').mkdir()
            deep = target_path / 'l1' / 'l2' / 'l3' / 'l4' / 'deep.txt'
            deep.write_bytes(b'deep')
            copy_times(archive_dir / 'l1' / 'l2' / 'l3' / 'l4' / 'deep.txt', deep)
            (target_path / 'l1' / 'l2' / 'l3' / 'l4' / 'extra.txt').write_bytes(b'extra')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Use default max_depth by passing 3 explicitly
            output = self.capture_output(do_diff_tree, target_path, archive_dir, max_depth=3)
            # Should show l1, l2, l3
            self.assertIn('l1', output)
            self.assertIn('l2', output)
            self.assertIn('l3', output)
            # Should NOT show deep files at level 4
            self.assertNotIn('deep.txt', output)
            self.assertNotIn('extra.txt', output)
            # Should show "..." for elided content
            self.assertIn('...', output)


class DiffTreeShowFilterTest(unittest.TestCase):
    """Tests for --show filter functionality."""

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

    def test_show_analyzed_only(self):
        """--show analyzed should show only files in analyzed directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'archive_only.txt').write_bytes(b'archive')
            (archive_dir / 'different.txt').write_bytes(b'archive version')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common = target_path / 'common.txt'
            common.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common)
            (target_path / 'analyzed_only.txt').write_bytes(b'analyzed')
            (target_path / 'different.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='analyzed')
            # Should show files in analyzed (analyzed_only, different)
            self.assertIn('analyzed_only.txt', output)
            self.assertIn('[A]', output)
            self.assertIn('different.txt', output)
            self.assertIn('[D]', output)
            # Should NOT show archive_only
            self.assertNotIn('archive_only.txt', output)

    def test_show_archive_only(self):
        """--show archive should show only files in archive directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'archive_only.txt').write_bytes(b'archive')
            (archive_dir / 'different.txt').write_bytes(b'archive version')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common = target_path / 'common.txt'
            common.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common)
            (target_path / 'analyzed_only.txt').write_bytes(b'analyzed')
            (target_path / 'different.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='archive')
            # Should show files in archive (archive_only, different)
            self.assertIn('archive_only.txt', output)
            self.assertIn('[R]', output)
            self.assertIn('different.txt', output)
            self.assertIn('[D]', output)
            # Should NOT show analyzed_only
            self.assertNotIn('analyzed_only.txt', output)

    def test_show_analyzed_with_hide_content_match(self):
        """--show analyzed combined with --hide-content-match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'content_match.txt').write_bytes(b'same content')
            (archive_dir / 'archive_only.txt').write_bytes(b'archive')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common = target_path / 'common.txt'
            common.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common)
            content_match = target_path / 'content_match.txt'
            content_match.write_bytes(b'same content')
            tweak_times(content_match, 1000000000)
            (target_path / 'analyzed_only.txt').write_bytes(b'analyzed')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # With show=analyzed and hide_content_match=True
            output = self.capture_output(
                do_diff_tree, target_path, archive_dir,
                show_filter='analyzed', hide_content_match=True
            )
            # Should show analyzed_only
            self.assertIn('analyzed_only.txt', output)
            # Should NOT show content_match (hidden)
            self.assertNotIn('content_match.txt', output)
            # Should NOT show archive_only (filtered)
            self.assertNotIn('archive_only.txt', output)


class DiffTreeValidationTest(unittest.TestCase):
    """Tests for do_diff_tree validation and error handling."""

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

    def test_archive_path_not_duplicate(self):
        """Error when archive path is not a known duplicate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'actual_dup'
            archive_dir.mkdir()
            (archive_dir / 'file.txt').write_bytes(b'content')
            other_dir = archive_path / 'other_dir'
            other_dir.mkdir()

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            target_file = target_path / 'file.txt'
            target_file.write_bytes(b'content')
            copy_times(archive_dir / 'file.txt', target_file)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Try to diff against a non-duplicate directory
            output = self.capture_output(do_diff_tree, target_path, other_dir)
            self.assertIn('Error', output)
            self.assertIn('not a known duplicate', output)


if __name__ == '__main__':
    unittest.main()
