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
            # Verify complete tree structure
            self.assertIn('└── analyzed_only.txt [A]', output)

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
            # Verify complete tree structure
            self.assertIn('└── archive_only.txt [R]', output)

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
            # Verify complete tree structure
            self.assertIn('└── different.txt [D]', output)

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
            # Verify complete tree structure
            self.assertIn('└── content_match.txt [M]', output)

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

            # Without hide_content_match - should show both files
            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            expected_tree = (
                '├── content_match.txt [M]\n'
                '└── different.txt [D]'
            )
            self.assertIn(expected_tree, output)

            # With hide_content_match - should only show different.txt
            output = self.capture_output(do_diff_tree, target_path, archive_dir, hide_content_match=True)
            self.assertIn('└── different.txt [D]', output)
            self.assertNotIn('content_match.txt', output)

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
            # Verify complete nested tree structure
            expected_tree = (
                '└── subdir [D]\n'
                '    └── extra.txt [A]'
            )
            self.assertIn(expected_tree, output)

    def test_nested_three_level_directories(self):
        """Three-level nested directories with identical files at each level and mixed content.

        Structure:
        - level1/
          - identical_l1.txt (identical)
          - level2a/
            - identical_l2a.txt (identical)
            - level3a/
              - analyzed_only_1.txt [A]
              - analyzed_only_2.txt [A]
              - archive_only_1.txt [R]
              - archive_only_2.txt [R]
          - level2b/
            - identical_l2b.txt (identical)
            - level3b/
              - different.txt [D]

        Expected behavior:
        - Each level's identical file should be hidden
        - level3a should show multiple files with different markers
        - level3b should show only the different file
        - All directory levels should be marked [D] (different/partial)
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()

            # Create archive structure
            (archive_dir / 'level1').mkdir()
            (archive_dir / 'level1' / 'identical_l1.txt').write_bytes(b'level 1 content')

            # First branch - level2a -> level3a with mixed files
            (archive_dir / 'level1' / 'level2a').mkdir()
            (archive_dir / 'level1' / 'level2a' / 'identical_l2a.txt').write_bytes(b'level 2a content')
            (archive_dir / 'level1' / 'level2a' / 'level3a').mkdir()
            (archive_dir / 'level1' / 'level2a' / 'level3a' / 'archive_only_1.txt').write_bytes(b'archive 1')
            (archive_dir / 'level1' / 'level2a' / 'level3a' / 'archive_only_2.txt').write_bytes(b'archive 2')

            # Second branch - level2b -> level3b with different file
            (archive_dir / 'level1' / 'level2b').mkdir()
            (archive_dir / 'level1' / 'level2b' / 'identical_l2b.txt').write_bytes(b'level 2b content')
            (archive_dir / 'level1' / 'level2b' / 'level3b').mkdir()
            (archive_dir / 'level1' / 'level2b' / 'level3b' / 'different.txt').write_bytes(b'archive version')

            # Create analyzed structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'level1').mkdir()

            # Copy identical file at level 1
            l1_identical = target_path / 'level1' / 'identical_l1.txt'
            l1_identical.write_bytes(b'level 1 content')
            copy_times(archive_dir / 'level1' / 'identical_l1.txt', l1_identical)

            # First branch - level2a -> level3a with mixed files
            (target_path / 'level1' / 'level2a').mkdir()
            l2a_identical = target_path / 'level1' / 'level2a' / 'identical_l2a.txt'
            l2a_identical.write_bytes(b'level 2a content')
            copy_times(archive_dir / 'level1' / 'level2a' / 'identical_l2a.txt', l2a_identical)
            (target_path / 'level1' / 'level2a' / 'level3a').mkdir()
            (target_path / 'level1' / 'level2a' / 'level3a' / 'analyzed_only_1.txt').write_bytes(b'analyzed 1')
            (target_path / 'level1' / 'level2a' / 'level3a' / 'analyzed_only_2.txt').write_bytes(b'analyzed 2')

            # Second branch - level2b -> level3b with different file
            (target_path / 'level1' / 'level2b').mkdir()
            l2b_identical = target_path / 'level1' / 'level2b' / 'identical_l2b.txt'
            l2b_identical.write_bytes(b'level 2b content')
            copy_times(archive_dir / 'level1' / 'level2b' / 'identical_l2b.txt', l2b_identical)
            (target_path / 'level1' / 'level2b' / 'level3b').mkdir()
            (target_path / 'level1' / 'level2b' / 'level3b' / 'different.txt').write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)

            # Verify complete tree structure
            expected_tree = (
                '└── level1 [D]\n'
                '    ├── level2a [D]\n'
                '    │   └── level3a [D]\n'
                '    │       ├── analyzed_only_1.txt [A]\n'
                '    │       ├── analyzed_only_2.txt [A]\n'
                '    │       ├── archive_only_1.txt [R]\n'
                '    │       └── archive_only_2.txt [R]\n'
                '    └── level2b [D]\n'
                '        └── level3b [D]\n'
                '            └── different.txt [D]'
            )
            self.assertIn(expected_tree, output)

            # Verify identical files are hidden
            self.assertNotIn('identical_l1.txt', output)
            self.assertNotIn('identical_l2a.txt', output)
            self.assertNotIn('identical_l2b.txt', output)

    def test_nested_three_level_directories_with_hide_content_match(self):
        """Three-level nested directories with metadata-only difference that gets hidden.

        Structure:
        - level1/
          - identical_l1.txt (identical)
          - level2a/
            - identical_l2a.txt (identical)
            - level3a/
              - analyzed_only_1.txt [A]
              - analyzed_only_2.txt [A]
              - archive_only_1.txt [R]
              - archive_only_2.txt [R]
          - level2b/
            - identical_l2b.txt (identical)
            - level3b/
              - metadata_diff.txt [M] (content match, metadata differs)

        Expected behavior with --hide-content-match:
        - metadata_diff.txt should be hidden
        - level3b should be hidden (no visible children)
        - level2b should be hidden (no visible children after level3b is hidden)
        - Only level1 -> level2a -> level3a branch should be visible
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()

            # Create archive structure
            (archive_dir / 'level1').mkdir()
            (archive_dir / 'level1' / 'identical_l1.txt').write_bytes(b'level 1 content')

            # First branch - level2a -> level3a with mixed files
            (archive_dir / 'level1' / 'level2a').mkdir()
            (archive_dir / 'level1' / 'level2a' / 'identical_l2a.txt').write_bytes(b'level 2a content')
            (archive_dir / 'level1' / 'level2a' / 'level3a').mkdir()
            (archive_dir / 'level1' / 'level2a' / 'level3a' / 'archive_only_1.txt').write_bytes(b'archive 1')
            (archive_dir / 'level1' / 'level2a' / 'level3a' / 'archive_only_2.txt').write_bytes(b'archive 2')

            # Second branch - level2b -> level3b with metadata-only difference
            (archive_dir / 'level1' / 'level2b').mkdir()
            (archive_dir / 'level1' / 'level2b' / 'identical_l2b.txt').write_bytes(b'level 2b content')
            (archive_dir / 'level1' / 'level2b' / 'level3b').mkdir()
            (archive_dir / 'level1' / 'level2b' / 'level3b' / 'metadata_diff.txt').write_bytes(b'same content')

            # Create analyzed structure
            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            (target_path / 'level1').mkdir()

            # Copy identical file at level 1
            l1_identical = target_path / 'level1' / 'identical_l1.txt'
            l1_identical.write_bytes(b'level 1 content')
            copy_times(archive_dir / 'level1' / 'identical_l1.txt', l1_identical)

            # First branch - level2a -> level3a with mixed files
            (target_path / 'level1' / 'level2a').mkdir()
            l2a_identical = target_path / 'level1' / 'level2a' / 'identical_l2a.txt'
            l2a_identical.write_bytes(b'level 2a content')
            copy_times(archive_dir / 'level1' / 'level2a' / 'identical_l2a.txt', l2a_identical)
            (target_path / 'level1' / 'level2a' / 'level3a').mkdir()
            (target_path / 'level1' / 'level2a' / 'level3a' / 'analyzed_only_1.txt').write_bytes(b'analyzed 1')
            (target_path / 'level1' / 'level2a' / 'level3a' / 'analyzed_only_2.txt').write_bytes(b'analyzed 2')

            # Second branch - level2b -> level3b with metadata-only difference
            (target_path / 'level1' / 'level2b').mkdir()
            l2b_identical = target_path / 'level1' / 'level2b' / 'identical_l2b.txt'
            l2b_identical.write_bytes(b'level 2b content')
            copy_times(archive_dir / 'level1' / 'level2b' / 'identical_l2b.txt', l2b_identical)
            (target_path / 'level1' / 'level2b' / 'level3b').mkdir()
            metadata_diff_file = target_path / 'level1' / 'level2b' / 'level3b' / 'metadata_diff.txt'
            metadata_diff_file.write_bytes(b'same content')
            # Tweak times to create metadata difference
            tweak_times(metadata_diff_file, 1000000000)  # 1 second difference

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # Without hide_content_match - should show both branches
            output = self.capture_output(do_diff_tree, target_path, archive_dir)
            self.assertIn('level2a', output)
            self.assertIn('level3a', output)
            self.assertIn('level2b', output)
            self.assertIn('level3b', output)
            self.assertIn('metadata_diff.txt [M]', output)

            # With hide_content_match - level2b/level3b branch should be hidden
            output = self.capture_output(do_diff_tree, target_path, archive_dir, hide_content_match=True)

            expected_tree = (
                '└── level1 [D]\n'
                '    └── level2a [D]\n'
                '        └── level3a [D]\n'
                '            ├── analyzed_only_1.txt [A]\n'
                '            ├── analyzed_only_2.txt [A]\n'
                '            ├── archive_only_1.txt [R]\n'
                '            └── archive_only_2.txt [R]'
            )
            self.assertIn(expected_tree, output)

            # Verify level2b branch is completely hidden
            self.assertNotIn('level2b', output)
            self.assertNotIn('level3b', output)
            self.assertNotIn('metadata_diff.txt', output)

            # Verify identical files are hidden
            self.assertNotIn('identical_l1.txt', output)
            self.assertNotIn('identical_l2a.txt', output)
            self.assertNotIn('identical_l2b.txt', output)

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
            (archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5' / 'different_deep.txt')\
                .write_bytes(b'archive version')

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
            copy_times(
                archive_dir / 'mixed_depth_dir' / 'level3' / 'level4' / 'identical_level4.txt',
                mixed_identical_4
            )

            (target_path / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5').mkdir()
            (target_path / 'mixed_depth_dir' / 'level3' / 'level4' / 'level5' / 'different_deep.txt')\
                .write_bytes(b'analyzed version')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            output = self.capture_output(do_diff_tree, target_path, archive_dir)

            # Verify complete tree structure
            expected_tree = (
                '├── analyzed_only.txt [A]\n'
                '├── archive_only.txt [R]\n'
                '├── different.txt [D]\n'
                '├── metadata_diff.txt [M]\n'
                '├── mixed_depth_dir [D]\n'
                '│   └── level3 [D]\n'
                '│       └── level4 [D]\n'
                '│           └── level5 [D]\n'
                '│               └── different_deep.txt [D]\n'
                '├── partial_dir [D]\n'
                '│   └── analyzed_extra.txt [A]\n'
                '└── superset_dir [+]\n'
                '    └── archive_extra.txt [R]'
            )
            self.assertIn(expected_tree, output)

            # Verify items that should NOT appear (identical files/dirs)
            self.assertNotIn('identical.txt', output)
            self.assertNotIn('identical_dir', output)
            self.assertNotIn('identical_level2.txt', output)
            self.assertNotIn('identical_level3.txt', output)
            self.assertNotIn('identical_level4.txt', output)


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
            # Verify complete tree structure with depth limit
            expected_tree = (
                '└── subdir [D]\n'
                '    └── ...'
            )
            self.assertIn(expected_tree, output)
            self.assertNotIn('nested.txt', output)
            self.assertNotIn('extra.txt', output)

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
            # Verify complete tree structure with depth limit
            expected_tree = (
                '└── subdir [D]\n'
                '    └── deep [D]\n'
                '        └── ...'
            )
            self.assertIn(expected_tree, output)
            self.assertNotIn('file.txt', output)
            self.assertNotIn('extra.txt', output)

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
            # Verify complete tree structure with all levels
            expected_tree = (
                '└── subdir [D]\n'
                '    └── deep [D]\n'
                '        └── extra.txt [A]'
            )
            self.assertIn(expected_tree, output)

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
            # Verify complete tree structure with depth limit
            expected_tree = (
                '└── l1 [D]\n'
                '    └── l2 [D]\n'
                '        └── l3 [D]\n'
                '            └── ...'
            )
            self.assertIn(expected_tree, output)
            self.assertNotIn('deep.txt', output)
            self.assertNotIn('extra.txt', output)


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
            # Verify complete tree structure
            expected_tree = (
                '├── analyzed_only.txt [A]\n'
                '└── different.txt [D]'
            )
            self.assertIn(expected_tree, output)
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
            # Verify complete tree structure
            expected_tree = (
                '├── archive_only.txt [R]\n'
                '└── different.txt [D]'
            )
            self.assertIn(expected_tree, output)
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
            # Verify complete tree structure
            self.assertIn('└── analyzed_only.txt [A]', output)
            self.assertNotIn('content_match.txt', output)
            self.assertNotIn('archive_only.txt', output)

    def test_nested_directory_hidden_when_only_child_filtered(self):
        """Nested directory should be hidden when its only different child is filtered out.

        Structure:
        - common.txt (identical, at root to enable analysis)
        - parent_dir/
          - child_dir/
            - archive_only.txt [R] (only in archive, filtered out with --show analyzed)

        With --show analyzed:
        - child_dir has no children that pass the filter, so it should be hidden
        - parent_dir has no children that pass the filter (child_dir is hidden), so it should be hidden
        - Result: no directories shown
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'parent_dir').mkdir()
            (archive_dir / 'parent_dir' / 'child_dir').mkdir()
            (archive_dir / 'parent_dir' / 'child_dir' / 'archive_only.txt').write_bytes(b'archive')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            (target_path / 'parent_dir').mkdir()
            (target_path / 'parent_dir' / 'child_dir').mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # With --show analyzed: archive_only.txt is filtered out
            # So child_dir has no visible children and should be hidden
            # So parent_dir has no visible children and should be hidden
            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='analyzed')
            self.assertNotIn('parent_dir', output)
            self.assertNotIn('child_dir', output)
            self.assertNotIn('archive_only.txt', output)

            # With --show archive: archive_only.txt passes filter
            # So child_dir and parent_dir should be visible
            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='archive')
            expected_tree = (
                '└── parent_dir [D]\n'
                '    └── child_dir [D]\n'
                '        └── archive_only.txt [R]'
            )
            self.assertIn(expected_tree, output)

    def test_directory_with_mixed_filtered_children(self):
        """Directory should remain visible when one child is filtered out but another remains.

        Structure:
        - common.txt (identical, at root to enable analysis)
        - parent_dir/
          - analyzed_subdir/
            - identical.txt (identical)
            - analyzed_only.txt [A] (only in analyzed)
          - archive_subdir/
            - identical.txt (identical)
            - archive_only.txt [R] (only in archive)

        With --show analyzed:
        - analyzed_only.txt passes filter, so analyzed_subdir is visible
        - archive_only.txt is filtered out, and archive_subdir has no other visible children, so archive_subdir is
          hidden
        - parent_dir should still be visible because it has one visible child (analyzed_subdir)
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            archive_dir = archive_path / 'mydir'
            archive_dir.mkdir()
            (archive_dir / 'common.txt').write_bytes(b'content')
            (archive_dir / 'parent_dir').mkdir()

            # archive_subdir with identical file and archive_only file
            (archive_dir / 'parent_dir' / 'archive_subdir').mkdir()
            (archive_dir / 'parent_dir' / 'archive_subdir' / 'identical.txt').write_bytes(b'identical content')
            (archive_dir / 'parent_dir' / 'archive_subdir' / 'archive_only.txt').write_bytes(b'archive')

            # analyzed_subdir with identical file (only in archive)
            (archive_dir / 'parent_dir' / 'analyzed_subdir').mkdir()
            (archive_dir / 'parent_dir' / 'analyzed_subdir' / 'identical.txt').write_bytes(b'identical content')

            target_path = Path(tmpdir) / 'target'
            target_path.mkdir()
            common_file = target_path / 'common.txt'
            common_file.write_bytes(b'content')
            copy_times(archive_dir / 'common.txt', common_file)
            (target_path / 'parent_dir').mkdir()

            # analyzed_subdir with identical file and analyzed_only file
            (target_path / 'parent_dir' / 'analyzed_subdir').mkdir()
            analyzed_identical = target_path / 'parent_dir' / 'analyzed_subdir' / 'identical.txt'
            analyzed_identical.write_bytes(b'identical content')
            copy_times(archive_dir / 'parent_dir' / 'analyzed_subdir' / 'identical.txt', analyzed_identical)
            (target_path / 'parent_dir' / 'analyzed_subdir' / 'analyzed_only.txt').write_bytes(b'analyzed')

            # archive_subdir with identical file (only in analyzed)
            (target_path / 'parent_dir' / 'archive_subdir').mkdir()
            archive_identical = target_path / 'parent_dir' / 'archive_subdir' / 'identical.txt'
            archive_identical.write_bytes(b'identical content')
            copy_times(archive_dir / 'parent_dir' / 'archive_subdir' / 'identical.txt', archive_identical)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.analyze([target_path])

            # With --show analyzed: only analyzed_only.txt is visible
            # archive_subdir is hidden (only has identical file, which is hidden), but analyzed_subdir is visible
            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='analyzed')

            expected_tree = (
                '└── parent_dir [D]\n'
                '    └── analyzed_subdir [D]\n'
                '        └── analyzed_only.txt [A]'
            )
            self.assertIn(expected_tree, output)
            self.assertNotIn('archive_only.txt', output)
            self.assertNotIn('archive_subdir', output)
            self.assertNotIn('identical.txt', output)

            # With --show archive: only archive_only.txt is visible
            # analyzed_subdir is hidden (only has identical file, which is hidden), but archive_subdir is visible
            output = self.capture_output(do_diff_tree, target_path, archive_dir, show_filter='archive')

            expected_tree = (
                '└── parent_dir [D]\n'
                '    └── archive_subdir [+]\n'
                '        └── archive_only.txt [R]'
            )
            self.assertIn(expected_tree, output)
            self.assertNotIn('analyzed_only.txt', output)
            self.assertNotIn('analyzed_subdir', output)
            self.assertNotIn('identical.txt', output)


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
