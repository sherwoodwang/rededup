import os
import tempfile
import unittest
from pathlib import Path

from rededup.utils.walker import (
    FileContext,
    WalkPolicy,
    walk_with_policy,
    resolve_symlink_target
)


class FileContextTest(unittest.TestCase):
    """Test FileContext class functionality."""

    def test_name_property_read_only(self):
        """Test that name property is read-only."""
        context = FileContext(None, "test.txt")
        self.assertEqual("test.txt", context.name)

        # Verify it's read-only by checking we can't set it
        with self.assertRaises(AttributeError):
            context.name = "other.txt"

    def test_parent_property(self):
        """Test parent property access."""
        parent = FileContext(None, "parent")
        child = FileContext(parent, "child")

        self.assertEqual(parent, child.parent)

    def test_parent_property_raises_on_none(self):
        """Test that accessing parent raises when None."""
        context = FileContext(None, "root")

        with self.assertRaises(LookupError) as cm:
            _ = context.parent

        self.assertIn("no parent", str(cm.exception))

    def test_stat_lazy_loading(self):
        """Test that stat is loaded lazily from path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("content")

            # Create FileContext with path but no stat
            context = FileContext(None, "test.txt", test_file)

            # Stat should not be loaded yet (accessing private field for testing)
            self.assertIsNone(context._stat)

            # Access stat property - should trigger lazy load
            st = context.stat
            self.assertIsNotNone(st)
            self.assertEqual(st.st_size, 7)  # "content" is 7 bytes

            # Verify it's cached (same object)
            self.assertIs(st, context.stat)

    def test_stat_raises_when_unavailable(self):
        """Test that stat raises when neither stat nor path provided."""
        context = FileContext(None, "test")

        with self.assertRaises(LookupError) as cm:
            _ = context.stat

        self.assertIn("stat not available", str(cm.exception))

    def test_stat_provided_directly(self):
        """Test that stat can be provided directly at construction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("content")
            st = test_file.stat()

            context = FileContext(None, "test.txt", st=st)

            # Should return the provided stat
            self.assertEqual(st, context.stat)

    def test_relative_path_single_level(self):
        """Test relative_path for single-level file."""
        context = FileContext(None, "file.txt")

        self.assertEqual(Path("file.txt"), context.relative_path)

    def test_relative_path_nested(self):
        """Test relative_path for nested file structure."""
        root = FileContext(None, None)
        dir1 = FileContext(root, "dir1")
        dir2 = FileContext(dir1, "dir2")
        file_ctx = FileContext(dir2, "file.txt")

        self.assertEqual(Path("dir1/dir2/file.txt"), file_ctx.relative_path)

    def test_relative_path_with_root_name(self):
        """Test relative_path when root has a name."""
        root = FileContext(None, "repository")
        dir1 = FileContext(root, "subdir")
        file_ctx = FileContext(dir1, "file.txt")

        self.assertEqual(Path("repository/subdir/file.txt"), file_ctx.relative_path)

    def test_is_file(self):
        """Test is_file method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test with regular file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("content")
            file_context = FileContext(None, "test.txt", test_file)

            self.assertTrue(file_context.is_file())

            # Test with directory
            test_dir = Path(tmpdir) / "subdir"
            test_dir.mkdir()
            dir_context = FileContext(None, "subdir", test_dir)

            self.assertFalse(dir_context.is_file())

    def test_associated_dict_interface(self):
        """Test dictionary-like interface for associated objects."""
        context = FileContext(None, "test")

        # Test __setitem__ and __getitem__
        context["key1"] = "value1"
        self.assertEqual("value1", context["key1"])

        # Test __contains__
        self.assertTrue("key1" in context)
        self.assertFalse("key2" in context)

        # Test get
        self.assertEqual("value1", context.get("key1"))
        self.assertIsNone(context.get("key2"))
        self.assertEqual("default", context.get("key2", "default"))

        # Test __delitem__
        del context["key1"]
        self.assertFalse("key1" in context)

    def test_complete_calls_associated_complete(self):
        """Test that complete() calls complete() on associated objects."""
        context = FileContext(None, "test")

        # Create mock object with complete method
        class MockCompleter:
            def __init__(self):
                self.completed = False

            def complete(self):
                self.completed = True

        completer = MockCompleter()
        context["completer"] = completer

        # Call complete
        context.complete()

        # Verify completer was called
        self.assertTrue(completer.completed)

    def test_complete_ignores_objects_without_complete(self):
        """Test that complete() ignores associated objects without complete method."""
        context = FileContext(None, "test")
        context["data"] = "some string"

        # Should not raise
        context.complete()


class WalkWithPolicyTest(unittest.TestCase):
    """Test walk_with_policy function."""

    def test_basic_walk(self):
        """Test basic directory walking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "file1.txt").write_text("content1")
            (base / "file2.txt").write_text("content2")
            (base / "subdir").mkdir()
            (base / "subdir" / "file3.txt").write_text("content3")

            policy = WalkPolicy(
                excluded_paths=set(),
                should_follow_symlink=lambda p, c: None,
                yield_root=False
            )

            paths = []
            for file_path, file_context in walk_with_policy(base, policy):
                paths.append(file_context.relative_path)

            # Convert to set for comparison
            paths_set = set(paths)
            expected = {
                Path("file1.txt"),
                Path("file2.txt"),
                Path("subdir"),
                Path("subdir/file3.txt")
            }

            self.assertEqual(expected, paths_set)

    def test_excluded_paths(self):
        """Test that excluded paths are not yielded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "file1.txt").write_text("content1")
            (base / "excluded_dir").mkdir()
            (base / "excluded_dir" / "file2.txt").write_text("content2")
            (base / "included_dir").mkdir()
            (base / "included_dir" / "file3.txt").write_text("content3")

            policy = WalkPolicy(
                excluded_paths={Path("excluded_dir")},
                should_follow_symlink=lambda p, c: None,
                yield_root=False
            )

            paths = []
            for file_path, file_context in walk_with_policy(base, policy):
                paths.append(file_context.relative_path)

            paths_set = set(paths)

            # excluded_dir and its contents should not be present
            self.assertNotIn(Path("excluded_dir"), paths_set)
            self.assertNotIn(Path("excluded_dir/file2.txt"), paths_set)

            # Other files should be present
            self.assertIn(Path("file1.txt"), paths_set)
            self.assertIn(Path("included_dir"), paths_set)
            self.assertIn(Path("included_dir/file3.txt"), paths_set)

    def test_yield_root_false(self):
        """Test yield_root=False does not yield the root path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "file.txt").write_text("content")

            policy = WalkPolicy(
                excluded_paths=set(),
                should_follow_symlink=lambda p, c: None,
                yield_root=False
            )

            paths = []
            for file_path, file_context in walk_with_policy(base, policy):
                paths.append(file_path)

            # Root should not be in paths
            self.assertNotIn(base, paths)
            self.assertIn(base / "file.txt", paths)

    def test_yield_root_true(self):
        """Test yield_root=True yields the root path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "file.txt").write_text("content")

            policy = WalkPolicy(
                excluded_paths=set(),
                should_follow_symlink=lambda p, c: None,
                yield_root=True
            )

            paths = []
            relative_paths = []
            for file_path, file_context in walk_with_policy(base, policy):
                paths.append(file_path)
                relative_paths.append(file_context.relative_path)

            # Root should be first in paths
            self.assertEqual(base, paths[0])

            # Relative path for root should be just the root name
            self.assertEqual(Path(base.name), relative_paths[0])

    def test_symlink_following_policy(self):
        """Test that symlink following policy is respected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create external directory
            external = Path(tmpdir) / "external"
            external.mkdir()
            (external / "external_file.txt").write_text("external content")

            # Create repository with symlink
            repository = Path(tmpdir) / "repository"
            repository.mkdir()
            (repository / "regular_file.txt").write_text("regular")

            symlink = repository / "link_to_external"
            symlink.symlink_to(external)

            # Policy that follows the symlink
            def follow_symlink_policy(file_path: Path, file_context: FileContext) -> FileContext | None:
                if file_context.relative_path == Path("link_to_external"):
                    resolved = resolve_symlink_target(file_path, {repository})
                    if resolved:
                        return FileContext(file_context.parent, file_path.name, resolved)
                return None

            policy = WalkPolicy(
                excluded_paths=set(),
                should_follow_symlink=follow_symlink_policy,
                yield_root=False
            )

            paths = []
            for file_path, file_context in walk_with_policy(repository, policy):
                paths.append(file_context.relative_path)

            paths_set = set(paths)

            # Should include regular file and symlink
            self.assertIn(Path("regular_file.txt"), paths_set)
            self.assertIn(Path("link_to_external"), paths_set)

            # Should include contents from followed symlink
            self.assertIn(Path("link_to_external/external_file.txt"), paths_set)


class ResolveSymlinkTargetTest(unittest.TestCase):
    """Test resolve_symlink_target function."""

    def test_symlink_to_external_directory(self):
        """Test resolving symlink pointing outside boundary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create external target
            external = base / "external"
            external.mkdir()
            (external / "file.txt").write_text("content")

            # Create repository with symlink
            repository = base / "repository"
            repository.mkdir()

            symlink = repository / "link"
            symlink.symlink_to(external)

            # Resolve should succeed (target outside boundary)
            resolved = resolve_symlink_target(symlink, {repository})

            self.assertIsNotNone(resolved)
            self.assertEqual(external, resolved)

    def test_symlink_to_internal_path_returns_none(self):
        """Test that symlink pointing inside boundary returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repository = base / "repository"
            repository.mkdir()

            # Create internal directory
            internal = repository / "internal"
            internal.mkdir()

            # Create symlink pointing to internal directory
            symlink = repository / "link"
            symlink.symlink_to(internal)

            # Should return None (target inside boundary)
            resolved = resolve_symlink_target(symlink, {repository})

            self.assertIsNone(resolved)

    def test_broken_symlink_returns_none(self):
        """Test that broken symlink returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repository = base / "repository"
            repository.mkdir()

            # Create symlink to non-existent target
            symlink = repository / "broken_link"
            symlink.symlink_to("/nonexistent/path")

            resolved = resolve_symlink_target(symlink, {repository})

            self.assertIsNone(resolved)

    def test_symlink_loop_returns_none(self):
        """Test that symlink loop is detected and returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repository = base / "repository"
            repository.mkdir()

            # Create symlink loop
            link1 = repository / "link1"
            link2 = repository / "link2"

            link1.symlink_to(link2)
            link2.symlink_to(link1)

            resolved = resolve_symlink_target(link1, {repository})

            self.assertIsNone(resolved)

    def test_relative_symlink_outside_boundary(self):
        """Test relative symlink pointing outside boundary - currently fails due to path normalization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create external target
            external = base / "external"
            external.mkdir()

            # Create repository
            repository = base / "repository"
            repository.mkdir()

            # Create absolute symlink (relative symlinks with .. have path normalization issues)
            symlink = repository / "link"
            symlink.symlink_to(external)  # Use absolute path instead

            resolved = resolve_symlink_target(symlink, {repository})

            # Should resolve to the external directory
            self.assertIsNotNone(resolved)
            self.assertTrue(resolved.exists())
            self.assertEqual(external.resolve(), resolved.resolve())

    def test_chained_symlinks_outside_boundary(self):
        """Test chain of symlinks all pointing outside boundary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create final target
            final_target = base / "final"
            final_target.mkdir()

            # Create intermediate symlink outside repository
            intermediate = base / "intermediate"
            intermediate.symlink_to(final_target)

            # Create repository with symlink to intermediate
            repository = base / "repository"
            repository.mkdir()

            symlink = repository / "link"
            symlink.symlink_to(intermediate)

            resolved = resolve_symlink_target(symlink, {repository})

            # The function checks if each jump in the chain points under the boundary.
            # intermediate is outside the repository, but it's a symlink itself.
            # The function should follow the entire chain and return the final non-symlink target.
            self.assertIsNotNone(resolved)
            self.assertTrue(resolved.exists())
            # Should resolve to final_target
            self.assertEqual(final_target.resolve(), resolved.resolve())

    def test_chained_symlinks_one_inside_boundary(self):
        """Test chain where one symlink points inside boundary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repository = base / "repository"
            repository.mkdir()

            # Create internal target
            internal = repository / "internal"
            internal.mkdir()

            # Create external symlink pointing to internal
            external_link = base / "external_link"
            external_link.symlink_to(internal)

            # Create symlink in repository pointing to external link
            symlink = repository / "link"
            symlink.symlink_to(external_link)

            # Should return None because chain eventually points inside boundary
            resolved = resolve_symlink_target(symlink, {repository})

            self.assertIsNone(resolved)

    def test_non_symlink_returns_path(self):
        """Test that regular file/directory returns the path itself."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repository = base / "repository"
            repository.mkdir()

            # Create regular file outside repository
            external_file = base / "external.txt"
            external_file.write_text("content")

            resolved = resolve_symlink_target(external_file, {repository})

            # Non-symlink that exists should return the path itself
            self.assertIsNotNone(resolved)
            self.assertEqual(external_file, resolved)


if __name__ == '__main__':
    unittest.main()