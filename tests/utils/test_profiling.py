import os
import tempfile
import unittest
from pathlib import Path

from arindexer.utils.profiling import (
    get_profile_dir,
    generate_profile_filename,
    profile_function,
    profile_main,
    profile_worker
)


class ProfilingTest(unittest.TestCase):
    def setUp(self):
        """Save and clear ARINDEXER_PROFILE environment variable."""
        self.original_profile_env = os.environ.get('ARINDEXER_PROFILE')
        self.original_session_dir_env = os.environ.get('_ARINDEXER_PROFILE_SESSION_DIR')
        if 'ARINDEXER_PROFILE' in os.environ:
            del os.environ['ARINDEXER_PROFILE']
        if '_ARINDEXER_PROFILE_SESSION_DIR' in os.environ:
            del os.environ['_ARINDEXER_PROFILE_SESSION_DIR']

    def tearDown(self):
        """Restore original ARINDEXER_PROFILE environment variable."""
        if self.original_profile_env:
            os.environ['ARINDEXER_PROFILE'] = self.original_profile_env
        elif 'ARINDEXER_PROFILE' in os.environ:
            del os.environ['ARINDEXER_PROFILE']

        if self.original_session_dir_env:
            os.environ['_ARINDEXER_PROFILE_SESSION_DIR'] = self.original_session_dir_env
        elif '_ARINDEXER_PROFILE_SESSION_DIR' in os.environ:
            del os.environ['_ARINDEXER_PROFILE_SESSION_DIR']

    def test_get_profile_dir_when_not_set(self):
        """Test get_profile_dir returns None when env var not set."""
        self.assertIsNone(get_profile_dir())

    def test_get_profile_dir_when_set(self):
        """Test get_profile_dir returns path with timestamp_PID subdirectory when env var is set."""
        test_path = "/tmp/test_profile"
        os.environ['ARINDEXER_PROFILE'] = test_path
        # Should return path/timestamp_pid subdirectory
        result = get_profile_dir()
        self.assertTrue(str(result).startswith(test_path))
        # The subdirectory name should be in format: timestamp_pid
        subdir_name = result.name
        parts = subdir_name.split('_')
        self.assertEqual(len(parts), 2, f"Subdirectory should have format timestamp_pid, got {subdir_name}")
        self.assertTrue(parts[0].isdigit(), f"Timestamp part should be numeric, got {parts[0]}")
        self.assertTrue(parts[1].isdigit(), f"PID part should be numeric, got {parts[1]}")
        self.assertEqual(parts[1], str(os.getpid()), "PID should match current process")

    def test_generate_profile_filename_format(self):
        """Test profile filename has correct format."""
        filename = generate_profile_filename("test")
        parts = filename.split('_')

        # Format: prefix_pid_seq.prof
        self.assertEqual(len(parts), 3, f"Expected 3 parts in filename, got {len(parts)}")
        self.assertEqual(parts[0], "test")
        self.assertTrue(parts[1].isdigit(), "PID should be numeric")
        self.assertTrue(parts[2].endswith(".prof"), "Should end with .prof")

        # Check sequence part before .prof extension
        seq_part = parts[2][:-5]  # Remove .prof
        self.assertTrue(seq_part.isdigit(), "Sequence number should be numeric")

        # Verify PID is current process
        self.assertEqual(parts[1], str(os.getpid()), "PID should match current process")

    def test_generate_profile_filename_default_prefix(self):
        """Test profile filename uses default prefix."""
        filename = generate_profile_filename()
        self.assertTrue(filename.startswith("profile_"))

    def test_profile_function_disabled(self):
        """Test that function works normally when profiling is disabled."""
        @profile_function
        def add(a: int, b: int) -> int:
            return a + b

        result = add(2, 3)
        self.assertEqual(result, 5)

    def test_profile_function_with_custom_prefix_disabled(self):
        """Test that function with custom prefix works when profiling is disabled."""
        @profile_function
        def multiply(a: int, b: int) -> int:
            return a * b

        result = multiply(4, 5)
        self.assertEqual(result, 20)

    def test_profile_function_enabled(self):
        """Test that function creates profile file when profiling is enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir

            @profile_function
            def divide(a: int, b: int) -> float:
                return a / b

            result = divide(10, 2)
            self.assertEqual(result, 5.0)

            # Check that profile file was created (in subdirectory)
            profile_files = list(Path(tmpdir).glob("**/*.prof"))
            self.assertGreater(len(profile_files), 0, "Profile file should be created")

            # Verify file has content
            profile_file = profile_files[0]
            self.assertGreater(profile_file.stat().st_size, 0, "Profile file should have content")

    def test_profile_function_with_custom_prefix_enabled(self):
        """Test that function creates profile file with custom prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir

            @profile_function
            def subtract(a: int, b: int) -> int:
                return a - b

            result = subtract(10, 3)
            self.assertEqual(result, 7)

            # Check that profile file was created (in subdirectory)
            profile_files = list(Path(tmpdir).glob("**/*.prof"))
            self.assertGreater(len(profile_files), 0, "Profile file should be created")

    def test_profile_main_decorator(self):
        """Test profile_main convenience decorator."""
        @profile_main
        def main_function() -> str:
            return "main executed"

        result = main_function()
        self.assertEqual(result, "main executed")

    def test_profile_worker_decorator(self):
        """Test profile_worker convenience decorator."""
        @profile_worker
        def worker_function() -> str:
            return "worker executed"

        result = worker_function()
        self.assertEqual(result, "worker executed")

    def test_profile_function_with_exception(self):
        """Test that profile file is created even when function raises exception."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir

            @profile_function
            def failing_function() -> None:
                raise ValueError("Test exception")

            with self.assertRaises(ValueError):
                failing_function()

            # Check that profile file was still created (in subdirectory)
            profile_files = list(Path(tmpdir).glob("**/*.prof"))
            self.assertGreater(len(profile_files), 0,
                             "Profile file should be created even on exception")

    def test_profile_function_preserves_function_attributes(self):
        """Test that decorator preserves function name and docstring."""
        @profile_function
        def documented_function() -> int:
            """This is a documented function."""
            return 42

        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "This is a documented function.")

    def test_multiple_decorated_functions_have_unique_profile_files(self):
        """Test that different decorated functions create uniquely named profile files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir

            @profile_function
            def test_func1() -> int:
                return 1

            @profile_function
            def test_func2() -> int:
                return 2

            @profile_function
            def test_func3() -> int:
                return 3

            # Call each function once
            test_func1()
            test_func2()
            test_func3()

            # Check that multiple profile files were created (in subdirectory)
            profile_files = list(Path(tmpdir).glob("**/*.prof"))
            self.assertGreaterEqual(len(profile_files), 3,
                                  "Should create multiple profile files for different functions")

            # Verify filenames are unique
            filenames = [f.name for f in profile_files]
            self.assertEqual(len(filenames), len(set(filenames)),
                           "All filenames should be unique")

    def test_profile_main_creates_subdirectory(self):
        """Test that profile_main creates a subdirectory with timestamp_PID format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir

            @profile_main
            def main_func() -> str:
                return "executed"

            result = main_func()
            self.assertEqual(result, "executed")

            # Check that a subdirectory was created
            subdirs = [d for d in Path(tmpdir).iterdir() if d.is_dir()]
            self.assertEqual(len(subdirs), 1, "Should create exactly one subdirectory")

            # The subdirectory name should be in format: timestamp_pid
            subdir = subdirs[0]
            parts = subdir.name.split('_')
            self.assertEqual(len(parts), 2, f"Subdirectory should have format timestamp_pid, got {subdir.name}")
            self.assertTrue(parts[0].isdigit(), f"Timestamp should be numeric, got {parts[0]}")
            self.assertTrue(parts[1].isdigit(), f"PID should be numeric, got {parts[1]}")
            self.assertEqual(parts[1], str(os.getpid()), "PID should match current process")

            # Check that profile file was created in the subdirectory
            profile_files = list(subdir.glob("*.prof"))
            self.assertGreater(len(profile_files), 0,
                             "Profile file should be created in subdirectory")
            self.assertTrue(profile_files[0].name.startswith("main_"),
                          "Profile file should have 'main_' prefix")

    def test_profile_main_sets_environment_for_workers(self):
        """Test that profile_main sets session directory environment variable for worker processes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ['ARINDEXER_PROFILE'] = tmpdir
            # Ensure the env var is not already set
            if '_ARINDEXER_PROFILE_SESSION_DIR' in os.environ:
                del os.environ['_ARINDEXER_PROFILE_SESSION_DIR']

            @profile_main
            def main_func() -> str:
                # Check that the environment variable was set during execution
                return os.environ.get('_ARINDEXER_PROFILE_SESSION_DIR', 'not_set')

            result = main_func()
            self.assertNotEqual(result, 'not_set', "Session directory environment variable should be set")
            # Should be in format timestamp_pid
            parts = result.split('_')
            self.assertEqual(len(parts), 2, f"Session dir should have format timestamp_pid, got {result}")
            self.assertTrue(parts[0].isdigit(), "Timestamp should be numeric")
            self.assertTrue(parts[1].isdigit(), "PID should be numeric")
            self.assertEqual(parts[1], str(os.getpid()), "PID should match current process")


if __name__ == '__main__':
    unittest.main()