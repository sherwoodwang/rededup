import argparse
import logging
import os
import sys
import textwrap
from functools import wraps
from pathlib import Path

from . import Repository, Processor, IndexNotFound
from .utils.profiling import profile_main


def needs_repository(func):
    """Decorator for commands that need the repository to be loaded.

    The decorated function will receive (repository, output, args).
    The wrapper function takes (load_repository_fn, output, args) and creates Processor and calls load_repository_fn.
    """
    @wraps(func)
    def wrapper(load_repository_fn, output, args):
        with Processor() as processor:
            with load_repository_fn(processor) as repository:
                return func(repository, output, args)
    return wrapper


def no_repository(func):
    """Decorator for commands that don't need the repository.

    The decorated function will receive (output, args).
    The wrapper function takes (load_repository_fn, output, args) but doesn't call load_repository_fn or create Processor.
    """
    @wraps(func)
    def wrapper(load_repository_fn, output, args):
        return func(output, args)
    return wrapper


@profile_main
def rededup_main():
    parser = argparse.ArgumentParser(
        prog='rededup',
        description='Create an index for a collection of files with hash functions and deduplicate files against the '
                    'indexed collection.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              rededup rebuild
              rededup analyze /path/to/check
            ''').strip()
    )
    parser.add_argument(
        '--repository',
        metavar='PATH',
        help='Path to the repository directory. If not provided, uses REDEDUP_REPOSITORY environment variable or searches '
             'from current directory upward.')
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output for detailed information during operations')
    parser.add_argument(
        '--log-file',
        metavar='PATH',
        help='Path to log file for operation logging. If not provided, uses logging.path from repository settings or no '
             'logging.')
    parser.add_argument(
        '--log-level',
        metavar='LEVEL',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to INFO when --log-file is provided.')
    subparsers = parser.add_subparsers(
        dest='command',
        title='Commands',
        description='Available commands for repository operations',
        help='Use "rededup COMMAND --help" for command-specific help'
    )

    parser_rebuild = subparsers.add_parser(
        'rebuild',
        help='Completely rebuild the repository index from scratch',
        description='Rebuilds the entire repository index by scanning all files and computing their hashes. This '
                    'operation will overwrite any existing index.')
    parser_rebuild.set_defaults(method=_rebuild, create=True)

    parser_refresh = subparsers.add_parser(
        'refresh',
        help='Refresh the repository index with any changes',
        description='Updates the repository index by scanning for new, modified, or deleted files. More efficient than '
                    'rebuild for incremental updates.')
    parser_refresh.set_defaults(method=_refresh, create=True)

    parser_import = subparsers.add_parser(
        'import',
        help='Import index entries from another repository',
        description='Import index entries from another repository. If the source repository is a nested directory of the '
                    'current repository, entries are imported with the relative path prepended as a prefix. If the '
                    'source repository is an ancestor directory, only entries within the current repository\'s scope are '
                    'imported with their prefix removed.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              # Import from nested directory
              rededup import /repository/subdir

              # Import from ancestor directory
              cd /repository/subdir && rededup import /repository
            ''').strip())
    parser_import.add_argument(
        'source_repository',
        metavar='SOURCE',
        help='Path to the source repository directory to import from')
    parser_import.set_defaults(method=_import_index, create=False)

    parser_analyze = subparsers.add_parser(
        'analyze',
        help='Generate analysis reports for files or directories',
        description='Analyzes the specified paths against the repository and generates persistent reports '
                    'in .report directories. Each report includes duplicate detection results.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              rededup analyze /home/user/documents
              rededup analyze /path/to/file1.txt /path/to/dir2

            Each input path will get its own report directory:
              /home/user/documents.report/
              /path/to/file1.txt.report/
              /path/to/dir2.report/
            ''').strip())
    parser_analyze.add_argument(
        'paths',
        nargs='+',
        metavar='PATH',
        help='Files or directories to analyze against the repository')
    parser_analyze.add_argument(
        '--include-atime',
        action='store_true',
        help='Include access time (atime) when determining if files are identical (default: excluded)')
    parser_analyze.add_argument(
        '--include-ctime',
        action='store_true',
        help='Include change time (ctime) when determining if files are identical (default: excluded)')
    parser_analyze.add_argument(
        '--exclude-owner',
        action='store_true',
        help='Exclude file owner (UID) when determining if files are identical (default: included)')
    parser_analyze.add_argument(
        '--exclude-group',
        action='store_true',
        help='Exclude file group (GID) when determining if files are identical (default: included)')
    parser_analyze.set_defaults(method=_analyze, create=False)

    parser_describe = subparsers.add_parser(
        'describe',
        help='Show duplicate information from existing analysis reports',
        description='Displays duplicate information for files or directories from existing .report directories. '
                    'Searches upward from the specified path to find relevant reports. '
                    'If no path is provided, describes the current working directory.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              rededup describe
              rededup describe /home/user/documents/file.txt
              rededup describe /home/user/documents
              rededup describe --directory /home/user/documents
              rededup describe /path/file1 /path/file2 /path/file3
              rededup describe --all /home/user/documents/file.txt
              rededup describe --limit 5 --sort-by path /home/user/documents

            Single path: Shows list of duplicates found in the repository.
            Directory with --directory: Shows only directory info (no contents table).
            Multiple paths: Shows all paths in a table (no duplicates details).
            ''').strip())
    parser_describe.add_argument(
        'paths',
        nargs='*',
        metavar='PATH',
        help='File or directory to describe from analysis reports (default: current working directory)')
    parser_describe.add_argument(
        '--directory',
        action='store_true',
        help='Describe only the path itself, not its contents (for directories only)')
    parser_describe.add_argument(
        '--all',
        action='store_true',
        help='Show all duplicates (default: show only the most relevant)')
    parser_describe.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Maximum number of duplicates to show (default: 1 if not --all)')
    parser_describe.add_argument(
        '--sort-by',
        choices=['size', 'items', 'identical', 'path'],
        default='size',
        help='Sort duplicates by: size (duplicated_size, default), items (duplicated_items), identical (identity '
             'status), or path (path length)')
    parser_describe.add_argument(
        '--sort-children',
        choices=['dup-size', 'dup-items', 'total-size', 'name'],
        default='dup-size',
        help='Sort directory children by: dup-size (duplicated size descending, default), dup-items (duplicated items '
             'descending), total-size (total size descending), or name (alphabetically)')
    parser_describe.add_argument(
        '--keep-input-order',
        action='store_true',
        help='Keep the input order of paths when multiple paths are provided (default: sort by same criteria as '
             'directory children)')
    parser_describe.add_argument(
        '--bytes',
        action='store_true',
        help='Show sizes in bytes instead of human-readable format (e.g., 1048576 instead of 1.00 MB)')
    parser_describe.add_argument(
        '--details',
        action='store_true',
        help='Show detailed metadata including Report, Analyzed, Repository, Timestamp, Directory/File type, and '
             'Duplicates count')
    parser_describe.set_defaults(method=_describe, create=False)

    parser_diff_tree = subparsers.add_parser(
        'diff-tree',
        help='Compare directory trees between analyzed path and repository duplicate',
        description='Displays a file tree comparison between an analyzed directory and one of its '
                    'duplicates in the repository. Shows which files exist in both, only in analyzed, '
                    'or only in repository.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              rededup diff-tree /path/to/analyzed/dir /repository/path/to/duplicate

            The analyzed path must have an existing analysis report.
            The repository path must be a known duplicate of the analyzed path.
            ''').strip())
    parser_diff_tree.add_argument(
        'analyzed_path',
        metavar='ANALYZED_PATH',
        help='Path to the analyzed directory (must have an existing report)')
    parser_diff_tree.add_argument(
        'repository_path',
        metavar='REPOSITORY_PATH',
        help='Path to the duplicate directory in the repository')
    parser_diff_tree.add_argument(
        '--hide-content-match',
        action='store_true',
        help='Hide files that match content but differ in metadata (only show structural differences)')
    parser_diff_tree.add_argument(
        '--max-depth',
        type=int,
        metavar='N',
        default=3,
        help='Maximum depth to display (default: 3, show "..." for deeper levels)')
    parser_diff_tree.add_argument(
        '--unlimited',
        action='store_true',
        help='Show unlimited depth (overrides --max-depth)')
    parser_diff_tree.add_argument(
        '--show',
        choices=['both', 'analyzed', 'repository'],
        default='both',
        help='Filter which files to show: both (default), analyzed (files in analyzed dir), or repository (files in repository dir)')
    parser_diff_tree.set_defaults(method=_diff_tree, create=False)

    parser_inspect = subparsers.add_parser(
        'inspect',
        help='Inspect and display repository records',
        description='Displays information about the files and records stored in the repository index.')
    parser_inspect.set_defaults(method=_inspect, create=False)

    args = parser.parse_args()

    # Configure logging from CLI argument if provided
    if hasattr(args, 'log_file') and args.log_file:
        # Determine log level: use --log-level if provided, otherwise default to INFO
        log_level = getattr(args, 'log_level', None)
        if log_level is None:
            log_level = 'INFO'

        logging.basicConfig(
            filename=args.log_file,
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    repository_path = args.repository
    if repository_path is None:
        repository_path = os.environ.get('REDEDUP_REPOSITORY')

    def load_repository(processor):
        if repository_path is None:
            working_directory = os.getcwd()
            first_exception = None
            attempt = Path(working_directory)
            while True:
                try:
                    repository = Repository(processor, attempt)
                    break
                except IndexNotFound as e:
                    if attempt == attempt.parent:
                        if args.create:
                            repository = Repository(processor, working_directory, create=True)
                            break
                        else:
                            if first_exception is not None:
                                raise first_exception
                            else:
                                raise

                    attempt = attempt.parent
                    if first_exception is None:
                        first_exception = e
        else:
            repository = Repository(processor, repository_path)

        if not (hasattr(args, 'log_file') and args.log_file):
            repository.configure_logging_from_settings()
        return repository

    # Call the method with load_repository function
    # The @needs_repository decorator will create Processor and load the repository
    # The @no_repository decorator will skip both Processor and repository loading
    args.method(load_repository, None, args)


@needs_repository
def _rebuild(repository: Repository, output, args):
    repository.rebuild()


@needs_repository
def _refresh(repository: Repository, output, args):
    repository.refresh()


@needs_repository
def _analyze(repository: Repository, output, args):
    from .report.duplicate_match import DuplicateMatchRule

    paths = [Path(p) for p in args.paths]

    # Build comparison rule from command-line arguments
    comparison_rule = DuplicateMatchRule(
        include_mtime=True,  # Always included
        include_atime=args.include_atime,  # Default: False
        include_ctime=args.include_ctime,  # Default: False
        include_mode=True,  # Always included
        include_owner=not args.exclude_owner,  # Default: True
        include_group=not args.exclude_group  # Default: True
    )

    repository.analyze(paths, comparison_rule)


@no_repository
def _describe(output, args):
    from .commands.describe import do_describe, DescribeOptions

    # Handle default path (current working directory if no paths provided)
    if not args.paths:
        paths = [Path.cwd()]
    else:
        paths = [Path(p) for p in args.paths]

    # Determine limit
    if args.all:
        limit = None  # No limit
    elif args.limit is not None:
        limit = args.limit
    elif args.details:
        limit = None  # Show all when --details is on (unless --limit is specified)
    else:
        limit = 1  # Default: show only most relevant

    options = DescribeOptions(
        limit=limit,
        sort_by=args.sort_by,
        sort_children=args.sort_children,
        use_bytes=args.bytes,
        show_details=args.details,
        directory_only=args.directory,
        keep_input_order=args.keep_input_order
    )

    # If directory flag is set with multiple paths, error
    if args.directory and len(paths) > 1:
        print("Error: --directory flag can only be used with a single path", file=sys.stderr)
        sys.exit(1)

    # If directory flag is set, validate path is a directory
    if args.directory:
        for path in paths:
            if not path.is_dir():
                print(f"Error: --directory flag can only be used with directories, not files: {path}", file=sys.stderr)
                sys.exit(1)

    # Always pass a list to do_describe
    do_describe(paths, options)


@no_repository
def _diff_tree(output, args):
    from .commands.diff_tree import do_diff_tree

    analyzed_path = Path(args.analyzed_path)
    repository_path = Path(args.repository_path)

    # Handle unlimited flag
    max_depth = None if args.unlimited else args.max_depth

    do_diff_tree(
        analyzed_path,
        repository_path,
        hide_content_match=args.hide_content_match,
        max_depth=max_depth,
        show_filter=args.show
    )


@needs_repository
def _inspect(repository: Repository, output, args):
    for record in repository.inspect():
        print(record)


@needs_repository
def _import_index(repository: Repository, output, args):
    repository.import_index(args.source_repository)


if __name__ == '__main__':
    rededup_main()
