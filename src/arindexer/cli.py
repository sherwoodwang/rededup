import argparse
import logging
import os
import textwrap
from functools import wraps
from pathlib import Path

from . import Archive, Processor, FileMetadataDifferencePattern, FileMetadataDifferenceType, StandardOutput, \
    ArchiveIndexNotFound
from .utils.profiling import profile_main


def needs_archive(func):
    """Decorator for commands that need the archive to be loaded.

    The decorated function will receive (archive, output, args).
    The wrapper function takes (load_archive_fn, output, args) and creates Processor and calls load_archive_fn.
    """
    @wraps(func)
    def wrapper(load_archive_fn, output, args):
        with Processor() as processor:
            with load_archive_fn(processor) as archive:
                return func(archive, output, args)
    return wrapper


def no_archive(func):
    """Decorator for commands that don't need the archive.

    The decorated function will receive (output, args).
    The wrapper function takes (load_archive_fn, output, args) but doesn't call load_archive_fn or create Processor.
    """
    @wraps(func)
    def wrapper(load_archive_fn, output, args):
        return func(output, args)
    return wrapper


@profile_main
def archive_indexer():
    parser = argparse.ArgumentParser(
        prog='arindexer',
        description='Create an index for a collection of files with hash functions and deduplicate files against the '
                    'indexed collection.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              arindexer rebuild
              arindexer find-duplicates /path/to/check
              arindexer --archive /backup/archive find-duplicates --ignore mtime ~/documents
            ''').strip()
    )
    parser.add_argument(
        '--archive',
        metavar='PATH',
        help='Path to the archive directory. If not provided, uses ARINDEXER_ARCHIVE environment variable or searches '
             'from current directory upward.')
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output for detailed information during operations')
    parser.add_argument(
        '--log-file',
        metavar='PATH',
        help='Path to log file for operation logging. If not provided, uses logging.path from archive settings or no logging.')
    parser.add_argument(
        '--log-level',
        metavar='LEVEL',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to INFO when --log-file is provided.')
    subparsers = parser.add_subparsers(
        dest='command',
        title='Commands',
        description='Available commands for archive operations',
        help='Use "arindexer COMMAND --help" for command-specific help'
    )

    parser_rebuild = subparsers.add_parser(
        'rebuild',
        help='Completely rebuild the archive index from scratch',
        description='Rebuilds the entire archive index by scanning all files and computing their hashes. This '
                    'operation will overwrite any existing index.')
    parser_rebuild.set_defaults(method=_rebuild, create=True)

    parser_refresh = subparsers.add_parser(
        'refresh',
        help='Refresh the archive index with any changes',
        description='Updates the archive index by scanning for new, modified, or deleted files. More efficient than '
                    'rebuild for incremental updates.')
    parser_refresh.set_defaults(method=_refresh, create=True)

    parser_import = subparsers.add_parser(
        'import',
        help='Import index entries from another archive',
        description='Import index entries from another archive. If the source archive is a nested directory of the '
                    'current archive, entries are imported with the relative path prepended as a prefix. If the '
                    'source archive is an ancestor directory, only entries within the current archive\'s scope are '
                    'imported with their prefix removed.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              # Import from nested directory
              arindexer import /archive/subdir

              # Import from ancestor directory
              cd /archive/subdir && arindexer import /archive
            ''').strip())
    parser_import.add_argument(
        'source_archive',
        metavar='SOURCE',
        help='Path to the source archive directory to import from')
    parser_import.set_defaults(method=_import_index, create=False)

    parser_find_duplicates = subparsers.add_parser(
        'find-duplicates',
        help='Find duplicate files against the archive',
        description='Searches for files in the specified paths that are duplicates of files in the archive index.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              arindexer find-duplicates /home/user/documents
              arindexer find-duplicates --ignore size,mtime /path/to/check
              arindexer find-duplicates --show-possible-duplicates /media/usb
            ''').strip())
    parser_find_duplicates.add_argument(
        '--ignore',
        metavar='TYPES',
        help='Comma-separated list of metadata difference types to ignore when comparing files (e.g., "size,mtime")')
    parser_find_duplicates.add_argument(
        '--show-possible-duplicates',
        action='store_true',
        help='Show content-wise duplicates that might be actual duplicates')
    parser_find_duplicates.add_argument(
        'file_or_directory',
        nargs='*',
        metavar='PATH',
        help='Files or directories to check for duplicates against the archive')
    parser_find_duplicates.set_defaults(method=_find_duplicates, create=False)

    parser_analyze = subparsers.add_parser(
        'analyze',
        help='Generate analysis reports for files or directories',
        description='Analyzes the specified paths against the archive and generates persistent reports '
                    'in .report directories. Each report includes duplicate detection results.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              arindexer analyze /home/user/documents
              arindexer analyze /path/to/file1.txt /path/to/dir2

            Each input path will get its own report directory:
              /home/user/documents.report/
              /path/to/file1.txt.report/
              /path/to/dir2.report/
            ''').strip())
    parser_analyze.add_argument(
        'paths',
        nargs='+',
        metavar='PATH',
        help='Files or directories to analyze against the archive')
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
        description='Displays duplicate information for a file or directory from existing .report directories. '
                    'Searches upward from the specified path to find relevant reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Examples:
              arindexer describe /home/user/documents/file.txt
              arindexer describe /home/user/documents

            For files: Shows list of duplicates found in the archive.
            For directories: Shows summary of duplicated files with sizes.
            ''').strip())
    parser_describe.add_argument(
        'path',
        metavar='PATH',
        help='File or directory to describe from analysis reports')
    parser_describe.set_defaults(method=_describe, create=False)

    parser_inspect = subparsers.add_parser(
        'inspect',
        help='Inspect and display archive records',
        description='Displays information about the files and records stored in the archive index.')
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

    archive_path = args.archive
    if archive_path is None:
        archive_path = os.environ.get('ARINDEXER_ARCHIVE')

    output = StandardOutput()
    if args.verbose:
        output.verbosity = 1

    def load_archive(processor):
        if archive_path is None:
            working_directory = os.getcwd()
            first_exception = None
            attempt = Path(working_directory)
            while True:
                try:
                    archive = Archive(processor, attempt, output=output)
                    break
                except ArchiveIndexNotFound as e:
                    if attempt == attempt.parent:
                        if args.create:
                            archive = Archive(processor, working_directory, create=True, output=output)
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
            archive = Archive(processor, archive_path, output=output)

        if not (hasattr(args, 'log_file') and args.log_file):
            archive.configure_logging_from_settings()
        return archive

    # Call the method with load_archive function
    # The @needs_archive decorator will create Processor and load the archive
    # The @no_archive decorator will skip both Processor and archive loading
    args.method(load_archive, output, args)


@needs_archive
def _rebuild(archive: Archive, output: StandardOutput, args):
    archive.rebuild()


@needs_archive
def _refresh(archive: Archive, output: StandardOutput, args):
    archive.refresh()


@needs_archive
def _find_duplicates(archive: Archive, output: StandardOutput, args):
    diffptn = FileMetadataDifferencePattern()
    if args.ignore:
        for kind in args.ignore.split(','):
            kind = kind.strip()
            if not kind:
                continue

            diffptn.add(FileMetadataDifferenceType(kind))
    else:
        diffptn.add_trivial_attributes()

    saved_showing_content_wise_duplicates = output.showing_content_wise_duplicates
    try:
        if args.show_possible_duplicates:
            output.showing_content_wise_duplicates = True

        for file_or_directory in args.file_or_directory:
            archive.find_duplicates(Path(file_or_directory), ignore=diffptn)
    finally:
        output.showing_content_wise_duplicates = saved_showing_content_wise_duplicates


@needs_archive
def _analyze(archive: Archive, output: StandardOutput, args):
    from .commands.analyzer import DuplicateMatchRule

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

    archive.analyze(paths, comparison_rule)


@no_archive
def _describe(output: StandardOutput, args):
    from .commands.analyzer import do_describe

    path = Path(args.path)
    do_describe(path)


@needs_archive
def _inspect(archive: Archive, output: StandardOutput, args):
    for record in archive.inspect():
        print(record)


@needs_archive
def _import_index(archive: Archive, output: StandardOutput, args):
    archive.import_index(args.source_archive)


if __name__ == '__main__':
    archive_indexer()
