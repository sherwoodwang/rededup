# Rededup

A utility to create an index for a collection of files with hash functions and deduplicate another collection of files
against the indexed one.

## Command Line Usage

### Global Options

- `--repository PATH`: Specify the path to the repository directory. If not provided, the tool will use the
  `REDEDUP_REPOSITORY` environment variable or search for a repository starting from the current directory and moving up
  the directory tree.
- `--verbose`: Enable verbose output for detailed information during operations.
- `--log-file PATH`: Path to log file for operation logging. If not provided, uses `logging.path` from repository settings
  or no logging.
- `--log-level LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to INFO when `--log-file` is provided.

### Commands

#### `rebuild`

Completely rebuilds the repository index from scratch.

```bash
rededup rebuild
rededup --repository /path/to/repository rebuild
```

#### `refresh`

Refreshes the repository index by updating it with any changes.

```bash
rededup refresh
rededup --repository /path/to/repository refresh
```

#### `import SOURCE`

Imports index entries from another repository. If the source repository is a nested directory of the current repository,
entries are imported with the relative path prepended as a prefix. If the source repository is an ancestor directory,
only entries within the current repository's scope are imported with their prefix removed.

```bash
# Import from nested directory
rededup import /repository/subdir

# Import from ancestor directory
cd /repository/subdir && rededup import /repository
```

#### `analyze PATH [PATH ...]`

Analyzes the specified paths against the repository and generates persistent reports in `.report` directories.
Each report includes duplicate detection results.

**Options:**
- `--include-atime` - Include access time (atime) when determining if files are identical (default: excluded)
- `--include-ctime` - Include change time (ctime) when determining if files are identical (default: excluded)
- `--exclude-owner` - Exclude file owner (UID) when determining if files are identical (default: included)
- `--exclude-group` - Exclude file group (GID) when determining if files are identical (default: included)

```bash
rededup analyze /home/user/documents
rededup analyze /path/to/file1.txt /path/to/dir2
```

Each input path will get its own report directory:
- `/home/user/documents.report/`
- `/path/to/file1.txt.report/`
- `/path/to/dir2.report/`

#### `describe [PATH ...]`

Displays duplicate information for files or directories from existing `.report` directories. Searches upward from the
specified path to find relevant reports. If no path is provided, describes the current working directory.

**Options:**
- `--directory` - Describe only the path itself, not its contents (for directories only)
- `--all` - Show all duplicates (default: show only the most relevant)
- `--limit N` - Maximum number of duplicates to show (default: 1 if not `--all`)
- `--sort-by {size,items,identical,path}` - Sort duplicates by: size (duplicated_size, default), items (duplicated_items), identical (identity status), or path (path length)
- `--sort-children {dup-size,dup-items,total-size,name}` - Sort directory children by: dup-size (duplicated size descending, default), dup-items (duplicated items descending), total-size (total size descending), or name (alphabetically)
- `--keep-input-order` - Keep the input order of paths when multiple paths are provided (default: sort by same criteria as directory children)
- `--bytes` - Show sizes in bytes instead of human-readable format (e.g., 1048576 instead of 1.00 MB)
- `--details` - Show detailed metadata including Report, Analyzed, Repository, Timestamp, Directory/File type, and Duplicates count

```bash
rededup describe
rededup describe /home/user/documents/file.txt
rededup describe /home/user/documents
rededup describe --directory /home/user/documents
rededup describe /path/file1 /path/file2 /path/file3
rededup describe --all /home/user/documents/file.txt
rededup describe --limit 5 --sort-by path /home/user/documents
```

**Output behavior:**
- Single path: Shows list of duplicates found in the repository
- Directory with `--directory`: Shows only directory info (no contents table)
- Multiple paths: Shows all paths in a table (no duplicates details)

#### `diff-tree ANALYZED_PATH REPOSITORY_PATH`

Displays a file tree comparison between an analyzed directory and one of its duplicates in the repository.
Shows which files exist in both, only in analyzed, or only in repository.

**Options:**
- `--hide-content-match` - Hide files that match content but differ in metadata (only show structural differences)
- `--max-depth N` - Maximum depth to display (default: 3, show "..." for deeper levels)
- `--unlimited` - Show unlimited depth (overrides `--max-depth`)
- `--show {both,analyzed,repository}` - Filter which files to show: both (default), analyzed (files in analyzed dir), or repository (files in repository dir)

```bash
rededup diff-tree /path/to/analyzed/dir /repository/path/to/duplicate
```

The analyzed path must have an existing analysis report. The repository path must be a known duplicate of the analyzed path.

#### `inspect`

Inspects and displays information about the repository records.

```bash
rededup inspect
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `REDEDUP_REPOSITORY` | Default repository path when `--repository` is not specified |
| `REDEDUP_PROFILE` | Enable profiling and specify output directory for cProfile data. When set, creates timestamped subdirectories containing `.prof` files for the main process and any worker processes. |

### Examples

```bash
# Create or rebuild a repository index in the current directory
rededup rebuild

# Analyze a directory for duplicates
rededup analyze /home/user/documents

# Describe duplicates found for a specific file
rededup describe /home/user/documents/file.txt

# Show all duplicates for a directory
rededup describe --all /home/user/documents

# Compare directory trees between analyzed path and repository duplicate
rededup diff-tree /home/user/documents /mnt/backup/documents

# Use a specific repository location
rededup --repository /mnt/backup/repository analyze /home/user/documents
```

## Index Format

Rededup creates a `.rededup` directory in the repository root containing a LevelDB database that stores the
index. The index uses three main data structures:

### Database Structure

The LevelDB database uses prefixed keys to organize different types of data:

- **Config entries** (`c:` prefix): Store configuration settings
- **File hash entries** (`h:` prefix): Map content hashes to equivalent classes
- **File signature entries** (`m:` prefix): Store file metadata signatures

### Key-Value Schema

#### Configuration Entries
- **Key**: `c:<config_name>`
- **Value**: Configuration value as string
- **Examples**:
  - `c:hash-algorithm` → `"sha256"`
  - `c:truncating` → `"truncate"` (during rebuild operations)

#### File Hash Entries  
- **Key**: `h:<digest><ec_id>`
  - `<digest>`: Binary hash digest (32 bytes for SHA-256)
  - `<ec_id>`: 4-byte big-endian equivalent class ID
- **Value**: MessagePack-encoded list of file paths in the equivalent class
- **Purpose**: Groups files with identical content

#### File Signature Entries
- **Key**: `m:<path_components>` 
  - Path components separated by null bytes (`\0`)
- **Value**: MessagePack-encoded array `[digest, mtime_ns, ec_id]`
  - `digest`: Binary hash of file content
  - `mtime_ns`: Modification time in nanoseconds since epoch
  - `ec_id`: Equivalent class ID (null during initial processing)

### Content Equivalent Classes

Files with identical content are grouped into "equivalent classes" identified by:
1. **Content hash**: SHA-256 digest of file contents
2. **Equivalent class ID**: Local identifier for files with the same hash

This two-level system handles hash collisions by comparing actual file content when hashes match.

### Inspect Output Format

The `inspect` command displays index contents in human-readable format:

```
config <entry> <value>
file-hash <hex_digest> <ec_id> <quoted_paths>
file-metadata <quoted_path> digest:<hex_digest> mtime:<iso_timestamp> ec_id:<id>
```

Where:
- Paths are URL-encoded with `+` for spaces
- Timestamps use ISO 8601 format with nanosecond precision
- Multiple paths in equivalent classes are space-separated