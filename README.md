# Archive Indexer

A utility to create an index for a collection of files with hash functions and deduplicate another collection of files
against the indexed one.

## Command Line Usage

### Global Options

- `--archive ARCHIVE_PATH`: Specify the path to the archive directory. If not provided, the tool will use the
  `ARINDEXER_ARCHIVE` environment variable or search for an archive starting from the current directory and moving up
  the directory tree.
- `--verbose`: Enable verbose output for more detailed information during operations.

### Commands

#### `rebuild`

Completely rebuilds the archive index from scratch.

```bash
arindexer rebuild
arindexer --archive /path/to/archive rebuild
```

#### `refresh`

Refreshes the archive index by updating it with any changes.

```bash
arindexer refresh
arindexer --archive /path/to/archive refresh
```

#### `find-duplicates [FILES_OR_DIRECTORIES...]`

Finds duplicate files in the specified files or directories against the archive.

**Options:**
- `--ignore TYPES` - Comma-separated list of metadata difference types to ignore when comparing files
- `--show-possible-duplicates` - Show content-wise duplicates that might be actual duplicates

```bash
arindexer find-duplicates /path/to/check
arindexer find-duplicates --ignore size,mtime /path/to/check
arindexer find-duplicates --show-possible-duplicates /path/to/check
```

#### `inspect`

Inspects and displays information about the archive records.

```bash
arindexer inspect
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ARINDEXER_ARCHIVE` | Default archive path when `--archive` is not specified |
| `ARINDEXER_PROFILE` | Enable profiling and specify output directory for cProfile data. When set, creates timestamped subdirectories containing `.prof` files for the main process and any worker processes. |

### Examples

```bash
# Create or rebuild an archive in the current directory
arindexer rebuild

# Find duplicates in a specific directory
arindexer find-duplicates /home/user/documents

# Find duplicates ignoring file modification time differences
arindexer find-duplicates --ignore mtime /home/user/documents

# Use a specific archive location
arindexer --archive /mnt/backup/archive find-duplicates /home/user/documents
```

## Index Format

The Archive Indexer creates a `.aridx` directory in the archive root containing a LevelDB database that stores the
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