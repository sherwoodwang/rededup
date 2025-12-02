# Rededup

A utility to create an index for a collection of files with hash functions and deduplicate another collection of files
against the indexed one.

## Command Line Usage

### Global Options

- `--repository REPOSITORY_PATH`: Specify the path to the repository directory. If not provided, the tool will use the
  `REDEDUP_REPOSITORY` environment variable or search for a repository starting from the current directory and moving up
  the directory tree.
- `--verbose`: Enable verbose output for more detailed information during operations.

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

#### `find-duplicates [FILES_OR_DIRECTORIES...]`

Finds duplicate files in the specified files or directories against the repository.

**Options:**
- `--ignore TYPES` - Comma-separated list of metadata difference types to ignore when comparing files
- `--show-possible-duplicates` - Show content-wise duplicates that might be actual duplicates

```bash
rededup find-duplicates /path/to/check
rededup find-duplicates --ignore size,mtime /path/to/check
rededup find-duplicates --show-possible-duplicates /path/to/check
```

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

# Find duplicates in a specific directory
rededup find-duplicates /home/user/documents

# Find duplicates ignoring file modification time differences
rededup find-duplicates --ignore mtime /home/user/documents

# Use a specific repository location
rededup --repository /mnt/backup/repository find-duplicates /home/user/documents
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