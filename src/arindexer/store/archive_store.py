import urllib.parse
from pathlib import Path
from threading import Lock
from typing import Iterator, Iterable, Any

import mmh3
import msgpack
import plyvel

from ..utils.keyed_lock import KeyedLock
from ..utils.varint import encode_varint, decode_varint
from ..utils.walker import FileContext, WalkPolicy, walk_with_policy, resolve_symlink_target
from .archive_settings import ArchiveSettings, SETTING_FOLLOWED_SYMLINKS


class FileSignature:
    """File metadata signature for content tracking.

    Attributes:
        path: File path relative to archive root
        digest: Content digest (hash) of the file
        mtime_ns: File modification time in nanoseconds since epoch
        ec_id: Equivalence Class ID - identifies which EC class this file belongs to
               for the given digest. Can be None if not yet assigned to an EC class.

    Notes:
        - The ec_id is scoped to the digest; different digests can have the same ec_id
        - The combination (digest, ec_id) identifies an EC class containing files with
          identical content
        - Files with the same digest but in different EC classes have different content
          (hash collision case)
    """

    def __init__(self, path: Path, digest: bytes, mtime_ns: int | None, ec_id: int | None):
        self.path = path
        self.digest = digest
        self.mtime_ns = mtime_ns
        self.ec_id = ec_id


class ArchiveIndexNotFound(FileNotFoundError):
    pass


class ArchiveStore:
    """Low-level data operations layer for archive file indexing.

    This class represents the archive from a data operations perspective, providing
    direct access to the persistent storage layer (LevelDB). It handles:
    - Database lifecycle (open, close, context management)
    - Manifest storage and retrieval
    - File signature persistence (digest, mtime, equivalent class ID)
    - Content equivalent class management
    - File system traversal

    ArchiveStore operates at the storage level, exposing primitive operations for
    reading and writing archive metadata. It makes no assumptions about workflows
    or business logic - it simply provides the foundational data operations that
    higher-level components can compose into more complex behaviors.

    Contrast with Archive class, which provides high-level operations like rebuild()
    and refresh() that orchestrate multiple ArchiveStore operations into complete
    workflows with proper error handling and concurrency management.

    Content Equivalent Classes (EC Classes):

    EC classes group files that have identical content. The key requirements are:

    1. **Digest Scoping**: EC IDs are scoped per digest. Multiple digests can have
       EC ID 0, EC ID 1, etc. The combination of (digest, ec_id) uniquely identifies
       an EC class.

    2. **Content Identity**: All files within an EC class MUST have byte-for-byte
       identical content. This is critical for handling hash collisions - files with
       the same digest but different content MUST be in separate EC classes.

    3. **Hash Collision Handling**: When a hash collision occurs (same digest,
       different content), files are separated into distinct EC classes with
       different ec_ids. For example:
       - Files A, B with digest "abc123" and content X → (digest="abc123", ec_id=0)
       - Files C, D with digest "abc123" and content Y → (digest="abc123", ec_id=1)

    4. **EC ID Assignment**: EC IDs start at 0 for each digest and increment as
       needed. When importing or merging archives, EC IDs may need remapping to
       avoid collisions while preserving content equivalence relationships.

    5. **Content Verification**: Operations that merge EC classes (like import)
       MUST verify that files actually have identical content by comparing bytes,
       not just by assuming same digest means same content.
    """
    __MANIFEST_PROPERTY_PREFIX = b'p'
    __FILE_HASH_PREFIX = b'h'
    __FILE_SIGNATURE_PREFIX = b's'

    MANIFEST_HASH_ALGORITHM = 'hash-algorithm'
    MANIFEST_PENDING_ACTION = 'truncating'
    MANIFEST_ARCHIVE_ID = 'archive-id'

    def __init__(self, settings: ArchiveSettings, archive_path: Path, create: bool = False):
        """Initialize raw archive with LevelDB database.

        Args:
            settings: Archive settings for configuration
            archive_path: Archive root directory path
            create: Create .aridx directory if missing

        Raises:
            FileNotFoundError: Archive directory does not exist
            NotADirectoryError: Archive path is not a directory
            ArchiveIndexNotFound: Index directory missing and create=False
        """
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive {archive_path} does not exist")

        if not archive_path.is_dir():
            raise NotADirectoryError(f"Archive {archive_path} is not a directory")

        index_path = archive_path / '.aridx'

        if create:
            index_path.mkdir(exist_ok=True)

        if not index_path.exists():
            raise ArchiveIndexNotFound(f"The index for archive {archive_path} has not been created")

        if not index_path.is_dir():
            raise NotADirectoryError(f"The index for archive {archive_path} is not a directory")

        database_path = index_path / 'database'

        database = None
        try:
            database = plyvel.DB(str(database_path), create_if_missing=True)
            manifest_database: plyvel.DB = database.prefixed_db(ArchiveStore.__MANIFEST_PROPERTY_PREFIX)
            file_hash_database: plyvel.DB = database.prefixed_db(ArchiveStore.__FILE_HASH_PREFIX)
            file_signature_database: plyvel.DB = database.prefixed_db(ArchiveStore.__FILE_SIGNATURE_PREFIX)
        except:
            if database is not None:
                database.close()
            raise

        self._archive_path = archive_path
        self._alive = True
        self._database = database
        self._manifest_database = manifest_database
        self._file_hash_database = file_hash_database
        self._file_signature_database = file_signature_database
        self._settings = settings

        # Concurrency control for database operations
        self._ec_prefix_lock = KeyedLock()  # For EC class operations
        self._path_hash_lock = KeyedLock()  # For file signature operations
        self._manifest_lock = Lock()  # For manifest operations (e.g., ensure_archive_id)

    def __del__(self):
        """Destructor ensures database is closed."""
        self.close()

    def __enter__(self):
        """Context manager entry, validates raw archive is still alive."""
        if not self._alive:
            raise BrokenPipeError(f"Raw archive was closed")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, closes database connection."""
        self.close()

    def close(self):
        """Close LevelDB database and mark raw archive as closed."""
        if not getattr(self, '_alive', False):
            return

        self._alive = False
        self._file_hash_database = None
        self._database.close()
        self._database = None

    @property
    def archive_path(self) -> Path:
        """Get the archive root directory path."""
        return self._archive_path

    def truncate(self):
        """Clear all file hash and signature entries, reset manifest."""
        self.write_manifest(ArchiveStore.MANIFEST_PENDING_ACTION, 'truncate')
        self.write_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM, None)

        batch = self._file_signature_database.write_batch()
        for key, _ in self._file_signature_database.iterator():
            batch.delete(key)
        batch.write()

        batch = self._file_hash_database.write_batch()
        for key, _ in self._file_hash_database.iterator():
            batch.delete(key)
        batch.write()

        self.write_manifest(ArchiveStore.MANIFEST_PENDING_ACTION, None)

    def write_manifest(self, entry: str, value: str | None) -> None:
        """Write or delete manifest property. None value deletes the key."""
        if value is None:
            self._manifest_database.delete(entry.encode())
        else:
            self._manifest_database.put(entry.encode(), value.encode())

    def read_manifest(self, entry: str) -> str | None:
        """Read manifest property value from 'p' prefixed database entry."""
        value = self._manifest_database.get(entry.encode())

        if value is not None:
            value = value.decode()

        return value

    def ensure_archive_id(self) -> str:
        """Ensure archive ID exists in manifest, generating if needed.

        Thread-safe: Uses a lock to prevent race conditions where multiple threads
        might simultaneously check for the archive ID and generate different IDs.

        This is an atomic operation that checks for existence and generates
        a new ID only if one doesn't already exist.

        Returns:
            The archive ID (existing or newly generated)
        """
        import uuid

        key = ArchiveStore.MANIFEST_ARCHIVE_ID.encode()

        # Acquire lock to ensure atomicity of check-and-set operation
        with self._manifest_lock:
            existing = self._manifest_database.get(key)

            if existing is not None:
                return existing.decode()

            # Generate new ID and store it
            archive_id = str(uuid.uuid4())
            self._manifest_database.put(key, archive_id.encode())
            return archive_id

    def get_archive_id(self) -> str | None:
        """Get the current archive identifier from the manifest.

        Returns:
            The archive ID, or None if not set
        """
        return self.read_manifest(ArchiveStore.MANIFEST_ARCHIVE_ID)

    def register_file(self, path, signature: FileSignature) -> None:
        """Store file signature in 's:' prefixed database entry.

        Uses a 128-bit Murmur3 hash of the path as a prefix, with sequence numbers
        to handle hash collisions robustly.

        Thread-safe: Uses a keyed lock to coordinate concurrent access to the same path hash.
        Multiple threads can safely call this method in parallel for different path hashes,
        but threads working on the same path hash will serialize their operations.

        Args:
            path: Relative path from archive root
            signature: File metadata including path, digest, mtime_ns, and ec_id

        Notes:
            - Key format: <16-byte path hash><varint sequence number>
            - Value format: msgpack([path_components, digest, mtime_ns, ec_id])
            - Multiple paths with the same hash are stored with different sequence numbers
            - If path already exists, it's updated in place
        """
        path_hash = self._compute_long_path_hash(path)
        path_components = [str(part) for part in path.parts]

        # Acquire exclusive access to this path hash
        with self._path_hash_lock.lock(path_hash):
            # Create prefixed database for this hash
            hash_db = self._file_signature_database.prefixed_db(path_hash)

            # Scan existing sequence numbers to check if path already exists
            next_seq_num = 0
            found_existing = False
            for key, data in hash_db.iterator():
                # Key here is just the varint sequence number (hash prefix is already stripped)
                seq_num, _ = decode_varint(key, 0)
                next_seq_num = max(next_seq_num, seq_num + 1)

                # Check if this is the same path
                existing_data: list[Any] = msgpack.loads(data)
                existing_path = Path(*existing_data[0])
                if existing_path == path:
                    # Update existing entry
                    hash_db.put(
                        key,
                        msgpack.dumps([path_components, signature.digest, signature.mtime_ns, signature.ec_id])
                    )
                    found_existing = True
                    break

            # If path doesn't exist, insert with new sequence number
            if not found_existing:
                seq_num_bytes = encode_varint(next_seq_num)
                hash_db.put(
                    seq_num_bytes,
                    msgpack.dumps([path_components, signature.digest, signature.mtime_ns, signature.ec_id])
                )

    def deregister_file(self, path):
        """Remove file signature entry from database.

        Thread-safe: Uses a keyed lock to coordinate concurrent access to the same path hash.
        Multiple threads can safely call this method in parallel for different path hashes,
        but threads working on the same path hash will serialize their operations.

        Args:
            path: Relative path from archive root

        Notes:
            - Uses 128-bit Murmur3 hash of the path as a prefix
            - Searches through sequence numbers to find and remove matching path
        """
        path_hash = self._compute_long_path_hash(path)

        # Acquire exclusive access to this path hash
        with self._path_hash_lock.lock(path_hash):
            hash_db = self._file_signature_database.prefixed_db(path_hash)

            # Search for the specific path entry and delete it
            for key, data in hash_db.iterator():
                # Unpack: [path_components, digest, mtime_ns, ec_id]
                stored_data: list[Any] = msgpack.loads(data)
                stored_path = Path(*stored_data[0])

                # Check if this is the path we're looking for
                if stored_path == path:
                    hash_db.delete(key)
                    return

    def lookup_file(self, path) -> FileSignature | None:
        """Retrieve stored file signature by path.

        Args:
            path: Relative path from archive root

        Returns:
            FileSignature if found, None otherwise

        Notes:
            - Uses 128-bit Murmur3 hash of the path as a prefix
            - Searches through sequence numbers to find matching path
            - Returns FileSignature with path field populated from stored value
        """
        path_hash = self._compute_long_path_hash(path)
        hash_db = self._file_signature_database.prefixed_db(path_hash)

        # Search through all entries for this hash
        for key, data in hash_db.iterator():
            # Unpack: [path_components, digest, mtime_ns, ec_id]
            stored_data: list[Any] = msgpack.loads(data)
            stored_path = Path(*stored_data[0])

            # Check if this is the path we're looking for
            if stored_path == path:
                return FileSignature(stored_path, stored_data[1], stored_data[2], stored_data[3])

        return None

    def list_registered_files(self) -> Iterator[tuple[Path, FileSignature]]:
        """Iterate all file signature entries, yielding (path, signature) pairs.

        Notes:
            - Iterates through hash-based keys with sequence numbers
            - Path is extracted from the stored value
            - Returns FileSignature with path field populated
            - Multiple paths with the same hash are stored with different sequence numbers
        """
        for key, value in self._file_signature_database.iterator():
            # Unpack: [path_components, digest, mtime_ns, ec_id]
            data = msgpack.loads(value)
            path = Path(*data[0])
            signature = FileSignature(path, data[1], data[2], data[3])
            yield path, signature

    @staticmethod
    def _compute_short_path_hash(path: Path) -> int:
        """Compute Murmur3 hash for a path.

        Args:
            path: File path to hash

        Returns:
            32-bit unsigned integer hash value
        """
        # Convert path to string with consistent separator
        path_str = '\0'.join(str(part) for part in path.parts)
        # mmh3.hash returns signed 32-bit, convert to unsigned
        hash_value = mmh3.hash(path_str, signed=False)
        return hash_value

    @staticmethod
    def _compute_long_path_hash(path: Path) -> bytes:
        """Compute 128-bit Murmur3 hash for a path.

        Args:
            path: File path to hash

        Returns:
            16 bytes representing the 128-bit hash value
        """
        # Convert path to string with consistent separator
        path_str = '\0'.join(str(part) for part in path.parts)
        # mmh3.hash128 returns a 128-bit hash as bytes
        hash_value = mmh3.hash128(path_str, signed=False)
        # Convert the integer to 16 bytes (big-endian)
        return hash_value.to_bytes(16, 'big')

    @staticmethod
    def _make_ec_path_key(digest: bytes, ec_id: int, path_hash: int, seq_num: int) -> bytes:
        """Create a database key for a path in an EC class.

        Key format: <digest><4-byte ec_id><4-byte path_hash><varint seq_num>

        Args:
            digest: Content digest (hash value)
            ec_id: Equivalence class ID
            path_hash: Murmur3 hash of the path
            seq_num: Sequence number for this hash (0 for first occurrence, 1 for second, etc.)

        Returns:
            Database key as bytes
        """
        return (digest +
                ec_id.to_bytes(4, 'big') +
                path_hash.to_bytes(4, 'big') +
                encode_varint(seq_num))

    @staticmethod
    def _parse_ec_path_key(key: bytes, digest_len: int) -> tuple[bytes, int, int, int]:
        """Parse a database key to extract digest, ec_id, path_hash, and seq_num.

        Args:
            key: Database key to parse
            digest_len: Length of the digest in bytes

        Returns:
            Tuple of (digest, ec_id, path_hash, seq_num)
        """
        digest = key[:digest_len]
        ec_id = int.from_bytes(key[digest_len:digest_len + 4], 'big')
        path_hash = int.from_bytes(key[digest_len + 4:digest_len + 8], 'big')
        seq_num, _ = decode_varint(key, digest_len + 8)
        return digest, ec_id, path_hash, seq_num

    def list_content_equivalent_classes(self, digest: bytes) -> Iterable[tuple[int, list[Path]]]:
        """List all equivalent classes for files with the specified digest.

        Returns all EC classes where files have the given digest. Each EC class contains
        files with identical content. Multiple EC classes with the same digest indicate
        a hash collision - files with the same digest but different actual content.

        This method reads the hash-based key format where each path is stored
        independently with key: <digest><ec_id><path_hash><varint seq_num>.

        Args:
            digest: Content digest to query (hash value)

        Yields:
            Tuples of (ec_id, paths) where:
            - ec_id: Equivalence class ID (scoped to this digest)
            - paths: List of file paths with identical content in this EC class

        Example:
            For digest "abc123" with a hash collision:
            - (0, [Path("file1.txt"), Path("file2.txt")])  # Content X
            - (1, [Path("file3.txt"), Path("file4.txt")])  # Content Y (different from X)
        """
        # Get all entries for this digest
        digest_db = self._file_hash_database.prefixed_db(digest)

        # Group paths by EC ID
        ec_classes: dict[int, list[Path]] = {}

        for key, data in digest_db.iterator():
            # Key format: <4-byte ec_id><4-byte path_hash><varint seq_num>
            # Parse EC ID from first 4 bytes
            ec_id = int.from_bytes(key[:4], 'big')

            # Deserialize path data (format: [component1, component2, ...])
            path_components: list[str] = msgpack.loads(data)
            path = Path(*path_components)

            # Add to the appropriate EC class
            if ec_id not in ec_classes:
                ec_classes[ec_id] = []
            ec_classes[ec_id].append(path)

        # Yield EC classes sorted by EC ID
        for ec_id in sorted(ec_classes.keys()):
            # Sort paths for consistent ordering
            ec_classes[ec_id].sort()
            yield ec_id, ec_classes[ec_id]

    def add_paths_to_equivalent_class(self, digest: bytes, ec_id: int, paths_to_add: list[Path]) -> None:
        """Add paths to an existing equivalent class or create new EC class.

        Uses a hash-based key format with sequence numbers for collision handling.
        Each path is stored independently using Murmur3 hash + variable-length sequence number.

        Thread-safe: Uses a keyed lock to coordinate concurrent access to the same EC prefix.
        Multiple threads can safely call this method in parallel for different EC prefixes,
        but threads working on the same (digest, ec_id) pair will serialize their sequence
        number scanning operations.

        Args:
            digest: Content digest of the files (hash value)
            ec_id: Equivalence class ID to add the paths to
            paths_to_add: List of file paths to add, relative to archive root

        Notes:
            - Each path is stored with key: <digest><ec_id><path_hash><varint seq_num>
            - Collision handling: paths with same hash get sequential sequence numbers (0, 1, 2, ...)
            - Paths already in the EC class are skipped (no error)
            - For a single path, wrap it in a list: add_paths_to_equivalent_class(digest, ec_id, [path])
        """
        # Create prefix for this EC class (digest + ec_id)
        ec_prefix = digest + ec_id.to_bytes(4, 'big')

        # Acquire exclusive access to this EC prefix
        with self._ec_prefix_lock.lock(ec_prefix):
            ec_db = self._file_hash_database.prefixed_db(ec_prefix)

            # Group paths by their hash to process collisions efficiently
            paths_by_hash: dict[int, set[Path]] = {}
            for path in paths_to_add:
                path_hash = self._compute_short_path_hash(path)
                if path_hash not in paths_by_hash:
                    paths_by_hash[path_hash] = set()
                paths_by_hash[path_hash].add(path)

            # Process each hash group
            for path_hash, paths_set in paths_by_hash.items():
                # Track which paths we've already found in the database to skip them
                paths_to_insert = paths_set.copy()

                # Create prefix for this path hash (ec_prefix already includes digest + ec_id)
                hash_prefix = path_hash.to_bytes(4, 'big')
                hash_db = ec_db.prefixed_db(hash_prefix)

                # Scan existing sequence numbers to find what's already there and what's the next available
                next_seq_num = 0
                for key, data in hash_db.iterator():
                    # Key here is just the varint sequence number (hash prefix is already stripped)
                    seq_num, _ = decode_varint(key, 0)
                    next_seq_num = max(next_seq_num, seq_num + 1)

                    # Check if this path is already stored
                    path_components: list[str] = msgpack.loads(data)
                    existing_path = Path(*path_components)
                    paths_to_insert.discard(existing_path)

                # Insert remaining paths with sequential sequence numbers
                for path in paths_to_insert:
                    path_data = msgpack.dumps([str(part) for part in path.parts])
                    seq_num_bytes = encode_varint(next_seq_num)
                    hash_db.put(seq_num_bytes, path_data)
                    next_seq_num += 1

    def remove_paths_from_equivalent_class(self, digest: bytes, ec_id: int, paths_to_remove: list[Path]) -> None:
        """Remove paths from an equivalent class.

        Uses hash-based key format with sequence numbers. Removes entries directly
        without compaction - sequence numbers remain stable.

        Thread-safe: Uses a keyed lock to coordinate concurrent access to the same EC prefix.
        Multiple threads can safely call this method in parallel for different EC prefixes,
        but threads working on the same (digest, ec_id) pair will serialize their operations.

        Args:
            digest: Content digest of the file (hash value)
            ec_id: Equivalence class ID to remove the paths from
            paths_to_remove: List of file paths to remove, relative to archive root

        Notes:
            - Searches for path by iterating through sequence numbers for each hash
            - When value matches, removes the entry
            - Sequence numbers are NOT compacted - gaps may remain after deletion
            - If a path doesn't exist in the EC class, it's skipped (no error)
            - For a single path, wrap it in a list: remove_paths_from_equivalent_class(digest, ec_id, [path])
        """
        # Create prefix for this EC class (digest + ec_id)
        ec_prefix = digest + ec_id.to_bytes(4, 'big')

        # Acquire exclusive access to this EC prefix
        with self._ec_prefix_lock.lock(ec_prefix):
            ec_db = self._file_hash_database.prefixed_db(ec_prefix)

            # Group paths by their hash to process efficiently
            paths_by_hash: dict[int, set[Path]] = {}
            for path in paths_to_remove:
                path_hash = self._compute_short_path_hash(path)
                if path_hash not in paths_by_hash:
                    paths_by_hash[path_hash] = set()
                paths_by_hash[path_hash].add(path)

            # Process each hash group
            for path_hash, paths_set in paths_by_hash.items():
                # Track which paths we still need to remove
                paths_to_delete = paths_set.copy()

                # Create prefix for this path hash
                hash_prefix = path_hash.to_bytes(4, 'big')
                hash_db = ec_db.prefixed_db(hash_prefix)

                # Scan all sequence numbers for this hash
                keys_to_delete: list[bytes] = []
                for key, data in hash_db.iterator():
                    # Check if this path should be deleted
                    path_components: list[str] = msgpack.loads(data)
                    existing_path = Path(*path_components)

                    if existing_path in paths_to_delete:
                        keys_to_delete.append(key)
                        paths_to_delete.discard(existing_path)

                # Delete all matching keys
                for key in keys_to_delete:
                    hash_db.delete(key)

    def inspect(self, hash_algorithms: dict[str, tuple[int, Any]]) -> Iterator[str]:
        """Generate human-readable index entries for debugging and inspection.

        Args:
            hash_algorithms: Dictionary mapping algorithm names to (length, function) tuples

        Yields:
            Formatted strings showing manifest-property, file-hash, and file-metadata entries
            with hex digests, timestamps, and URL-encoded paths
        """
        hash_algorithm = self.read_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM)
        if hash_algorithm in hash_algorithms:
            hash_length, _ = hash_algorithms[hash_algorithm]
        else:
            hash_length = None

        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(ArchiveStore.__MANIFEST_PROPERTY_PREFIX):
                entry = key[len(ArchiveStore.__MANIFEST_PROPERTY_PREFIX):].decode()
                yield f'manifest-property {entry} {value.decode()}'
            elif key.startswith(ArchiveStore.__FILE_HASH_PREFIX):
                digest_and_rest = key[len(ArchiveStore.__FILE_HASH_PREFIX):]
                if hash_length is not None and len(digest_and_rest) >= hash_length + 4 + 4:
                    digest = digest_and_rest[:hash_length]
                    # Parse ec_id, path_hash, seq_num
                    ec_id = int.from_bytes(digest_and_rest[hash_length:hash_length+4], 'big')
                    path_hash_bytes = digest_and_rest[hash_length+4:hash_length+8]
                    seq_num, _ = decode_varint(digest_and_rest, hash_length+8)
                    path_components: list[str] = msgpack.loads(value)
                    path = '/'.join((urllib.parse.quote_plus(part) for part in path_components))
                    hex_digest = digest.hex()
                    hex_path_hash = '0x' + path_hash_bytes.hex()
                    yield f'file-hash {hex_digest} ec_id:{ec_id} path_hash:{hex_path_hash} seq:{seq_num} {path}'
                else:
                    hex_digest_and_rest = digest_and_rest.hex()
                    yield f'file-hash *{hex_digest_and_rest} {value}'
            elif key.startswith(ArchiveStore.__FILE_SIGNATURE_PREFIX):
                from datetime import datetime, timezone
                # Key format: <16-byte path hash><varint sequence number>
                sig_key = key[len(ArchiveStore.__FILE_SIGNATURE_PREFIX):]
                # Extract path hash (first 16 bytes)
                path_hash_hex = sig_key[:16].hex()
                # Decode sequence number from remaining bytes
                if len(sig_key) > 16:
                    seq_num, _ = decode_varint(sig_key, 16)
                else:
                    seq_num = 0  # Fallback for malformed data
                # Value: [path_components, digest, mtime_ns, ec_id]
                data = msgpack.loads(value)
                path_components = data[0]
                digest = data[1]
                mtime = data[2]
                ec_id = data[3]
                quoted_path = '/'.join((urllib.parse.quote_plus(part) for part in path_components))
                hex_digest = digest.hex()
                mtime_string = \
                    datetime.fromtimestamp(mtime / 1000000000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                yield f'file-metadata path_hash:{path_hash_hex} seq:{seq_num} {quoted_path} digest:{hex_digest} mtime:{mtime_string} ec_id:{ec_id}'
            else:
                yield f'OTHER {key} {value}'

    def walk_archive(self) -> Iterator[tuple[Path, FileContext]]:
        """Traverse archive directory excluding .aridx, yielding (path, context) pairs.

        Symlinks configured in the 'followed_symlinks' setting will be followed during traversal.
        When a symlink is followed, a substitute FileContext is created with stat information from
        the symlink target rather than the symlink itself.
        """
        # Parse symlink follow configuration from settings
        follow_list = self._settings.get(SETTING_FOLLOWED_SYMLINKS, [])
        symlinks_to_follow: set[str] = set()
        if isinstance(follow_list, list):
            symlinks_to_follow = set(str(p) for p in follow_list)

        def should_follow_symlink_wrapper(file_path: Path, file_context: FileContext) -> FileContext | None:
            """Wrapper to check symlink following policy."""
            if str(file_context.relative_path) in symlinks_to_follow:
                resolved_path = resolve_symlink_target(file_path, {self._archive_path, self._archive_path.resolve()})
                if resolved_path is not None:
                    return FileContext(file_context.parent, file_path.name, resolved_path, resolved_path.stat())
            return None

        policy = WalkPolicy(
            excluded_paths={Path('.aridx')},
            should_follow_symlink=should_follow_symlink_wrapper
        )
        yield from walk_with_policy(self._archive_path, policy)

    @staticmethod
    def walk(path: Path) -> Iterator[tuple[Path, FileContext]]:
        """Traverse arbitrary path, yielding (path, context) pairs for duplicate detection."""
        policy = WalkPolicy(
            excluded_paths=set(),
            should_follow_symlink=lambda file_path, file_context: None,
            yield_root=True
        )
        yield from walk_with_policy(path, policy)
