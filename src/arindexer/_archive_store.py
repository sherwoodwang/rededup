import urllib.parse
from pathlib import Path
from typing import Iterator, Iterable, Any

import mmh3
import msgpack
import plyvel

from ._walker import FileContext, WalkPolicy, walk_with_policy, resolve_symlink_target
from ._archive_settings import ArchiveSettings, SETTING_FOLLOWED_SYMLINKS


class FileSignature:
    """File metadata signature for content tracking.

    Attributes:
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

    def __init__(self, digest: bytes, mtime_ns: int | None, ec_id: int | None):
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

    Contrast with Archive class, which provides high-level operations like rebuild(),
    refresh(), and find_duplicates() that orchestrate multiple ArchiveStore operations
    into complete workflows with proper error handling and concurrency management.

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

    def register_file(self, path, signature: FileSignature) -> None:
        """Store file signature in m: prefixed database entry.

        Args:
            path: Relative path from archive root
            signature: File metadata including digest, mtime_ns, and ec_id
        """
        self._file_signature_database.put(
            b'\0'.join((str(part).encode() for part in path.parts)),
            msgpack.dumps([signature.digest, signature.mtime_ns, signature.ec_id])
        )

    def deregister_file(self, path):
        """Remove file signature entry from database."""
        self._file_signature_database.delete(b'\0'.join((str(part).encode() for part in path.parts)))

    def lookup_file(self, path) -> FileSignature | None:
        """Retrieve stored file signature by path.

        Args:
            path: Relative path from archive root

        Returns:
            FileSignature if found, None otherwise
        """
        value = self._file_signature_database.get(b'\0'.join((str(part).encode() for part in path.parts)))

        if value is None:
            return None

        return FileSignature(*msgpack.loads(value))

    def list_registered_files(self) -> Iterator[tuple[Path, FileSignature]]:
        """Iterate all file signature entries, yielding (path, signature) pairs."""
        for key, value in self._file_signature_database.iterator():
            path = Path(*[part.decode() for part in key.split(b'\0')])
            signature = FileSignature(*msgpack.loads(value))
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
        path_str = '/'.join(str(part) for part in path.parts)
        # mmh3.hash returns signed 32-bit, convert to unsigned
        hash_value = mmh3.hash(path_str, signed=False)
        return hash_value

    @staticmethod
    def _encode_varint(value: int) -> bytes:
        """Encode an integer as variable-length bytes (UTF-8 style).

        Supports values from 0 to 2^63-1 (63-bit integers).

        Encoding format:
        - 0 to 127 (2^7-1): 1 byte - 0xxxxxxx
        - 128 to 16383 (2^14-1): 2 bytes - 10xxxxxx xxxxxxxx
        - 16384 to 2097151 (2^21-1): 3 bytes - 110xxxxx xxxxxxxx xxxxxxxx
        - ... up to 9 bytes for 63-bit values

        Args:
            value: Non-negative integer to encode (max 2^63-1)

        Returns:
            Variable-length byte encoding

        Raises:
            ValueError: If value is negative or exceeds 2^63-1
        """
        if value < 0:
            raise ValueError(f"Cannot encode negative value: {value}")
        if value >= (1 << 63):
            raise ValueError(f"Value {value} exceeds maximum (2^63-1)")

        # Determine how many bytes we need
        if value < (1 << 7):
            # 1 byte: 0xxxxxxx
            return bytes([value])
        elif value < (1 << 14):
            # 2 bytes: 10xxxxxx xxxxxxxx
            return bytes([
                0b10000000 | (value >> 8),
                value & 0xFF
            ])
        elif value < (1 << 21):
            # 3 bytes: 110xxxxx xxxxxxxx xxxxxxxx
            return bytes([
                0b11000000 | (value >> 16),
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        elif value < (1 << 28):
            # 4 bytes: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
            return bytes([
                0b11100000 | (value >> 24),
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        elif value < (1 << 35):
            # 5 bytes
            return bytes([
                0b11110000 | (value >> 32),
                (value >> 24) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        elif value < (1 << 42):
            # 6 bytes
            return bytes([
                0b11111000 | (value >> 40),
                (value >> 32) & 0xFF,
                (value >> 24) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        elif value < (1 << 49):
            # 7 bytes
            return bytes([
                0b11111100 | (value >> 48),
                (value >> 40) & 0xFF,
                (value >> 32) & 0xFF,
                (value >> 24) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        elif value < (1 << 56):
            # 8 bytes
            return bytes([
                0b11111110,
                (value >> 48) & 0xFF,
                (value >> 40) & 0xFF,
                (value >> 32) & 0xFF,
                (value >> 24) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])
        else:
            # 9 bytes (for values up to 2^63-1)
            return bytes([
                0b11111111,
                (value >> 56) & 0xFF,
                (value >> 48) & 0xFF,
                (value >> 40) & 0xFF,
                (value >> 32) & 0xFF,
                (value >> 24) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 8) & 0xFF,
                value & 0xFF
            ])

    @staticmethod
    def _decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
        """Decode a variable-length integer from bytes.

        Args:
            data: Byte array containing the encoded value
            offset: Starting position in data (default 0)

        Returns:
            Tuple of (decoded_value, bytes_consumed)

        Raises:
            ValueError: If data is invalid or truncated
        """
        if offset >= len(data):
            raise ValueError("Cannot decode varint from empty data")

        first_byte = data[offset]

        # 1 byte: 0xxxxxxx
        if (first_byte & 0b10000000) == 0:
            return first_byte, 1

        # 2 bytes: 10xxxxxx xxxxxxxx
        if (first_byte & 0b11000000) == 0b10000000:
            if offset + 2 > len(data):
                raise ValueError("Truncated varint encoding (expected 2 bytes)")
            value = ((first_byte & 0b00111111) << 8) | data[offset + 1]
            return value, 2

        # 3 bytes: 110xxxxx xxxxxxxx xxxxxxxx
        if (first_byte & 0b11100000) == 0b11000000:
            if offset + 3 > len(data):
                raise ValueError("Truncated varint encoding (expected 3 bytes)")
            value = ((first_byte & 0b00011111) << 16) | (data[offset + 1] << 8) | data[offset + 2]
            return value, 3

        # 4 bytes: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if (first_byte & 0b11110000) == 0b11100000:
            if offset + 4 > len(data):
                raise ValueError("Truncated varint encoding (expected 4 bytes)")
            value = ((first_byte & 0b00001111) << 24) | (data[offset + 1] << 16) | \
                    (data[offset + 2] << 8) | data[offset + 3]
            return value, 4

        # 5 bytes: 11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if (first_byte & 0b11111000) == 0b11110000:
            if offset + 5 > len(data):
                raise ValueError("Truncated varint encoding (expected 5 bytes)")
            value = ((first_byte & 0b00000111) << 32) | (data[offset + 1] << 24) | \
                    (data[offset + 2] << 16) | (data[offset + 3] << 8) | data[offset + 4]
            return value, 5

        # 6 bytes: 111110xx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if (first_byte & 0b11111100) == 0b11111000:
            if offset + 6 > len(data):
                raise ValueError("Truncated varint encoding (expected 6 bytes)")
            value = ((first_byte & 0b00000011) << 40) | (data[offset + 1] << 32) | \
                    (data[offset + 2] << 24) | (data[offset + 3] << 16) | \
                    (data[offset + 4] << 8) | data[offset + 5]
            return value, 6

        # 7 bytes: 1111110x xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if (first_byte & 0b11111110) == 0b11111100:
            if offset + 7 > len(data):
                raise ValueError("Truncated varint encoding (expected 7 bytes)")
            value = ((first_byte & 0b00000001) << 48) | (data[offset + 1] << 40) | \
                    (data[offset + 2] << 32) | (data[offset + 3] << 24) | \
                    (data[offset + 4] << 16) | (data[offset + 5] << 8) | data[offset + 6]
            return value, 7

        # 8 bytes: 11111110 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if first_byte == 0b11111110:
            if offset + 8 > len(data):
                raise ValueError("Truncated varint encoding (expected 8 bytes)")
            value = (data[offset + 1] << 48) | (data[offset + 2] << 40) | \
                    (data[offset + 3] << 32) | (data[offset + 4] << 24) | \
                    (data[offset + 5] << 16) | (data[offset + 6] << 8) | data[offset + 7]
            return value, 8

        # 9 bytes: 11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        if first_byte == 0b11111111:
            if offset + 9 > len(data):
                raise ValueError("Truncated varint encoding (expected 9 bytes)")
            value = (data[offset + 1] << 56) | (data[offset + 2] << 48) | \
                    (data[offset + 3] << 40) | (data[offset + 4] << 32) | \
                    (data[offset + 5] << 24) | (data[offset + 6] << 16) | \
                    (data[offset + 7] << 8) | data[offset + 8]
            return value, 9

        raise ValueError(f"Invalid varint encoding at offset {offset}")

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
                ArchiveStore._encode_varint(seq_num))

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
        seq_num, _ = ArchiveStore._decode_varint(key, digest_len + 8)
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
                seq_num, _ = self._decode_varint(key, 0)
                next_seq_num = max(next_seq_num, seq_num + 1)

                # Check if this path is already stored
                path_components: list[str] = msgpack.loads(data)
                existing_path = Path(*path_components)
                paths_to_insert.discard(existing_path)

            # Insert remaining paths with sequential sequence numbers
            for path in paths_to_insert:
                path_data = msgpack.dumps([str(part) for part in path.parts])
                seq_num_bytes = self._encode_varint(next_seq_num)
                hash_db.put(seq_num_bytes, path_data)
                next_seq_num += 1

    def remove_paths_from_equivalent_class(self, digest: bytes, ec_id: int, paths_to_remove: list[Path]) -> None:
        """Remove paths from an equivalent class.

        Uses hash-based key format with sequence numbers. Removes entries directly
        without compaction - sequence numbers remain stable.

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
                    seq_num, _ = ArchiveStore._decode_varint(digest_and_rest, hash_length+8)
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
                path = Path(*[part.decode() for part in key[len(ArchiveStore.__FILE_SIGNATURE_PREFIX):].split(b'\0')])
                [digest, mtime, ec_id] = msgpack.loads(value)
                quoted_path = '/'.join((urllib.parse.quote_plus(part) for part in path.parts))
                hex_digest = digest.hex()
                mtime_string = \
                    datetime.fromtimestamp(mtime / 1000000000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                yield f'file-metadata {quoted_path} digest:{hex_digest} mtime:{mtime_string} ec_id:{ec_id}'
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
            if str(file_context.relative_path()) in symlinks_to_follow:
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
