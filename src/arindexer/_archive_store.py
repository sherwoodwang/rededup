import os
import urllib.parse
from pathlib import Path
from typing import Iterator, Iterable

import msgpack
import plyvel

from ._file_context import FileContext, walk_recursively


class FileSignature:
    """File metadata signature for content tracking."""
    def __init__(self, digest: bytes, mtime_ns: int, ec_id: int | None):
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
    - Configuration storage and retrieval
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
    """
    __CONFIG_PREFIX = b'c:'
    __FILE_HASH_PREFIX = b'h:'
    __FILE_SIGNATURE_PREFIX = b'm:'

    __CONFIG_HASH_ALGORITHM = 'hash-algorithm'
    __CONFIG_PENDING_ACTION = 'truncating'

    def __init__(self, archive_path: Path, create: bool = False):
        """Initialize raw archive with LevelDB database.

        Args:
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
            config_database: plyvel.DB = database.prefixed_db(ArchiveStore.__CONFIG_PREFIX)
            file_hash_database: plyvel.DB = database.prefixed_db(ArchiveStore.__FILE_HASH_PREFIX)
            file_signature_database: plyvel.DB = database.prefixed_db(ArchiveStore.__FILE_SIGNATURE_PREFIX)
        except:
            if database is not None:
                database.close()
            raise

        self._archive_path = archive_path
        self._alive = True
        self._database = database
        self._config_database = config_database
        self._file_hash_database = file_hash_database
        self._file_signature_database = file_signature_database

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
        """Clear all file hash and signature entries, reset configuration."""
        self.write_config(ArchiveStore.__CONFIG_PENDING_ACTION, 'truncate')
        self.write_config(ArchiveStore.__CONFIG_HASH_ALGORITHM, None)

        batch = self._file_signature_database.write_batch()
        for key, _ in self._file_signature_database.iterator():
            batch.delete(key)
        batch.write()

        batch = self._file_hash_database.write_batch()
        for key, _ in self._file_hash_database.iterator():
            batch.delete(key)
        batch.write()

        self.write_config(ArchiveStore.__CONFIG_PENDING_ACTION, None)

    def write_config(self, entry: str, value: str | None) -> None:
        """Write or delete configuration entry. None value deletes the key."""
        if value is None:
            self._config_database.delete(entry.encode())
        else:
            self._config_database.put(entry.encode(), value.encode())

    def read_config(self, entry: str) -> str | None:
        """Read configuration value from c: prefixed database entry."""
        value = self._config_database.get(entry.encode())

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

    def store_content_equivalent_class(self, digest: bytes, ec_id: int, paths: list[Path]) -> None:
        """
        Store an equivalent class in which all the files share the same content exactly.

        :param digest: the digest of the content of files
        :param ec_id: the id of this equivalent class, local to this particular digest
        :param paths: the paths of files in the equivalent class, relative to the archive root
        """
        key = digest + ec_id.to_bytes(length=4).lstrip(b'\0')

        if not paths:
            self._file_hash_database.delete(key)
        else:
            data = [[str(part) for part in path.parts] for path in paths]
            data.sort()
            data = msgpack.dumps(data)
            self._file_hash_database.put(key, data)

    def list_content_equivalent_classes(self, digest: bytes) -> Iterable[tuple[int, list[Path]]]:
        """
        List all the equivalent classes where the digest of content the files of each equivalent class matches the
        specified argument.

        :param digest: the digest of the content of files
        """
        ec_db: plyvel.DB = self._file_hash_database.prefixed_db(digest)
        for key, data in ec_db.iterator():
            ec_id = int.from_bytes(key)
            data: list[list[str]] = msgpack.loads(data)
            yield ec_id, [Path(*parts) for parts in data]

    def inspect(self, hash_algorithms: dict[str, tuple[int, any]]) -> Iterator[str]:
        """Generate human-readable index entries for debugging and inspection.

        Args:
            hash_algorithms: Dictionary mapping algorithm names to (length, function) tuples

        Yields:
            Formatted strings showing config, file-hash, and file-metadata entries
            with hex digests, timestamps, and URL-encoded paths
        """
        hash_algorithm = self.read_config(ArchiveStore.__CONFIG_HASH_ALGORITHM)
        if hash_algorithm in hash_algorithms:
            hash_length, _ = hash_algorithms[hash_algorithm]
        else:
            hash_length = None

        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(ArchiveStore.__CONFIG_PREFIX):
                entry = key[len(ArchiveStore.__CONFIG_PREFIX):].decode()
                yield f'config {entry} {value.decode()}'
            elif key.startswith(ArchiveStore.__FILE_HASH_PREFIX):
                digest_and_ec_id = key[len(ArchiveStore.__FILE_HASH_PREFIX):]
                paths = ' '.join((
                    '/'.join((urllib.parse.quote_plus(part) for part in path))
                    for path in msgpack.loads(value)))
                if hash_length is not None:
                    hex_digest = digest_and_ec_id[:hash_length].hex()
                    ec_id = int.from_bytes(digest_and_ec_id[hash_length:])
                    yield f'file-hash {hex_digest} {ec_id} {paths}'
                else:
                    hex_digest_and_ec_id = digest_and_ec_id.hex()
                    yield f'file-hash *{hex_digest_and_ec_id} {paths}'
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
        """Traverse archive directory excluding .aridx, yielding (path, context) pairs."""
        context = FileContext(None, None, self._archive_path.stat())
        context.exclude('.aridx')
        yield from walk_recursively(self._archive_path, context)
        context.complete()

    def walk(self, path: Path) -> Iterator[tuple[Path, FileContext]]:
        """Traverse arbitrary path, yielding (path, context) pairs for duplicate detection."""
        st = path.stat(follow_symlinks=False)
        pseudo_parent = FileContext(None, None, st)
        context = FileContext(pseudo_parent, path.name, st)
        yield path, context
        yield from walk_recursively(path, context)
        context.complete()
        pseudo_parent.complete()

    @property
    def config_hash_algorithm_key(self) -> str:
        """Get the configuration key for hash algorithm."""
        return ArchiveStore.__CONFIG_HASH_ALGORITHM