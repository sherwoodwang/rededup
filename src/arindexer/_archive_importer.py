from pathlib import Path
from typing import NamedTuple

from ._archive_store import ArchiveStore, FileSignature


class ImportArgs(NamedTuple):
    """Arguments for import operation."""
    source_archive_path: Path
    processor: any  # Processor for file content comparison


class ImportProcessor:
    """Processes import operations from source archive to target archive."""

    def __init__(self, store: ArchiveStore, args: ImportArgs):
        """Initialize the import processor.

        Args:
            store: Target archive store
            args: Import arguments containing source archive path and processor
        """
        self.store = store
        self.processor = args.processor
        self.current_path = store.archive_path.absolute()
        self.source_path = args.source_archive_path.absolute()
        self.source_store = None

        # Path transformation state
        self.is_nested = False
        self.is_ancestor = False
        self.prefix_to_add = None
        self.prefix_to_remove = None

    def _validate_archives(self):
        """Validate that source and current archives have valid relationship."""
        # Validation: source and current must not be the same
        if self.source_path == self.current_path:
            raise ValueError("Source archive cannot be the same as the current archive")

        # Validation: source must not be inside .aridx
        aridx_path = self.current_path / '.aridx'
        try:
            self.source_path.relative_to(aridx_path)
            raise ValueError("Source archive cannot be inside .aridx directory")
        except ValueError:
            # relative_to raises ValueError if source_path is not relative to aridx_path
            # This is the expected case, so we continue
            pass

    def _determine_relationship(self):
        """Determine whether source is nested in current or current is nested in source."""
        try:
            # Try to get relative path from current to source
            rel_path = self.source_path.relative_to(self.current_path)
            self.is_nested = True
            self.prefix_to_add = rel_path
        except ValueError:
            # Source is not nested in current, try the other direction
            try:
                rel_path = self.current_path.relative_to(self.source_path)
                self.is_ancestor = True
                self.prefix_to_remove = rel_path
            except ValueError:
                # Neither nested nor ancestor
                raise ValueError(
                    f"Source archive must be either a nested directory of the current archive "
                    f"or an ancestor directory containing the current archive"
                )

    def _open_source_archive(self):
        """Open the source archive store."""
        from ._archive_settings import ArchiveSettings
        try:
            source_settings = ArchiveSettings(self.source_path)
            self.source_store = ArchiveStore(source_settings, self.source_path, create=False)
        except Exception as e:
            raise ValueError(f"Failed to open source archive: {e}")

    def _check_hash_algorithm_compatibility(self):
        """Check and synchronize hash algorithms between source and target."""
        source_hash_algo = self.source_store.read_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM)
        current_hash_algo = self.store.read_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM)

        if current_hash_algo is None:
            # Current archive hasn't been built yet, adopt source's algorithm
            if source_hash_algo is not None:
                self.store.write_manifest(ArchiveStore.MANIFEST_HASH_ALGORITHM, source_hash_algo)
        elif source_hash_algo != current_hash_algo:
            raise ValueError(
                f"Hash algorithm mismatch: source uses {source_hash_algo}, "
                f"current uses {current_hash_algo}"
            )

    def _transform_path(self, path: Path) -> Path | None:
        """Transform a path based on the relationship between archives.

        Args:
            path: Original path from source archive

        Returns:
            Transformed path, or None if path should be excluded
        """
        if self.is_nested:
            # Source is nested in current: prepend relative path
            return self.prefix_to_add / path
        elif self.is_ancestor:
            # Source is ancestor: filter and strip prefix
            try:
                # Check if path is under the current archive's scope
                relative = path.relative_to(self.prefix_to_remove)
                return relative
            except ValueError:
                # Path is outside current archive's scope, exclude it
                return None
        else:
            # Should not happen due to validation, but return as-is
            return path

    async def _process_digest_and_files(self, digest: str):
        """Process all EC classes for a digest and import file signatures.

        For each source EC class, we compare file content with existing EC classes
        to determine if it should be merged or kept separate. This properly handles
        hash collisions where files have the same digest but different content.

        Args:
            digest: The content digest to process
        """
        # Get all existing EC classes in current archive for this digest
        existing_ec_classes = list(self.store.list_content_equivalent_classes(digest))
        next_available_ec_id = max((ec_id for ec_id, _ in existing_ec_classes), default=-1) + 1

        # Import each source EC class
        for source_ec_id, source_paths in self.source_store.list_content_equivalent_classes(digest):
            # Transform paths from source to current archive's namespace
            transformed_paths = self._transform_paths(source_paths)

            if not transformed_paths:
                continue

            # Pick one file from the source EC to use as reference for content comparison
            source_reference_path = source_paths[0]
            source_file_full_path = self.source_path / source_reference_path

            # Try to find an existing EC class with matching content
            target_ec_id = None
            for ec_id, ec_paths in existing_ec_classes:
                # Pick one file from this existing EC class to compare
                ec_reference_path = ec_paths[0]
                current_file_full_path = self.current_path / ec_reference_path

                # Compare file contents
                if await self._files_have_identical_content(
                    source_file_full_path, current_file_full_path
                ):
                    # Content matches! Merge into this existing EC class
                    target_ec_id = ec_id
                    # Add transformed paths to this EC class
                    all_paths = list(ec_paths)
                    for p in transformed_paths:
                        if p not in all_paths:
                            all_paths.append(p)
                    # Update the EC class
                    self.store.store_content_equivalent_class(digest, target_ec_id, all_paths)
                    # Update our cached list
                    existing_ec_classes = [(ec_id, all_paths if ec_id == target_ec_id else paths)
                                          for ec_id, paths in existing_ec_classes]
                    break

            # No matching EC class found, create a new one
            if target_ec_id is None:
                target_ec_id = next_available_ec_id
                next_available_ec_id += 1
                # Store the new EC class
                self.store.store_content_equivalent_class(digest, target_ec_id, transformed_paths)
                # Add to our cached list
                existing_ec_classes.append((target_ec_id, transformed_paths))

            # Import file signatures for files in this EC class
            for source_path in source_paths:
                transformed_path = self._transform_path(source_path)
                if transformed_path is not None:
                    # Get the signature from source
                    source_signature = self._get_file_signature(source_path)
                    if source_signature is not None:
                        # Create signature with the target EC ID
                        new_signature = FileSignature(
                            source_signature.digest,
                            source_signature.mtime_ns,
                            target_ec_id
                        )
                        self.store.register_file(transformed_path, new_signature)

    async def _files_have_identical_content(self, file1: Path, file2: Path) -> bool:
        """Compare two files to check if they have identical content.

        Args:
            file1: Path to first file
            file2: Path to second file

        Returns:
            True if files have identical content, False otherwise
        """
        # Read both files and compare byte-by-byte
        try:
            content1 = file1.read_bytes()
            content2 = file2.read_bytes()
            return content1 == content2
        except (IOError, OSError):
            # If we can't read the files, assume they're different
            return False

    def _get_file_signature(self, file_path: Path) -> FileSignature | None:
        """Get file signature from source store.

        Args:
            file_path: Path to file in source archive

        Returns:
            File signature or None if not found
        """
        for path, signature in self.source_store.list_registered_files():
            if path == file_path:
                return signature
        return None

    def _transform_paths(self, paths: list[Path]) -> list[Path]:
        """Transform multiple paths, filtering out excluded paths.

        Args:
            paths: List of paths from source archive

        Returns:
            List of transformed paths (excludes None results)
        """
        transformed_paths = []
        for path in paths:
            transformed_path = self._transform_path(path)
            if transformed_path is not None:
                transformed_paths.append(transformed_path)
        return transformed_paths

    async def process(self):
        """Process the import operation.

        Strategy:
        - Outer loop scans file signatures from source
        - When a digest is encountered for the first time (not yet in target archive),
          process all its EC classes and import file signatures for files in those EC classes
        - Skip files whose signatures are already present in the target archive
          (presence indicates the digest has already been processed)
        """
        self._validate_archives()
        self._determine_relationship()
        self._open_source_archive()

        try:
            self._check_hash_algorithm_compatibility()

            # Outer loop: scan file signatures from source
            for file_path, signature in self.source_store.list_registered_files():
                transformed_path = self._transform_path(file_path)
                if transformed_path is None:
                    # File is outside scope, skip
                    continue

                # Check if this file is already registered in target archive
                # If it is, the digest has already been processed
                existing_signature = None
                for path, sig in self.store.list_registered_files():
                    if path == transformed_path:
                        existing_signature = sig
                        break

                if existing_signature is not None:
                    # File already registered, digest already processed, skip
                    continue

                # First time encountering this file/digest
                # Process all EC classes for this digest and import file signatures
                await self._process_digest_and_files(signature.digest)

        finally:
            if self.source_store:
                self.source_store.close()


async def do_import(store: ArchiveStore, args: ImportArgs):
    """Import index entries from another archive."""
    processor = ImportProcessor(store, args)
    await processor.process()
