import hashlib
import os
import re
import tempfile
import unittest
import urllib.parse
from pathlib import Path

from arindexer import Archive, FileMetadataDifferencePattern, FileMetadataDifferenceType, Output
# noinspection PyProtectedMember
from arindexer._processor import Processor


async def compute_xor(path: Path):
    data = path.read_bytes()
    value = 0
    while data:
        if len(data) > 4:
            seg = data[:4]
            data = data[4:]
        else:
            seg = (data + b'\0\0\0\0')[:4]
            data = b''

        value = value ^ int.from_bytes(seg)

    value = int.to_bytes(value, length=4)

    return value


# noinspection DuplicatedCode
class ArchiveTest(unittest.TestCase):
    def test_common_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            seed = bytes.fromhex('5e628956f09045be6321c389af2d7405fafc18a0d7f44e8727a886f7fb5b5beb')

            def generate(path, salt: str):
                encoded_salt = salt.encode('utf-8')
                segment = seed

                with open(path, 'wb') as f:
                    for _ in range(128):
                        segment = bytes(hashlib.sha256(encoded_salt + segment + encoded_salt).digest())
                        f.write(segment)

            generate(archive_path / 'sample0', 'sample0')
            (archive_path / 'sample1').mkdir()
            generate(archive_path / 'sample1' / 'sample2', 'sample2')
            generate(archive_path / 'sample1' / 'sample3', 'sample3')
            (archive_path / 'sample1' / 'sample4').mkdir()
            generate(archive_path / 'sample1' / 'sample4' / 'sample5', 'sample5')
            (archive_path / 'sample6').mkdir()
            (archive_path / 'sample6' / 'sample7').mkdir()
            (archive_path / 'sample8').mkdir()
            for i in range(9, 9 + 64):
                generate(archive_path / 'sample8' / f'sample{i}', f'sample{i}')
            (archive_path / 'sample74').mkdir()
            for i in range(9, 9 + 16):
                generate(archive_path / 'sample74' / f'sample{i}-another', f'sample{i}')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()
                    archive.rebuild()
                    self.assertEqual(
                        _test_common_lifecycle_0,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

                with Archive(processor, str(archive_path)) as archive:
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_0,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

                    (archive_path / 'sample8' / 'sample10').unlink()
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_1,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

                    (archive_path / 'sample74' / 'sample10-another').unlink()
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_2,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

                    generate(archive_path / 'sample75', 'sample75')
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_3,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

                target = Path(tmpdir) / 'target'

                target.mkdir()
                generate(target / 'sample-a', 'sample5')
                generate(target / 'sample-b', 'sample9')
                generate(target / 'sample-c', 'sample72')
                generate(target / 'sample-d', 'sample75')
                generate(target / 'retained-a', 'retained')
                generate(target / 'retained-b', 'sample10')

                os.utime(target / 'sample-d', ns=(
                    (archive_path / 'sample75').stat().st_atime_ns + 60000000000,
                    (archive_path / 'sample75').stat().st_mtime_ns + 60000000000,
                ))

                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), output=output) as archive:
                    diffptn = FileMetadataDifferencePattern()
                    diffptn.add(FileMetadataDifferenceType.ATIME)
                    diffptn.add(FileMetadataDifferenceType.CTIME)
                    diffptn.add(FileMetadataDifferenceType.MTIME)
                    archive.find_duplicates(target, ignore=diffptn)
                    self.assertEqual(
                        {(f'{target}/sample-a',),
                         (f'{target}/sample-b',),
                         (f'{target}/sample-c',),
                         (f'{target}/sample-d',)},
                        set((tuple(r) for r in output.data))
                    )

                    output.data.clear()
                    output.verbosity = 1
                    archive.find_duplicates(target, ignore=diffptn)
                    self.assertEqual(
                        {(f'{target}/sample-a',
                          '## identical file: sample1/sample4/sample5',
                          '## ignored difference - mtime'),
                         (f'{target}/sample-b',
                          '## identical file: sample74/sample9-another',
                          '## ignored difference - mtime',
                          '## identical file: sample8/sample9',
                          '## ignored difference - mtime'),
                         (f'{target}/sample-c',
                          '## identical file: sample8/sample72',
                          '## ignored difference - mtime'),
                         (f'{target}/sample-d',
                          '## identical file: sample75',
                          '## ignored difference - mtime')},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

                    output.data.clear()
                    output.verbosity = 1
                    archive.find_duplicates(target)
                    self.assertEqual([], output.data)

                    output.data.clear()
                    output.verbosity = 1
                    output.showing_content_wise_duplicates = True
                    archive.find_duplicates(target)
                    self.assertEqual(
                        {(f'# content-wise duplicate: {target}/sample-a',
                          '## file with identical content: sample1/sample4/sample5',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/sample-b',
                          '## file with identical content: sample74/sample9-another',
                          '## difference - mtime',
                          '## file with identical content: sample8/sample9',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/sample-c',
                          '## file with identical content: sample8/sample72',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/sample-d',
                          '## file with identical content: sample75',
                          '## difference - mtime')},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## difference - [ac]time:', p)))
                            for r in output.data)),
                    )

    def test_rebuild_with_collision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'sample0').write_bytes(b'\0\0\0\0\1\1\1\1')
            (archive_path / 'sample1').write_bytes(b'\1\1\1\1\2\2\2\2')
            for i in range(2, 300):
                (archive_path / f'sample{i}').write_bytes(
                    (i * 2).to_bytes(length=2) * 2 + (i * 2 + 1).to_bytes(length=2) * 2)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive._hash_algorithms['xor'] = (4, compute_xor)
                    archive._default_hash_algorithm = 'xor'

                    archive.rebuild()

                    self.assertEqual(
                        _test_rebuild_with_collision_data,
                        set((re.sub('^(file-metadata .* mtime:)\\S*( .*)$', '\\1\\2', rec)
                             for rec in archive.inspect()))
                    )

    def test_directory_duplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'dir0').mkdir()
            (archive_path / 'dir0' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1' / 'sample1').write_bytes(b'sample1')

            target = Path(tmpdir) / 'target'
            target.mkdir()

            (target / 'dir-dup').mkdir()
            (target / 'dir-dup' / 'sample0').write_bytes(b'sample0')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))

            with Processor() as processor:
                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    output.verbosity = 1
                    archive.find_duplicates(target, ignore=FileMetadataDifferencePattern.TRIVIAL)
                    self.assertEqual(
                        {(f'{target}/dir-dup/',
                          f'## identical directory: dir0/dir1/',)},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

    def test_directory_duplicates_with_nested_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'dir0').mkdir()
            (archive_path / 'dir0' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1' / 'dir-nested').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample1').write_bytes(b'sample1')

            target = Path(tmpdir) / 'target'
            target.mkdir()

            (target / 'dir-dup').mkdir()
            (target / 'dir-dup' / 'sample0').write_bytes(b'sample0')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'dir-nested').mkdir()
            (target / 'dir-dup' / 'dir-nested' / 'sample1').write_bytes(b'sample1')
            self._copy_times(
                (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample1'),
                (target / 'dir-dup' / 'dir-nested' / 'sample1'))

            with Processor() as processor:
                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    output.verbosity = 1
                    archive.find_duplicates(target, ignore=FileMetadataDifferencePattern.TRIVIAL)
                    self.assertEqual(
                        {(f'{target}/dir-dup/',
                          f'## identical directory: dir0/dir1/',)},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

    def test_directory_duplicates_with_symbolic_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'dir0').mkdir()
            (archive_path / 'dir0' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1' / 'sample1').write_bytes(b'sample1')
            (archive_path / 'dir0' / 'dir1' / 'sample-link0').symlink_to('sample-link0')

            target = Path(tmpdir) / 'target'
            target.mkdir()

            (target / 'dir-dup').mkdir()
            (target / 'dir-dup' / 'sample0').write_bytes(b'sample0')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            (target / 'dir-dup' / 'sample-link0').symlink_to('sample-link0')

            with Processor() as processor:
                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    output.verbosity = 1
                    archive.find_duplicates(target, ignore=FileMetadataDifferencePattern.TRIVIAL)
                    self.assertEqual(
                        {(f'{target}/dir-dup/',
                          f'## identical directory: dir0/dir1/',)},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

    def test_directory_content_wise_duplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'dir0').mkdir()
            (archive_path / 'dir0' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1' / 'sample1').write_bytes(b'sample1')

            target = Path(tmpdir) / 'target'
            target.mkdir()

            (target / 'dir-dup').mkdir()
            (target / 'dir-dup' / 'sample0').write_bytes(b'sample0')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            self._tweak_times((target / 'dir-dup' / 'sample0'), 1000000000)
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            self._tweak_times((target / 'dir-dup' / 'sample1'), 1000000000)

            with Processor() as processor:
                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    output.verbosity = 1
                    output.showing_content_wise_duplicates = True
                    archive.find_duplicates(target, ignore=FileMetadataDifferencePattern.TRIVIAL)
                    self.assertEqual(
                        {(f'# content-wise duplicate: {target}/dir-dup/',
                          '## directory with identical content: dir0/dir1/'),
                         (f'# content-wise duplicate: {target}/dir-dup/sample0',
                          '## file with identical content: dir0/dir1/sample0',
                          '## difference - mtime',
                          '## file with identical content: dir0/sample0',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/dir-dup/sample1',
                          '## file with identical content: dir0/dir1/sample1',
                          '## difference - mtime')},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

    def test_directory_content_wise_duplicates_with_nested_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'dir0').mkdir()
            (archive_path / 'dir0' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'sample0').write_bytes(b'sample0')
            (archive_path / 'dir0' / 'dir1' / 'sample1').write_bytes(b'sample1')
            (archive_path / 'dir0' / 'dir1' / 'dir-nested').mkdir()
            (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample0').write_bytes(b'sample0')

            target = Path(tmpdir) / 'target'
            target.mkdir()

            (target / 'dir-dup').mkdir()
            (target / 'dir-dup' / 'sample0').write_bytes(b'sample0')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            self._tweak_times((target / 'dir-dup' / 'sample0'), 1000000000)
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            self._copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            self._tweak_times((target / 'dir-dup' / 'sample1'), 1000000000)
            (target / 'dir-dup' / 'dir-nested').mkdir()
            (target / 'dir-dup' / 'dir-nested' / 'sample0').write_bytes(b'sample0')
            self._copy_times(
                (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample0'),
                (target / 'dir-dup' / 'dir-nested' / 'sample0'))
            self._tweak_times((target / 'dir-dup' / 'dir-nested' / 'sample0'), 1000000000)

            with Processor() as processor:
                output = ArchiveTest.CollectingOutput()
                with Archive(processor, str(archive_path), create=True, output=output) as archive:
                    archive.rebuild()
                    output.verbosity = 1
                    output.showing_content_wise_duplicates = True
                    archive.find_duplicates(target, ignore=FileMetadataDifferencePattern.TRIVIAL)
                    self.assertEqual(
                        {(f'# content-wise duplicate: {target}/dir-dup/',
                          '## directory with identical content: dir0/dir1/'),
                         (f'# content-wise duplicate: {target}/dir-dup/dir-nested/',
                          '## directory with identical content: dir0/dir1/dir-nested/'),
                         (f'# content-wise duplicate: {target}/dir-dup/dir-nested/sample0',
                          '## file with identical content: dir0/dir1/dir-nested/sample0',
                          '## difference - mtime',
                          '## file with identical content: dir0/dir1/sample0',
                          '## difference - mtime',
                          '## file with identical content: dir0/sample0',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/dir-dup/sample0',
                          '## file with identical content: dir0/dir1/dir-nested/sample0',
                          '## difference - mtime',
                          '## file with identical content: dir0/dir1/sample0',
                          '## difference - mtime',
                          '## file with identical content: dir0/sample0',
                          '## difference - mtime'),
                         (f'# content-wise duplicate: {target}/dir-dup/sample1',
                          '## file with identical content: dir0/dir1/sample1',
                          '## difference - mtime')},
                        set((tuple(
                            (re.sub('^(##[^:]* difference[^:]*):.*', '\\1', p)
                             for p in r if not re.match('^## ignored difference - [ac]time:', p)))
                            for r in output.data))
                    )

    @staticmethod
    def _copy_times(src: Path, dest: Path):
        st = src.lstat()
        os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns), follow_symlinks=False)

    @staticmethod
    def _tweak_times(path: Path, shift: int):
        st = path.lstat()
        os.utime(path, ns=(st.st_atime_ns + shift, st.st_mtime_ns + shift), follow_symlinks=False)

    class CollectingOutput(Output):
        def __init__(self):
            super().__init__()
            self.data: list[list[str]] = []

        def _offer(self, record):
            self.data.append(record)


# Load _test_common_lifecycle_0 from external file
with open(Path(__file__).parent / "test_common_lifecycle_0.txt") as f:
    _test_common_lifecycle_0 = set(line.rstrip("\n") for line in f if line.strip())

_test_common_lifecycle_1 = \
    (_test_common_lifecycle_0 | {
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another',
    }) - {
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
        'file-metadata sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    }

_test_common_lifecycle_2 = _test_common_lifecycle_0 - {
    'file-metadata sample74/sample10-another digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
    'file-metadata sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
}

_test_common_lifecycle_3 = \
    (_test_common_lifecycle_0 | {
        'file-hash 8b1cebc0d516efab2efe357a3bb49fe2dc96a45263e20312b433fa0e11fb909d 0 sample75',
        'file-metadata sample75 digest:8b1cebc0d516efab2efe357a3bb49fe2dc96a45263e20312b433fa0e11fb909d mtime: ec_id:0',
    }) - {
        'file-metadata sample74/sample10-another digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
        'file-metadata sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
    }

# Load _test_common_lifecycle_0 from external file
with open(Path(__file__).parent / "test_rebuild_with_collision_data.txt") as f:
    _test_rebuild_with_collision_data = set(line.rstrip("\n") for line in f if line.strip())


class SymlinkFollowingTest(unittest.TestCase):
    def test_symlink_not_followed_without_settings(self):
        """Test that symlinks are not followed when not configured in settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create a regular directory with a file
            (archive_path / 'real_dir').mkdir()
            (archive_path / 'real_dir' / 'file1.txt').write_text('content1')

            # Create a symlink to the directory
            (archive_path / 'linked_dir').symlink_to('real_dir')

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    archive.rebuild()

                    # Without settings, symlink should not be followed
                    # Only real_dir/file1.txt should be indexed
                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            # Extract path from line
                            parts = line.split()
                            indexed_files.append(parts[1])

                    self.assertIn('real_dir/file1.txt', indexed_files)
                    # linked_dir should appear as a file (symlink), not as a directory
                    self.assertNotIn('linked_dir/file1.txt', indexed_files)

    def test_symlink_followed_with_settings(self):
        """Test that symlinks are followed when configured in settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create .aridx directory and settings file
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            settings_content = """
followed_symlinks = ["parent/linked_dir"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create an external directory with files (outside the archive)
            external_dir = Path(tmpdir) / 'external_data'
            external_dir.mkdir()
            (external_dir / 'file1.txt').write_text('content1')
            (external_dir / 'file2.txt').write_text('content2')

            # Create a parent directory and a symlink inside it pointing outside the archive
            (archive_path / 'parent').mkdir()
            (archive_path / 'parent' / 'linked_dir').symlink_to(external_dir)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=False) as archive:
                    archive.rebuild()

                    # With settings, symlink should be followed (points outside archive)
                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            indexed_files.append(parts[1])

                    # Files should be indexed through the symlink path
                    self.assertIn('parent/linked_dir/file1.txt', indexed_files)
                    self.assertIn('parent/linked_dir/file2.txt', indexed_files)

    def test_symlink_selective_following(self):
        """Test that only configured symlinks are followed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            # Create .aridx directory and settings file
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            # Only follow link1, not link2
            settings_content = """
followed_symlinks = ["link1"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create two external directories (outside the archive)
            external_dir1 = Path(tmpdir) / 'external_dir1'
            external_dir1.mkdir()
            (external_dir1 / 'file1.txt').write_text('content1')

            external_dir2 = Path(tmpdir) / 'external_dir2'
            external_dir2.mkdir()
            (external_dir2 / 'file2.txt').write_text('content2')

            # Create two symlinks pointing outside the archive
            (archive_path / 'link1').symlink_to(external_dir1)
            (archive_path / 'link2').symlink_to(external_dir2)

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=False) as archive:
                    archive.rebuild()

                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            indexed_files.append(parts[1])

                    # link1 should be followed (configured in settings, points outside archive)
                    self.assertIn('link1/file1.txt', indexed_files)

                    # link2 should NOT be followed (not configured in settings)
                    self.assertNotIn('link2/file2.txt', indexed_files)

    def test_archive_path_is_symlink(self):
        """Test that symlinks can be followed when archive path itself is a symlink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the real archive directory
            real_archive_path = Path(tmpdir) / 'real_archive'
            real_archive_path.mkdir()

            # Create a symlink to the archive
            symlink_archive_path = Path(tmpdir) / 'symlink_archive'
            symlink_archive_path.symlink_to(real_archive_path)

            # Create .aridx directory and settings file using symlink path
            aridx_path = symlink_archive_path / '.aridx'
            aridx_path.mkdir()

            settings_content = """
followed_symlinks = ["link1"]
"""
            (aridx_path / 'settings.toml').write_text(settings_content)

            # Create an external directory (outside the archive)
            external_dir = Path(tmpdir) / 'external_data'
            external_dir.mkdir()
            (external_dir / 'file1.txt').write_text('content1')
            (external_dir / 'file2.txt').write_text('content2')

            # Create a symlink inside the archive pointing outside
            (symlink_archive_path / 'link1').symlink_to(external_dir)

            # Also create a regular file in the archive
            (symlink_archive_path / 'regular_file.txt').write_text('regular content')

            with Processor() as processor:
                # Open archive using the symlink path
                with Archive(processor, str(symlink_archive_path), create=False) as archive:
                    archive.rebuild()

                    indexed_files = []
                    for line in archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            indexed_files.append(parts[1])

                    # Regular file should be indexed
                    self.assertIn('regular_file.txt', indexed_files)

                    # External files through followed symlink should be indexed
                    self.assertIn('link1/file1.txt', indexed_files)
                    self.assertIn('link1/file2.txt', indexed_files)


class ImportTest(unittest.TestCase):
    """Test import functionality for importing index entries from other archives."""

    def test_import_from_nested_archive(self):
        """Test importing from a nested subdirectory archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'subdir' / 'nested_archive'
            nested_path.mkdir(parents=True)

            # Add files to nested archive
            (nested_path / 'file1.txt').write_text('content1')
            (nested_path / 'file2.txt').write_text('content2')
            (nested_path / 'dir1').mkdir()
            (nested_path / 'dir1' / 'file3.txt').write_text('content3')

            with Processor() as processor:
                # Build nested archive index
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import into parent archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that files are imported with correct prefix
                    indexed_files = []
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            # URL decode the path
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    self.assertIn('subdir/nested_archive/file1.txt', indexed_files)
                    self.assertIn('subdir/nested_archive/file2.txt', indexed_files)
                    self.assertIn('subdir/nested_archive/dir1/file3.txt', indexed_files)

    def test_import_from_ancestor_archive(self):
        """Test importing from an ancestor archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested directory
            nested_path = parent_path / 'subdir' / 'nested_archive'
            nested_path.mkdir(parents=True)

            # Add files to both parent and nested locations
            (parent_path / 'root_file.txt').write_text('root content')
            (nested_path / 'nested_file.txt').write_text('nested content')
            (parent_path / 'other_dir').mkdir()
            (parent_path / 'other_dir' / 'other_file.txt').write_text('other content')

            with Processor() as processor:
                # Build parent archive index
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Import into nested archive (should only get files under nested path)
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.import_index(str(parent_path))

                    # Check that only nested files are imported with prefix removed
                    indexed_files = []
                    for line in nested_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            # URL decode the path
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    # Should have the nested file with prefix removed
                    self.assertIn('nested_file.txt', indexed_files)
                    # Should NOT have files outside the nested scope
                    self.assertNotIn('root_file.txt', indexed_files)
                    self.assertNotIn('other_dir/other_file.txt', indexed_files)

    def test_import_rejects_same_archive(self):
        """Test that importing from the same archive is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    with self.assertRaises(ValueError) as context:
                        archive.import_index(str(archive_path))
                    self.assertIn('cannot be the same', str(context.exception))

    def test_import_rejects_aridx_directory(self):
        """Test that importing from .aridx directory is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'archive'
            archive_path.mkdir()
            aridx_path = archive_path / '.aridx'
            aridx_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive_path), create=True) as archive:
                    with self.assertRaises(ValueError) as context:
                        archive.import_index(str(aridx_path))
                    self.assertIn('.aridx', str(context.exception))

    def test_import_rejects_invalid_relationship(self):
        """Test that importing from unrelated archive is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive1_path = Path(tmpdir) / 'archive1'
            archive1_path.mkdir()
            archive2_path = Path(tmpdir) / 'archive2'
            archive2_path.mkdir()

            with Processor() as processor:
                with Archive(processor, str(archive1_path), create=True) as archive1:
                    with Archive(processor, str(archive2_path), create=True) as archive2:
                        with self.assertRaises(ValueError) as context:
                            archive1.import_index(str(archive2_path))
                        self.assertIn('nested', str(context.exception))
                        self.assertIn('ancestor', str(context.exception))

    def test_import_with_ec_id_collision(self):
        """Test that EC ID collisions are handled by remapping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files with identical content that will have EC ID 0 in both archives
            content = 'identical content'
            (parent_path / 'parent_file1.txt').write_text(content)
            (parent_path / 'parent_file2.txt').write_text(content)
            (nested_path / 'nested_file1.txt').write_text(content)
            (nested_path / 'nested_file2.txt').write_text(content)

            with Processor() as processor:
                # Build parent archive - files will get EC ID 0
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Build nested archive - files will also get EC ID 0
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Now import nested into parent - should remap EC IDs to avoid collision
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present
                    indexed_files = {}
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            # Extract EC ID from the line
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if ec_id_match:
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = ec_id

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # Files with identical content should be in the same EC class
                    # (They all have the same digest and content)
                    # After import, all should be in same EC class since content is identical
                    ec_ids = set(indexed_files.values())
                    # All files have identical content, so should be in same EC class
                    self.assertEqual(len(ec_ids), 1, f"Expected all files in same EC class, got: {indexed_files}")

    def test_import_with_ec_id_collision_different_content(self):
        """Test that files with different content maintain separate EC classes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files with same content in each archive (so they get EC ID 0)
            # But different content between archives
            (parent_path / 'parent_file1.txt').write_text('parent content A')
            (parent_path / 'parent_file2.txt').write_text('parent content A')
            (nested_path / 'nested_file1.txt').write_text('nested content B')
            (nested_path / 'nested_file2.txt').write_text('nested content B')

            with Processor() as processor:
                # Build parent archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive.rebuild()

                # Build nested archive
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import nested into parent
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present with correct EC assignments
                    indexed_files = {}  # path -> (digest, ec_id)
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            digest_match = re.search(r'digest:([0-9a-f]+)', line)
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if digest_match and ec_id_match:
                                digest = digest_match.group(1)
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = (digest, ec_id)

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # Files with same content should have same digest and EC ID
                    self.assertEqual(indexed_files['parent_file1.txt'], indexed_files['parent_file2.txt'])
                    self.assertEqual(indexed_files['nested/nested_file1.txt'], indexed_files['nested/nested_file2.txt'])

                    # Files with different content should have different digests
                    parent_digest = indexed_files['parent_file1.txt'][0]
                    nested_digest = indexed_files['nested/nested_file1.txt'][0]
                    self.assertNotEqual(parent_digest, nested_digest)

    def test_import_with_hash_collision(self):
        """Test that hash collisions are properly handled during import."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create nested archive
            nested_path = parent_path / 'nested'
            nested_path.mkdir()

            # Create files that will have XOR hash collision but different content
            # XOR hash works by XORing 4-byte chunks
            # b'\0\0\0\1' XOR b'\0\0\0\1' = b'\0\0\0\0' (hash: 00000000)
            # b'\0\0\0\2' XOR b'\0\0\0\2' = b'\0\0\0\0' (hash: 00000000)
            # Both will have same hash 00000000 but different content
            (parent_path / 'parent_file1.txt').write_bytes(b'\0\0\0\1\0\0\0\1')
            (parent_path / 'parent_file2.txt').write_bytes(b'\0\0\0\1\0\0\0\1')  # Same as parent_file1
            (nested_path / 'nested_file1.txt').write_bytes(b'\0\0\0\2\0\0\0\2')
            (nested_path / 'nested_file2.txt').write_bytes(b'\0\0\0\2\0\0\0\2')  # Same as nested_file1

            with Processor() as processor:
                # Build parent archive with XOR hash
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    parent_archive._hash_algorithms['xor'] = (4, compute_xor)
                    parent_archive._default_hash_algorithm = 'xor'
                    parent_archive.rebuild()

                # Build nested archive with XOR hash
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive._hash_algorithms['xor'] = (4, compute_xor)
                    nested_archive._default_hash_algorithm = 'xor'
                    nested_archive.rebuild()

                # Import nested into parent
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    parent_archive.import_index(str(nested_path))

                    # Check that all files are present with correct EC assignments
                    indexed_files = {}  # path -> (digest, ec_id)
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            digest_match = re.search(r'digest:([0-9a-f]+)', line)
                            ec_id_match = re.search(r'ec_id:(\d+)', line)
                            if digest_match and ec_id_match:
                                digest = digest_match.group(1)
                                ec_id = int(ec_id_match.group(1))
                                indexed_files[path] = (digest, ec_id)

                    # All files should be present
                    self.assertIn('parent_file1.txt', indexed_files)
                    self.assertIn('parent_file2.txt', indexed_files)
                    self.assertIn('nested/nested_file1.txt', indexed_files)
                    self.assertIn('nested/nested_file2.txt', indexed_files)

                    # All files should have the same digest (hash collision)
                    all_digests = {digest for digest, ec_id in indexed_files.values()}
                    self.assertEqual(len(all_digests), 1, "All files should have same digest due to hash collision")

                    # Files with same content should be in same EC class
                    self.assertEqual(indexed_files['parent_file1.txt'][1], indexed_files['parent_file2.txt'][1])
                    self.assertEqual(indexed_files['nested/nested_file1.txt'][1],
                                     indexed_files['nested/nested_file2.txt'][1])

                    # Files with different content should be in different EC classes
                    parent_ec_id = indexed_files['parent_file1.txt'][1]
                    nested_ec_id = indexed_files['nested/nested_file1.txt'][1]
                    self.assertNotEqual(parent_ec_id, nested_ec_id,
                                        "Files with different content should have different EC IDs even with same digest")

    def test_import_rejects_path_through_unfollowed_symlink(self):
        """Test that importing through an unfollowed symlink is rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create a real directory
            real_dir = parent_path / 'real_dir'
            real_dir.mkdir()

            # Create nested archive inside the real directory
            nested_path = real_dir / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create a symlink pointing to the real directory
            symlink_path = parent_path / 'symlink_dir'
            symlink_path.symlink_to(real_dir)

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Try to import using the symlink path - should fail
                # The import path goes through: parent_path -> symlink_dir -> nested_archive
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    # Use symlink path to reference the nested archive
                    symlink_nested_path = symlink_path / 'nested_archive'
                    with self.assertRaises(ValueError) as context:
                        parent_archive.import_index(str(symlink_nested_path))
                    self.assertIn('symlink', str(context.exception).lower())
                    self.assertIn('not configured to be followed', str(context.exception))

    def test_import_accepts_path_through_followed_symlink(self):
        """Test that importing through a followed symlink is accepted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create a real directory
            real_dir = parent_path / 'real_dir'
            real_dir.mkdir()

            # Create nested archive inside the real directory
            nested_path = real_dir / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create a symlink pointing to the real directory
            symlink_path = parent_path / 'symlink_dir'
            symlink_path.symlink_to(real_dir)

            # Configure parent archive to follow the symlink
            aridx_path = parent_path / '.aridx'
            aridx_path.mkdir()
            settings_path = aridx_path / 'settings.toml'
            settings_path.write_text('followed_symlinks = ["symlink_dir"]\n')

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Import using the symlink path - should succeed
                with Archive(processor, str(parent_path), create=False) as parent_archive:
                    symlink_nested_path = symlink_path / 'nested_archive'
                    parent_archive.import_index(str(symlink_nested_path))

                    # Verify the file was imported
                    indexed_files = []
                    for line in parent_archive.inspect():
                        if line.startswith('file-metadata'):
                            parts = line.split()
                            path = urllib.parse.unquote_plus(parts[1])
                            indexed_files.append(path)

                    self.assertIn('symlink_dir/nested_archive/file.txt', indexed_files)

    def test_import_rejects_nested_path_through_unfollowed_symlink(self):
        """Test that importing rejects paths with symlinks deep in the hierarchy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create parent archive
            parent_path = Path(tmpdir) / 'parent_archive'
            parent_path.mkdir()

            # Create directory structure: dir1/symlink_dir/dir2/nested_archive
            dir1 = parent_path / 'dir1'
            dir1.mkdir()

            # Create the real target
            real_target = parent_path / 'real_target'
            real_target.mkdir()
            dir2 = real_target / 'dir2'
            dir2.mkdir()

            # Create nested archive
            nested_path = dir2 / 'nested_archive'
            nested_path.mkdir()
            (nested_path / 'file.txt').write_text('content')

            # Create symlink in the middle of the path
            symlink_path = dir1 / 'symlink_dir'
            symlink_path.symlink_to(real_target)

            # Build the nested archive
            with Processor() as processor:
                with Archive(processor, str(nested_path), create=True) as nested_archive:
                    nested_archive.rebuild()

                # Try to import - should fail because path goes through unfollowed symlink
                with Archive(processor, str(parent_path), create=True) as parent_archive:
                    symlink_nested_path = dir1 / 'symlink_dir' / 'dir2' / 'nested_archive'
                    with self.assertRaises(ValueError) as context:
                        parent_archive.import_index(str(symlink_nested_path))
                    self.assertIn('symlink', str(context.exception).lower())
                    self.assertIn('dir1/symlink_dir', str(context.exception))

    def test_import_ancestor_validates_symlinks_in_containing_path(self):
        """Test that importing from ancestor validates symlinks in the contained archive's path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create ancestor archive
            ancestor_path = Path(tmpdir) / 'ancestor'
            ancestor_path.mkdir()

            # Create a real directory in ancestor
            real_dir = ancestor_path / 'real_subdir'
            real_dir.mkdir()

            # Create a symlink in ancestor pointing to real_dir
            symlink = ancestor_path / 'symlink_subdir'
            symlink.symlink_to(real_dir)

            # Create contained archive through symlink path
            # Physical path: ancestor/real_subdir/contained
            # Symlink path: ancestor/symlink_subdir/contained
            contained_path = real_dir / 'contained'
            contained_path.mkdir()
            (ancestor_path / 'file.txt').write_text('ancestor file')
            (contained_path / 'contained_file.txt').write_text('contained file')

            # Build ancestor archive
            with Processor() as processor:
                with Archive(processor, str(ancestor_path), create=True) as ancestor_archive:
                    ancestor_archive.rebuild()

                # Try to import into contained archive using symlink path
                # The contained archive is at ancestor/symlink_subdir/contained
                # When validating, it needs to check if symlink_subdir is followed
                symlink_contained_path = symlink / 'contained'
                with Archive(processor, str(symlink_contained_path), create=True) as contained_archive:
                    with self.assertRaises(ValueError) as context:
                        contained_archive.import_index(str(ancestor_path))
                    self.assertIn('symlink', str(context.exception).lower())
