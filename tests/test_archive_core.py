import hashlib
import os
import re
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive, FileMetadataDifferencePattern, FileMetadataDifferenceType
# noinspection PyProtectedMember
from arindexer._processor import Processor

from .test_utils import CollectingOutput, compute_xor, copy_times, tweak_times


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
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
                             for rec in archive.inspect()))
                    )

                with Archive(processor, str(archive_path)) as archive:
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_0,
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
                             for rec in archive.inspect()))
                    )

                    (archive_path / 'sample8' / 'sample10').unlink()
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_1,
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
                             for rec in archive.inspect()))
                    )

                    (archive_path / 'sample74' / 'sample10-another').unlink()
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_2,
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
                             for rec in archive.inspect()))
                    )

                    generate(archive_path / 'sample75', 'sample75')
                    archive.refresh()
                    self.assertEqual(
                        _test_common_lifecycle_3,
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
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

                output = CollectingOutput()
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
                        set((re.sub('^file-metadata path_hash:\\S+ (.+?) (digest:.* mtime:)\\S*( .*)$', 'file-metadata path_hash:PLACEHOLDER \\1 \\2\\3', rec)
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
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))

            with Processor() as processor:
                output = CollectingOutput()
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
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'dir-nested').mkdir()
            (target / 'dir-dup' / 'dir-nested' / 'sample1').write_bytes(b'sample1')
            copy_times(
                (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample1'),
                (target / 'dir-dup' / 'dir-nested' / 'sample1'))

            with Processor() as processor:
                output = CollectingOutput()
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
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            (target / 'dir-dup' / 'sample-link0').symlink_to('sample-link0')

            with Processor() as processor:
                output = CollectingOutput()
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
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            tweak_times((target / 'dir-dup' / 'sample0'), 1000000000)
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            tweak_times((target / 'dir-dup' / 'sample1'), 1000000000)

            with Processor() as processor:
                output = CollectingOutput()
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
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample0'), (target / 'dir-dup' / 'sample0'))
            tweak_times((target / 'dir-dup' / 'sample0'), 1000000000)
            (target / 'dir-dup' / 'sample1').write_bytes(b'sample1')
            copy_times((archive_path / 'dir0' / 'dir1' / 'sample1'), (target / 'dir-dup' / 'sample1'))
            tweak_times((target / 'dir-dup' / 'sample1'), 1000000000)
            (target / 'dir-dup' / 'dir-nested').mkdir()
            (target / 'dir-dup' / 'dir-nested' / 'sample0').write_bytes(b'sample0')
            copy_times(
                (archive_path / 'dir0' / 'dir1' / 'dir-nested' / 'sample0'),
                (target / 'dir-dup' / 'dir-nested' / 'sample0'))
            tweak_times((target / 'dir-dup' / 'dir-nested' / 'sample0'), 1000000000)

            with Processor() as processor:
                output = CollectingOutput()
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


# Load _test_common_lifecycle_0 from external file
with open(Path(__file__).parent / "test_common_lifecycle_0.txt") as f:
    _test_common_lifecycle_0 = set(line.rstrip("\n") for line in f if line.strip())

_test_common_lifecycle_1 = \
    (_test_common_lifecycle_0 | {
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0x2c4caee3 seq:0 sample74/sample10-another',
    }) - {
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0xf8fa8ebf seq:0 sample8/sample10',
        'file-metadata path_hash:PLACEHOLDER sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    }

_test_common_lifecycle_2 = _test_common_lifecycle_0 - {
    'file-metadata path_hash:PLACEHOLDER sample74/sample10-another digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0x2c4caee3 seq:0 sample74/sample10-another',
    'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0xf8fa8ebf seq:0 sample8/sample10',
    'file-metadata path_hash:PLACEHOLDER sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
}

_test_common_lifecycle_3 = \
    (_test_common_lifecycle_0 | {
        'file-hash 8b1cebc0d516efab2efe357a3bb49fe2dc96a45263e20312b433fa0e11fb909d ec_id:0 path_hash:0x2391050c seq:0 sample75',
        'file-metadata path_hash:PLACEHOLDER sample75 digest:8b1cebc0d516efab2efe357a3bb49fe2dc96a45263e20312b433fa0e11fb909d mtime: ec_id:0',
    }) - {
        'file-metadata path_hash:PLACEHOLDER sample74/sample10-another digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
        'file-metadata path_hash:PLACEHOLDER sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0xf8fa8ebf seq:0 sample8/sample10',
        'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 ec_id:0 path_hash:0x2c4caee3 seq:0 sample74/sample10-another',
    }

# Load _test_rebuild_with_collision_data from external file
with open(Path(__file__).parent / "test_rebuild_with_collision_data.txt") as f:
    _test_rebuild_with_collision_data = set(line.rstrip("\n") for line in f if line.strip())
