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


_test_common_lifecycle_0 = {
    'manifest-property hash-algorithm sha256',
    'file-hash 012e06abd2e40aebd85e416c4c84d0409e131e7831b10fae1944972d01c03753 0 sample8/sample41',
    'file-hash 0576fefc966dc91b9860dc21a525cbbf4999330d3bd193973ca7e67624a4951b 0 sample8/sample38',
    'file-hash 11aa7f108a573d417055076df566d7aa9eebc0cdadf1a56c5a8fe863ba9c9215 0 sample8/sample26',
    'file-hash 12a8fbcf449740b0f29b2fbfb3a230bba37a4e6646edfbcf5ee496e3890654db 0 sample8/sample25',
    'file-hash 135becd8bed65d802d364bb78de13346a224b868f916cf94890abf98132be6cf 0 sample8/sample63',
    'file-hash 136cdcb759165287d147fec152647e978863d4e4afa690a92edade60b45767d0 0 sample74/sample12-another sample8/sample12',
    'file-hash 1a80c910748e958a17c224d3b81964651fa21a39aa5c8c35349be236347e9f5c 0 sample8/sample46',
    'file-hash 2214d29b396a8a90ed00bc2f3eaa256bbafad4e1c6c435a854abd6292b07c5d4 0 sample74/sample19-another sample8/sample19',
    'file-hash 223d7c8fd01523f372e59d37a2f2687412517a2e6e1de056ef7b0b8d72983bda 0 sample74/sample17-another sample8/sample17',
    'file-hash 238263920e4a3f8d16b059e579f5cc230c460002a489a8b72f4d153c3f09b37c 0 sample74/sample18-another sample8/sample18',
    'file-hash 2449ceaa12a2aa69d6ddddd8fbdc0082e1f2502bfe2f0981902be110f6cdee9a 0 sample8/sample34',
    'file-hash 2474eaf85cb0639cde7fe627d2ac1b4c35d2954bea741cd27467fd0b09c08ee8 0 sample74/sample24-another sample8/sample24',
    'file-hash 257eaf26e2d6a2d32df4071cb13b03fda30fae73c43f687c67002e9712e51dea 0 sample74/sample20-another sample8/sample20',
    'file-hash 2634e9330f01fe4c58dc6cc662950d5052cfcfaad548a4fd628abdf5606173c0 0 sample1/sample3',
    'file-hash 26bf32952a0dc59435259194b9323291d76a42260f659324a628816d3dace8be 0 sample8/sample39',
    'file-hash 28762fc981421f49914f83d787db9b01eae2a8164f354ad26040d1ec2e0a4ab7 0 sample8/sample28',
    'file-hash 29b86ae2615ab49d97dba25f4ffc5cc978dc0e3d0f430249fee899bf1288ae55 0 sample8/sample67',
    'file-hash 2afa535b548d86aa2a4df31d9888f39781673d96f96858cb2d828e4ed2e7f9d0 0 sample8/sample36',
    'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
    'file-hash 3d3a9471ba773371c2e8bc0062902998ae9670c6ebdffddef099dacff689947e 0 sample8/sample32',
    'file-hash 3fa58d89f4bd3da260e82c97c52797da02ae2023e0212af8d0002dd50cfb05de 0 sample8/sample30',
    'file-hash 404d2f903a37f5ced38fffeb3de448042e6ef3d53b68e3cccc2e0dc22ce3345f 0 sample8/sample50',
    'file-hash 42bd2f28d3981fba5c5724643295a1e260573252d225091854ca29e3961bb264 0 sample74/sample15-another sample8/sample15',
    'file-hash 497eb0f9148be123fb0da47cdcbbd295059967a25f1cd7ea689cba6e2032d92a 0 sample8/sample68',
    'file-hash 4b11d98c1abf0c21a2bf580dd9ac93465b5b758597c4234f0fcf44c3aa2ac3d6 0 sample8/sample58',
    'file-hash 4cf0fcd52aab31f22e81c6be7b2f1af2debb2fe54c5d79e58b215abcb51f7ffc 0 sample8/sample35',
    'file-hash 4d14b93abe681be5ddbd1b91f5a0baf90e78ee1915650d2b0b1133d12c287259 0 sample8/sample60',
    'file-hash 4e9b494860bf0965e6747e6afecc83d533ec8beb5ab2afbf3f5c22c53813e673 0 sample8/sample55',
    'file-hash 534313de331e22512357096fb352920c9ece6b95da8b04463616c67b2a2bd6a2 0 sample8/sample71',
    'file-hash 5689b392afa5dacae25f8bf28911ee15cc0286674f97b29cc21b35798f5065ea 0 sample8/sample53',
    'file-hash 58f702416181867426ada5024387ed8a9550045c7dad5e91e93f2e29d0d00580 0 sample8/sample64',
    'file-hash 5eb822a2efa053972594a90f391f5f4a034c3012a480d25757dd4517e1d26a84 0 sample74/sample11-another sample8/sample11',
    'file-hash 6e29bcf2e47b64a1e2d627f713c19346ab66f65d21224bdad3c42e39bde14fa2 0 sample1/sample4/sample5',
    'file-hash 6f5053cc306f2c5b6e628ad81f1dffe22dd0f96fb7b458e61fdc1e9a7288d10d 0 sample8/sample56',
    'file-hash 72a5d130758b490c4ef1028880497f7aed35f023656078bdf1089a9f4dd47305 0 sample8/sample49',
    'file-hash 77a38ff151a30571a4e7714cc769ab6f5f6625a8794ec4e1e964d8ba0db2b2a7 0 sample8/sample37',
    'file-hash 7d9fd3d23b4667e38bd42a423a05bf8f9ccca0c02de4ef0c33362172b75e1bb7 0 sample8/sample61',
    'file-hash 7f4ae9a602f9465922d963522a0ab6769d0e5cc82fc673f57c60acf989d1c932 0 sample8/sample72',
    'file-hash 82905bea0acc3cb31743bf33742400f012853b2e594bf54223de8215dc78903f 0 sample8/sample48',
    'file-hash 86706b83c230caa30452914e83c5195eac86843533ff431da5c79f9d6f50f6c8 0 sample8/sample29',
    'file-hash 87e4e3ffe9ba611988963fd46650556d4614a87052eb1a242a7a9af862a8f895 0 sample8/sample43',
    'file-hash 892fb0fe890cae54f9b2ee6d69b878a4e4bfa92dc347d8a3247b7354cbf13c1d 0 sample8/sample65',
    'file-hash 8977f673a6362d5449dda5624436760da8eff0f3795e2695a7b6ebdd8d0c7e2d 0 sample8/sample47',
    'file-hash 89f7ec3b7a7f896f51437f4c4e4495c5375e9c9d67fd136884a7da1a2ed4fff2 0 sample8/sample27',
    'file-hash 8e69352112d0bfa715150620ab4e8795d7a573d4c3d37704a6a05898d7eac19f 0 sample8/sample42',
    'file-hash 91982a1e448df468100989f8a636da81db548bd172e818a48fbf1835c8aa28ed 0 sample8/sample33',
    'file-hash 98c4b5a8000f6a8d5b78a50bb63b12db94713fe11d7ffffd16109823dfe4ea5b 0 sample8/sample40',
    'file-hash 9cddc2186f98808b3ea1499a2f8ece90924d24ef2c209c3c639178805c1dfd3d 0 sample8/sample54',
    'file-hash a17a6e8362432e2ee7861e5dea9ee2d4f0db4a8cb4d8f13db2cb91d22cc05849 0 sample8/sample44',
    'file-hash a22a64ba859507f8507c67b59fea9464c3a9221a821d66f19e61573ef7764e2c 0 sample8/sample57',
    'file-hash b6a978dc68d17edb4dfe1a32607b1caa1cc87a48b8b37195cb256beb2b4bc908 0 sample8/sample59',
    'file-hash b9eca1d9632e955e1eb8aeb5bee3626033832877dfa8e5c72e35f0c3d112dbff 0 sample8/sample45',
    'file-hash c0739ec6913c9d1cf3488822bc80f858821e824f71882f0d30ed0f59b0e837dc 0 sample8/sample66',
    'file-hash c1a8534d37e8e43e575340a502e8a636d5ea162776a68b355f2aaa37981377a3 0 sample8/sample70',
    'file-hash c2d30048b140845806f6b2b6842d2c2e19508493c5a83e34525f6feedaddd5c5 0 sample74/sample16-another sample8/sample16',
    'file-hash d1bf55ac9bfda1cd87bbed609cdd471d41cfee58d9545db9a46f0333f3b36be8 0 sample8/sample69',
    'file-hash d7b1cbc3589403c892895dd1e6a71bf3e39af2e52d64ff47013e994e4822b8ca 0 sample8/sample52',
    'file-hash d8989c5d4110956a0867b4d6732219c7ce9688a1c57aa941ea1410f7aa47d6d0 0 sample0',
    'file-hash da548001ce90cae64be87093dd476b345e7176f2ba2bab1d446481feb27b4fc6 0 sample74/sample22-another sample8/sample22',
    'file-hash dac70bc79724cd282f182ab6aa09847b4ecc2ba82e12060ee078994a996f56f2 0 sample8/sample51',
    'file-hash de98d295d4d4e0cb52d67ea13f5120f2d6324e51c6594761313ab80cb2171a12 0 sample74/sample9-another sample8/sample9',
    'file-hash df21dfc9357179eb37e510bafee9172b332d19df560c7c2170ffad8fdb9707c6 0 sample8/sample31',
    'file-hash e41c79dfcf75f793a69cc3e294356baa09481dd7ee3e08da9e4584cdaaa859ed 0 sample74/sample13-another sample8/sample13',
    'file-hash ef99729801a2a675fba190fe8036f7fbac4ceed2e6d6f3880af370ef76b1610f 0 sample74/sample14-another sample8/sample14',
    'file-hash f4125996c2c17e2002149bcfc3b580e7a2e5e1d6ca6a9e173e4517b78ad6cc56 0 sample1/sample2',
    'file-hash f72073aa85c5e441ade232bc7123ce1a220062bfa1e2227f9d285139a8344163 0 sample74/sample23-another sample8/sample23',
    'file-hash fa2ff5ac7b6ebe8c10c5bac5a0c84f59cead8405f1e5e6006f026fcdb7976209 0 sample8/sample62',
    'file-hash fff29af91fad946f0cca38161a5081880386d14091a405a3d96dc66522e44f78 0 sample74/sample21-another sample8/sample21',
    'file-metadata sample8/sample41 digest:012e06abd2e40aebd85e416c4c84d0409e131e7831b10fae1944972d01c03753 mtime: ec_id:0',
    'file-metadata sample8/sample66 digest:c0739ec6913c9d1cf3488822bc80f858821e824f71882f0d30ed0f59b0e837dc mtime: ec_id:0',
    'file-metadata sample8/sample53 digest:5689b392afa5dacae25f8bf28911ee15cc0286674f97b29cc21b35798f5065ea mtime: ec_id:0',
    'file-metadata sample8/sample17 digest:223d7c8fd01523f372e59d37a2f2687412517a2e6e1de056ef7b0b8d72983bda mtime: ec_id:0',
    'file-metadata sample8/sample9 digest:de98d295d4d4e0cb52d67ea13f5120f2d6324e51c6594761313ab80cb2171a12 mtime: ec_id:0',
    'file-metadata sample1/sample3 digest:2634e9330f01fe4c58dc6cc662950d5052cfcfaad548a4fd628abdf5606173c0 mtime: ec_id:0',
    'file-metadata sample8/sample52 digest:d7b1cbc3589403c892895dd1e6a71bf3e39af2e52d64ff47013e994e4822b8ca mtime: ec_id:0',
    'file-metadata sample8/sample65 digest:892fb0fe890cae54f9b2ee6d69b878a4e4bfa92dc347d8a3247b7354cbf13c1d mtime: ec_id:0',
    'file-metadata sample8/sample43 digest:87e4e3ffe9ba611988963fd46650556d4614a87052eb1a242a7a9af862a8f895 mtime: ec_id:0',
    'file-metadata sample8/sample54 digest:9cddc2186f98808b3ea1499a2f8ece90924d24ef2c209c3c639178805c1dfd3d mtime: ec_id:0',
    'file-metadata sample8/sample33 digest:91982a1e448df468100989f8a636da81db548bd172e818a48fbf1835c8aa28ed mtime: ec_id:0',
    'file-metadata sample74/sample23-another digest:f72073aa85c5e441ade232bc7123ce1a220062bfa1e2227f9d285139a8344163 mtime: ec_id:0',
    'file-metadata sample8/sample24 digest:2474eaf85cb0639cde7fe627d2ac1b4c35d2954bea741cd27467fd0b09c08ee8 mtime: ec_id:0',
    'file-metadata sample8/sample31 digest:df21dfc9357179eb37e510bafee9172b332d19df560c7c2170ffad8fdb9707c6 mtime: ec_id:0',
    'file-metadata sample8/sample68 digest:497eb0f9148be123fb0da47cdcbbd295059967a25f1cd7ea689cba6e2032d92a mtime: ec_id:0',
    'file-metadata sample74/sample24-another digest:2474eaf85cb0639cde7fe627d2ac1b4c35d2954bea741cd27467fd0b09c08ee8 mtime: ec_id:0',
    'file-metadata sample8/sample46 digest:1a80c910748e958a17c224d3b81964651fa21a39aa5c8c35349be236347e9f5c mtime: ec_id:0',
    'file-metadata sample74/sample14-another digest:ef99729801a2a675fba190fe8036f7fbac4ceed2e6d6f3880af370ef76b1610f mtime: ec_id:0',
    'file-metadata sample8/sample71 digest:534313de331e22512357096fb352920c9ece6b95da8b04463616c67b2a2bd6a2 mtime: ec_id:0',
    'file-metadata sample74/sample11-another digest:5eb822a2efa053972594a90f391f5f4a034c3012a480d25757dd4517e1d26a84 mtime: ec_id:0',
    'file-metadata sample0 digest:d8989c5d4110956a0867b4d6732219c7ce9688a1c57aa941ea1410f7aa47d6d0 mtime: ec_id:0',
    'file-metadata sample8/sample21 digest:fff29af91fad946f0cca38161a5081880386d14091a405a3d96dc66522e44f78 mtime: ec_id:0',
    'file-metadata sample8/sample48 digest:82905bea0acc3cb31743bf33742400f012853b2e594bf54223de8215dc78903f mtime: ec_id:0',
    'file-metadata sample8/sample20 digest:257eaf26e2d6a2d32df4071cb13b03fda30fae73c43f687c67002e9712e51dea mtime: ec_id:0',
    'file-metadata sample8/sample26 digest:11aa7f108a573d417055076df566d7aa9eebc0cdadf1a56c5a8fe863ba9c9215 mtime: ec_id:0',
    'file-metadata sample74/sample13-another digest:e41c79dfcf75f793a69cc3e294356baa09481dd7ee3e08da9e4584cdaaa859ed mtime: ec_id:0',
    'file-metadata sample8/sample15 digest:42bd2f28d3981fba5c5724643295a1e260573252d225091854ca29e3961bb264 mtime: ec_id:0',
    'file-metadata sample8/sample47 digest:8977f673a6362d5449dda5624436760da8eff0f3795e2695a7b6ebdd8d0c7e2d mtime: ec_id:0',
    'file-metadata sample74/sample19-another digest:2214d29b396a8a90ed00bc2f3eaa256bbafad4e1c6c435a854abd6292b07c5d4 mtime: ec_id:0',
    'file-metadata sample8/sample29 digest:86706b83c230caa30452914e83c5195eac86843533ff431da5c79f9d6f50f6c8 mtime: ec_id:0',
    'file-metadata sample8/sample42 digest:8e69352112d0bfa715150620ab4e8795d7a573d4c3d37704a6a05898d7eac19f mtime: ec_id:0',
    'file-metadata sample8/sample63 digest:135becd8bed65d802d364bb78de13346a224b868f916cf94890abf98132be6cf mtime: ec_id:0',
    'file-metadata sample8/sample60 digest:4d14b93abe681be5ddbd1b91f5a0baf90e78ee1915650d2b0b1133d12c287259 mtime: ec_id:0',
    'file-metadata sample74/sample15-another digest:42bd2f28d3981fba5c5724643295a1e260573252d225091854ca29e3961bb264 mtime: ec_id:0',
    'file-metadata sample8/sample70 digest:c1a8534d37e8e43e575340a502e8a636d5ea162776a68b355f2aaa37981377a3 mtime: ec_id:0',
    'file-metadata sample74/sample20-another digest:257eaf26e2d6a2d32df4071cb13b03fda30fae73c43f687c67002e9712e51dea mtime: ec_id:0',
    'file-metadata sample8/sample30 digest:3fa58d89f4bd3da260e82c97c52797da02ae2023e0212af8d0002dd50cfb05de mtime: ec_id:0',
    'file-metadata sample8/sample18 digest:238263920e4a3f8d16b059e579f5cc230c460002a489a8b72f4d153c3f09b37c mtime: ec_id:0',
    'file-metadata sample8/sample55 digest:4e9b494860bf0965e6747e6afecc83d533ec8beb5ab2afbf3f5c22c53813e673 mtime: ec_id:0',
    'file-metadata sample8/sample72 digest:7f4ae9a602f9465922d963522a0ab6769d0e5cc82fc673f57c60acf989d1c932 mtime: ec_id:0',
    'file-metadata sample8/sample13 digest:e41c79dfcf75f793a69cc3e294356baa09481dd7ee3e08da9e4584cdaaa859ed mtime: ec_id:0',
    'file-metadata sample74/sample9-another digest:de98d295d4d4e0cb52d67ea13f5120f2d6324e51c6594761313ab80cb2171a12 mtime: ec_id:0',
    'file-metadata sample8/sample56 digest:6f5053cc306f2c5b6e628ad81f1dffe22dd0f96fb7b458e61fdc1e9a7288d10d mtime: ec_id:0',
    'file-metadata sample8/sample36 digest:2afa535b548d86aa2a4df31d9888f39781673d96f96858cb2d828e4ed2e7f9d0 mtime: ec_id:0',
    'file-metadata sample8/sample40 digest:98c4b5a8000f6a8d5b78a50bb63b12db94713fe11d7ffffd16109823dfe4ea5b mtime: ec_id:0',
    'file-metadata sample8/sample25 digest:12a8fbcf449740b0f29b2fbfb3a230bba37a4e6646edfbcf5ee496e3890654db mtime: ec_id:0',
    'file-metadata sample8/sample57 digest:a22a64ba859507f8507c67b59fea9464c3a9221a821d66f19e61573ef7764e2c mtime: ec_id:0',
    'file-metadata sample8/sample69 digest:d1bf55ac9bfda1cd87bbed609cdd471d41cfee58d9545db9a46f0333f3b36be8 mtime: ec_id:0',
    'file-metadata sample1/sample2 digest:f4125996c2c17e2002149bcfc3b580e7a2e5e1d6ca6a9e173e4517b78ad6cc56 mtime: ec_id:0',
    'file-metadata sample74/sample12-another digest:136cdcb759165287d147fec152647e978863d4e4afa690a92edade60b45767d0 mtime: ec_id:0',
    'file-metadata sample74/sample17-another digest:223d7c8fd01523f372e59d37a2f2687412517a2e6e1de056ef7b0b8d72983bda mtime: ec_id:0',
    'file-metadata sample8/sample35 digest:4cf0fcd52aab31f22e81c6be7b2f1af2debb2fe54c5d79e58b215abcb51f7ffc mtime: ec_id:0',
    'file-metadata sample8/sample16 digest:c2d30048b140845806f6b2b6842d2c2e19508493c5a83e34525f6feedaddd5c5 mtime: ec_id:0',
    'file-metadata sample8/sample58 digest:4b11d98c1abf0c21a2bf580dd9ac93465b5b758597c4234f0fcf44c3aa2ac3d6 mtime: ec_id:0',
    'file-metadata sample74/sample22-another digest:da548001ce90cae64be87093dd476b345e7176f2ba2bab1d446481feb27b4fc6 mtime: ec_id:0',
    'file-metadata sample8/sample27 digest:89f7ec3b7a7f896f51437f4c4e4495c5375e9c9d67fd136884a7da1a2ed4fff2 mtime: ec_id:0',
    'file-metadata sample8/sample64 digest:58f702416181867426ada5024387ed8a9550045c7dad5e91e93f2e29d0d00580 mtime: ec_id:0',
    'file-metadata sample8/sample12 digest:136cdcb759165287d147fec152647e978863d4e4afa690a92edade60b45767d0 mtime: ec_id:0',
    'file-metadata sample8/sample11 digest:5eb822a2efa053972594a90f391f5f4a034c3012a480d25757dd4517e1d26a84 mtime: ec_id:0',
    'file-metadata sample8/sample45 digest:b9eca1d9632e955e1eb8aeb5bee3626033832877dfa8e5c72e35f0c3d112dbff mtime: ec_id:0',
    'file-metadata sample8/sample49 digest:72a5d130758b490c4ef1028880497f7aed35f023656078bdf1089a9f4dd47305 mtime: ec_id:0',
    'file-metadata sample74/sample10-another digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    'file-metadata sample8/sample61 digest:7d9fd3d23b4667e38bd42a423a05bf8f9ccca0c02de4ef0c33362172b75e1bb7 mtime: ec_id:0',
    'file-metadata sample8/sample34 digest:2449ceaa12a2aa69d6ddddd8fbdc0082e1f2502bfe2f0981902be110f6cdee9a mtime: ec_id:0',
    'file-metadata sample8/sample22 digest:da548001ce90cae64be87093dd476b345e7176f2ba2bab1d446481feb27b4fc6 mtime: ec_id:0',
    'file-metadata sample8/sample59 digest:b6a978dc68d17edb4dfe1a32607b1caa1cc87a48b8b37195cb256beb2b4bc908 mtime: ec_id:0',
    'file-metadata sample8/sample38 digest:0576fefc966dc91b9860dc21a525cbbf4999330d3bd193973ca7e67624a4951b mtime: ec_id:0',
    'file-metadata sample8/sample67 digest:29b86ae2615ab49d97dba25f4ffc5cc978dc0e3d0f430249fee899bf1288ae55 mtime: ec_id:0',
    'file-metadata sample8/sample10 digest:3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 mtime: ec_id:0',
    'file-metadata sample8/sample62 digest:fa2ff5ac7b6ebe8c10c5bac5a0c84f59cead8405f1e5e6006f026fcdb7976209 mtime: ec_id:0',
    'file-metadata sample74/sample21-another digest:fff29af91fad946f0cca38161a5081880386d14091a405a3d96dc66522e44f78 mtime: ec_id:0',
    'file-metadata sample1/sample4/sample5 digest:6e29bcf2e47b64a1e2d627f713c19346ab66f65d21224bdad3c42e39bde14fa2 mtime: ec_id:0',
    'file-metadata sample8/sample19 digest:2214d29b396a8a90ed00bc2f3eaa256bbafad4e1c6c435a854abd6292b07c5d4 mtime: ec_id:0',
    'file-metadata sample8/sample39 digest:26bf32952a0dc59435259194b9323291d76a42260f659324a628816d3dace8be mtime: ec_id:0',
    'file-metadata sample74/sample16-another digest:c2d30048b140845806f6b2b6842d2c2e19508493c5a83e34525f6feedaddd5c5 mtime: ec_id:0',
    'file-metadata sample8/sample50 digest:404d2f903a37f5ced38fffeb3de448042e6ef3d53b68e3cccc2e0dc22ce3345f mtime: ec_id:0',
    'file-metadata sample8/sample44 digest:a17a6e8362432e2ee7861e5dea9ee2d4f0db4a8cb4d8f13db2cb91d22cc05849 mtime: ec_id:0',
    'file-metadata sample8/sample51 digest:dac70bc79724cd282f182ab6aa09847b4ecc2ba82e12060ee078994a996f56f2 mtime: ec_id:0',
    'file-metadata sample74/sample18-another digest:238263920e4a3f8d16b059e579f5cc230c460002a489a8b72f4d153c3f09b37c mtime: ec_id:0',
    'file-metadata sample8/sample23 digest:f72073aa85c5e441ade232bc7123ce1a220062bfa1e2227f9d285139a8344163 mtime: ec_id:0',
    'file-metadata sample8/sample32 digest:3d3a9471ba773371c2e8bc0062902998ae9670c6ebdffddef099dacff689947e mtime: ec_id:0',
    'file-metadata sample8/sample28 digest:28762fc981421f49914f83d787db9b01eae2a8164f354ad26040d1ec2e0a4ab7 mtime: ec_id:0',
    'file-metadata sample8/sample14 digest:ef99729801a2a675fba190fe8036f7fbac4ceed2e6d6f3880af370ef76b1610f mtime: ec_id:0',
    'file-metadata sample8/sample37 digest:77a38ff151a30571a4e7714cc769ab6f5f6625a8794ec4e1e964d8ba0db2b2a7 mtime: ec_id:0',
}

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

_test_rebuild_with_collision_data = {
    'manifest-property hash-algorithm xor',
    'file-hash 00010001 0 sample130',
    'file-hash 00010001 1 sample190',
    'file-hash 00010001 256 sample75',
    'file-hash 00010001 257 sample109',
    'file-hash 00010001 258 sample49',
    'file-hash 00010001 259 sample3',
    'file-hash 00010001 260 sample32',
    'file-hash 00010001 261 sample47',
    'file-hash 00010001 262 sample129',
    'file-hash 00010001 263 sample36',
    'file-hash 00010001 264 sample285',
    'file-hash 00010001 265 sample112',
    'file-hash 00010001 266 sample145',
    'file-hash 00010001 267 sample151',
    'file-hash 00010001 268 sample74',
    'file-hash 00010001 269 sample238',
    'file-hash 00010001 270 sample191',
    'file-hash 00010001 271 sample182',
    'file-hash 00010001 272 sample16',
    'file-hash 00010001 273 sample110',
    'file-hash 00010001 274 sample131',
    'file-hash 00010001 275 sample185',
    'file-hash 00010001 276 sample50',
    'file-hash 00010001 277 sample193',
    'file-hash 00010001 278 sample95',
    'file-hash 00010001 279 sample8',
    'file-hash 00010001 280 sample150',
    'file-hash 00010001 281 sample142',
    'file-hash 00010001 282 sample23',
    'file-hash 00010001 283 sample232',
    'file-hash 00010001 284 sample89',
    'file-hash 00010001 285 sample155',
    'file-hash 00010001 286 sample263',
    'file-hash 00010001 287 sample104',
    'file-hash 00010001 288 sample254',
    'file-hash 00010001 289 sample292',
    'file-hash 00010001 290 sample253',
    'file-hash 00010001 291 sample48',
    'file-hash 00010001 292 sample106',
    'file-hash 00010001 293 sample45',
    'file-hash 00010001 294 sample120',
    'file-hash 00010001 295 sample115',
    'file-hash 00010001 296 sample143',
    'file-hash 00010001 297 sample246',
    'file-hash 00010001 2 sample137',
    'file-hash 00010001 3 sample63',
    'file-hash 00010001 4 sample111',
    'file-hash 00010001 5 sample231',
    'file-hash 00010001 6 sample258',
    'file-hash 00010001 7 sample98',
    'file-hash 00010001 8 sample153',
    'file-hash 00010001 9 sample187',
    'file-hash 00010001 10 sample247',
    'file-hash 00010001 11 sample38',
    'file-hash 00010001 12 sample161',
    'file-hash 00010001 13 sample297',
    'file-hash 00010001 14 sample260',
    'file-hash 00010001 15 sample208',
    'file-hash 00010001 16 sample60',
    'file-hash 00010001 17 sample139',
    'file-hash 00010001 18 sample207',
    'file-hash 00010001 19 sample195',
    'file-hash 00010001 20 sample90',
    'file-hash 00010001 21 sample279',
    'file-hash 00010001 22 sample205',
    'file-hash 00010001 23 sample229',
    'file-hash 00010001 24 sample225',
    'file-hash 00010001 25 sample81',
    'file-hash 00010001 26 sample114',
    'file-hash 00010001 27 sample83',
    'file-hash 00010001 28 sample92',
    'file-hash 00010001 29 sample164',
    'file-hash 00010001 30 sample122',
    'file-hash 00010001 31 sample21',
    'file-hash 00010001 32 sample234',
    'file-hash 00010001 33 sample176',
    'file-hash 00010001 34 sample273',
    'file-hash 00010001 35 sample11',
    'file-hash 00010001 36 sample119',
    'file-hash 00010001 37 sample256',
    'file-hash 00010001 38 sample138',
    'file-hash 00010001 39 sample123',
    'file-hash 00010001 40 sample206',
    'file-hash 00010001 41 sample24',
    'file-hash 00010001 42 sample189',
    'file-hash 00010001 43 sample233',
    'file-hash 00010001 44 sample28',
    'file-hash 00010001 45 sample236',
    'file-hash 00010001 46 sample213',
    'file-hash 00010001 47 sample93',
    'file-hash 00010001 48 sample4',
    'file-hash 00010001 49 sample265',
    'file-hash 00010001 50 sample126',
    'file-hash 00010001 51 sample55',
    'file-hash 00010001 52 sample269',
    'file-hash 00010001 53 sample168',
    'file-hash 00010001 54 sample125',
    'file-hash 00010001 55 sample278',
    'file-hash 00010001 56 sample237',
    'file-hash 00010001 57 sample134',
    'file-hash 00010001 58 sample61',
    'file-hash 00010001 59 sample282',
    'file-hash 00010001 60 sample262',
    'file-hash 00010001 61 sample15',
    'file-hash 00010001 62 sample154',
    'file-hash 00010001 63 sample298',
    'file-hash 00010001 64 sample25',
    'file-hash 00010001 65 sample2',
    'file-hash 00010001 66 sample85',
    'file-hash 00010001 67 sample108',
    'file-hash 00010001 68 sample103',
    'file-hash 00010001 69 sample276',
    'file-hash 00010001 70 sample73',
    'file-hash 00010001 71 sample34',
    'file-hash 00010001 72 sample255',
    'file-hash 00010001 73 sample118',
    'file-hash 00010001 74 sample26',
    'file-hash 00010001 75 sample228',
    'file-hash 00010001 76 sample200',
    'file-hash 00010001 77 sample144',
    'file-hash 00010001 78 sample100',
    'file-hash 00010001 79 sample299',
    'file-hash 00010001 80 sample68',
    'file-hash 00010001 81 sample132',
    'file-hash 00010001 82 sample12',
    'file-hash 00010001 83 sample166',
    'file-hash 00010001 84 sample127',
    'file-hash 00010001 85 sample196',
    'file-hash 00010001 86 sample180',
    'file-hash 00010001 87 sample53',
    'file-hash 00010001 88 sample135',
    'file-hash 00010001 89 sample209',
    'file-hash 00010001 90 sample214',
    'file-hash 00010001 91 sample147',
    'file-hash 00010001 92 sample257',
    'file-hash 00010001 93 sample65',
    'file-hash 00010001 94 sample221',
    'file-hash 00010001 95 sample172',
    'file-hash 00010001 96 sample181',
    'file-hash 00010001 97 sample160',
    'file-hash 00010001 98 sample272',
    'file-hash 00010001 99 sample7',
    'file-hash 00010001 100 sample220',
    'file-hash 00010001 101 sample78',
    'file-hash 00010001 102 sample290',
    'file-hash 00010001 103 sample116',
    'file-hash 00010001 104 sample250',
    'file-hash 00010001 105 sample107',
    'file-hash 00010001 106 sample264',
    'file-hash 00010001 107 sample175',
    'file-hash 00010001 108 sample67',
    'file-hash 00010001 109 sample64',
    'file-hash 00010001 110 sample18',
    'file-hash 00010001 111 sample30',
    'file-hash 00010001 112 sample82',
    'file-hash 00010001 113 sample197',
    'file-hash 00010001 114 sample288',
    'file-hash 00010001 115 sample51',
    'file-hash 00010001 116 sample286',
    'file-hash 00010001 117 sample194',
    'file-hash 00010001 118 sample294',
    'file-hash 00010001 119 sample13',
    'file-hash 00010001 120 sample252',
    'file-hash 00010001 121 sample184',
    'file-hash 00010001 122 sample71',
    'file-hash 00010001 123 sample198',
    'file-hash 00010001 124 sample179',
    'file-hash 00010001 125 sample29',
    'file-hash 00010001 126 sample54',
    'file-hash 00010001 127 sample157',
    'file-hash 00010001 128 sample43',
    'file-hash 00010001 129 sample183',
    'file-hash 00010001 130 sample40',
    'file-hash 00010001 131 sample17',
    'file-hash 00010001 132 sample167',
    'file-hash 00010001 133 sample128',
    'file-hash 00010001 134 sample124',
    'file-hash 00010001 135 sample287',
    'file-hash 00010001 136 sample192',
    'file-hash 00010001 137 sample5',
    'file-hash 00010001 138 sample86',
    'file-hash 00010001 139 sample162',
    'file-hash 00010001 140 sample59',
    'file-hash 00010001 141 sample222',
    'file-hash 00010001 142 sample226',
    'file-hash 00010001 143 sample159',
    'file-hash 00010001 144 sample261',
    'file-hash 00010001 145 sample56',
    'file-hash 00010001 146 sample77',
    'file-hash 00010001 147 sample31',
    'file-hash 00010001 148 sample289',
    'file-hash 00010001 149 sample219',
    'file-hash 00010001 150 sample173',
    'file-hash 00010001 151 sample35',
    'file-hash 00010001 152 sample243',
    'file-hash 00010001 153 sample171',
    'file-hash 00010001 154 sample156',
    'file-hash 00010001 155 sample14',
    'file-hash 00010001 156 sample188',
    'file-hash 00010001 157 sample210',
    'file-hash 00010001 158 sample105',
    'file-hash 00010001 159 sample146',
    'file-hash 00010001 160 sample271',
    'file-hash 00010001 161 sample39',
    'file-hash 00010001 162 sample102',
    'file-hash 00010001 163 sample44',
    'file-hash 00010001 164 sample216',
    'file-hash 00010001 165 sample249',
    'file-hash 00010001 166 sample117',
    'file-hash 00010001 167 sample266',
    'file-hash 00010001 168 sample87',
    'file-hash 00010001 169 sample37',
    'file-hash 00010001 170 sample174',
    'file-hash 00010001 171 sample66',
    'file-hash 00010001 172 sample133',
    'file-hash 00010001 173 sample212',
    'file-hash 00010001 174 sample244',
    'file-hash 00010001 175 sample97',
    'file-hash 00010001 176 sample20',
    'file-hash 00010001 177 sample268',
    'file-hash 00010001 178 sample72',
    'file-hash 00010001 179 sample170',
    'file-hash 00010001 180 sample42',
    'file-hash 00010001 181 sample121',
    'file-hash 00010001 182 sample140',
    'file-hash 00010001 183 sample148',
    'file-hash 00010001 184 sample217',
    'file-hash 00010001 185 sample224',
    'file-hash 00010001 186 sample277',
    'file-hash 00010001 187 sample149',
    'file-hash 00010001 188 sample94',
    'file-hash 00010001 189 sample274',
    'file-hash 00010001 190 sample158',
    'file-hash 00010001 191 sample113',
    'file-hash 00010001 192 sample46',
    'file-hash 00010001 193 sample186',
    'file-hash 00010001 194 sample245',
    'file-hash 00010001 195 sample215',
    'file-hash 00010001 196 sample242',
    'file-hash 00010001 197 sample295',
    'file-hash 00010001 198 sample84',
    'file-hash 00010001 199 sample235',
    'file-hash 00010001 200 sample69',
    'file-hash 00010001 201 sample293',
    'file-hash 00010001 202 sample70',
    'file-hash 00010001 203 sample79',
    'file-hash 00010001 204 sample211',
    'file-hash 00010001 205 sample88',
    'file-hash 00010001 206 sample10',
    'file-hash 00010001 207 sample6',
    'file-hash 00010001 208 sample241',
    'file-hash 00010001 209 sample204',
    'file-hash 00010001 210 sample165',
    'file-hash 00010001 211 sample33',
    'file-hash 00010001 212 sample99',
    'file-hash 00010001 213 sample178',
    'file-hash 00010001 214 sample96',
    'file-hash 00010001 215 sample281',
    'file-hash 00010001 216 sample62',
    'file-hash 00010001 217 sample227',
    'file-hash 00010001 218 sample283',
    'file-hash 00010001 219 sample218',
    'file-hash 00010001 220 sample267',
    'file-hash 00010001 221 sample27',
    'file-hash 00010001 222 sample136',
    'file-hash 00010001 223 sample270',
    'file-hash 00010001 224 sample275',
    'file-hash 00010001 225 sample248',
    'file-hash 00010001 226 sample19',
    'file-hash 00010001 227 sample291',
    'file-hash 00010001 228 sample251',
    'file-hash 00010001 229 sample203',
    'file-hash 00010001 230 sample152',
    'file-hash 00010001 231 sample169',
    'file-hash 00010001 232 sample223',
    'file-hash 00010001 233 sample296',
    'file-hash 00010001 234 sample259',
    'file-hash 00010001 235 sample76',
    'file-hash 00010001 236 sample230',
    'file-hash 00010001 237 sample101',
    'file-hash 00010001 238 sample202',
    'file-hash 00010001 239 sample280',
    'file-hash 00010001 240 sample41',
    'file-hash 00010001 241 sample80',
    'file-hash 00010001 242 sample9',
    'file-hash 00010001 243 sample284',
    'file-hash 00010001 244 sample52',
    'file-hash 00010001 245 sample177',
    'file-hash 00010001 246 sample199',
    'file-hash 00010001 247 sample57',
    'file-hash 00010001 248 sample141',
    'file-hash 00010001 249 sample91',
    'file-hash 00010001 250 sample58',
    'file-hash 00010001 251 sample239',
    'file-hash 00010001 252 sample240',
    'file-hash 00010001 253 sample163',
    'file-hash 00010001 254 sample22',
    'file-hash 00010001 255 sample201',
    'file-hash 01010101 0 sample0',
    'file-hash 03030303 0 sample1',
    'file-metadata sample93 digest:00010001 mtime: ec_id:47',
    'file-metadata sample86 digest:00010001 mtime: ec_id:138',
    'file-metadata sample285 digest:00010001 mtime: ec_id:264',
    'file-metadata sample237 digest:00010001 mtime: ec_id:56',
    'file-metadata sample151 digest:00010001 mtime: ec_id:267',
    'file-metadata sample288 digest:00010001 mtime: ec_id:114',
    'file-metadata sample249 digest:00010001 mtime: ec_id:165',
    'file-metadata sample298 digest:00010001 mtime: ec_id:63',
    'file-metadata sample9 digest:00010001 mtime: ec_id:242',
    'file-metadata sample234 digest:00010001 mtime: ec_id:32',
    'file-metadata sample63 digest:00010001 mtime: ec_id:3',
    'file-metadata sample279 digest:00010001 mtime: ec_id:21',
    'file-metadata sample117 digest:00010001 mtime: ec_id:166',
    'file-metadata sample5 digest:00010001 mtime: ec_id:137',
    'file-metadata sample82 digest:00010001 mtime: ec_id:112',
    'file-metadata sample299 digest:00010001 mtime: ec_id:79',
    'file-metadata sample263 digest:00010001 mtime: ec_id:286',
    'file-metadata sample118 digest:00010001 mtime: ec_id:73',
    'file-metadata sample259 digest:00010001 mtime: ec_id:234',
    'file-metadata sample175 digest:00010001 mtime: ec_id:107',
    'file-metadata sample156 digest:00010001 mtime: ec_id:154',
    'file-metadata sample12 digest:00010001 mtime: ec_id:82',
    'file-metadata sample172 digest:00010001 mtime: ec_id:95',
    'file-metadata sample146 digest:00010001 mtime: ec_id:159',
    'file-metadata sample49 digest:00010001 mtime: ec_id:258',
    'file-metadata sample250 digest:00010001 mtime: ec_id:104',
    'file-metadata sample99 digest:00010001 mtime: ec_id:212',
    'file-metadata sample137 digest:00010001 mtime: ec_id:2',
    'file-metadata sample277 digest:00010001 mtime: ec_id:186',
    'file-metadata sample125 digest:00010001 mtime: ec_id:54',
    'file-metadata sample194 digest:00010001 mtime: ec_id:117',
    'file-metadata sample98 digest:00010001 mtime: ec_id:7',
    'file-metadata sample241 digest:00010001 mtime: ec_id:208',
    'file-metadata sample217 digest:00010001 mtime: ec_id:184',
    'file-metadata sample47 digest:00010001 mtime: ec_id:261',
    'file-metadata sample212 digest:00010001 mtime: ec_id:173',
    'file-metadata sample120 digest:00010001 mtime: ec_id:294',
    'file-metadata sample27 digest:00010001 mtime: ec_id:221',
    'file-metadata sample225 digest:00010001 mtime: ec_id:24',
    'file-metadata sample202 digest:00010001 mtime: ec_id:238',
    'file-metadata sample73 digest:00010001 mtime: ec_id:70',
    'file-metadata sample129 digest:00010001 mtime: ec_id:262',
    'file-metadata sample29 digest:00010001 mtime: ec_id:125',
    'file-metadata sample7 digest:00010001 mtime: ec_id:99',
    'file-metadata sample255 digest:00010001 mtime: ec_id:72',
    'file-metadata sample131 digest:00010001 mtime: ec_id:274',
    'file-metadata sample3 digest:00010001 mtime: ec_id:259',
    'file-metadata sample121 digest:00010001 mtime: ec_id:181',
    'file-metadata sample179 digest:00010001 mtime: ec_id:124',
    'file-metadata sample59 digest:00010001 mtime: ec_id:140',
    'file-metadata sample191 digest:00010001 mtime: ec_id:270',
    'file-metadata sample141 digest:00010001 mtime: ec_id:248',
    'file-metadata sample123 digest:00010001 mtime: ec_id:39',
    'file-metadata sample43 digest:00010001 mtime: ec_id:128',
    'file-metadata sample270 digest:00010001 mtime: ec_id:223',
    'file-metadata sample37 digest:00010001 mtime: ec_id:169',
    'file-metadata sample208 digest:00010001 mtime: ec_id:15',
    'file-metadata sample107 digest:00010001 mtime: ec_id:105',
    'file-metadata sample1 digest:03030303 mtime: ec_id:0',
    'file-metadata sample166 digest:00010001 mtime: ec_id:83',
    'file-metadata sample89 digest:00010001 mtime: ec_id:284',
    'file-metadata sample294 digest:00010001 mtime: ec_id:118',
    'file-metadata sample244 digest:00010001 mtime: ec_id:174',
    'file-metadata sample83 digest:00010001 mtime: ec_id:27',
    'file-metadata sample295 digest:00010001 mtime: ec_id:197',
    'file-metadata sample155 digest:00010001 mtime: ec_id:285',
    'file-metadata sample127 digest:00010001 mtime: ec_id:84',
    'file-metadata sample232 digest:00010001 mtime: ec_id:283',
    'file-metadata sample261 digest:00010001 mtime: ec_id:144',
    'file-metadata sample19 digest:00010001 mtime: ec_id:226',
    'file-metadata sample133 digest:00010001 mtime: ec_id:172',
    'file-metadata sample17 digest:00010001 mtime: ec_id:131',
    'file-metadata sample186 digest:00010001 mtime: ec_id:193',
    'file-metadata sample230 digest:00010001 mtime: ec_id:236',
    'file-metadata sample284 digest:00010001 mtime: ec_id:243',
    'file-metadata sample236 digest:00010001 mtime: ec_id:45',
    'file-metadata sample147 digest:00010001 mtime: ec_id:91',
    'file-metadata sample286 digest:00010001 mtime: ec_id:116',
    'file-metadata sample221 digest:00010001 mtime: ec_id:94',
    'file-metadata sample248 digest:00010001 mtime: ec_id:225',
    'file-metadata sample262 digest:00010001 mtime: ec_id:60',
    'file-metadata sample211 digest:00010001 mtime: ec_id:204',
    'file-metadata sample31 digest:00010001 mtime: ec_id:147',
    'file-metadata sample62 digest:00010001 mtime: ec_id:216',
    'file-metadata sample290 digest:00010001 mtime: ec_id:102',
    'file-metadata sample169 digest:00010001 mtime: ec_id:231',
    'file-metadata sample8 digest:00010001 mtime: ec_id:279',
    'file-metadata sample14 digest:00010001 mtime: ec_id:155',
    'file-metadata sample157 digest:00010001 mtime: ec_id:127',
    'file-metadata sample216 digest:00010001 mtime: ec_id:164',
    'file-metadata sample210 digest:00010001 mtime: ec_id:157',
    'file-metadata sample203 digest:00010001 mtime: ec_id:229',
    'file-metadata sample178 digest:00010001 mtime: ec_id:213',
    'file-metadata sample128 digest:00010001 mtime: ec_id:133',
    'file-metadata sample39 digest:00010001 mtime: ec_id:161',
    'file-metadata sample132 digest:00010001 mtime: ec_id:81',
    'file-metadata sample23 digest:00010001 mtime: ec_id:282',
    'file-metadata sample96 digest:00010001 mtime: ec_id:214',
    'file-metadata sample180 digest:00010001 mtime: ec_id:86',
    'file-metadata sample10 digest:00010001 mtime: ec_id:206',
    'file-metadata sample103 digest:00010001 mtime: ec_id:68',
    'file-metadata sample88 digest:00010001 mtime: ec_id:205',
    'file-metadata sample163 digest:00010001 mtime: ec_id:253',
    'file-metadata sample15 digest:00010001 mtime: ec_id:61',
    'file-metadata sample140 digest:00010001 mtime: ec_id:182',
    'file-metadata sample239 digest:00010001 mtime: ec_id:251',
    'file-metadata sample246 digest:00010001 mtime: ec_id:297',
    'file-metadata sample271 digest:00010001 mtime: ec_id:160',
    'file-metadata sample235 digest:00010001 mtime: ec_id:199',
    'file-metadata sample108 digest:00010001 mtime: ec_id:67',
    'file-metadata sample38 digest:00010001 mtime: ec_id:11',
    'file-metadata sample18 digest:00010001 mtime: ec_id:110',
    'file-metadata sample30 digest:00010001 mtime: ec_id:111',
    'file-metadata sample154 digest:00010001 mtime: ec_id:62',
    'file-metadata sample116 digest:00010001 mtime: ec_id:103',
    'file-metadata sample257 digest:00010001 mtime: ec_id:92',
    'file-metadata sample65 digest:00010001 mtime: ec_id:93',
    'file-metadata sample268 digest:00010001 mtime: ec_id:177',
    'file-metadata sample135 digest:00010001 mtime: ec_id:88',
    'file-metadata sample101 digest:00010001 mtime: ec_id:237',
    'file-metadata sample195 digest:00010001 mtime: ec_id:19',
    'file-metadata sample11 digest:00010001 mtime: ec_id:35',
    'file-metadata sample16 digest:00010001 mtime: ec_id:272',
    'file-metadata sample122 digest:00010001 mtime: ec_id:30',
    'file-metadata sample76 digest:00010001 mtime: ec_id:235',
    'file-metadata sample223 digest:00010001 mtime: ec_id:232',
    'file-metadata sample109 digest:00010001 mtime: ec_id:257',
    'file-metadata sample153 digest:00010001 mtime: ec_id:8',
    'file-metadata sample272 digest:00010001 mtime: ec_id:98',
    'file-metadata sample50 digest:00010001 mtime: ec_id:276',
    'file-metadata sample200 digest:00010001 mtime: ec_id:76',
    'file-metadata sample136 digest:00010001 mtime: ec_id:222',
    'file-metadata sample296 digest:00010001 mtime: ec_id:233',
    'file-metadata sample72 digest:00010001 mtime: ec_id:178',
    'file-metadata sample192 digest:00010001 mtime: ec_id:136',
    'file-metadata sample240 digest:00010001 mtime: ec_id:252',
    'file-metadata sample4 digest:00010001 mtime: ec_id:48',
    'file-metadata sample56 digest:00010001 mtime: ec_id:145',
    'file-metadata sample58 digest:00010001 mtime: ec_id:250',
    'file-metadata sample75 digest:00010001 mtime: ec_id:256',
    'file-metadata sample207 digest:00010001 mtime: ec_id:18',
    'file-metadata sample287 digest:00010001 mtime: ec_id:135',
    'file-metadata sample196 digest:00010001 mtime: ec_id:85',
    'file-metadata sample213 digest:00010001 mtime: ec_id:46',
    'file-metadata sample238 digest:00010001 mtime: ec_id:269',
    'file-metadata sample282 digest:00010001 mtime: ec_id:59',
    'file-metadata sample190 digest:00010001 mtime: ec_id:1',
    'file-metadata sample275 digest:00010001 mtime: ec_id:224',
    'file-metadata sample71 digest:00010001 mtime: ec_id:122',
    'file-metadata sample105 digest:00010001 mtime: ec_id:158',
    'file-metadata sample78 digest:00010001 mtime: ec_id:101',
    'file-metadata sample280 digest:00010001 mtime: ec_id:239',
    'file-metadata sample242 digest:00010001 mtime: ec_id:196',
    'file-metadata sample111 digest:00010001 mtime: ec_id:4',
    'file-metadata sample22 digest:00010001 mtime: ec_id:254',
    'file-metadata sample41 digest:00010001 mtime: ec_id:240',
    'file-metadata sample254 digest:00010001 mtime: ec_id:288',
    'file-metadata sample42 digest:00010001 mtime: ec_id:180',
    'file-metadata sample35 digest:00010001 mtime: ec_id:151',
    'file-metadata sample74 digest:00010001 mtime: ec_id:268',
    'file-metadata sample113 digest:00010001 mtime: ec_id:191',
    'file-metadata sample28 digest:00010001 mtime: ec_id:44',
    'file-metadata sample44 digest:00010001 mtime: ec_id:163',
    'file-metadata sample247 digest:00010001 mtime: ec_id:10',
    'file-metadata sample158 digest:00010001 mtime: ec_id:190',
    'file-metadata sample173 digest:00010001 mtime: ec_id:150',
    'file-metadata sample206 digest:00010001 mtime: ec_id:40',
    'file-metadata sample13 digest:00010001 mtime: ec_id:119',
    'file-metadata sample227 digest:00010001 mtime: ec_id:217',
    'file-metadata sample281 digest:00010001 mtime: ec_id:215',
    'file-metadata sample115 digest:00010001 mtime: ec_id:295',
    'file-metadata sample199 digest:00010001 mtime: ec_id:246',
    'file-metadata sample229 digest:00010001 mtime: ec_id:23',
    'file-metadata sample188 digest:00010001 mtime: ec_id:156',
    'file-metadata sample289 digest:00010001 mtime: ec_id:148',
    'file-metadata sample87 digest:00010001 mtime: ec_id:168',
    'file-metadata sample94 digest:00010001 mtime: ec_id:188',
    'file-metadata sample26 digest:00010001 mtime: ec_id:74',
    'file-metadata sample297 digest:00010001 mtime: ec_id:13',
    'file-metadata sample150 digest:00010001 mtime: ec_id:280',
    'file-metadata sample165 digest:00010001 mtime: ec_id:210',
    'file-metadata sample222 digest:00010001 mtime: ec_id:141',
    'file-metadata sample2 digest:00010001 mtime: ec_id:65',
    'file-metadata sample144 digest:00010001 mtime: ec_id:77',
    'file-metadata sample34 digest:00010001 mtime: ec_id:71',
    'file-metadata sample266 digest:00010001 mtime: ec_id:167',
    'file-metadata sample218 digest:00010001 mtime: ec_id:219',
    'file-metadata sample276 digest:00010001 mtime: ec_id:69',
    'file-metadata sample226 digest:00010001 mtime: ec_id:142',
    'file-metadata sample278 digest:00010001 mtime: ec_id:55',
    'file-metadata sample292 digest:00010001 mtime: ec_id:289',
    'file-metadata sample81 digest:00010001 mtime: ec_id:25',
    'file-metadata sample149 digest:00010001 mtime: ec_id:187',
    'file-metadata sample160 digest:00010001 mtime: ec_id:97',
    'file-metadata sample205 digest:00010001 mtime: ec_id:22',
    'file-metadata sample33 digest:00010001 mtime: ec_id:211',
    'file-metadata sample80 digest:00010001 mtime: ec_id:241',
    'file-metadata sample283 digest:00010001 mtime: ec_id:218',
    'file-metadata sample185 digest:00010001 mtime: ec_id:275',
    'file-metadata sample253 digest:00010001 mtime: ec_id:290',
    'file-metadata sample55 digest:00010001 mtime: ec_id:51',
    'file-metadata sample54 digest:00010001 mtime: ec_id:126',
    'file-metadata sample92 digest:00010001 mtime: ec_id:28',
    'file-metadata sample176 digest:00010001 mtime: ec_id:33',
    'file-metadata sample20 digest:00010001 mtime: ec_id:176',
    'file-metadata sample134 digest:00010001 mtime: ec_id:57',
    'file-metadata sample60 digest:00010001 mtime: ec_id:16',
    'file-metadata sample177 digest:00010001 mtime: ec_id:245',
    'file-metadata sample114 digest:00010001 mtime: ec_id:26',
    'file-metadata sample143 digest:00010001 mtime: ec_id:296',
    'file-metadata sample264 digest:00010001 mtime: ec_id:106',
    'file-metadata sample85 digest:00010001 mtime: ec_id:66',
    'file-metadata sample119 digest:00010001 mtime: ec_id:36',
    'file-metadata sample182 digest:00010001 mtime: ec_id:271',
    'file-metadata sample68 digest:00010001 mtime: ec_id:80',
    'file-metadata sample231 digest:00010001 mtime: ec_id:5',
    'file-metadata sample0 digest:01010101 mtime: ec_id:0',
    'file-metadata sample139 digest:00010001 mtime: ec_id:17',
    'file-metadata sample24 digest:00010001 mtime: ec_id:41',
    'file-metadata sample126 digest:00010001 mtime: ec_id:50',
    'file-metadata sample67 digest:00010001 mtime: ec_id:108',
    'file-metadata sample106 digest:00010001 mtime: ec_id:292',
    'file-metadata sample245 digest:00010001 mtime: ec_id:194',
    'file-metadata sample193 digest:00010001 mtime: ec_id:277',
    'file-metadata sample184 digest:00010001 mtime: ec_id:121',
    'file-metadata sample91 digest:00010001 mtime: ec_id:249',
    'file-metadata sample228 digest:00010001 mtime: ec_id:75',
    'file-metadata sample243 digest:00010001 mtime: ec_id:152',
    'file-metadata sample201 digest:00010001 mtime: ec_id:255',
    'file-metadata sample171 digest:00010001 mtime: ec_id:153',
    'file-metadata sample174 digest:00010001 mtime: ec_id:170',
    'file-metadata sample148 digest:00010001 mtime: ec_id:183',
    'file-metadata sample197 digest:00010001 mtime: ec_id:113',
    'file-metadata sample64 digest:00010001 mtime: ec_id:109',
    'file-metadata sample32 digest:00010001 mtime: ec_id:260',
    'file-metadata sample269 digest:00010001 mtime: ec_id:52',
    'file-metadata sample104 digest:00010001 mtime: ec_id:287',
    'file-metadata sample161 digest:00010001 mtime: ec_id:12',
    'file-metadata sample138 digest:00010001 mtime: ec_id:38',
    'file-metadata sample66 digest:00010001 mtime: ec_id:171',
    'file-metadata sample61 digest:00010001 mtime: ec_id:58',
    'file-metadata sample274 digest:00010001 mtime: ec_id:189',
    'file-metadata sample183 digest:00010001 mtime: ec_id:129',
    'file-metadata sample214 digest:00010001 mtime: ec_id:90',
    'file-metadata sample25 digest:00010001 mtime: ec_id:64',
    'file-metadata sample189 digest:00010001 mtime: ec_id:42',
    'file-metadata sample51 digest:00010001 mtime: ec_id:115',
    'file-metadata sample52 digest:00010001 mtime: ec_id:244',
    'file-metadata sample162 digest:00010001 mtime: ec_id:139',
    'file-metadata sample252 digest:00010001 mtime: ec_id:120',
    'file-metadata sample256 digest:00010001 mtime: ec_id:37',
    'file-metadata sample110 digest:00010001 mtime: ec_id:273',
    'file-metadata sample36 digest:00010001 mtime: ec_id:263',
    'file-metadata sample233 digest:00010001 mtime: ec_id:43',
    'file-metadata sample220 digest:00010001 mtime: ec_id:100',
    'file-metadata sample84 digest:00010001 mtime: ec_id:198',
    'file-metadata sample265 digest:00010001 mtime: ec_id:49',
    'file-metadata sample167 digest:00010001 mtime: ec_id:132',
    'file-metadata sample209 digest:00010001 mtime: ec_id:89',
    'file-metadata sample291 digest:00010001 mtime: ec_id:227',
    'file-metadata sample219 digest:00010001 mtime: ec_id:149',
    'file-metadata sample198 digest:00010001 mtime: ec_id:123',
    'file-metadata sample40 digest:00010001 mtime: ec_id:130',
    'file-metadata sample251 digest:00010001 mtime: ec_id:228',
    'file-metadata sample187 digest:00010001 mtime: ec_id:9',
    'file-metadata sample204 digest:00010001 mtime: ec_id:209',
    'file-metadata sample293 digest:00010001 mtime: ec_id:201',
    'file-metadata sample97 digest:00010001 mtime: ec_id:175',
    'file-metadata sample90 digest:00010001 mtime: ec_id:20',
    'file-metadata sample258 digest:00010001 mtime: ec_id:6',
    'file-metadata sample6 digest:00010001 mtime: ec_id:207',
    'file-metadata sample152 digest:00010001 mtime: ec_id:230',
    'file-metadata sample273 digest:00010001 mtime: ec_id:34',
    'file-metadata sample95 digest:00010001 mtime: ec_id:278',
    'file-metadata sample164 digest:00010001 mtime: ec_id:29',
    'file-metadata sample70 digest:00010001 mtime: ec_id:202',
    'file-metadata sample130 digest:00010001 mtime: ec_id:0',
    'file-metadata sample215 digest:00010001 mtime: ec_id:195',
    'file-metadata sample69 digest:00010001 mtime: ec_id:200',
    'file-metadata sample79 digest:00010001 mtime: ec_id:203',
    'file-metadata sample100 digest:00010001 mtime: ec_id:78',
    'file-metadata sample142 digest:00010001 mtime: ec_id:281',
    'file-metadata sample145 digest:00010001 mtime: ec_id:266',
    'file-metadata sample21 digest:00010001 mtime: ec_id:31',
    'file-metadata sample77 digest:00010001 mtime: ec_id:146',
    'file-metadata sample267 digest:00010001 mtime: ec_id:220',
    'file-metadata sample159 digest:00010001 mtime: ec_id:143',
    'file-metadata sample112 digest:00010001 mtime: ec_id:265',
    'file-metadata sample53 digest:00010001 mtime: ec_id:87',
    'file-metadata sample46 digest:00010001 mtime: ec_id:192',
    'file-metadata sample45 digest:00010001 mtime: ec_id:293',
    'file-metadata sample224 digest:00010001 mtime: ec_id:185',
    'file-metadata sample181 digest:00010001 mtime: ec_id:96',
    'file-metadata sample102 digest:00010001 mtime: ec_id:162',
    'file-metadata sample48 digest:00010001 mtime: ec_id:291',
    'file-metadata sample168 digest:00010001 mtime: ec_id:53',
    'file-metadata sample57 digest:00010001 mtime: ec_id:247',
    'file-metadata sample260 digest:00010001 mtime: ec_id:14',
    'file-metadata sample124 digest:00010001 mtime: ec_id:134',
    'file-metadata sample170 digest:00010001 mtime: ec_id:179',
}

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
[symlinks]
follow = ["parent/linked_dir"]
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
[symlinks]
follow = ["link1"]
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
[symlinks]
follow = ["link1"]
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
                    self.assertEqual(indexed_files['nested/nested_file1.txt'][1], indexed_files['nested/nested_file2.txt'][1])

                    # Files with different content should be in different EC classes
                    parent_ec_id = indexed_files['parent_file1.txt'][1]
                    nested_ec_id = indexed_files['nested/nested_file1.txt'][1]
                    self.assertNotEqual(parent_ec_id, nested_ec_id,
                                       "Files with different content should have different EC IDs even with same digest")


if __name__ == '__main__':
    unittest.main()
