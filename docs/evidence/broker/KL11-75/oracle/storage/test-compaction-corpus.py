#!/usr/bin/env python3
"""Check the independent cleaner oracle against CRC-valid altered raw copies."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import struct

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('corpus', HERE / 'check-compaction-corpus.py')
CORPUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CORPUS)
CHECK = CORPUS.CHECK


def checksum(raw):
    raw[-4:] = struct.pack('>I', CHECK.crc(raw[:-4]))


def replace_payload(path, payload):
    manifest = bytearray(CHECK.read(path / 'manifest'))
    revision = int.from_bytes(manifest[8:16], 'big')
    stem = f'{0:016x}-{revision:016x}'
    file = path / (stem + '.journal')
    raw = bytearray(CHECK.read(file)[:56]) + payload
    struct.pack_into('>I', raw, 32, len(payload))
    struct.pack_into('>I', raw, 48, CHECK.crc(payload))
    struct.pack_into('>I', raw, 52, CHECK.crc(raw[24:52]))
    file.write_bytes(raw)
    struct.pack_into('>Q', manifest, 72 + 24, len(raw))
    maximum = max((b['max_timestamp'] for b in CORPUS.batches(payload)), default=CORPUS.MIN_TIME)
    struct.pack_into('>q', manifest, 72 + 40, maximum)
    struct.pack_into('>I', manifest, 72 + 48, CHECK.crc(raw))
    checksum(manifest)
    (path / 'manifest').write_bytes(manifest)
    seek = bytearray(CHECK.read(path / (stem + '.seek')))
    seek[8:64] = manifest[72:128]
    checksum(seek)
    (path / (stem + '.seek')).write_bytes(seek)
    CHECK.protected(bytes(manifest), b'PLSEGM03', 76)
    CHECK.protected(bytes(seek), b'PLSEEK02', 72)
    CHECK.require(CHECK.crc(raw[:20]) == int.from_bytes(raw[20:24], 'big')
                  and CHECK.crc(raw[24:52]) == int.from_bytes(raw[52:56], 'big')
                  and CHECK.crc(raw[56:]) == int.from_bytes(raw[48:52], 'big'),
                  'control file and entry checksums valid')


def mutate(path, fixture_directory, name):
    case = json.loads(CHECK.read(path / 'case.json'))
    payload = bytearray(CHECK.read(fixture_directory / (case['expected_fixture'] + '.bin')))
    if name == 'invented-keyed-value':
        position = payload.index(b'new')
        payload[position:position + 3] = b'bad'
    elif name == 'wrong-delete-horizon':
        CHECK.require(int.from_bytes(payload[21:23], 'big') == 64, 'actual horizon fixture')
        struct.pack_into('>q', payload, 27, 3001)
    elif name == 'wrong-retained-timestamp':
        struct.pack_into('>q', payload, 27, 3001)
    elif name == 'invented-empty-header-time':
        CHECK.require(int.from_bytes(payload[57:61], 'big') == 0, 'actual empty fixture')
        struct.pack_into('>q', payload, 35, 9999)
    elif name in ('kept-expired-tombstones', 'expired-tombstones-early', 'kept-null-keys'):
        replacement = {'kept-expired-tombstones': 'cleaner-first-horizon-sparse',
                       'expired-tombstones-early': 'cleaner-equal-horizon-sparse',
                       'kept-null-keys': 'cleaner-mixed-input'}[name]
        payload = bytearray(CHECK.read(fixture_directory / (replacement + '.bin')))
    else:
        raise ValueError(name)
    # Each chosen fixture consists of one ordinary batch. The altered payload
    # still decodes and has valid Kafka protection, before all outer bindings
    # are repaired. The rejection therefore does not depend on a broken CRC.
    payload[17:21] = struct.pack('>I', CHECK.crc(payload[21:]))
    CORPUS.batches(payload)
    replace_payload(path, payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    CHECK.require(not args.work.exists(), 'fresh isolated control directory')
    files = sorted(p for p in args.corpus.rglob('*') if p.is_file())
    before = {str(p): CHECK.sha(CHECK.read(p)) for p in files}
    controls = [('mixed', 2000, 0, 'invented-keyed-value'),
                ('mixed', 2000, 0, 'wrong-delete-horizon'),
                ('mixed', 2000, 0, 'wrong-retained-timestamp'),
                ('mixed', 2000, 0, 'expired-tombstones-early'),
                ('mixed', 3000, 2, 'kept-expired-tombstones'),
                ('mixed', 2000, 0, 'kept-null-keys'),
                ('nullonly', 3000, 1, 'invented-empty-header-time')]
    results = []
    for release in CORPUS.RELEASES:
        directory = args.fixtures / release
        goldens = json.loads((directory / 'goldens.json').read_bytes())
        for scenario, clock, ordinal, name in controls:
            for phase in ('selected', 'reopened'):
                source = args.corpus / release / scenario / str(clock) / phase
                CORPUS.state(source, release, scenario, clock, phase, ordinal, directory, goldens)
                copy = args.work / release / name / phase
                shutil.copytree(source, copy)
                mutate(copy, directory, name)
                try:
                    CORPUS.state(copy, release, scenario, clock, phase, ordinal, directory, goldens)
                except ValueError as error:
                    CHECK.require(str(error) in ('exact selected file envelope',
                                                'selected logical entry/CRC/exact Apache bytes'),
                                  f'{name}: wrong rejecting invariant {error}')
                    reason = str(error)
                else:
                    raise ValueError(f'{name}: altered output accepted')
                results.append(dict(release=release, name=name, phase=phase, rejected_by=reason,
                                    repaired_Kafka_file_entry_manifest_seek_checksums=True,
                                    altered_files_sha256={p.name: CHECK.sha(CHECK.read(p))
                                                        for p in sorted(copy.iterdir())}))
    CHECK.require(before == {str(p): CHECK.sha(CHECK.read(p)) for p in files}, 'original corpus changed')
    receipt = dict(passed=True, negative_controls=len(results), positive_original_checks=len(results),
                   inputs_unchanged=True, results=results, checker_sha256=CHECK.sha(Path(__file__).read_bytes()),
                   scope='Finite semantic cleaner controls on isolated copies of three-release actual disk captures')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({k: receipt[k] for k in ('passed', 'negative_controls', 'positive_original_checks', 'inputs_unchanged')}))


if __name__ == '__main__':
    main()
