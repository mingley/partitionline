#!/usr/bin/env python3
"""Adjudicate semantic corruption of isolated copies of real disk captures."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('history', HERE / 'check-compaction-history.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def checksum(data):
    data[-4:] = struct.pack('>I', CHECK.crc(data[:-4]))
    return data


def field(data, offset, fmt, value):
    struct.pack_into(fmt, data, offset, value)
    return checksum(data)


def hashes(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(path.rglob('*')) if p.is_file()}


def manifest_field(path, offset, fmt, value):
    file = path / 'manifest'
    file.write_bytes(field(bytearray(file.read_bytes()), offset, fmt, value))


def sidecar_field(path, offset, fmt, value):
    file = path / '0000000000000002-0000000000000004.seek'
    file.write_bytes(field(bytearray(file.read_bytes()), offset, fmt, value))


def selected_payload(path, mode):
    file = path / '0000000000000002-0000000000000004.journal'
    data = bytearray(file.read_bytes())
    if mode == 'value':
        # BASIC has key k, value v and header h=x. Change the value only;
        # recompute Kafka and enclosing journal checksums and all bindings.
        assert data[56 + 68] == ord('v')
        data[56 + 68] = ord('w')
        data[56 + 17:56 + 21] = struct.pack('>I', CHECK.crc(data[56 + 21:]))
    elif mode == 'missing':
        del data[56:]
        struct.pack_into('>I', data, 32, 0)
    else:
        raise ValueError(mode)
    struct.pack_into('>I', data, 48, CHECK.crc(data[56:]))
    struct.pack_into('>I', data, 52, CHECK.crc(data[24:52]))
    file.write_bytes(data)
    manifest = bytearray((path / 'manifest').read_bytes())
    descriptor = 72 + 2 * 56
    struct.pack_into('>Q', manifest, descriptor + 24, len(data))
    struct.pack_into('>I', manifest, descriptor + 48, CHECK.crc(data))
    if mode == 'missing':
        struct.pack_into('>q', manifest, descriptor + 40, -(1 << 63))
    checksum(manifest)
    (path / 'manifest').write_bytes(manifest)
    seek = bytearray((path / '0000000000000002-0000000000000004.seek').read_bytes())
    seek[8:64] = manifest[descriptor:descriptor + 56]
    (path / '0000000000000002-0000000000000004.seek').write_bytes(checksum(seek))


def active_value(path):
    file = path / '0000000000000003-0000000000000003.journal'
    data = bytearray(file.read_bytes())
    assert data[56 + 68] == ord('v')
    data[56 + 68] = ord('w')
    data[56 + 17:56 + 21] = struct.pack('>I', CHECK.crc(data[56 + 21:]))
    struct.pack_into('>I', data, 48, CHECK.crc(data[56:]))
    struct.pack_into('>I', data, 52, CHECK.crc(data[24:52]))
    file.write_bytes(data)


def duplicate_obsolete(path):
    file = path / 'manifest'
    data = bytearray(file.read_bytes())
    assert struct.unpack_from('>I', data, 64)[0] == 3
    data[72 + 4 * 56:72 + 5 * 56] = data[72 + 3 * 56:72 + 4 * 56]
    file.write_bytes(checksum(data))


def entry_span(path):
    file = path / '0000000000000002-0000000000000004.journal'
    data = bytearray(file.read_bytes())
    struct.pack_into('>I', data, 44, 2)
    struct.pack_into('>I', data, 52, CHECK.crc(data[24:52]))
    file.write_bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--histories', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    CHECK.require(not args.work.exists(), 'fresh isolated control directory')
    before = hashes(args.histories)
    fixture = CHECK.read(args.fixture)
    controls = [
        ('wrong-floor', lambda p: manifest_field(p, 32, '>Q', 1), 'seeded origin/floor/active authority'),
        ('wrong-active-directory', lambda p: manifest_field(p, 48, '>Q', 2), 'seeded origin/floor/active authority'),
        ('wrong-revision', lambda p: manifest_field(p, 8, '>Q', 5), 'obsolete range/count/older-generation/uniqueness binding'),
        ('legacy-prefix-victim', lambda p: manifest_field(p, 60, '>I', 1), 'exact selected/obsolete denominator'),
        ('reserved-field', lambda p: manifest_field(p, 68, '>I', 1), 'V3 reserved field'),
        ('selected-kind', lambda p: manifest_field(p, 72 + 2 * 56 + 52, '>I', 0), 'descriptor exact selected bytes/maximum/count'),
        ('selected-time', lambda p: manifest_field(p, 72 + 2 * 56 + 40, '>q', 1001), 'descriptor exact selected bytes/maximum/count'),
        ('selected-fingerprint', lambda p: manifest_field(p, 72 + 2 * 56 + 48, '>I', 0), 'descriptor exact selected bytes/maximum/count'),
        ('obsolete-new-generation', lambda p: manifest_field(p, 72 + 3 * 56 + 16, '>Q', 4), 'obsolete range/count/older-generation/uniqueness binding'),
        ('obsolete-wrong-range', lambda p: manifest_field(p, 72 + 3 * 56 + 8, '>Q', 2), 'obsolete range/count/older-generation/uniqueness binding'),
        ('obsolete-duplicate', duplicate_obsolete, 'obsolete range/count/older-generation/uniqueness binding'),
        ('seek-wrong-physical-position', lambda p: sidecar_field(p, 76, '>Q', 25), 'sidecar physical/prefix maximum'),
        ('seek-wrong-prefix-time', lambda p: sidecar_field(p, 84, '>q', 1000), 'sidecar physical/prefix maximum'),
        ('invented-retained-value', lambda p: selected_payload(p, 'value'), 'selected canonical sparse/dense payload and logical extent'),
        ('missing-retained-value', lambda p: selected_payload(p, 'missing'), 'selected canonical sparse/dense payload and logical extent'),
        ('protected-active-value', active_value, 'protected active record unchanged'),
        ('wrong-logical-entry-span', entry_span, 'selected canonical sparse/dense payload and logical extent'),
    ]
    results = []
    for kind in ('io-error', 'process-exit'):
        source = args.histories / kind / 'ManifestRenamed' / 'interrupted'
        CHECK.state(source, fixture, kind, 'ManifestRenamed', 'interrupted')
        for name, mutate, expected in controls:
            copy = args.work / kind / name
            shutil.copytree(source, copy)
            mutate(copy)
            # All modified manifest/seek protection checksums remain valid;
            # expected rejection must name the semantic invariant above.
            CHECK.protected(CHECK.read(copy / 'manifest'), b'PLSEGM03', 76)
            for seek in copy.glob('*.seek'):
                data = CHECK.read(seek)
                CHECK.protected(data, data[:8], 72)
            try:
                CHECK.state(copy, fixture, kind, 'ManifestRenamed', 'interrupted')
            except ValueError as error:
                CHECK.require(str(error) == expected, f'{name}: unexpected rejection {error}')
            else:
                raise ValueError(f'{name}: semantic corruption accepted')
            results.append(dict(kind=kind, name=name, rejected_by=expected,
                                modified_capture_sha256=hashes(copy)))
        CHECK.state(source, fixture, kind, 'ManifestRenamed', 'interrupted')
    CHECK.require(before == hashes(args.histories), 'original histories changed')
    receipt = dict(passed=True, negative_controls=len(results), positive_original_checks=4,
                   originals_unchanged=True, results=results,
                   checker_sha256=hashlib.sha256((HERE / 'check-compaction-history.py').read_bytes()).hexdigest(),
                   control_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   scope='Finite seeded local IO/process captures, semantic controls on isolated copies')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({k: receipt[k] for k in ('passed', 'negative_controls', 'positive_original_checks', 'originals_unchanged')}))


if __name__ == '__main__':
    main()
