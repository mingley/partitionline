#!/usr/bin/env python3
"""Preserve a positive retention state and explicit checksum-valid guard probes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct
import sys

from legacy_oracle import crc, require

spec = importlib.util.spec_from_file_location('retention_history', Path(__file__).with_name('check-retention-history.py'))
checker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = checker
spec.loader.exec_module(checker)


def repair(data):
    data[-4:] = struct.pack('>I', crc(data[:-4]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--histories', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'fresh counterexample directory required')
    args.output.mkdir(parents=True)
    seed = args.histories / 'retention-error/3/ManifestRenamed/interrupted'
    fixture = args.fixture.read_bytes()
    positive = checker.state(seed, fixture, 'retention-error', 3, 'ManifestRenamed', 'interrupted')
    results = []
    controls = [
        ('floor-above-watermark', 'confirmed acknowledged floor/phase mismatch'),
        ('foreign-origin', 'floor/physical/origin/generation bounds'),
        ('victim-overlaps-survivor', 'victim prefix/physical binding'),
        ('impossible-victim-bytes', 'protected descriptor structural bounds'),
        ('wrong-victim-fingerprint', 'victim seeded bytes/timestamp/fingerprint'),
        ('changed-protected-seed', 'acknowledged ordinary seed payload bytes'),
        ('wrong-seek-position', 'seek physical position/prefix maximum'),
        ('incomplete-cleanup-reported-recovered', 'recovered cleanup not complete'),
    ]
    for name, expected in controls:
        case = args.output / name
        shutil.copytree(seed, case)
        data = bytearray((case / 'manifest').read_bytes())
        selected = struct.unpack_from('>I', data, 56)[0]
        victim = 64 + selected * 56
        stage = 'interrupted'
        if name == 'floor-above-watermark':
            struct.pack_into('>Q', data, 32, 6)
        elif name == 'foreign-origin':
            struct.pack_into('>Q', data, 16, 1)
        elif name == 'victim-overlaps-survivor':
            struct.pack_into('>Q', data, victim + 8, 4)
        elif name == 'impossible-victim-bytes':
            struct.pack_into('>Q', data, victim + 24, 24)
        elif name == 'wrong-victim-fingerprint':
            fingerprint = struct.unpack_from('>I', data, victim + 48)[0]
            struct.pack_into('>I', data, victim + 48, fingerprint ^ 1)
        elif name == 'changed-protected-seed':
            base, generation = struct.unpack_from('>Q', data, 64)[0], struct.unpack_from('>Q', data, 80)[0]
            path = case / f'{base:016x}-{generation:016x}.journal'
            journal = bytearray(path.read_bytes())
            size = struct.unpack_from('>I', journal, 32)[0]
            payload = bytearray(journal[56:56 + size])
            # The declared static fixture's actual record value byte is68.
            require(len(payload) == 74 and payload[68] == ord('v'), 'explicit BASIC mutation byte')
            payload[68] = ord('w')
            struct.pack_into('>I', payload, 17, crc(payload[21:]))
            journal[56:56 + size] = payload
            struct.pack_into('>I', journal, 48, crc(payload))
            struct.pack_into('>I', journal, 52, crc(journal[24:52]))
            path.write_bytes(journal)
            struct.pack_into('>I', data, 112, crc(journal))
        elif name == 'wrong-seek-position':
            base, generation = struct.unpack_from('>Q', data, 64)[0], struct.unpack_from('>Q', data, 80)[0]
            path = case / f'{base:016x}-{generation:016x}.seek'
            index = bytearray(path.read_bytes())
            position = struct.unpack_from('>Q', index, 76)[0]
            struct.pack_into('>Q', index, 76, position + 1)
            repair(index)
            path.write_bytes(index)
        elif name == 'incomplete-cleanup-reported-recovered':
            metadata = json.loads((case / 'case.json').read_text())
            metadata['stage'] = stage = 'recovered'
            (case / 'case.json').write_text(json.dumps(metadata, indent=2) + '\n')
        repair(data)
        (case / 'manifest').write_bytes(data)
        try:
            checker.state(case, fixture, 'retention-error', 3, 'ManifestRenamed', stage)
            raise AssertionError('counterexample accepted: ' + name)
        except ValueError as error:
            require(expected in str(error), 'wrong rejection for ' + name + ': ' + str(error))
            results.append({'name': name, 'expected_guard': expected, 'actual_rejection': str(error),
                            'manifest_crc_repaired': True,
                            'artifacts_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                                for p in sorted(case.iterdir()) if p.is_file()}})
    report = {'scope': 'Finite deliberately changed checker controls, separate from actual owner fault inputs',
              'passed': True, 'positive_control': positive, 'negative_controls': results}
    (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': True, 'negative_controls': len(results)}))


if __name__ == '__main__':
    main()
