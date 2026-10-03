#!/usr/bin/env python3
"""Alter genuine captured files and require the independent checker to reject."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import struct

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('storage_checker', ROOT / 'check-storage-history.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def seal(data):
    data[-4:] = checker.crc(data[:-4]).to_bytes(4, 'big')


def selected(directory):
    manifest = bytearray(checker.bounded(directory / 'manifest'))
    checker.require(int.from_bytes(manifest[40:44], 'big') > 0, 'sealed counterexample baseline required')
    desc = checker.descriptor(manifest[44:100])
    name = f"{desc['base']:016x}-{desc['generation']:016x}"
    return manifest, directory / (name + '.journal'), directory / (name + '.seek')


def mutation(directory, case):
    manifest, journal, index = selected(directory)
    bundle = bytearray(checker.bounded(index))
    if case == 'wrong-checkpoint-position':
        value = int.from_bytes(bundle[76:84], 'big')
        bundle[76:84] = (value + 1).to_bytes(8, 'big')
    elif case == 'wrong-prefix-maximum':
        bundle[84:92] = ((1 << 63) - 1).to_bytes(8, 'big', signed=True)
    elif case == 'wrong-selected-fingerprint':
        fingerprint = int.from_bytes(manifest[92:96], 'big') ^ 1
        manifest[92:96] = fingerprint.to_bytes(4, 'big')
        bundle[8:64] = manifest[44:100]
    elif case == 'checksum-valid-wrong-seed':
        data = bytearray(checker.bounded(journal))
        length = int.from_bytes(data[32:36], 'big')
        payload = bytearray(data[56:56 + length])
        checker.require(payload[68] == ord('v'), 'actual BASIC value byte for semantic mutation')
        payload[68] = ord('w')
        payload[17:21] = checker.crc(payload[21:]).to_bytes(4, 'big')
        data[56:56 + length] = payload
        data[48:52] = checker.crc(payload).to_bytes(4, 'big')
        data[52:56] = checker.crc(data[24:52]).to_bytes(4, 'big')
        journal.write_bytes(data)
        manifest[92:96] = checker.crc(data).to_bytes(4, 'big')
        bundle[8:64] = manifest[44:100]
    elif case == 'future-selected-generation':
        revision = int.from_bytes(manifest[8:16], 'big')
        manifest[60:68] = (revision + 1).to_bytes(8, 'big')
        bundle[8:64] = manifest[44:100]
    elif case == 'partial-selected-payload':
        data = checker.bounded(journal)
        journal.write_bytes(data[:-1])
    else:
        raise ValueError('unknown controlled mutation')
    seal(manifest)
    seal(bundle)
    (directory / 'manifest').write_bytes(manifest)
    index.write_bytes(bundle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    fixture = checker.bounded(args.fixture)
    baseline = checker.state(args.baseline, fixture, 'roll-error', 'interrupted')
    expected = {
        'wrong-checkpoint-position': 'seek physical position/prefix maximum',
        'wrong-prefix-maximum': 'seek physical position/prefix maximum',
        'wrong-selected-fingerprint': 'selected descriptor fingerprint',
        'checksum-valid-wrong-seed': 'acknowledged ordinary seed payload bytes',
        'future-selected-generation': 'selected contiguous generations',
        'partial-selected-payload': 'entry payload CRC',
    }
    results = []
    for name, reason in expected.items():
        directory = args.output / name
        shutil.copytree(args.baseline, directory)
        mutation(directory, name)
        try:
            checker.state(directory, fixture, 'roll-error', 'interrupted')
        except ValueError as error:
            checker.require(str(error) == reason, 'counterexample rejected at unintended check: ' + str(error))
            results.append({'mutation': name, 'rejected': True, 'reason': reason,
                            'classification': ('structural incomplete payload' if name == 'partial-selected-payload'
                                               else 'checksum-valid selected metadata/payload semantic mutation'),
                            'files_sha256': {p.name: checker.sha(checker.bounded(p)) for p in sorted(directory.iterdir())}})
        else:
            raise ValueError('unsafe counterexample accepted: ' + name)
    report = {'baseline_manifest_sha256': baseline['manifest_sha256'],
              'checker_sha256': checker.sha((ROOT / 'check-storage-history.py').read_bytes()),
              'generator_sha256': checker.sha(Path(__file__).read_bytes()),
              'baseline_accepted': True, 'expected_rejections': len(results), 'results': results,
              'scope': 'Altered genuine checkpoint files, not a compiled Rust mutant or exhaustive fault schedule.'}
    (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'baseline_accepted': True, 'expected_rejections': len(results)}))


if __name__ == '__main__':
    main()
