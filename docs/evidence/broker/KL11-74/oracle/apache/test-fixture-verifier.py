#!/usr/bin/env python3
"""Meaningful re-pinned native corruption and fixture-provenance controls."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import struct

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('membership_fixture_checker', HERE / 'verify-fixtures.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def repin(directory, manifest, name):
    manifest['files_sha256'][name] = CHECK.sha((directory / name).read_bytes())


def native_mutation(directory, manifest, mode):
    trace_path = directory / 'traces/add-new-majority.json'
    trace = json.loads(trace_path.read_bytes())
    capture = next(row for row in trace['steps'] if row['event'] == 'control-batch')
    sidecar = next(record for record in capture['records'] if (directory / 'native' / record['key_file']).read_bytes() == bytes.fromhex('00000006'))
    batch_path = directory / 'native' / capture['file']
    raw = bytearray(batch_path.read_bytes())
    if mode == 'corrupt-native-crc':
        raw[-1] ^= 1
    elif mode == 'valid-crc-wrong-control-key':
        key_path = directory / 'native' / sidecar['key_file']
        original = key_path.read_bytes(); start = raw.find(original, 61)
        assert start >= 61
        value = bytes.fromhex('00000007'); raw[start:start + 4] = value; key_path.write_bytes(value)
        repin(directory, manifest, 'native/' + sidecar['key_file'])
        struct.pack_into('>I', raw, 17, CHECK.crc32c(raw[21:]))
    elif mode == 'valid-crc-duplicate-voter-id':
        value_path = directory / 'native' / sidecar['value_file']
        original = value_path.read_bytes(); value = bytearray(original)
        cursor = CHECK.Cursor(original); assert cursor.integer('h') == 0; assert cursor.array() == 3
        positions = []
        for _ in range(3):
            positions.append(cursor.pos); cursor.take(20)
            for _ in range(cursor.array(maximum=8)):
                cursor.text(64); cursor.text(256); cursor.take(2); cursor.tags()
            cursor.take(4); cursor.tags(); cursor.tags()
        value[positions[1]:positions[1] + 4] = value[positions[0]:positions[0] + 4]
        start = raw.find(original, 61); assert start >= 61
        raw[start:start + len(original)] = value; value_path.write_bytes(value)
        repin(directory, manifest, 'native/' + sidecar['value_file'])
        struct.pack_into('>I', raw, 17, CHECK.crc32c(raw[21:]))
    elif mode == 'valid-crc-wrong-native-epoch':
        struct.pack_into('>i', raw, 12, 6)
    elif mode == 'valid-crc-wrong-record-count':
        struct.pack_into('>i', raw, 57, 1)
        struct.pack_into('>I', raw, 17, CHECK.crc32c(raw[21:]))
    else:
        raise ValueError('Unknown mutation')
    batch_path.write_bytes(raw); repin(directory, manifest, 'native/' + capture['file'])
    return [batch_path]


def mutate(directory, kind):
    manifest_path = directory / 'manifest.json'; manifest = json.loads(manifest_path.read_bytes())
    changed = [manifest_path]
    if kind.startswith('valid-crc-') or kind == 'corrupt-native-crc':
        changed += native_mutation(directory, manifest, kind)
    elif kind == 'wrong-upstream-source':
        manifest['source_sha'] = '0' * 40
    elif kind == 'wrong-executable-probe':
        manifest['probe_sha256'] = '0' * 64
    elif kind == 'wrong-client-pin':
        manifest['client_jar_sha256'] = '0' * 64
    elif kind == 'wrong-offset-term-mapping':
        manifest['local_normalized_term'] = 5
    elif kind == 'missing-native-sidecar':
        name = next(name for name in manifest['files_sha256'] if name.endswith('.value.bin'))
        (directory / name).unlink()
    elif kind == 'duplicate-case-id':
        path = directory / 'cases.tsv'; lines = path.read_text().splitlines()
        fields = lines[2].split('\t'); fields[0] = lines[1].split('\t')[0]; lines[2] = '\t'.join(fields)
        path.write_text('\n'.join(lines) + '\n'); repin(directory, manifest, path.name); changed.append(path)
    elif kind == 'forged-active-voters':
        path = directory / 'traces/add-new-majority.json'; trace = json.loads(path.read_bytes())
        row = next(row for row in trace['steps'] if row['event'] == 'state'); row['voters'][0]['id'] = 999
        write_json(path, trace); repin(directory, manifest, str(path.relative_to(directory))); changed.append(path)
    elif kind == 'failed-assertion-labeled-positive':
        path = directory / 'traces/add-new-majority.json'; trace = json.loads(path.read_bytes())
        row = next(row for row in trace['steps'] if row['event'] == 'assertion'); row['passed'] = False
        write_json(path, trace); repin(directory, manifest, str(path.relative_to(directory))); changed.append(path)
    elif kind == 'oversized-trace':
        path = directory / 'traces/add-new-majority.json'; path.write_bytes(b' ' * (CHECK.MAX_FILE + 1))
        repin(directory, manifest, str(path.relative_to(directory))); changed.append(path)
    elif kind == 'unsafe-fixture-path':
        name = next(iter(manifest['files_sha256'])); manifest['files_sha256']['../outside'] = manifest['files_sha256'].pop(name)
    elif kind == 'wrong-assertion-count':
        manifest['assertions'] += 1
    else:
        raise ValueError('Unknown mutation')
    write_json(manifest_path, manifest)
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(); args.work.mkdir(parents=True, exist_ok=False); args.output.mkdir(parents=True, exist_ok=False)
    kinds = ['wrong-upstream-source', 'wrong-executable-probe', 'wrong-client-pin', 'wrong-offset-term-mapping',
        'missing-native-sidecar', 'duplicate-case-id', 'forged-active-voters', 'failed-assertion-labeled-positive',
        'oversized-trace', 'unsafe-fixture-path', 'wrong-assertion-count', 'corrupt-native-crc',
        'valid-crc-wrong-control-key', 'valid-crc-duplicate-voter-id', 'valid-crc-wrong-native-epoch', 'valid-crc-wrong-record-count']
    report = {'schema_version': 1, 'checker_sha256': CHECK.sha((HERE / 'verify-fixtures.py').read_bytes()),
        'test_source_sha256': CHECK.sha(Path(__file__).read_bytes()), 'passed': False, 'baselines': [], 'negative_controls': []}
    try:
        for version in ['4.1.2', '4.2.1', '4.3.1']:
            result = CHECK.verify_release(args.fixtures / version)
            report['baselines'].append({key: result[key] for key in ['release', 'manifest_sha256', 'cases', 'assertions', 'native_batches', 'native_records']})
            for kind in kinds:
                directory = args.work / version / kind
                shutil.copytree(args.fixtures / version, directory)
                changed = mutate(directory, kind)
                rejected = False; reason = None
                try:
                    CHECK.verify_release(directory)
                except ValueError as error:
                    rejected = True; reason = str(error)
                if not rejected:
                    raise AssertionError('Mutation unexpectedly accepted: ' + version + '/' + kind)
                capture = args.output / version / kind; capture.mkdir(parents=True)
                retained = {}
                for path in changed:
                    if path.stat().st_size <= CHECK.MAX_FILE:
                        target = capture / path.name; shutil.copyfile(path, target); retained[target.name] = CHECK.sha(target.read_bytes())
                report['negative_controls'].append({'release': version, 'mutation': kind, 'rejected': rejected, 'reason': reason, 'changed_files_sha256': retained})
                shutil.rmtree(directory)
        report['passed'] = True
    finally:
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'positive_release_baselines': len(report['baselines']), 'rejected_controls': len(report['negative_controls'])}))


if __name__ == '__main__':
    main()
