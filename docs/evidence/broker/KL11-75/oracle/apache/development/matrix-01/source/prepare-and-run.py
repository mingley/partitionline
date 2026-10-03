#!/usr/bin/env python3
"""Execute pinned official compaction components; retain actual fixtures/outcomes."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import struct
import subprocess
import tarfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[5]
TABLE = []
for value in range(256):
    for _ in range(8):
        value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    TABLE.append(value)


def crc(data):
    value = 0xffffffff
    for byte in data:
        value = TABLE[(value ^ byte) & 255] ^ (value >> 8)
    return value ^ 0xffffffff


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def pack_history(directory, target):
    paths = [p for p in sorted(directory.rglob('*')) if p.is_file()]
    require(len(paths) <= 192, 'component history file budget')
    raw, pins = io.BytesIO(), {}
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for path in paths:
            require(not path.is_symlink() and path.stat().st_size <= 2 * 1024 * 1024, 'component file budget')
            data = path.read_bytes()
            name = str(path.relative_to(directory))
            item = tarfile.TarInfo(name); item.size = len(data); item.mode = 0o644; item.mtime = 0
            archive.addfile(item, io.BytesIO(data)); pins[name] = hashlib.sha256(data).hexdigest()
    target.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {'archive': target.name, 'sha256': sha(target), 'files_sha256': pins}


def recheck(data):
    data[17:21] = struct.pack('>I', crc(data[21:]))
    return bytes(data)


def mutations(input_bytes):
    results = []
    def add(name, change, policy):
        data = bytearray(input_bytes); change(data); results.append((name, recheck(data), policy))
    corrupt = bytearray(input_bytes); corrupt[-1] ^= 1
    results.append(('hostile-protected-crc', bytes(corrupt), 'reject_corrupt_crc'))
    add('hostile-count-negative', lambda d: d.__setitem__(slice(57, 61), struct.pack('>i', -1)), 'reject_negative_count')
    add('hostile-count-huge', lambda d: d.__setitem__(slice(57, 61), struct.pack('>i', 2147483647)), 'reject_count_or_work_budget')
    add('hostile-count-zero-with-record-bytes', lambda d: d.__setitem__(slice(57, 61), bytes(4)), 'reject_zero_count_trailing_record_bytes')
    add('hostile-negative-last-delta', lambda d: d.__setitem__(slice(23, 27), struct.pack('>i', -1)), 'reject_negative_logical_extent')
    add('hostile-record-outside-extent', lambda d: d.__setitem__(slice(23, 27), struct.pack('>i', 1)), 'reject_record_outside_extent')
    add('hostile-offset-overflow', lambda d: d.__setitem__(slice(0, 8), struct.pack('>q', 9223372036854775807)), 'reject_checked_offset_overflow')
    add('hostile-record-length-varint', lambda d: d.__setitem__(slice(61, 66), bytes([128]) * 5), 'reject_record_varint')
    results.append(('hostile-trailing-byte', input_bytes + b'\0', 'reject_full_consumption'))
    return results


def contexts():
    return {
        'ordinary_profile': 'magic2/uncompressed/CreateTime/producer=-1; no transactional/control/producer-state cleaning claim',
        'delete_retention_ms': 1000,
        'null_key_seed': 'Existing delete-policy log receives null keys, then switches to compact config before actual Cleaner.doClean; this does not claim compact-topic Produce admission accepts null keys.',
        'scenarios': [
            {'name': 'mixed', 'input': 'cleaner-mixed-input', 'active': 'cleaner-mixed-active-protected',
             'logical_floor': 0, 'first_dirty_offset': 0, 'first_uncleanable_offset': 8, 'high_watermark': 10,
             'round_last_batch_last_offset': 7,
             'rounds': [{'now_ms': 2000, 'output': 'cleaner-first-horizon-sparse', 'retained_offsets': [2, 4, 5, 7]},
                        {'now_ms': 2999, 'output': 'cleaner-before-horizon-sparse', 'retained_offsets': [2, 4, 5, 7]},
                        {'now_ms': 3000, 'output': 'cleaner-equal-horizon-sparse', 'retained_offsets': [2, 5]},
                        {'now_ms': 3001, 'output': 'cleaner-after-horizon-sparse', 'retained_offsets': [2, 5]}]},
            {'name': 'allremoved', 'input': 'cleaner-allremoved-input', 'active': 'cleaner-allremoved-active-protected',
             'logical_floor': 0, 'first_dirty_offset': 0, 'first_uncleanable_offset': 3, 'high_watermark': 4,
             'round_last_batch_last_offset': 2,
             'rounds': [{'now_ms': 2000, 'output': 'cleaner-intermediate-empty-dropped', 'retained_offsets': [2]},
                        {'now_ms': 3000, 'output': 'cleaner-last-empty-61', 'retained_offsets': []}]},
            {'name': 'nullonly', 'first_dirty_offset': 0, 'first_uncleanable_offset': 2, 'high_watermark': 3,
             'round_last_batch_last_offset': 1, 'now_ms': 2000, 'output': 'cleaner-nullonly-empty-61', 'retained_offsets': []},
        ],
        'filter_only_cases': 'Caller supplies an explicit keep-offset predicate and RETAIN_EMPTY/DELETE_EMPTY; Apache filterTo performs actual serialization. These cases are not relabeled as genuine Cleaner policy execution.',
        'malformed_cases': 'Checksum-valid mutations except the named CRC case; actual official parser/iteration outcomes retained. Any permissive Apache outcome remains separate from stricter local ordinary-read safety policy.',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fixture-root', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True); args.output.mkdir(parents=True)
    report = {'schema_version': 1, 'passed': False, 'scope': 'Actual official Cleaner.doClean and MemoryRecords.filterTo/parser components; no Rust implementation imports or Kafka broker/live session claim.',
              'commands': [], 'releases': [], 'contexts': contexts(),
              'limits': ['JVM heap128MiB, key-map65536B, read/writebuffers16384B, selected prefix16384B, inspected records128/batches8, subprocess60s, history192files each2MiB.',
                         'Ordinary uncompressed/nontransactional producer=-1 only; child75 does not qualify parent11 transactions/control/producer-state semantics.',
                         'Real component file swaps/reopen are distinct from partitionline crash-safe publication/runtime proof.']}
    def save():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    def execute(command, label, negative=False):
        command = ['taskset', '-c', '0-2,4'] + command
        before = time.monotonic(); completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60, check=False)
        log = args.output / (label + '.log'); log.write_bytes(completed.stdout)
        accepted = completed.returncode != 0 if negative else completed.returncode == 0
        report['commands'].append({'command': command, 'exit_code': completed.returncode,
                                   'expected': 'named negative assertion' if negative else 'zero', 'accepted': accepted,
                                   'elapsed_seconds': round(time.monotonic() - before, 3),
                                   'log': log.name, 'log_sha256': sha(log)})
        save(); require(accepted, 'unexpected command result ' + label)
        return completed.stdout.decode()
    execute(['java', '-version'], 'java-version')
    pins = json.loads((HERE / 'pins.json').read_text())
    prior = json.loads((ROOT / 'docs/evidence/broker/KL11-10/oracle/apache/final-4ffd7557/components/validation.json').read_text())
    canonical, canonical_mutations = None, None
    for p in pins['releases']:
        version = p['release']
        for name, digest in p['retained_source_files_sha256'].items():
            require(sha(HERE / 'source' / version / name) == digest, 'retained exact source reference')
        jar_dir = Path('/workspace/work/retention-apache/final-4ffd7557/components/jars') / version
        previous = next(r for r in prior['releases'] if r['release'] == version)
        for name, artifact in previous['jars'].items():
            require(sha(jar_dir / name) == artifact['sha256'], 'official distribution jar identity')
        classpath = ':'.join(str(jar_dir / name) for name in sorted(previous['jars']))
        work = args.work / version; work.mkdir(); classes = work / 'classes'; classes.mkdir()
        sources = []
        for name in ['ApacheCompactionProbe.java', 'InspectCompacted.java']:
            source = (HERE / name).read_text()
            if version != '4.3.1':
                source = source.replace('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.')
            path = work / name; path.write_text(source); shutil.copyfile(path, args.output / (version + '-' + name)); sources.append(str(path))
        execute(['java', '-Xmx128m', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror',
                 '-cp', classpath, '-d', str(classes)] + sources, version + '-compile')
        java = ['java', '-ea', '-Xmx128m', '-cp', str(classes) + ':' + classpath]
        manifests = []
        for replay in [1, 2]:
            corpus = args.output / f'{version}-replay-{replay}'
            state = work / f'states-{replay}'
            execute(java + ['ApacheCompactionProbe', version, str(state), str(corpus)], f'{version}-replay-{replay}')
            manifest = json.loads((corpus / 'goldens.json').read_text())
            require(manifest['passed'] and len(manifest['cases']) == 21, 'actual positive component corpus')
            manifest['contexts'] = contexts()
            (corpus / 'goldens.json').write_text(json.dumps(manifest, indent=2) + '\n')
            signatures = {c['name']: c['sha256'] for c in manifest['cases']}
            require(all(sha(corpus / c['file']) == c['sha256'] for c in manifest['cases']), 'actual generated fixture identities')
            if canonical is None: canonical = signatures
            require(signatures == canonical, 'identical real component bytes across releases/replays')
            history = pack_history(state, args.output / f'{version}-replay-{replay}-history.tar.gz')
            manifests.append({'replay': replay, 'assertions': manifest['assertions'], 'manifest_sha256': sha(corpus / 'goldens.json'), 'history': history})
        canonical_corpus = args.output / f'{version}-replay-1'
        hostile = args.output / (version + '-hostile'); hostile.mkdir()
        hostile_cases = []
        for name, raw, policy in mutations((canonical_corpus / 'filter-first-horizon-sparse.bin').read_bytes()):
            path = hostile / (name + '.bin'); path.write_bytes(raw); outcome = hostile / (name + '.apache.json')
            execute(java + ['InspectCompacted', str(path), str(outcome)], version + '-' + name)
            observed = json.loads(outcome.read_text())
            hostile_cases.append({'name': name, 'file': path.name, 'bytes': len(raw), 'sha256': sha(path),
                                  'origin': 'Declared mutation of an independently Apache-filtered valid batch; actual official parser outcome retained.',
                                  'handler_policy': policy, 'apache_outcome': observed})
        if canonical_mutations is None: canonical_mutations = [c['apache_outcome'] for c in hostile_cases]
        require([c['apache_outcome'] for c in hostile_cases] == canonical_mutations, 'actual malformed parser outcomes identical across releases')
        (hostile / 'goldens.json').write_text(json.dumps({'schema_version': 1, 'release': version, 'cases': hostile_cases}, indent=2) + '\n')
        for control in ['--wrong-null', '--wrong-equality', '--wrong-empty']:
            label = version + control
            execute(java + ['ApacheCompactionProbe', version, str(work / ('negative-state' + control)),
                            str(args.output / ('negative-corpus-' + label)), control], label, negative=True)
        destination = args.fixture_root / version; destination.mkdir(parents=True)
        for case in json.loads((canonical_corpus / 'goldens.json').read_text())['cases']:
            shutil.copyfile(canonical_corpus / case['file'], destination / case['file'])
        for case in hostile_cases:
            shutil.copyfile(hostile / case['file'], destination / case['file'])
        merged = json.loads((canonical_corpus / 'goldens.json').read_text())
        merged['cases'].extend(hostile_cases)
        (destination / 'goldens.json').write_text(json.dumps(merged, indent=2) + '\n')
        report['releases'].append({'release': version, 'source_sha': p['source_sha'], 'jars': previous['jars'],
                                   'actual_replays': manifests, 'fixture_cases': len(merged['cases']),
                                   'malformed_parser_outcomes': len(hostile_cases),
                                   'fixture_manifest_sha256': sha(destination / 'goldens.json')})
        save()
    report['passed'] = True
    report['counts'] = {'releases': 3, 'independent_replays': 6,
                        'actual_component_assertions': sum(r['assertions'] for v in report['releases'] for r in v['actual_replays']),
                        'actual_component_cases_per_release': 21, 'actual_malformed_cases_per_release': 9,
                        'fixture_cases': 90, 'deliberate_assertion_failures_detected': 9,
                        'exact_component_binary_bytes_identical_across_releases_replays': True,
                        'actual_malformed_outcomes_identical_across_releases': True}
    report['source_files_sha256'] = {name: sha(HERE / name) for name in ['ApacheCompactionProbe.java', 'InspectCompacted.java', 'prepare-and-run.py', 'pins.json']}
    save()
    print(json.dumps({'passed': True, 'counts': report['counts']}))


if __name__ == '__main__':
    main()
