import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time

REPO = Path('/workspace/partitionline')
SOURCE = Path('/workspace/work/membership-runtime/dev-source')
RUNTIME = REPO / 'docs/evidence/broker/KL11-74/runtime'
DEV = RUNTIME / 'development'
MANIFEST = DEV / 'corrected-origin-source.json'
FROZEN = json.loads(MANIFEST.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_map(root):
    return {p: sha(root / p) for p in FROZEN['files']}


def write_new(path, value):
    with path.open('x') as out:
        out.write(json.dumps(value, indent=2) + '\n')


def file_map(root):
    result = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise RuntimeError(f'Symlink in proof tree: {path}')
        if path.is_file():
            result[str(path.relative_to(root))] = {
                'sha256': sha(path), 'bytes': path.stat().st_size,
                'mode': stat.S_IMODE(path.stat().st_mode),
            }
    return result


expected = {p: value['sha256'] for p, value in FROZEN['files'].items()}
assert source_map(SOURCE) == expected
assert source_map(REPO) == expected
env = os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup')
env['PATH'] = '/workspace/work/cargo/bin:' + env['PATH']
format_files = [p for p in FROZEN['files'] if not p.endswith('/mod.rs')]
for tool in ['stable', '1.85.0']:
    name = 'corrected-complete-' + ('stable' if tool == 'stable' else 'msrv') + '-fmt'
    log = DEV / (name + '.log')
    argv = ['taskset', '-c', '2,4', 'rustfmt', '+' + tool,
            '--edition', '2021', '--check', *format_files]
    before = source_map(SOURCE)
    start = time.monotonic()
    with log.open('xb') as out:
        result = subprocess.run(argv, cwd=SOURCE, env=env, stdout=out, stderr=subprocess.STDOUT)
    after = source_map(SOURCE)
    write_new(DEV / (name + '.json'), {
        'schema_version': 1, 'command': argv, 'working_directory': str(SOURCE),
        'source_base_sha': FROZEN['source_base_sha'],
        'source_manifest': str(MANIFEST.relative_to(REPO)),
        'source_before': before, 'source_after': after,
        'source_unchanged': before == after, 'exit_code': result.returncode,
        'elapsed_seconds': time.monotonic() - start,
        'environment': {k: env[k] for k in ['CARGO_HOME', 'RUSTUP_HOME']},
        'log': log.name, 'log_sha256': sha(log),
        'qualification': 'Development focused scope only; no final Git source qualification.',
    })
    assert result.returncode == 0 and before == after

commands = []
lanes = []
for lane in ['stable', 'msrv']:
    name = f'corrected-complete-{lane}-tests'
    run = DEV / name
    receipt_path = DEV / (name + '.json')
    receipt = json.loads(receipt_path.read_text())
    text = (DEV / (name + '.log')).read_text()
    counts = [tuple(map(int, row)) for row in re.findall(
        r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', text)]
    assert counts == [(26, 0, 0, 0, 0), (11, 0, 0, 0, 0)]
    assert receipt['exit_code'] == 0 and receipt['source_before'] == expected == receipt['source_after']
    binaries = []
    paths = re.findall(r'Running tests/(raft_(?:membership|protocol))\.rs \(([^)]+)\)', text)
    assert len(paths) == 2
    for test, original in paths:
        original = Path(original)
        data = original.read_bytes()
        output = DEV / f'{name}.{test}.binary.gz'
        encoded = gzip.compress(data, compresslevel=9, mtime=0)
        with output.open('xb') as out:
            out.write(encoded)
        assert gzip.decompress(output.read_bytes()) == data
        binaries.append({
            'test': test, 'executed_path': str(original),
            'retained': str(output.relative_to(REPO)),
            'raw_sha256': hashlib.sha256(data).hexdigest(),
            'gzip_sha256': sha(output), 'raw_bytes': len(data),
            'gzip_bytes': output.stat().st_size,
            'mode': stat.S_IMODE(original.stat().st_mode),
            'verified_decompression': True,
        })
    files_before = file_map(run)
    histories = []
    for voters, event_count, checkpoints in [(3, 132, 28), (5, 201, 42)]:
        trace = run / f'captures/history-{voters}/trace.json'
        value = json.loads(trace.read_text())
        assert len(value['events']) == event_count and len(value['checkpoints']) == checkpoints
        assert value['source_sha'] == 'development-d21-owned-overlay'
        assert all(a['now_ms'] <= b['now_ms'] for a, b in zip(value['events'], value['events'][1:]))
        for checkpoint in value['checkpoints']:
            for key in ['wal_path', 'election_path']:
                assert (trace.parent / checkpoint[key]).is_file()
        histories.append({
            'initial_voters': voters, 'events': event_count,
            'paired_actual_journal_checkpoints': checkpoints,
            'trace': str(trace.relative_to(REPO)), 'trace_sha256': sha(trace),
            'actual_child_exit': 88, 'clock_handoff': {'last_child': 16, 'first_parent': 17},
            'scope': 'Caller-driven typed multi-owner process/restart history; autonomous transport and modern Kafka wire remain pending.',
        })
    counterexamples = []
    for folder in ['equal-view-source-forgery', 'equal-view-data-source-forgery']:
        mutation = run / 'captures' / folder / 'mutation.json'
        value = json.loads(mutation.read_text())
        assert value['result']['accepted'] is False
        counterexamples.append({
            'mutation': str(mutation.relative_to(REPO)), 'sha256': sha(mutation),
            'actual_configuration_change': value['actual_configuration_change'],
            'equal_view_decoy': value['same-result_commit_decoy'],
            'genuine_later_change': value['genuine_later_configuration_change'],
            'altered_election_offsets': value['altered_election_offsets'],
            'crc_generation': value['crc_generation'], 'reopen_accepted': False,
        })
    fixed = file_map(run / 'fixed-controller')
    direct = [p for p in fixed if not p.startswith('transport/')]
    tcp = [p for p in fixed if p.startswith('transport/')]
    assert len(direct) == 186 and len(tcp) == 36
    files_after = file_map(run)
    assert files_before == files_after
    seal = {
        'schema_version': 1, 'qualification': 'Development capture identity, not independent semantic qualification.',
        'source_base_sha': FROZEN['source_base_sha'], 'source_kind': 'Git base plus exact declared owned overlay',
        'source_manifest': str(MANIFEST.relative_to(REPO)), 'source_manifest_sha256': sha(MANIFEST),
        'command_receipt': str(receipt_path.relative_to(REPO)), 'command_receipt_sha256': sha(receipt_path),
        'capture_root': str(run.relative_to(REPO)), 'capture_environment': receipt['environment'],
        'tests': {'membership': 26, 'raft_protocol': 11, 'failed': 0, 'ignored': 0, 'filtered': 0},
        'histories': histories, 'source_ordinal_counterexamples': counterexamples,
        'fixed_v0_controller': {'golden_cases': 201, 'retained_direct_payloads': 186,
                                'actual_tcp_response_payloads': 18, 'actual_tcp_response_frames': 18},
        'executed_binaries': binaries, 'files': files_after,
        'capture_files_unchanged_during_seal': True,
    }
    seal_path = DEV / f'{name}.capture-seal.json'
    write_new(seal_path, seal)
    lanes.append({'toolchain': lane, 'seal': str(seal_path.relative_to(REPO)),
                  'seal_sha256': sha(seal_path), 'histories': histories, 'tests': seal['tests'],
                  'file_count': len(files_after), 'capture_bytes': sum(v['bytes'] for v in files_after.values())})
    for mode in ['tests', 'clippy', 'fmt']:
        receipt_path = DEV / f'corrected-complete-{lane}-{mode}.json'
        receipt = json.loads(receipt_path.read_text())
        assert receipt['exit_code'] == 0 and receipt['source_unchanged']
        commands.append({'command': receipt['command'], 'receipt': str(receipt_path.relative_to(REPO)),
                         'receipt_sha256': sha(receipt_path), 'exit_code': 0,
                         'log_sha256': receipt['log_sha256']})

assert source_map(SOURCE) == expected == source_map(REPO)
stage_source = [p for p in FROZEN['files'] if not p.endswith('/raft_election.rs')]
freeze = {
    'schema_version': 1, 'task': 'KL11-74',
    'disposition': 'Corrected coherent developmental source; ready for coordinator source checkpoint, not final qualification.',
    'source_base_sha': FROZEN['source_base_sha'],
    'source_kind': 'Exact Git base plus recorded owned source/test overlay and coordinator-owned raft/mod.rs only.',
    'source_manifest': str(MANIFEST.relative_to(REPO)), 'source_manifest_sha256': sha(MANIFEST),
    'frozen_for_review': FROZEN['files'], 'commands': commands, 'capture_lanes': lanes,
    'results': {'focused_tests_passed': 74, 'stable_passed': 37, 'rust_1_85_passed': 37,
                'ignored': 0, 'filtered': 0, 'strict_clippy_cells_passed': 2, 'fmt_cells_passed': 2,
                'actual_history_events': 666, 'paired_actual_journal_checkpoints': 140,
                'checksum_valid_origin_mutations_rejected': 4},
    'source_fix': 'Historical configuration origins require a real canonical configuration change from predecessor/genesis, preserving bounded fetch and exact identity checks.',
    'capture_helper_limit': 'All four text reads use File::take(32 MiB + 1) and reject results exceeding 32 MiB.',
    'failing_first_control': {
        'receipt': 'docs/evidence/broker/KL11-74/runtime/development/ordinal-counterexample-validation.json',
        'sha256': sha(DEV / 'ordinal-counterexample-validation.json'),
        'old_replication_sha256': '18808a5137cbbc53536163bd078d076503dd81749f749fed0d5f56e14e3b6e5d',
        'outcome': 'Old implementation accepted the checksum-valid historical source-ordinal forgery with otherwise matching final context; intended rejection assertion failed.',
    },
    'preserved_attempts': [
        'integrated-first-source.json', 'integrated-development-validation.json',
        'integrated-third-source.json', 'integrated-fourth-source.json',
        'integrated-fourth-stable-history/capture-manifest.json', 'integrated-fourth-stable-clippy.json',
        'ordinal-first-source.json', 'ordinal-first-stable.json',
        'ordinal-scaffold-fixed-source.json', 'ordinal-scaffold-fixed-stable.json',
        'ordinal-scaffold-fixed-stable-retry.json', 'corrected-origin-stable-tests.json',
    ],
    'skipped_receipt_scope': 'corrected-origin-stable-tests passed 35 with two loopback transport cases filtered under the earlier ambiguous restriction. It remains unchanged and is superseded for complete focused counts by these two 37-case runs.',
    'staging_paths': stage_source + ['docs/evidence/broker/KL11-74/runtime/'],
    'coordinator_owned_staging_path': 'partitionline-broker/src/raft/mod.rs',
    'unchanged_read_only_test': 'partitionline-broker/tests/raft_election.rs',
    'limits': [
        'No final immutable pushed-source four-feature/toolchain broker matrix yet.',
        'Independent causal/raw journal validation of the corrected captures is coordinator-owned and pending this handoff.',
        'Typed caller-driven runtime requires a known leader full directory/configuration; trusted discovery and autonomous transport remain KL11-76/70.',
        'Modern KRaft wire, new default advertisement, and early semantic configuration ACK are not implemented in this profile.',
    ],
}
write_new(RUNTIME / 'source-freeze.json', freeze)
print(json.dumps({'source_freeze': str((RUNTIME / 'source-freeze.json').relative_to(REPO)),
                  'sha256': sha(RUNTIME / 'source-freeze.json'), 'tests': freeze['results'],
                  'source_unchanged': True, 'staging_paths': freeze['staging_paths']}, indent=2))
