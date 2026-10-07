#!/usr/bin/env python3
"""Hosted exact-source focused Rust proof; no Docker or external SDK qualification."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tarfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--toolchain', choices=['stable'], required=True)
parser.add_argument('--features', choices=['default', 'all-features'], required=True)
args = parser.parse_args()
os.umask(0o077)
ROOT = Path.cwd().resolve()
CELL = ROOT / 'target/client-capabilities' / args.toolchain / args.features
EVIDENCE = CELL / 'evidence'
REVERSE = EVIDENCE / 'reverse'
BUILD = ROOT / 'target/client-capabilities-build' / args.toolchain / args.features
assert not CELL.exists(), 'never overwrite a previous evidence cell'
assert not BUILD.exists(), 'fresh dedicated compilation target required'
EVIDENCE.mkdir(parents=True)
LEDGER = []
RESULTS = []
BASELINE = None
EXPORTS = None
FAILURE = None
CURRENT_STAGE = 'provenance'
ACTIVE = None
SOURCE_SHA = None
TOOLCHAIN_VERIFIED = False
FLOOR = 350 * 1024 * 1024
SUITES = [
    ('write_txn_markers_v2', ['--test', 'write_txn_markers_v2'], 12),
    ('share_offsets_v1', ['--test', 'share_offsets_v1'], 10),
    ('partitioner', ['--lib', 'partitioner::'], 14),
    ('sticky_partitioner', ['--test', 'sticky_partitioner'], 29),
    ('streams_protocol', ['--test', 'streams_protocol'], 24),
]
FOUR = {
    'public_abort_negotiates_v2_and_default_transaction_version',
    'omitted_duplicate_or_unrelated_abort_results_cannot_succeed',
    'broker_and_replica_unavailability_refresh_the_selected_partition_leader',
    'typed_and_existing_operations_negotiate_and_consume_lag',
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def save(name, data):
    path = EVIDENCE / name
    assert not path.exists(), ('evidence overwrite', name)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(data, bytes):
        data = (json.dumps(data, indent=2) + '\n').encode()
    path.write_bytes(data)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    return {'path': path.relative_to(EVIDENCE).as_posix(), 'sha256': sha(data),
            'bytes': len(data), 'full_mode': 0o600}


def group_members(pgid):
    result = []
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            tail = path.read_text().rsplit(')', 1)[1].split()
            if int(tail[2]) == pgid and tail[0] != 'Z':
                result.append(int(path.parent.name))
        except FileNotFoundError:
            pass
    return result


def join_group(child, force):
    if force or child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    until = time.monotonic() + 3
    while group_members(child.pid) and time.monotonic() < until:
        time.sleep(0.05)
    members = group_members(child.pid)
    if members:
        os.killpg(child.pid, signal.SIGKILL)
    child.wait(timeout=5)
    until = time.monotonic() + 3
    while group_members(child.pid) and time.monotonic() < until:
        time.sleep(0.05)
    assert not group_members(child.pid), 'owned command group did not close'


def interrupted(signum, frame):
    raise RuntimeError('controlled workflow interruption: ' + str(signum))


signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)


def command(name, argv, env=None, timeout=600):
    global ACTIVE
    out = EVIDENCE / (name + '.stdout')
    err = EVIDENCE / (name + '.stderr')
    start = time.monotonic()
    trigger = None
    row = {'name': name, 'argv': argv, 'cwd': str(ROOT),
           'environment_additions': env or {}, 'timeout_seconds': timeout}
    child_env = dict(os.environ)
    child_env.update(env or {})
    with out.open('xb') as stdout, err.open('xb') as stderr:
        child = subprocess.Popen(argv, cwd=ROOT, env=child_env,
                                 stdout=stdout, stderr=stderr, start_new_session=True)
        ACTIVE = child
        row.update({'pid': child.pid, 'pgid': child.pid})
        try:
            while child.poll() is None:
                fs = os.statvfs(ROOT)
                if fs.f_bavail * fs.f_frsize < FLOOR:
                    trigger = 'disk_floor'
                if out.stat().st_size + err.stat().st_size > 32 * 1024 * 1024:
                    trigger = 'command_log_bound'
                if time.monotonic() - start > timeout:
                    trigger = 'command_deadline'
                if trigger:
                    break
                time.sleep(0.2)
        except BaseException as failure:
            trigger = 'controlled_interruption_or_observer_error'
            row['observer_failure'] = {'type': type(failure).__name__, 'detail': str(failure)}
        finally:
            join_group(child, trigger is not None)
            ACTIVE = None
    if out.stat().st_size + err.stat().st_size > 32 * 1024 * 1024:
        trigger = 'command_log_bound_final'
    row.update({'exit_code': child.returncode, 'trigger': trigger,
                'duration_seconds': time.monotonic() - start, 'joined': True,
                'stdout': {'path': out.name, 'sha256': sha(out.read_bytes()), 'bytes': out.stat().st_size},
                'stderr': {'path': err.name, 'sha256': sha(err.read_bytes()), 'bytes': err.stat().st_size}})
    LEDGER.append(row)
    assert trigger is None, (name, trigger)
    return row, out.read_bytes(), err.read_bytes()


def checked(name, argv, timeout=60):
    row, out, err = command(name, argv, timeout=timeout)
    assert row['exit_code'] == 0, (name, row['exit_code'])
    return out


def source_snapshot(tree):
    expected = {}
    for item in tree.split(b'\0'):
        if item:
            prefix, name = item.split(b'\t', 1)
            mode, kind, blob = prefix.decode().split()
            assert kind == 'blob' and mode in ('100644', '100755', '120000')
            expected[os.fsdecode(name)] = (mode, blob)
    actual = set()
    for directory, dirs, names in os.walk(ROOT, followlinks=False):
        rel = Path(directory).relative_to(ROOT)
        if str(rel) == '.':
            dirs[:] = [d for d in dirs if d not in ('.git', 'target')]
        for d in list(dirs):
            p = Path(directory) / d
            if p.is_symlink():
                names.append(d)
                dirs.remove(d)
        actual.update((rel / n).as_posix() for n in names)
    assert actual == set(expected), 'checkout contains missing or extra non-target paths'
    result = {}
    for name, (git_mode, blob) in sorted(expected.items()):
        p = ROOT / name
        st = p.lstat()
        if git_mode == '120000':
            assert stat.S_ISLNK(st.st_mode)
            data = os.fsencode(os.readlink(p))
        else:
            assert stat.S_ISREG(st.st_mode)
            assert bool(st.st_mode & 0o111) == (git_mode == '100755')
            data = p.read_bytes()
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == blob
        result[name] = {'git_mode': git_mode, 'git_blob_sha1': blob, 'sha256': sha(data),
                        'bytes': len(data), 'full_mode': stat.S_IMODE(st.st_mode)}
    return result


def list_names(data, prefix):
    names = re.findall(r'^([A-Za-z_][A-Za-z_0-9:]*): test$', data.decode(), re.M)
    assert len(names) == len(set(names)), 'duplicate discovered test'
    if prefix == 'partitioner':
        assert all(n.startswith('partitioner::') for n in names)
    return names


def execution(row, data, errors, names):
    text = data.decode()
    summaries = re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text, re.M)
    executed = re.findall(r'^test ([A-Za-z_][A-Za-z_0-9:]*) \.\.\. (ok|FAILED|ignored)$', text, re.M)
    assert len(executed) == len(names) and {n for n, _ in executed} == set(names)
    passed = sum(s == 'ok' for _, s in executed)
    failed = sum(s == 'FAILED' for _, s in executed)
    ignored = sum(s == 'ignored' for _, s in executed)
    assert summaries == [('ok' if failed == 0 else 'FAILED', str(passed), str(failed), str(ignored))]
    assert 'could not compile' not in errors.decode() and 'error[E' not in errors.decode()
    return {'command_name': row['name'], 'actual_names': [n for n, _ in executed],
            'actual_results': executed, 'actual_passed': passed, 'actual_failed': failed,
            'actual_ignored': ignored, 'passed': row['exit_code'] == 0 and failed == ignored == 0}


def export_inventory():
    expected = set()
    rows = []
    for release in ['4.1.2', '4.2.1', '4.3.1']:
        directory = REVERSE / release
        for index, columns, count in [('rust.tsv', 4, 26), ('rust-headers.tsv', 6, 12)]:
            fixture = ROOT / 'tests/fixtures/streams' / release
            source_table = (fixture / ('cases.tsv' if index == 'rust.tsv' else 'headers.tsv')).read_text().splitlines()
            source_rows = {}
            for line in source_table[1:]:
                cells = line.split('\t')
                if index == 'rust.tsv' and cells[6] not in ('accept', 'accept-normalize-boolean', 'accept-normalize-null-marker'):
                    continue
                assert cells[0] not in source_rows
                source_rows[cells[0]] = cells
            assert len(source_rows) == count
            path = directory / index
            table = path.read_text().splitlines()
            assert len(table) == count + 1
            expected.add(path.relative_to(REVERSE).as_posix())
            observed_names = set()
            for text in table[1:]:
                cells = text.split('\t')
                assert len(cells) == columns
                name = cells[0]
                assert re.fullmatch('[a-z0-9-]+', name)
                assert name not in observed_names and name in source_rows
                observed_names.add(name)
                source = source_rows[name]
                if index == 'rust.tsv':
                    assert cells[1] == source[2]
                else:
                    assert cells[1:4] == [source[1], source[2], source[3]]
                original, rust = cells[-2:]
                assert original == 'original-' + name + '.bin' and rust == 'rust-' + name + '.bin'
                for file in (original, rust):
                    expected.add(release + '/' + file)
                assert (directory / original).read_bytes() == (ROOT / 'tests/fixtures/streams' / release / (name + '.bin')).read_bytes()
            assert observed_names == set(source_rows)
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    actual = {p.relative_to(REVERSE).as_posix() for p in REVERSE.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual == expected and len(actual) == 234
    actual_dirs = {'reverse'} | {'reverse/' + p.relative_to(REVERSE).as_posix() for p in REVERSE.rglob('*') if p.is_dir()}
    assert actual_dirs == {'reverse', 'reverse/4.1.2', 'reverse/4.2.1', 'reverse/4.3.1'}
    assert {p.relative_to(REVERSE).as_posix() for p in REVERSE.iterdir()} == {'4.1.2', '4.2.1', '4.3.1'}
    assert stat.S_IMODE(REVERSE.stat().st_mode) == 0o700
    total = 0
    for name in sorted(actual):
        p = REVERSE / name
        st = p.lstat()
        assert stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o600
        data = p.read_bytes()
        total += len(data)
        rows.append({'path': 'reverse/' + name, 'bytes': len(data), 'sha256': sha(data), 'full_mode': 0o600})
    assert total <= 8 * 1024 * 1024
    return {'passed': True, 'file_count': 234, 'rust_payload_count': 114, 'original_payload_count': 114,
            'index_count': 6, 'bytes': total, 'maximum_bytes': 8 * 1024 * 1024,
            'directories': [{'path': 'reverse' + ('/' + r if r else ''), 'full_mode': 0o700}
                            for r in ['', '4.1.2', '4.2.1', '4.3.1']], 'files': rows}


try:
    assert not (os.getenv('RUSTFLAGS') or os.getenv('CARGO_ENCODED_RUSTFLAGS'))
    assert not os.getenv('PARTITIONLINE_CAPABILITY_PEER_DIR'), 'SDK probe opt-in must be unset'
    SOURCE_SHA = checked('source-commit', ['git', 'rev-parse', 'HEAD']).decode().strip()
    assert SOURCE_SHA == os.environ['GITHUB_SHA']
    tree = checked('source-tree', ['git', 'ls-tree', '-r', '-z', SOURCE_SHA])
    BASELINE = source_snapshot(tree)
    raw = (json.dumps(BASELINE, sort_keys=True, separators=(',', ':')) + '\n').encode()
    compressed = gzip.compress(raw, mtime=0)
    assert gzip.decompress(compressed) == raw
    source_manifest = save('source-inventory.json.gz', compressed)
    source_manifest.update({'uncompressed_sha256': sha(raw), 'uncompressed_bytes': len(raw), 'file_count': len(BASELINE)})
    lock = save('Cargo.lock', (ROOT / 'Cargo.lock').read_bytes())
    rustc = checked('rustc-version', ['rustc', '-Vv']).decode()
    cargo = checked('cargo-version', ['cargo', '-V']).decode()
    checked('openssl-version', ['openssl', 'version'])
    assert re.search(r'^release: [0-9]+\.[0-9]+\.[0-9]+$', rustc, re.M)
    assert os.environ['RUSTUP_TOOLCHAIN'] == args.toolchain
    TOOLCHAIN_VERIFIED = True
    additions = {'STREAMS_REVERSE_OUT': str(REVERSE), 'CARGO_TARGET_DIR': str(BUILD)}
    flags = [] if args.features == 'default' else ['--all-features']
    for label, selector, count in SUITES:
        CURRENT_STAGE = label + '-discovery'
        argv = ['cargo', 'test', '--locked'] + selector + flags
        discovery_row, discovery, discovery_errors = command(label + '-discovery', argv + ['--', '--list'], additions)
        assert source_snapshot(tree) == BASELINE
        if discovery_row['exit_code'] != 0:
            RESULTS.append({'suite': label, 'expected_count': count, 'discovered_names': [],
                            'actual_names': [], 'actual_results': [], 'actual_passed': 0,
                            'actual_failed': 0, 'actual_ignored': 0, 'passed': False,
                            'command_name': discovery_row['name'], 'setup_failure': 'discovery_or_compile_failed'})
            continue
        names = list_names(discovery, label)
        if len(names) != count:
            RESULTS.append({'suite': label, 'expected_count': count, 'discovered_names': names,
                            'actual_names': [], 'actual_results': [], 'actual_passed': 0,
                            'actual_failed': 0, 'actual_ignored': 0, 'passed': False,
                            'command_name': discovery_row['name'], 'setup_failure': 'source_case_count_changed'})
            continue
        row, out, err = command(label + '-execution', argv + ['--', '--test-threads=1'], additions)
        try:
            result = execution(row, out, err, names)
        except AssertionError as failure:
            result = {'command_name': row['name'], 'actual_names': [], 'actual_results': [],
                      'actual_passed': 0, 'actual_failed': 0, 'actual_ignored': 0, 'passed': False,
                      'setup_failure': 'execution_result_unverified', 'detail': str(failure)}
        result.update({'suite': label, 'expected_count': count, 'discovered_names': names})
        RESULTS.append(result)
        assert source_snapshot(tree) == BASELINE
    # Even a failed sticky case/discovery cannot prevent the independent
    # capability/partitioner/Streams commands and available reverse exports.
    CURRENT_STAGE = 'exports'
    EXPORTS = export_inventory()
    CURRENT_STAGE = 'aggregate'
    assert sum(len(r['actual_names']) for r in RESULTS) == 89
    actual = {n for r in RESULTS for n in r['actual_names']}
    assert FOUR <= actual
    assert 'unlimited_byte_budget_tracks_payload_reservations_and_terminal_releases' in actual
    assert all(r['passed'] for r in RESULTS), 'actual focused test failures retained'
except BaseException as failure:
    FAILURE = {'type': type(failure).__name__, 'detail': str(failure), 'stage': CURRENT_STAGE}
finally:
    if ACTIVE is not None:
        join_group(ACTIVE, True)
    after = None
    try:
        if BASELINE is not None:
            after = source_snapshot(tree)
            assert after == BASELINE
    except BaseException as failure:
        FAILURE = {'type': type(failure).__name__, 'detail': str(failure), 'stage': 'final_source_guard'}
    public = {k: os.getenv(k) for k in ['GITHUB_REPOSITORY', 'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_SERVER_URL']}
    streams = next((r for r in RESULTS if r['suite'] == 'streams_protocol'), None)
    streams_component_passed = (TOOLCHAIN_VERIFIED and BASELINE is not None and after == BASELINE
                                and streams is not None and streams['passed']
                                and streams['actual_passed'] == 24 and EXPORTS is not None and EXPORTS['passed']
                                and (FAILURE is None or FAILURE['stage'] == 'aggregate'))
    report = {'schema_version': 1, 'source_sha': SOURCE_SHA, 'toolchain': args.toolchain, 'features': args.features,
              'passed': FAILURE is None, 'failure': FAILURE, 'public_ci': public,
              'source_inventory': locals().get('source_manifest'), 'cargo_lock': locals().get('lock'),
              'source_before_after_all_Git_blobs_and_full_modes_match': BASELINE is not None and after == BASELINE,
              'rustc_identity': locals().get('rustc'), 'cargo_identity': locals().get('cargo'),
              'commands': LEDGER, 'actual_command_count': len(LEDGER), 'suites': RESULTS,
              'actual_executed_tests': sum(len(r['actual_names']) for r in RESULTS),
              'actual_passed_tests': sum(r['actual_passed'] for r in RESULTS), 'expected_unique_tests': 89,
              'actual_unique_verified_test_cases': len({(r['suite'], name) for r in RESULTS for name in r['actual_names']}),
              'exports': EXPORTS, 'SDK_runtime_or_Java_reverse_qualification': False,
              'components': {'streams': {'passed': streams_component_passed,
                  'qualification_scope': 'exact-source Rust Streams24 and234 exports; independent of unrelated focused suite failures',
                  'toolchain_verified': TOOLCHAIN_VERIFIED, 'overall_focused_passed': FAILURE is None,
                  'actual_passed_tests': streams['actual_passed'] if streams is not None else 0,
                  'expected_tests': 24, 'export_file_count': EXPORTS['file_count'] if EXPORTS is not None else 0,
                  'external_Java_reverse_completed': False}},
              'dedicated_build_target_initially_absent': True, 'build_target': str(BUILD),
              'opt_in_public_admin_probe_returns_without_external_SDK': True,
              'no_Docker_required': True, 'captured_environment': {k: os.getenv(k) for k in [
                  'RUSTUP_TOOLCHAIN', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG']}}
    save('validation.json', report)
    # Keep Unix modes inside a tar: artifact ZIP transport does not preserve them.
    archive = CELL / 'evidence.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(EVIDENCE, arcname='evidence', recursive=True)
    with tarfile.open(archive, 'r:gz') as tar:
        for member in tar.getmembers():
            assert not member.issym() and not member.islnk()
            original = CELL / member.name
            assert member.mode == stat.S_IMODE(original.stat().st_mode)
            if member.isfile():
                with tar.extractfile(member) as file:
                    assert file.read() == original.read_bytes()
    wrapper = {'schema_version': 1, 'source_sha': SOURCE_SHA, 'toolchain': args.toolchain, 'features': args.features,
               'passed': report['passed'], 'public_ci': public, 'archive': {'path': 'evidence.tar.gz',
               'sha256': sha(archive.read_bytes()), 'bytes': archive.stat().st_size,
               'original_full_mode': stat.S_IMODE(archive.stat().st_mode)},
               'inner_validation_path': 'evidence/validation.json',
               'inner_validation_sha256': sha((EVIDENCE / 'validation.json').read_bytes()),
               'export_transfer_modes_preserved_in_tar': True}
    (CELL / 'artifact-manifest.json').write_text(json.dumps(wrapper, indent=2) + '\n')
if FAILURE is not None:
    print(json.dumps(FAILURE), file=sys.stderr)
    sys.exit(1)
