#!/usr/bin/env python3
"""Qualify a pinned broker candidate outside the checked-out production source."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
OWNERSHIP_SPEC = importlib.util.spec_from_file_location('candidate_command_ownership',
    Path(__file__).with_name('candidate-command-ownership.py'))
OWNERSHIP = importlib.util.module_from_spec(OWNERSHIP_SPEC)
OWNERSHIP_SPEC.loader.exec_module(OWNERSHIP)
FLOOR = 1536 * 1024**2
BASELINE = {
    'produce::tests::partial_response_buffer_is_reserved_before_encoding',
    'fetch::tests::partial_response_buffer_is_reserved_before_encoding',
    'retention::tests::partial_response_buffer_is_reserved_before_encoding',
    'raft::protocol::response_buffer_tests::partial_response_buffer_is_reserved_before_encoding',
    'fetch::tests::partially_spare_fetch_buffer_reserves_the_entire_next_field',
    'journal::tests::checkpoint_replacement_preserves_permissions_before_publication',
    'replication::tests::accepted_epoch_replacement_preserves_private_file_permissions',
}
FIXED = BASELINE | {
    'journal::tests::checkpoint_file_sync_error_does_not_publish_the_replacement',
    'produce::tests::a_produce_response_reuses_a_parked_buffer',
    'fetch::tests::a_header_after_records_does_not_double_the_buffer',
    'retention::tests::a_delete_records_response_reuses_its_buffer',
    'raft::protocol::response_buffer_tests::a_controller_response_reuses_its_buffer',
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON property: '+key)
        result[key] = value
    return result


def load(path):
    return json.loads(Path(path).read_text(), object_pairs_hook=unique_object)


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2)+'\n')


def asset(spec_path, item):
    name = PurePosixPath(item['path'])
    require(not name.is_absolute() and '..' not in name.parts and '\\' not in str(name),
            'invalid asset path')
    path = (spec_path.parent/str(name)).resolve()
    require(path.is_relative_to(spec_path.parent.resolve()), 'asset escapes specification directory')
    require(path.is_file() and sha(path) == item['sha256'], 'asset hash mismatch: '+str(name))
    return path


def valid_name(name):
    path = PurePosixPath(name)
    return (bool(name) and not path.is_absolute() and '..' not in path.parts
            and str(path) == name and '\\' not in name and '\x00' not in name)


def materialize_patch(path, destination, expected):
    require(path.stat().st_size <= 1024**2, 'fixed patch exceeds compressed bound')
    with gzip.open(path, 'rb') as stream:
        data = stream.read(4*1024**2+1)
    require(len(data) <= 4*1024**2 and len(data) == expected['expanded_bytes']
            and hashlib.sha256(data).hexdigest() == expected['expanded_sha256'],
            'expanded fixed patch hash/size mismatch')
    with destination.open('xb') as stream:
        stream.write(data)
    return destination


def extract_source(archive, destination, expected):
    require(archive.stat().st_size <= 16*1024**2, 'source archive exceeds compressed bound')
    with gzip.open(archive, 'rb') as stream:
        raw = stream.read(64*1024**2+1)
    require(len(raw) <= 64*1024**2, 'source archive exceeds expanded bound')
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as bundle:
        members = bundle.getmembers()
        require(0 < len(members) <= 12000, 'invalid source member count')
        names = [member.name for member in members]
        require(len(names) == len(set(names)) and set(names) == set(expected),
                'source members differ from pinned file inventory')
        require(all(valid_name(name) for name in names), 'unsafe source member name')
        require(all(member.isfile() and member.size <= 8*1024**2 for member in members),
                'source members must be bounded regular files')
        require(sum(member.size for member in members) <= 48*1024**2, 'source file budget exceeded')
        for member in members:
            entry = expected[member.name]
            require(member.mode == entry['mode'], 'source member mode mismatch: '+member.name)
            data = bundle.extractfile(member).read()
            require(len(data) == entry['bytes'] and hashlib.sha256(data).hexdigest() == entry['sha256'],
                    'source member hash/size mismatch: '+member.name)
            path = destination/member.name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('xb') as stream:
                stream.write(data)
            path.chmod(entry['mode'])


def verify_source_bytes(source, expected):
    require(not any(path.is_symlink() for path in source.rglob('*')), 'source contains a symlink')
    files = {str(path.relative_to(source)): path for path in source.rglob('*') if path.is_file()}
    require(set(files) == set(expected), 'source file inventory changed')
    for name, path in files.items():
        entry = expected[name]
        require(not path.is_symlink() and path.stat().st_size == entry['bytes']
                and sha(path) == entry['sha256'], 'source bytes changed: '+name)
    return files


def verify_source(source, expected):
    for name, path in verify_source_bytes(source, expected).items():
        entry = expected[name]
        require(stat.S_IMODE(path.stat().st_mode) == entry['mode'], 'source mode changed: '+name)


def restore_patched_source_modes(source, expected, allowed, receipt_path):
    """Restore pinned checkout modes only after the complete byte inventory holds.

    Git applies a textual patch by replacing source files with the current umask.
    This changes checkout metadata on hosts whose umask differs from the capsule.
    Runtime-created journal files and binary fixtures are outside this allowlist.
    """
    receipt = {'passed': False, 'started_utc': utc(),
        'scope': 'Pinned source checkout metadata only; no runtime journal or fixture modes changed.',
        'byte_inventory_verified_before_restoration': False,
        'allowed_paths': sorted(allowed), 'observed_modes_before': {}, 'observed_modes_after': {}}
    try:
        require(not receipt_path.resolve().is_relative_to(source.resolve()),
                'source mode receipt must be outside the source checkout')
        files = verify_source_bytes(source, expected)
        receipt['byte_inventory_verified_before_restoration'] = True
        require(set(allowed) <= set(files), 'unknown source mode restoration path')
        modes = {name: stat.S_IMODE(path.stat().st_mode) for name, path in files.items()}
        receipt['observed_modes_before'] = {name: modes[name] for name in sorted(allowed)}
        changed = sorted(name for name in files if modes[name] != expected[name]['mode'])
        receipt['changed_paths'] = changed
        require(set(changed) <= set(allowed), 'unexpected source mode drift outside patched paths')
        for name in changed:
            files[name].chmod(expected[name]['mode'])
        receipt['observed_modes_after'] = {
            name: stat.S_IMODE(files[name].stat().st_mode) for name in sorted(allowed)}
        verify_source(source, expected)
        receipt['passed'] = True
    except Exception as error:
        receipt['error'] = str(error)
        raise
    finally:
        receipt['finished_utc'] = utc()
        save(receipt_path, receipt)
    return receipt


def oom_events():
    try:
        text = Path('/sys/fs/cgroup/memory.events').read_text()
    except FileNotFoundError:
        return None
    return {key: int(value) for key, value in (line.split() for line in text.splitlines())}


def oom_delta(before, after):
    if before is None or after is None:
        return 'unavailable', None
    keys = ('oom', 'oom_kill', 'oom_group_kill')
    values = {key: after[key]-before[key] for key in keys if key in before and key in after}
    if not values:
        return 'unavailable', None
    if any(value < 0 for value in values.values()):
        return 'reset', values
    return ('observed' if len(values) == len(keys) else 'partial'), values


def test_counts(text):
    return [tuple(map(int, match)) for match in re.findall(
        r'^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text, re.M)]


class CommandFailure(ValueError):
    """A completed command failed while its source and resource checks held."""


class Qualification:
    def __init__(self, output, source, expected):
        self.output, self.source, self.expected = output, source, expected
        self.records = []
        self.env = dict(os.environ)
        for key in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER',
                    'RUSTC_WORKSPACE_WRAPPER', 'RUST_TEST_THREADS', 'RUST_MIN_STACK',
                    'PLB_JOURNAL_CHECKPOINT_CRASH', 'PLB_EPOCH_HEADER_CRASH_DIR',
                    'PLB_REPLICATION_CRASH', 'PL_RUNTIME_CHILD_ROOT', 'PL_RUNTIME_CHILD_ID',
                    'PL_RUNTIME_CHILD_ACTIVE', 'PL_RUNTIME_CHILD_ROUTES',
                    'PL_RUNTIME_CHILD_BACKEND', 'PL_PEER_RUNTIME_CAPTURE_DIR',
                    'PL_PEER_RUNTIME_SOURCE_SHA'):
            self.env.pop(key, None)
        self.env.update(CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
            CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
            CARGO_TARGET_DIR=str(output.parent/(output.name+'-native-target')), TMPDIR=str(output/'runtime-temporary'),
            PYTHONDONTWRITEBYTECODE='1', CARGO_TERM_COLOR='never')
        (output/'runtime-temporary').mkdir()
        (output/'artifacts').mkdir()

    def run(self, name, command, timeout=900, expected_exit=0):
        verify_source(self.source, self.expected)
        require(shutil.disk_usage(self.output).free > FLOOR, 'disk floor before '+name)
        before = oom_events()
        runner_hashes = {'driver': sha(__file__), 'ownership_helper': sha(
            Path(__file__).with_name('candidate-command-ownership.py'))}
        log = self.output/(name+'.log')
        started_utc, started = utc(), time.monotonic()
        samples, stop = [], None
        owned, observer_error, code = None, None, None
        with log.open('xb') as stream:
            try:
                with OWNERSHIP.owned_command(command, cwd=self.source, env=self.env,
                        stdout=stream, stderr=subprocess.STDOUT) as owned:
                    while not owned.finished():
                        free = shutil.disk_usage(self.output).free
                        samples.append({'seconds': round(time.monotonic()-started, 3), 'free_bytes': free})
                        if free <= FLOOR:
                            stop = 'disk-floor'
                        elif time.monotonic()-started >= timeout:
                            stop = 'command-timeout'
                        elif log.stat().st_size > 64*1024**2:
                            stop = 'command-log-bound'
                        if stop:
                            break
                        time.sleep(.1)
            except BaseException as error:
                observer_error = str(error)
                stop = stop or 'observer-exception'
            if owned is not None:
                code = owned.process.returncode
                if owned.cleanup_receipt.get('descendants_present_at_close') and stop is None:
                    stop = 'descendants-outlived-command'
        after = oom_events()
        counter_status, deltas = oom_delta(before, after)
        final_free = shutil.disk_usage(self.output).free
        if final_free <= FLOOR:
            stop = stop or 'disk-floor'
        if log.stat().st_size > 64*1024**2:
            stop = stop or 'command-log-bound'
        runner_hashes_after = {'driver': sha(__file__), 'ownership_helper': sha(
            Path(__file__).with_name('candidate-command-ownership.py'))}
        if runner_hashes != runner_hashes_after:
            stop = stop or 'qualification-runner-changed'
        text = log.read_text(errors='replace')
        row = {'name': name, 'command': command, 'started_utc': started_utc,
            'finished_utc': utc(), 'elapsed_seconds': time.monotonic()-started,
            'exit': code, 'expected_exit': expected_exit, 'stop_reason': stop,
            'observer_error': observer_error,
            'owned_command_cleanup': None if owned is None else owned.cleanup_receipt,
            'disk_floor_bytes': FLOOR, 'disk_sampling_interval_seconds': .1,
            'disk_samples': samples, 'minimum_observed_free_bytes': min([
                final_free, *(sample['free_bytes'] for sample in samples)]),
            'oom_events_before': before, 'oom_events_after': after,
            'oom_event_deltas': deltas, 'oom_counter_status': counter_status,
            'runner_hashes_before': runner_hashes, 'runner_hashes_after': runner_hashes_after,
            'source_manifest_unchanged': False, 'passed': False, 'log_sha256': sha(log),
            'test_result_counts': test_counts(text),
            'test_failures': re.findall(r'^test (.*?) \.\.\. FAILED$', text, re.M)}
        row['ownership_complete'] = (owned is not None
            and owned.cleanup_receipt.get('leader_waited') is True
            and not owned.cleanup_receipt.get('remaining_descendants'))
        self.records.append(row)
        save(self.output/(name+'.json'), row)
        verify_source(self.source, self.expected)
        row['source_manifest_unchanged'] = True
        row['passed'] = code == expected_exit and stop is None and row['ownership_complete'] and (
            deltas is None or all(value == 0 for value in deltas.values()))
        save(self.output/(name+'.json'), row)
        require(stop is None and row['ownership_complete']
                and (deltas is None or all(value == 0 for value in deltas.values())),
                'resource guard failed: '+name)
        if code != expected_exit:
            raise CommandFailure('command failed: '+name)
        return text

    def retain(self, label, path, event):
        digest, size = sha(path), path.stat().st_size
        compressed = self.output/'artifacts'/(label+'.elf.gz')
        with path.open('rb') as source, compressed.open('xb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as target:
                while chunk := source.read(1024**2):
                    target.write(chunk)
                    require(shutil.disk_usage(self.output).free > FLOOR, 'disk floor retaining '+label)
        with gzip.open(compressed, 'rb') as stream:
            restored = hashlib.file_digest(stream, 'sha256').hexdigest()
        with gzip.open(compressed, 'rb') as stream:
            restored_size = sum(len(chunk) for chunk in iter(lambda: stream.read(1024**2), b''))
        require(sha(path) == restored == digest and restored_size == size, 'ELF retention mismatch')
        row = {'execution_path': str(path), 'bytes': size, 'sha256': digest,
            'gzip_path': str(compressed.relative_to(self.output)), 'gzip_sha256': sha(compressed),
            'cargo_fresh': event['fresh'], 'cargo_artifact': event}
        save(self.output/'artifacts'/(label+'.json'), row)
        return row

    def build(self, label, all_features=False, all_targets=False):
        command = ['cargo', '+stable', 'test', '--locked', '--manifest-path',
            'partitionline-broker/Cargo.toml', '--no-run', '--message-format=json-render-diagnostics',
            '--all-targets' if all_targets else '--lib']
        if all_features:
            command.append('--all-features')
        text = self.run(label+'-build', command, 1200)
        artifacts = []
        for line in text.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (event.get('reason') != 'compiler-artifact' or not event.get('executable')
                    or not event.get('profile', {}).get('test')
                    or Path(event['manifest_path']).parent != self.source/'partitionline-broker'):
                continue
            path = Path(event['executable'])
            target = event['target']['name']
            row = self.retain(label+'-'+target, path, event)
            artifacts.append(row)
        require(artifacts, 'no broker test artifacts: '+label)
        if not all_targets:
            require(len(artifacts) == 1 and artifacts[0]['cargo_fresh'] is False,
                    'missing fresh broker unit test artifact')
        return artifacts

    def exact(self, label, artifact, cases, negative=False):
        path = Path(artifact['execution_path'])
        require(sha(path) == artifact['sha256'], 'test executable changed before selection')
        listing = self.run(label+'-list', [str(path), '--list'])
        names = [line.removesuffix(': test') for line in listing.splitlines() if line.endswith(': test')]
        require(len(names) == len(set(names)) and set(cases) <= set(names), 'required tests missing')
        selected = sorted(cases)
        text = self.run(label, [str(path), *selected, '--exact', '--test-threads=1'],
                        expected_exit=101 if negative else 0)
        counts = test_counts(text)
        require(counts == [(0, len(cases), 0) if negative else (len(cases), 0, 0)],
                'exact test execution count mismatch: '+label)
        outcomes = re.findall(r'^test (\S+) \.\.\. (ok|FAILED|ignored)\b', text, re.M)
        status = 'FAILED' if negative else 'ok'
        require(len(outcomes) == len(cases) and {name for name, _ in outcomes} == set(cases)
                and all(actual == status for _, actual in outcomes),
                'execution produced a different test inventory or status')
        require(sha(path) == artifact['sha256'], 'test executable changed after selection')
        save(self.output/(label+'-selection.json'), {'selected': selected, 'counts': counts})

    def full(self, label, all_features=False):
        artifacts = self.build(label, all_features, all_targets=True)
        command = ['cargo', '+stable', 'test', '--locked', '--manifest-path',
            'partitionline-broker/Cargo.toml', '--all-targets', '--no-fail-fast']
        if all_features:
            command.append('--all-features')
        try:
            text = self.run(label+'-behavior', command, 1200)
        finally:
            for artifact in artifacts:
                require(sha(artifact['execution_path']) == artifact['sha256'], 'full-suite executable changed')
        require(test_counts(text) and sum(row[0] for row in test_counts(text)) >= 700,
                'full broker test run did not execute its library')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    require(not output.is_relative_to(ROOT), 'output must be outside the checkout')
    require(not output.exists(), 'output must be a new directory')
    output.mkdir(parents=True)
    report = {'passed': False, 'started_utc': utc(), 'commands': [],
        'scope': 'Broker candidate functional qualification. No production, global performance, power-loss, native KRaft or resource-bound certification.'}
    qualification = None
    try:
        spec_path = (ROOT/args.spec).resolve()
        require(spec_path.is_relative_to(ROOT), 'specification must be in the checkout')
        spec = load(spec_path)
        require(spec['schema_version'] == 1 and spec['kind'] == 'broker', 'invalid broker specification')
        require(platform.machine() == 'x86_64', 'broker candidate lane requires x86_64')
        require(set(spec['baseline_cases']) == BASELINE and len(spec['baseline_cases']) == len(BASELINE),
                'baseline test contract changed')
        require(set(spec['fixed_cases']) == FIXED and len(spec['fixed_cases']) == len(FIXED),
                'fixed test contract changed')
        archive = asset(spec_path, spec['baseline_archive'])
        manifests_path = asset(spec_path, spec['source_manifests'])
        patch_asset = asset(spec_path, spec['fixed_patch'])
        manifests = load(manifests_path)
        base = manifests['base_files']
        for name, entry in base.items():
            require(valid_name(name) and (ROOT/name).is_file()
                    and sha(ROOT/name) == entry['sha256'], 'checked-out base source drift: '+name)
        baseline = dict(base)
        baseline.update(manifests['baseline_overrides'])
        fixed = dict(baseline)
        fixed.update(manifests['fixed_overrides'])
        source = output/'source'
        source.mkdir()
        extract_source(archive, source, baseline)
        verify_source(source, baseline)
        assets = output/'assets'
        assets.mkdir()
        for path in (spec_path, manifests_path, archive, patch_asset, Path(__file__),
                     Path(__file__).with_name('candidate-command-ownership.py')):
            shutil.copyfile(path, assets/path.name)
        patch = materialize_patch(patch_asset, assets/'fixed-source.patch', spec['fixed_patch'])
        report.update(spec_sha256=sha(spec_path), driver_sha256=sha(__file__),
            ownership_helper_sha256=sha(Path(__file__).with_name('candidate-command-ownership.py')),
            actual_checkout_sha=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            declared_base=spec['base_main_sha'], base_prerequisite=spec['base_prerequisite'],
            source_files=len(baseline), host_machine=platform.machine(),
            build_profile='latest stable; one Cargo job, incremental off, test/dev debuginfo0; not identical to ordinary CI debug profile',
            disk_floor_bytes=FLOOR, production_source_modified=False)
        qualification = Qualification(output, source, baseline)
        qualification.run('rustc-version', ['rustc', '+stable', '-Vv'])
        qualification.run('cargo-version', ['cargo', '+stable', '-V'])
        qualification.run('openssl-prerequisite', ['openssl', 'version'])
        old = qualification.build('baseline')[0]
        qualification.exact('baseline-seven-negative-controls', old, BASELINE, negative=True)
        # Mutation is confined to the fresh extracted candidate source.
        subprocess.run(['git', 'apply', '--check', str(patch)], cwd=source, check=True)
        subprocess.run(['git', 'apply', str(patch)], cwd=source, check=True)
        report['patched_source_mode_restoration'] = restore_patched_source_modes(
            source, fixed, set(manifests['fixed_overrides']), output/'patched-source-modes.json')
        qualification.expected = fixed
        for path in (source/'partitionline-broker/src').rglob('*.rs'):
            path.touch()
        gate_errors = []
        report['gate_errors'] = gate_errors

        def attempt(label, operation):
            try:
                return operation()
            except CommandFailure as error:
                gate_errors.append({'gate': label, 'error': str(error)})
                return None

        for profile, all_features in [('default', False), ('all-features', True)]:
            unit = qualification.build('fixed-'+profile, all_features)[0]
            attempt('fixed-'+profile+'-focused', lambda: qualification.exact('fixed-'+profile+'-focused', unit, FIXED))
            attempt('fixed-'+profile+'-all-targets', lambda: qualification.full('fixed-'+profile+'-all-targets', all_features))
        attempt('source-format', lambda: qualification.run('source-format', ['cargo', '+stable', 'fmt',
            '--manifest-path', 'partitionline-broker/Cargo.toml', '--check']))
        attempt('strict-clippy', lambda: qualification.run('strict-clippy', ['cargo', '+stable', 'clippy',
            '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--all-targets',
            '--all-features', '--', '-D', 'warnings'], 1200))
        qualification.env['RUSTDOCFLAGS'] = '-D warnings'
        attempt('strict-docs', lambda: qualification.run('strict-docs', ['cargo', '+stable', 'doc',
            '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--all-features', '--no-deps'], 1200))
        for profile, flags in [('default', []), ('all-features', ['--all-features'])]:
            attempt('strict-doctests-'+profile, lambda: qualification.run('strict-doctests-'+profile,
                ['cargo', '+stable', 'test', '--locked', '--manifest-path',
                 'partitionline-broker/Cargo.toml', '--doc', *flags], 1200))
        attempt('broker-inventory', lambda: qualification.run('broker-inventory',
            ['python3', '-B', 'scripts/check-broker-api-matrix.py', '--report', str(output/'broker-inventory-report.json')]))
        controls = attempt('broker-inventory-negative-controls', lambda: qualification.run(
            'broker-inventory-negative-controls', ['python3', '-B', '-m', 'unittest', 'discover',
            '-s', 'tests/ci', '-p', 'test_broker_api_matrix.py', '-v'], 900))
        if controls is not None:
            require(re.search(r'^Ran 55 tests in ', controls, re.M) is not None
                    and re.search(r'^OK$', controls, re.M) is not None, 'inventory control suite incomplete')
        verify_source(source, fixed)
        report['gate_errors'] = gate_errors
        report['passed'] = not gate_errors
    except Exception as error:
        report['error'] = str(error)
    finally:
        if qualification is not None:
            report['commands'] = qualification.records
        report['finished_utc'] = utc()
        save(output/'qualification.json', report)
    print(json.dumps({'passed': report['passed'], 'error': report.get('error'),
        'output': str(output), 'completed_commands': len(report['commands'])}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
