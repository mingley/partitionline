#!/usr/bin/env python3
"""Qualify the pinned client candidate in an isolated CI source directory."""
import argparse
import datetime
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time

ROOT = None
OUT = None
ENV = None
EXPECTED = None
OWNERSHIP = None
RUNTIME_FLOOR = 3 * 1024 ** 3 // 2
COMPILE_FLOOR = RUNTIME_FLOOR

def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def manifest():
    result = subprocess.run(['rg', '--files', '--hidden', '-0', '-g', '!/target/**',
                             '-g', '!/docs/**'], cwd=ROOT, capture_output=True, check=True)
    return {os.fsdecode(path): digest(ROOT / os.fsdecode(path))
            for path in sorted(result.stdout.split(b'\0')) if path}


def save(name, data):
    path = OUT / name
    with path.open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')


def memory():
    try:
        return Path('/sys/fs/cgroup/memory.events').read_text()
    except FileNotFoundError:
        return None


def memory_delta(before, after):
    keys = ('oom', 'oom_kill', 'oom_group_kill')
    if before is None or after is None:
        return 'unknown', None
    try:
        old = dict(line.split() for line in before.splitlines())
        new = dict(line.split() for line in after.splitlines())
        delta = {key: int(new[key]) - int(old[key]) for key in keys if key in old and key in new}
    except (ValueError, TypeError):
        return 'unknown', None
    if not delta:
        return 'unknown', None
    status = 'known' if len(delta) == len(keys) else 'partial'
    if any(value < 0 for value in delta.values()):
        status = 'reset'
    return status, delta


def ownership_helper():
    global OWNERSHIP
    if OWNERSHIP is None:
        path = Path(__file__).with_name('candidate-command-ownership.py')
        spec = importlib.util.spec_from_file_location('candidate_command_ownership', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        OWNERSHIP = module
    return OWNERSHIP


def run(name, command, timeout=600, split_stderr=False, disk_floor=RUNTIME_FLOOR):
    OUT.mkdir(parents=True, exist_ok=True)
    if (OUT / (name + '.json')).exists() or (OUT / (name + '.log')).exists():
        raise RuntimeError('refusing to overwrite previous run ' + name)
    free = shutil.disk_usage(ROOT).free
    if free <= disk_floor:
        save(name + '.json', {'name': name, 'command': command, 'state': 'not_started',
             'stop_reason': 'disk_floor_before_start', 'free_bytes': free,
             'disk_stop_bytes': disk_floor})
        raise RuntimeError('disk reserve unavailable before ' + name)
    before = manifest()
    if before != EXPECTED:
        raise RuntimeError('source does not match the phase manifest before ' + name)
    save(name + '-inputs-before.json', before)
    helper_path = Path(__file__).with_name('candidate-command-ownership.py')
    row = dict(name=name, command=command, cwd=str(ROOT),
        runner_sha256=digest(Path(__file__)), ownership_helper_sha256=digest(helper_path),
        started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        memory_events_before=memory(), disk_stop_bytes=disk_floor,
        environment_overrides={key: ENV.get(key) for key in (
            'RUSTUP_HOME', 'CARGO_HOME', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS',
            'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUST_MIN_STACK')},
        source_scope='Exact sparse client inputs; stable toolchain identity and Cargo.lock retained. Dependency source bytes are not exhaustively pinned.')
    started = time.monotonic()
    minimum, samples, stopped, owner, failure = free, [], None, None, None
    stderr_path = OUT / (name + '.stderr.log')
    with (OUT / (name + '.log')).open('xb') as log:
        error_stream = stderr_path.open('xb') if split_stderr else None
        try:
            try:
                with ownership_helper().owned_command(command, cwd=ROOT, env=ENV,
                        stdout=log, stderr=error_stream or subprocess.STDOUT) as owner:
                    while not owner.finished():
                        free = shutil.disk_usage(ROOT).free
                        elapsed = time.monotonic() - started
                        minimum = min(minimum, free)
                        samples.append(dict(seconds=round(elapsed, 3), free_bytes=free))
                        if free < disk_floor or elapsed >= timeout:
                            stopped = 'disk_floor' if free < disk_floor else 'timeout'
                            break
                        time.sleep(.25)
            except BaseException as error:
                failure = {'type': type(error).__name__, 'message': str(error)}
                stopped = stopped or ('interrupted' if isinstance(error, (InterruptedError, KeyboardInterrupt)) else 'observer_error')
            finally:
                row['owned_command_cleanup'] = owner.cleanup_receipt if owner is not None else None
                row['exit'] = owner.process.returncode if owner is not None else None
                if owner is not None and (not owner.cleanup_receipt.get('leader_waited') or owner.cleanup_receipt.get('remaining_descendants')):
                    stopped = stopped or 'cleanup_incomplete'
                if owner is not None and owner.cleanup_receipt.get('descendants_present_at_close'):
                    stopped = stopped or 'descendants_at_command_exit'
        finally:
            if error_stream:
                error_stream.close()
    final_free = shutil.disk_usage(ROOT).free
    minimum = min(minimum, final_free)
    samples.append(dict(seconds=round(time.monotonic() - started, 3), free_bytes=final_free))
    if final_free < disk_floor:
        stopped = stopped or 'disk_floor_after_command'
    after = manifest()
    save(name + '-inputs-after.json', after)
    data = (OUT / (name + '.log')).read_text(errors='replace')
    row.update(elapsed_seconds=round(time.monotonic() - started, 3),
        stop_reason=stopped, observer_failure=failure,
        minimum_disk_free_bytes=minimum, disk_samples=samples,
        memory_events_after=memory(), log_sha256=digest(OUT / (name + '.log')),
        inputs_stable=before == after,
        runner_sha256_after=digest(Path(__file__)), ownership_helper_sha256_after=digest(helper_path),
        test_results=re.findall(r'^test result:.*$', data, re.MULTILINE),
        test_failures=re.findall(r'^test (.*?) \.\.\. FAILED$', data, re.MULTILINE))
    row['memory_counter_status'], row['oom_event_delta'] = memory_delta(row['memory_events_before'], row['memory_events_after'])
    row['oom_observed'] = row['oom_event_delta'] is not None and any(value > 0 for value in row['oom_event_delta'].values())
    if row['oom_observed'] or row['memory_counter_status'] == 'reset':
        row['stop_reason'] = row['stop_reason'] or ('observed_cgroup_oom_event' if row['oom_observed'] else 'cgroup_memory_counter_reset')
    if split_stderr:
        row['stderr_sha256'] = digest(stderr_path)
    save(name + '.json', row)
    print(json.dumps({key: row[key] for key in ('name', 'exit', 'stop_reason',
        'elapsed_seconds', 'inputs_stable', 'test_results', 'test_failures')}), flush=True)
    if (not row['inputs_stable'] or row['stop_reason'] or
        row['runner_sha256'] != row['runner_sha256_after'] or
        row['ownership_helper_sha256'] != row['ownership_helper_sha256_after']):
        raise RuntimeError('incomplete run: ' + name)
    return row


def retain(name, event):
    path = Path(event['executable'])
    original_hash, size = digest(path), path.stat().st_size
    compressed = OUT / (name + '-' + event['target']['name'] + '.elf.gz')
    with path.open('rb') as source, compressed.open('xb') as output:
        with gzip.GzipFile(filename='', mode='wb', fileobj=output, mtime=0) as archive:
            shutil.copyfileobj(source, archive)
    check = hashlib.sha256()
    total = 0
    with gzip.open(compressed, 'rb') as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b''):
            check.update(chunk)
            total += len(chunk)
    if total != size or check.hexdigest() != original_hash or digest(path) != original_hash:
        raise RuntimeError('retention verification failed for ' + str(path))
    return dict(source_path=str(path), sha256=original_hash, bytes=size,
                retained_gzip_path=str(compressed), gzip_sha256=digest(compressed),
                gzip_bytes=compressed.stat().st_size, fresh=False,
                package_id=event.get('package_id'))


def build(name, targets, all_features=False):
    if 'partitionline' in targets:
        for path in (ROOT / 'src').rglob('*.rs'):
            path.touch()
    for target in targets:
        if target != 'partitionline':
            (ROOT / 'tests' / (target + '.rs')).touch()
    command = ['cargo', '+stable', 'test', '--locked', '--no-run',
               '--message-format=json-render-diagnostics']
    for target in targets:
        command.extend(['--lib'] if target == 'partitionline' else ['--test', target])
    if all_features:
        command.append('--all-features')
    row = run(name, command, timeout=1200, split_stderr=True, disk_floor=COMPILE_FLOOR)
    if row['exit']:
        raise RuntimeError('build failed ' + name)
    events = {}
    for line in (OUT / (name + '.log')).read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get('reason') == 'compiler-artifact' and event.get('executable'):
            target = event.get('target', {}).get('name')
            if target in targets and event.get('profile', {}).get('test'):
                if event.get('fresh') is not False:
                    raise RuntimeError('freshness proof missing for ' + target)
                events[target] = event
    if set(events) != set(targets):
        raise RuntimeError('missing target artifacts ' + str(set(targets) - set(events)))
    artifacts = {target: retain(name, event) for target, event in events.items()}
    save(name + '-artifacts.json', artifacts)
    return artifacts


def execute(name, build_name, target, filters, timeout=600):
    artifact = json.loads((OUT / (build_name + '-artifacts.json')).read_text())[target]
    build_inputs_path = OUT / (build_name + '-inputs-before.json')
    if manifest() != json.loads(build_inputs_path.read_text()):
        raise RuntimeError('compiled source was changed before ' + name)
    binary = Path(artifact['source_path'])
    if digest(binary) != artifact['sha256']:
        raise RuntimeError('compiled artifact was replaced before ' + name)
    save(name + '-artifact.json', dict(
        build=build_name, target=target, sha256=artifact['sha256'],
        source_inputs_sha256=digest(build_inputs_path),
        retained_gzip_sha256=artifact['gzip_sha256']))
    row = run(name, [str(binary), *filters, '--test-threads=1'], timeout=timeout)
    if digest(binary) != artifact['sha256']:
        raise RuntimeError('compiled artifact was replaced during ' + name)
    log = (OUT / (name + '.log')).read_text()
    match = re.search(r'^running (\d+) tests?$', log, re.MULTILINE)
    if not match or int(match[1]) == 0:
        raise RuntimeError('no tests executed in ' + name)
    if '--exact' in filters:
        selected = [value for value in filters if not value.startswith('-')]
        if int(match[1]) != 1 or len(selected) != 1:
            raise RuntimeError('exact selection mismatch in ' + name)
        statuses = test_statuses(log)
        if set(statuses) != set(selected) or statuses[selected[0]] == 'ignored':
            raise RuntimeError('exact test identity or executed status mismatch in ' + name)
    return row


def relative_path(name):
    if not isinstance(name, str) or not name:
        raise RuntimeError('empty source path')
    path = PurePosixPath(name)
    if not path.parts or str(path) != name or path.is_absolute() or '..' in path.parts or '\\' in name or path.parts[0] == '.git':
        raise RuntimeError('unsafe source path: ' + name)
    return path


def read_asset(directory, entry):
    name = entry['path']
    relative_path(name)
    original = directory / name
    path = original.resolve()
    if original.is_symlink() or not path.is_relative_to(directory.resolve()) or not path.is_file():
        raise RuntimeError('asset escapes its directory: ' + name)
    if digest(path) != entry['sha256']:
        raise RuntimeError('asset digest mismatch: ' + name)
    if 'bytes' in entry and path.stat().st_size != entry['bytes']:
        raise RuntimeError('asset size mismatch: ' + name)
    return path


def materialize_patch(directory, entry):
    path = read_asset(directory, entry)
    size = entry.get('uncompressed_bytes')
    if (not path.name.endswith('.patch.gz') or type(size) is not int or
            not 0 < size <= 16 * 1024 * 1024):
        raise RuntimeError('invalid compressed patch contract')
    with gzip.open(path, 'rb') as stream:
        data = stream.read(size + 1)
    if len(data) != size or hashlib.sha256(data).hexdigest() != entry.get('uncompressed_sha256'):
        raise RuntimeError('decompressed patch differs from contract')
    output = OUT / 'assets' / path.name[:-3]
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('xb') as stream:
        stream.write(data)
    return output


def read_manifest(directory, entry):
    path = read_asset(directory, entry)
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not data:
        raise RuntimeError('empty source manifest')
    for name, checksum in data.items():
        relative_path(name)
        if not isinstance(checksum, str) or not re.fullmatch('[0-9a-f]{64}', checksum):
            raise RuntimeError('invalid source digest: ' + name)
    return data


def patch_paths(path):
    result = set()
    lines = path.read_text().splitlines()
    for index, line in enumerate(lines):
        if not line.startswith('--- '):
            continue
        if index + 1 >= len(lines) or not lines[index + 1].startswith('+++ '):
            raise RuntimeError('unpaired patch header')
        old, new = line[4:], lines[index + 1][4:]
        for name, prefix in [(old, 'a/'), (new, 'b/')]:
            if name == '/dev/null':
                continue
            if not name.startswith(prefix):
                raise RuntimeError('unexpected patch prefix')
            result.add(str(relative_path(name[len(prefix):])))
    if not result:
        raise RuntimeError('empty candidate patch')
    return result


def apply_patch(name, patch, before, after):
    global EXPECTED
    if manifest() != before:
        raise RuntimeError('source mismatch before ' + name)
    changed = {key for key in set(before) | set(after) if before.get(key) != after.get(key)}
    if patch_paths(patch) != changed:
        raise RuntimeError('patch paths differ from explicitly changed manifest paths')
    row = {'patch_sha256': digest(patch), 'paths': sorted(changed)}
    for action, flags in [('check', ['--check']), ('apply', []), ('reverse_check', ['--reverse', '--check'])]:
        result = subprocess.run(['git', 'apply', *flags, str(patch)], cwd=ROOT,
                                capture_output=True, text=True)
        row[action] = {'exit': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
        if result.returncode:
            save(name + '.json', row)
            raise RuntimeError('candidate patch failed: ' + action)
    EXPECTED = after
    row['after_matches_manifest'] = manifest() == after
    save(name + '.json', row)
    if not row['after_matches_manifest']:
        raise RuntimeError('source mismatch after ' + name)


def retain_source(name):
    archive = OUT / (name + '-source.tar.gz')
    with tarfile.open(archive, 'w:gz', compresslevel=6) as output:
        for member in sorted(EXPECTED):
            output.add(ROOT / member, arcname=member, recursive=False)
    readback = {}
    with tarfile.open(archive, 'r:gz') as source:
        for member in source:
            if not member.isfile() or member.name in readback:
                raise RuntimeError('invalid source archive member')
            relative_path(member.name)
            stream = source.extractfile(member)
            readback[member.name] = hashlib.file_digest(stream, 'sha256').hexdigest()
    if readback != EXPECTED or manifest() != EXPECTED:
        raise RuntimeError('source archive readback mismatch')
    save(name + '-source.json', {'path': archive.name, 'sha256': digest(archive),
                               'bytes': archive.stat().st_size, 'members': len(readback),
                               'readback_verified': True})


def listing(name, build_name, target, ignored=False):
    artifact = json.loads((OUT / (build_name + '-artifacts.json')).read_text())[target]
    binary = Path(artifact['source_path'])
    if digest(binary) != artifact['sha256']:
        raise RuntimeError('executable changed before discovery')
    args = [str(binary), '--list'] + (['--ignored'] if ignored else [])
    row = run(name, args, timeout=120)
    if row['exit'] or digest(binary) != artifact['sha256']:
        raise RuntimeError('compiled discovery failed')
    names = re.findall(r'^(.*): test$', (OUT / (name + '.log')).read_text(), re.MULTILINE)
    if len(names) != len(set(names)):
        raise RuntimeError('duplicate compiled test identity')
    return set(names)


def test_statuses(log):
    rows = re.findall(r'^test (.*?) \.\.\. (ok|FAILED|ignored)(?:, [^\n]*)?$', log, re.MULTILINE)
    if len(rows) != len({name for name, _ in rows}):
        raise RuntimeError('duplicate actual test identity')
    return dict(rows)


def result_counts(row):
    if not row['test_results']:
        raise RuntimeError('test result summary missing')
    match = re.search(r'(\d+) passed; (\d+) failed; (\d+) ignored;', row['test_results'][-1])
    if not match:
        raise RuntimeError('invalid test result summary')
    return tuple(int(value) for value in match.groups())


def full_target(name, build_name, target, required):
    names = listing(name + '-discovery', build_name, target)
    ignored = listing(name + '-ignored', build_name, target, ignored=True)
    if not names or not ignored <= names or not required <= names or required & ignored:
        raise RuntimeError('required test missing or ignored in ' + target)
    row = execute(name, build_name, target, [], timeout=1200)
    errors, counts, statuses = [], None, {}
    log = (OUT / (name + '.log')).read_text()
    try:
        passed, failed, skipped = result_counts(row)
        counts = {'passed': passed, 'failed': failed, 'ignored': skipped}
        if passed + failed + skipped != len(names) or skipped != len(ignored):
            errors.append('full test count differs from compiled discovery')
        statuses = test_statuses(log)
        if set(statuses) != names:
            errors.append('actual test identities differ from compiled discovery')
        actual = {'passed': sum(value == 'ok' for value in statuses.values()),
                  'failed': sum(value == 'FAILED' for value in statuses.values()),
                  'ignored': sum(value == 'ignored' for value in statuses.values())}
        if actual != counts:
            errors.append('actual statuses differ from test result summary')
        if {key for key, value in statuses.items() if value == 'ignored'} != ignored:
            errors.append('actual ignored identities differ from compiled discovery')
    except RuntimeError as error:
        errors.append(str(error))
    required_results = {test: statuses.get(test) for test in sorted(required)}
    passed_gate = (not errors and row['exit'] == 0 and counts is not None and
                   counts['failed'] == 0 and counts['passed'] > 0 and
                   all(value == 'ok' for value in required_results.values()))
    save(name + '-selection.json', {'required': sorted(required),
         'required_results': required_results,
         'discovered': len(names), 'ignored_not_executed': sorted(ignored),
         'result_counts': counts, 'verification_errors': errors, 'passed': passed_gate})
    return passed_gate


def qualify(spec_path, output):
    global ROOT, OUT, ENV, EXPECTED
    repo = Path(__file__).resolve().parents[1]
    OUT = output.resolve()
    if OUT == repo or OUT.is_relative_to(repo):
        raise RuntimeError('qualification output must be outside the checkout')
    OUT.mkdir(parents=True, exist_ok=False)
    spec_path = spec_path.resolve()
    if not spec_path.is_relative_to(repo):
        raise RuntimeError('qualification spec must be in the checkout')
    spec = json.loads(spec_path.read_text())
    if spec.get('schema') != 1 or spec.get('kind') != 'client':
        raise RuntimeError('unsupported client qualification spec')
    if platform.machine() != 'x86_64':
        raise RuntimeError('this candidate contract is x86_64 only')
    directory = spec_path.parent
    base = read_manifest(directory, spec['base_inputs'])
    baseline = read_manifest(directory, spec['baseline_inputs'])
    fixed = read_manifest(directory, spec['fixed_inputs'])
    controls_path = read_asset(directory, spec['controls'])
    controls = json.loads(controls_path.read_text())
    baseline_patch = materialize_patch(directory, spec['baseline_patch'])
    fixed_patch = materialize_patch(directory, spec['fixed_delta'])
    # All assets and current pinned checkout bytes are verified before copying
    # or modifying source. The original checkout is never a Cargo workspace.
    source_bytes = {}
    for name, checksum in base.items():
        path = repo / name
        if path.is_symlink() or not path.resolve().is_relative_to(repo) or not path.is_file() or digest(path) != checksum:
            raise RuntimeError('checkout does not match the pinned base: ' + name)
        source_bytes[name] = path
    ROOT = OUT / 'source'
    ROOT.mkdir()
    for name, path in source_bytes.items():
        target = ROOT / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    save('copied-source-modes.json', {'scope': 'Observed checkout permission bits preserved by copy2; no UID/GID/ACL qualification', 'modes': {name: stat.S_IMODE((ROOT / name).stat().st_mode) for name in base}})
    ENV = os.environ.copy()
    ENV.update(CARGO_TARGET_DIR=str(OUT.parent / (OUT.name + '-native-target')),
               CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0',
               CARGO_PROFILE_TEST_DEBUG='0', CARGO_TERM_COLOR='never', PYTHONDONTWRITEBYTECODE='1')
    for key in ('RUST_TEST_THREADS', 'RUST_MIN_STACK', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER'):
        ENV.pop(key, None)
    ENV['RUST_MIN_STACK'] = str(8 * 1024 * 1024)
    resource.setrlimit(resource.RLIMIT_CORE, (0, resource.getrlimit(resource.RLIMIT_CORE)[1]))
    EXPECTED = base
    (OUT / 'assets').mkdir(exist_ok=True)
    for path in [Path(__file__), Path(__file__).with_name('candidate-command-ownership.py')]:
        shutil.copy2(path, OUT / 'assets' / path.name)
    save('contract.json', {'spec_sha256': digest(spec_path), 'spec': spec, 'driver_sha256': digest(Path(__file__)),
         'checkout_commit': subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo, capture_output=True, text=True, check=True).stdout.strip(),
         'native_target_outside_output': ENV['CARGO_TARGET_DIR'], 'architecture': platform.machine(),
         'ownership_helper_sha256': digest(Path(__file__).with_name('candidate-command-ownership.py')), 'core_dump_soft_limit': 0, 'memory_counter_status': 'known' if memory() is not None else 'unknown'})
    apply_patch('baseline-apply', baseline_patch, base, baseline)
    retain_source('baseline')
    for name, command in [('rustc', ['rustc', '+stable', '-Vv']), ('cargo', ['cargo', '+stable', '-V'])]:
        row = run('toolchain-' + name, command, timeout=60)
        if row['exit']:
            raise RuntimeError('toolchain identity failed')
    baseline_targets = sorted({case['target'] for case in spec['baseline_cases']})
    for target in set(baseline_targets) | set(spec['fixed_full_targets']):
        if not re.fullmatch('[A-Za-z0-9_]+', target):
            raise RuntimeError('unsafe Cargo test target')
    build('baseline-build', baseline_targets)
    baseline_results = []
    baseline_ok = True
    for index, case in enumerate(spec['baseline_cases']):
        name = 'baseline-case-' + str(index + 1).zfill(2)
        names = listing(name + '-discovery', 'baseline-build', case['target'])
        ignored = listing(name + '-ignored', 'baseline-build', case['target'], ignored=True)
        if case['test'] not in names or case['test'] in ignored or not ignored <= names:
            raise RuntimeError('baseline control missing or ignored')
        row = execute(name, 'baseline-build', case['target'], [case['test'], '--exact'], timeout=120)
        passed, failed, skipped = result_counts(row)
        if passed + failed != 1 or skipped:
            raise RuntimeError('baseline control selected no executed test')
        expected = case['expect']
        matches = expected == 'observe' or (expected == 'pass' and row['exit'] == 0 and passed == 1) or (expected == 'fail' and row['exit'] == 101 and failed == 1)
        if case.get('failure_regex') and expected == 'fail':
            matches = matches and re.search(case['failure_regex'], (OUT / (name + '.log')).read_text()) is not None
        baseline_ok = baseline_ok and matches
        baseline_results.append({'case': case, 'exit': row['exit'], 'matches_expectation': matches, 'receipt': name + '.json'})
    save('baseline-results.json', {'all_expectations_matched': baseline_ok, 'cases': baseline_results})
    apply_patch('fixed-apply', fixed_patch, baseline, fixed)
    retain_source('fixed')
    gates = []
    for name, command in [('format', ['cargo', '+stable', 'fmt', '--all', '--', '--check']),
                          ('clippy', ['cargo', '+stable', 'clippy', '--locked', '--all-targets', '--all-features', '--', '-D', 'warnings'])]:
        row = run(name, command, timeout=1200, disk_floor=COMPILE_FLOOR)
        gates.append({'name': name, 'passed': row['exit'] == 0, 'receipt': name + '.json'})
    required = {'partitionline': {row['test'] for row in controls['library']}}
    for row in controls['integrations']:
        if not re.fullmatch('[A-Za-z0-9_]+', row['target']):
            raise RuntimeError('unsafe Cargo test target')
        required.setdefault(row['target'], set()).add(row['test'])
    targets = sorted(set(spec['fixed_full_targets']) | set(required))
    for lane, all_features in [('default', False), ('all-features', True)]:
        build_name = lane + '-build'
        build(build_name, targets, all_features)
        for target in targets:
            name = lane + '-' + target
            passed = full_target(name, build_name, target, required.get(target, set()))
            gates.append({'name': name, 'passed': passed, 'receipt': name + '.json'})
    summary = {'passed': baseline_ok and all(row['passed'] for row in gates), 'baseline_expectations_matched': baseline_ok,
               'required_library_controls': len(required['partitionline']),
               'required_integration_controls': sum(len(value) for key, value in required.items() if key != 'partitionline'),
               'gates': gates, 'limits': spec['limits'], 'production_ready': False, 'fastest_claim_qualified': False,
               'scope': 'Full runs of chosen binaries in default/all-feature lanes; this is not the ordinary whole-main all-target runtime matrix, a release, external broker qualification, or performance ranking.'}
    save('summary.json', summary)
    return 0 if summary['passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        return qualify(args.spec, args.output)
    except Exception as error:
        if OUT is not None and OUT.is_dir():
            path = OUT / 'fatal.json'
            if not path.exists():
                path.write_text(json.dumps({'error_type': type(error).__name__, 'message': str(error),
                                           'production_ready': False, 'fastest_claim_qualified': False}, indent=2) + '\n')
        print(str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
