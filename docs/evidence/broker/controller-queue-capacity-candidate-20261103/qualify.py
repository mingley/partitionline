#!/usr/bin/env python3
"""Run source-pinned controller queue controls on a managed x86 host."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone

SPEC_SHA = '3ee391a2802ac8620f30496ca138026453b620231e55e1a48fe7a61ec0fc80de'
BASE = '45cbff24de76490594970fc962cd04e7be9fafb1'
CAPSULE = Path(__file__).resolve().parent
NEGATIVE = 'raft::protocol::controller_queue_capacity_tests::queued_capacity_preserves_bytes_and_reuses_fitting_buffers'
PREFIX = 'raft::protocol::controller_queue_capacity_tests::'
QUEUE = [
    PREFIX + 'queued_capacity_preserves_bytes_and_reuses_fitting_buffers',
    PREFIX + 'full_queue_length_and_stop_results_are_preserved',
    PREFIX + 'canceling_copy_leaves_no_reserved_slot_or_queued_request',
    PREFIX + 'stop_during_copy_prevents_late_admission',
]
WITNESS = 'transport::read_ahead_tests::a_maximum_buffered_frame_keeps_more_than_the_body_limit'
PREFETCH = 'transport::controller_prefetch_queue_tests::prefetched_limit_frame_reaches_real_controller'
TCP = 'controller_reuses_connection_with_over_reserved_requests_and_response_spare'
REGISTRY = 'tests/conformance/broker/implemented-api-versions.json'
PROTOCOL = 'partitionline-broker/src/raft/protocol.rs'
TRANSPORT = 'partitionline-broker/src/transport.rs'
TEST = 'partitionline-broker/tests/raft_protocol.rs'
FLOOR = 1536 * 1024**2


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(value, message):
    if not value:
        raise ValueError(message)


def utc():
    return datetime.now(timezone.utc).isoformat()


def validated_output(output, env, system=None, machine=None, free=None):
    """Reject wrong hosts, unsafe paths and low disk before creating files."""
    require(env.get('GITHUB_ACTIONS') == 'true' and env.get('RUNNER_ENVIRONMENT') == 'github-hosted',
            'disposable GitHub-hosted Actions runner required')
    require((system or sys.platform) == 'linux' and (machine or platform.machine()) == 'x86_64',
            'Linux x86_64 required')
    temp_text = env.get('RUNNER_TEMP', '')
    workspace_text = env.get('GITHUB_WORKSPACE', '')
    require(bool(temp_text) and bool(workspace_text), 'missing runner directories')
    temp = Path(temp_text)
    workspace = Path(workspace_text)
    require(temp.is_absolute() and temp.is_dir() and not temp.is_symlink()
            and temp.resolve(strict=True) == temp, 'unsafe runner temp')
    require(workspace.is_absolute() and workspace.is_dir() and not workspace.is_symlink()
            and workspace.resolve(strict=True) == workspace, 'unsafe workspace')
    require(not temp.is_relative_to(workspace) and not workspace.is_relative_to(temp),
            'runner temp overlaps workspace')
    require(temp.stat().st_dev == workspace.stat().st_dev,
            'workspace and runner temp must share the monitored filesystem')
    require(output.is_absolute() and output == temp/'controller-queue-qualification'
            and not output.exists() and not output.is_symlink(), 'output must be the new runner temp child')
    for suffix in ('-native-target', '-candidate-target'):
        target = temp/(output.name + suffix)
        require(not target.exists() and not target.is_symlink(), 'build target already exists')
    available = shutil.disk_usage(temp).free if free is None else free
    require(available > FLOOR + 128 * 1024**2, 'less than floor plus extraction headroom')
    return output


def run_git(source, patch, reverse=False):
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    env['LC_ALL'] = 'C'
    args = ['git', 'apply'] + (['--reverse'] if reverse else [])
    for check in (True, False):
        subprocess.run([*args, *(['--check'] if check else []), str(patch)], cwd=source,
                       env=env, check=True, capture_output=True, timeout=120)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = validated_output(args.output, os.environ)
    require(not output.is_relative_to(CAPSULE) and not CAPSULE.is_relative_to(output), 'unsafe output')
    output.mkdir()
    report = {'passed': False, 'started_utc': utc(), 'base_commit': BASE,
              'scope': 'Controller queued Vec capacity functional qualification only; '
                       'not process memory, production readiness or performance leadership.',
              'trigger_sha': os.environ.get('GITHUB_SHA'),
              'run_id': os.environ.get('GITHUB_RUN_ID'),
              'runner': platform.platform(), 'machine': platform.machine(),
              'cpu_count': os.cpu_count(), 'errors': [], 'commands': []}
    runner = None
    try:
        spec_path = CAPSULE/'spec.json'
        require(not spec_path.is_symlink() and sha(spec_path) == SPEC_SHA, 'spec pin mismatch')
        spec = json.loads(spec_path.read_text())
        require(spec['base_commit'] == BASE, 'base commit mismatch')
        expected_assets = {'base-files.json', 'base-source.tar.gz', 'baseline-registry.patch',
            'baseline-registry.sha256', 'candidate-command-ownership.py', 'candidate-registry.patch',
            'candidate-registry.sha256', 'candidate.patch', 'controls-manifest.json',
            'controls-only.patch', 'legacy-driver.py', 'manifest.json', 'prefetch-control.patch',
            'prefetch-manifest.json', 'verify-git-tree.py'}
        require(set(spec['assets']) == expected_assets, 'asset inventory changed')
        for name, pin in spec['assets'].items():
            require('/' not in name and '\\' not in name and name not in ('.', '..'), 'unsafe asset name')
            path = CAPSULE/name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == pin['bytes']
                    and sha(path) == pin['sha256'], 'asset mismatch: '+name)
        saved = output/'assets'
        saved.mkdir()
        for name in ['spec.json', 'qualify.py', 'test-static.py', *sorted(expected_assets)]:
            shutil.copyfile(CAPSULE/name, saved/name)
        module_spec = importlib.util.spec_from_file_location('legacy_broker_guard', CAPSULE/'legacy-driver.py')
        legacy = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(legacy)
        require(legacy.FLOOR == FLOOR, 'resource floor changed')
        base = legacy.load(CAPSULE/'base-files.json')
        manifest = legacy.load(CAPSULE/'manifest.json')
        controls = legacy.load(CAPSULE/'controls-manifest.json')
        prefetch = legacy.load(CAPSULE/'prefetch-manifest.json')
        require(manifest['base_commit'] == controls['base_commit'] == BASE, 'manifest base mismatch')
        require(manifest['patch_sha256'] == sha(CAPSULE/'candidate.patch')
                and controls['patch_sha256'] == sha(CAPSULE/'controls-only.patch')
                and prefetch['patch_sha256'] == sha(CAPSULE/'prefetch-control.patch'), 'patch pin mismatch')
        source = output/'source'
        source.mkdir()
        legacy.extract_source(CAPSULE/'base-source.tar.gz', source, base)
        legacy.verify_source(source, base)
        for name in (PROTOCOL, TRANSPORT, TEST):
            require(base[name]['sha256'] == manifest['files'][name]['before_sha256']
                    == controls['files'][name]['before_sha256'], 'source preimage mismatch: '+name)

        def apply_variant(label, patch, pins):
            run_git(source, CAPSULE/patch)
            run_git(source, CAPSULE/(label+'-registry.patch'))
            run_git(source, CAPSULE/'prefetch-control.patch')
            expected_hashes = {name: pins['files'][name]['after_sha256'] for name in (PROTOCOL, TRANSPORT, TEST)}
            require(expected_hashes[TRANSPORT] == prefetch['before_sha256'], 'prefetch preimage mismatch')
            expected_hashes[TRANSPORT] = prefetch['after_sha256']
            expected_hashes[REGISTRY] = (CAPSULE/(label+'-registry.sha256')).read_text().strip()
            result = dict(base)
            for name, expected_hash in expected_hashes.items():
                path = source/name
                require(sha(path) == expected_hash, 'source postimage mismatch: '+name)
                result[name] = {'sha256': expected_hash, 'bytes': path.stat().st_size,
                                'mode': base[name]['mode']}
            legacy.restore_patched_source_modes(source, result, set(expected_hashes),
                                                output/(label+'-source-modes.json'))
            return result

        baseline = apply_variant('baseline', 'controls-only.patch', controls)
        runner = legacy.Qualification(output, source, baseline)
        for key in tuple(runner.env):
            if key.startswith('GIT_'):
                del runner.env[key]
        workspace = Path(os.environ['GITHUB_WORKSPACE']).resolve()
        trigger = runner.run('checkout-sha', ['git', '-C', str(workspace), 'rev-parse', 'HEAD']).strip()
        require(trigger == os.environ['GITHUB_SHA'], 'trigger checkout SHA mismatch')
        runner.run('fetch-base', ['git', '-C', str(workspace), 'fetch', '--no-tags', '--depth=1',
                   '--filter=blob:none', 'origin', BASE], timeout=900)
        runner.run('verify-git-tree', [sys.executable, '-B', str(CAPSULE/'verify-git-tree.py'),
                   '--checkout', str(workspace), '--manifest', str(CAPSULE/'base-files.json'),
                   '--archive', str(CAPSULE/'base-source.tar.gz')], timeout=900)
        runner.run('rustc-version', ['rustc', '+stable', '-Vv'])
        runner.run('cargo-version', ['cargo', '+stable', '-V'])
        runner.run('openssl-version', ['openssl', 'version'])

        def exact(label, name, profile, integration=False, negative=False):
            command = ['cargo', '+stable', 'test', '--locked', '--manifest-path',
                       'partitionline-broker/Cargo.toml']
            command += ['--test', 'raft_protocol'] if integration else ['--lib']
            if profile == 'all-features':
                command.append('--all-features')
            command += [name, '--', '--exact', '--test-threads=1']
            text = runner.run(label, command, timeout=1200, expected_exit=101 if negative else 0)
            counts = legacy.test_counts(text)
            require(counts == [(0, 1, 0) if negative else (1, 0, 0)], 'wrong exact test count: '+label)
            status = 'FAILED' if negative else 'ok'
            require(re.search(r'^test '+re.escape(name)+r' \.\.\. '+status+r'$', text, re.M),
                    'missing exact test outcome: '+label)
            if negative:
                lines = (source/PROTOCOL).read_text().splitlines()
                needle = 'assert!(job.request.capacity() + job.spare.capacity() <= 64);'
                sites = [index + 1 for index, line in enumerate(lines) if needle in line]
                require(len(sites) == 1, 'baseline assertion location changed')
                section = re.search(r'^---- '+re.escape(name)+r' stdout ----\s*$(.*?)(?=^failures:\s*$)',
                                    text, re.M | re.S)
                require(section is not None, 'baseline failure body missing')
                panic = r"thread '"+re.escape(name)+r"'(?: \(\d+\))? panicked at .*src/raft/protocol\.rs:"+str(sites[0])+r":\d+:"
                require(re.search(panic, section.group(1)) is not None, 'baseline failed at wrong site')
                normalized = re.sub(r'\s+', '', section.group(1))
                require('assertionfailed:job.request.capacity()+job.spare.capacity()<=64' in normalized,
                        'baseline failure did not witness queue capacity')

        exact('baseline-expected-negative', NEGATIVE, 'default', negative=True)
        exact('baseline-buffered-witness', WITNESS, 'default')
        exact('baseline-prefetch-controller', PREFETCH, 'default')
        exact('baseline-tcp-reuse', TCP, 'default', integration=True)
        run_git(source, CAPSULE/'prefetch-control.patch', reverse=True)
        run_git(source, CAPSULE/'baseline-registry.patch', reverse=True)
        run_git(source, CAPSULE/'controls-only.patch', reverse=True)
        legacy.restore_patched_source_modes(source, base, {PROTOCOL, TRANSPORT, TEST, REGISTRY},
                                            output/'base-restored-modes.json')
        fixed = apply_variant('candidate', 'candidate.patch', manifest)
        runner.expected = fixed
        runner.env['CARGO_TARGET_DIR'] = str(output.parent/(output.name+'-candidate-target'))
        for profile in ('default', 'all-features'):
            for index, name in enumerate(QUEUE):
                exact(f'candidate-{profile}-queue-{index}', name, profile)
            exact(f'candidate-{profile}-buffered-witness', WITNESS, profile)
            exact(f'candidate-{profile}-prefetch-controller', PREFETCH, profile)
            exact(f'candidate-{profile}-tcp-reuse', TCP, profile, integration=True)
            full = ['cargo', '+stable', 'test', '--locked', '--manifest-path',
                    'partitionline-broker/Cargo.toml', '--all-targets', '--no-fail-fast']
            if profile == 'all-features':
                full.append('--all-features')
            runner.run(f'candidate-{profile}-all-targets', full, timeout=1800)
        runner.run('source-format', ['cargo', '+stable', 'fmt', '--manifest-path',
                   'partitionline-broker/Cargo.toml', '--check'])
        runner.run('strict-clippy', ['cargo', '+stable', 'clippy', '--locked', '--manifest-path',
                   'partitionline-broker/Cargo.toml', '--all-targets', '--all-features', '--', '-D', 'warnings'], timeout=1800)
        runner.env['RUSTDOCFLAGS'] = '-D warnings'
        runner.run('strict-rustdoc', ['cargo', '+stable', 'doc', '--locked', '--manifest-path',
                   'partitionline-broker/Cargo.toml', '--all-features', '--no-deps'], timeout=1200)
        for profile in ('default', 'all-features'):
            command = ['cargo', '+stable', 'test', '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--doc']
            if profile == 'all-features':
                command.append('--all-features')
            runner.run('doctests-'+profile, command, timeout=1200)
        runner.run('api-inventory', ['python3', '-B', 'scripts/check-broker-api-matrix.py',
                   '--report', str(output/'api-inventory.json')], timeout=900)
        runner.run('api-inventory-controls', ['python3', '-B', '-m', 'unittest', 'discover', '-s',
                   'tests/ci', '-p', 'test_broker_api_matrix.py', '-v'], timeout=900)
        report['passed'] = True
    except Exception as error:
        report['errors'].append(f'{type(error).__name__}: {error}')
    finally:
        if runner is not None:
            report['commands'] = runner.records
        report['finished_utc'] = utc()
        (output/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
