"""Prepared runner. Execution requires root's actual-pin CPU/disk lease."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import threading
import time


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def inventory(source):
    result = {}
    for directory, subdirectories, files in os.walk(source):
        if Path(directory) == source and '.git' in subdirectories:
            subdirectories.remove('.git')
        for name in files:
            path = Path(directory) / name
            relative = str(path.relative_to(source))
            if relative == '.git':
                continue
            assert not path.is_symlink(), relative
            data = path.read_bytes()
            result[relative] = {'sha256': sha(data), 'bytes': len(data),
                                'git_blob': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest(),
                                'mode': stat.S_IMODE(path.stat().st_mode)}
    return result


def git_expected(repo, pin):
    subprocess.run(['git', 'cat-file', '-e', pin + '^{commit}'], cwd=repo, check=True)
    raw = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', pin], cwd=repo)
    expected = {}
    for entry in raw.split(b'\0'):
        if not entry:
            continue
        header, path = entry.split(b'\t', 1)
        mode, kind, blob = header.decode().split()
        assert kind == 'blob' and mode in {'100644', '100755'}, (mode, kind, path)
        expected[path.decode()] = {'git_blob': blob, 'mode': 0o700 if mode == '100755' else 0o600}
    return expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--pin', required=True)
    parser.add_argument('--git-repo', default='/workspace/partitionline')
    parser.add_argument('--origin-receipt', required=True)
    parser.add_argument('--origin-sha256', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--toolchain', choices=['stable', '1.85.0'], required=True)
    parser.add_argument('--features', choices=['default', 'all'], required=True)
    parser.add_argument('--target-retention-receipt')
    parser.add_argument('--target-retention-sha256')
    args = parser.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.pin), 'Actual pushed pin required'
    source = Path(args.source).resolve()
    target = Path(args.target).resolve()
    out = Path(args.out).resolve()
    assert source != Path(args.git_repo).resolve(), 'Do not build mutable WORK repository'
    assert not target.is_relative_to(source) and not out.is_relative_to(source)
    if target.exists() and any(target.rglob('*')):
        assert args.target_retention_receipt and args.target_retention_sha256, 'Retain all owned cache ELFs before mandatory package clean'
        assert sha(Path(args.target_retention_receipt).read_bytes()) == args.target_retention_sha256
    assert sha(Path(args.origin_receipt).read_bytes()) == args.origin_sha256
    assert shutil.disk_usage('/workspace').free >= 1150 * 1024 * 1024, 'Initial headroom guard'
    assert not out.exists(), 'Preserve previous executed history; use a fresh output directory'
    out.mkdir(parents=True, mode=0o700)
    expected = git_expected(args.git_repo, args.pin)
    initial = inventory(source)
    assert set(initial) == set(expected), 'Exact Git code-file path set'
    for path, contract in expected.items():
        assert initial[path]['git_blob'] == contract['git_blob'], path
        assert initial[path]['mode'] == contract['mode'], path
    raw = (json.dumps(initial, sort_keys=True, indent=2) + '\n').encode()
    with (out / 'complete-source.json.gz').open('wb') as destination:
        with gzip.GzipFile(fileobj=destination, mode='wb', filename='', mtime=0) as gz:
            gz.write(raw)
    write(out / 'origin.json', {'pin': args.pin, 'source': str(source),
                              'origin_receipt': args.origin_receipt, 'origin_sha256': args.origin_sha256,
                              'code_files': len(initial), 'full_modes': '0600/0700 actual baseline',
                              'Git_metadata_exclusion': 'Only root .git metadata; all Git code blobs are checked',
                              'raw_manifest_sha256': sha(raw), 'raw_manifest_bytes': len(raw),
                              'prepared_runner_executed_on_root_lease_only': True})

    def guard(label):
        current = inventory(source)
        mismatch = sorted(path for path in set(current) | set(initial) if current.get(path) != initial.get(path))
        receipt = {'label': label, 'pin': args.pin, 'checked_files': len(current),
                   'complete_initial_manifest_sha256': sha(raw),
                   'exact_set_bytes_full_modes_unchanged': not mismatch, 'mismatches': mismatch}
        write(out / (label + '.source-guard.json'), receipt)
        assert not mismatch, mismatch

    tools = Path('/workspace/work/rustup/toolchains') / (args.toolchain + '-x86_64-unknown-linux-gnu/bin')
    for tool in ['cargo', 'rustc', 'rustdoc', 'rustfmt', 'clippy-driver']:
        assert (tools / tool).is_file(), tool
    environment = {**os.environ, 'CARGO_HOME': '/workspace/work/cargo',
                   'RUSTUP_HOME': '/workspace/work/rustup', 'CARGO_TARGET_DIR': str(target),
                   'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0',
                   'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
                   'RUSTC': str(tools / 'rustc'), 'RUSTDOC': str(tools / 'rustdoc'),
                   'RUSTFMT': str(tools / 'rustfmt'), 'RUSTFLAGS': '',
                   'PATH': str(tools) + ':/workspace/work/cargo/bin:' + os.environ.get('PATH', '')}
    feature = ['--all-features'] if args.features == 'all' else []
    cargo = str(tools / 'cargo')
    commands = [
        ('format-first', ['fmt', '--all', '--', '--check']),
        ('force-owned-package-recompile', ['clean', '--package', 'partitionline']),
        ('strict-library-and-owned-tests', ['clippy', '--locked', '--lib', '--test', 'sticky_partitioner', '--test', 'client_api', *feature, '--', '-D', 'warnings']),
        ('actual-partitioner-library-tests', ['test', '--locked', '--lib', *feature, 'partitioner::', '--', '--nocapture']),
        ('actual-sticky-socket-tests', ['test', '--locked', '--test', 'sticky_partitioner', *feature, '--', '--nocapture']),
        ('existing-custom-keyed-routing', ['test', '--locked', '--test', 'client_api', *feature, 'custom_partitioner_pins_keyed_records', '--', '--exact', '--nocapture']),
        ('additive-descriptor-and-default-routing', ['test', '--locked', '--test', 'client_api', *feature, 'sticky_partitioner_descriptor_is_additive_and_default_routing_stays_compatible', '--', '--exact', '--nocapture']),
    ]
    for index, (label, arguments) in enumerate(commands):
        prefix = f'{index:02}-{label}'
        guard(prefix + '-before')
        command = ['taskset', '-c', '0,1', cargo, *arguments]
        write(out / (prefix + '.command.json'), {'command': command, 'cwd': str(source),
                                               'pin': args.pin, 'toolchain': args.toolchain,
                                               'feature_graph': args.features,
                                               'environment': {k: environment[k] for k in ['CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTC', 'RUSTDOC', 'RUSTFMT', 'RUSTFLAGS']}})
        samples = []
        stopped = threading.Event()
        breached = threading.Event()
        with (out / (prefix + '.stdout')).open('wb') as stdout, (out / (prefix + '.stderr')).open('wb') as stderr:
            process = subprocess.Popen(command, cwd=source, env=environment, stdout=stdout, stderr=stderr, start_new_session=True)

            def monitor():
                while not stopped.wait(0.1):
                    free = shutil.disk_usage('/workspace').free
                    samples.append({'monotonic_ns': time.monotonic_ns(), 'free_bytes': free})
                    if free < 350 * 1024 * 1024:
                        breached.set()
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        return

            thread = threading.Thread(target=monitor, daemon=True)
            thread.start()
            try:
                exit_code = process.wait(timeout=1800)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    exit_code = process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    exit_code = process.wait(timeout=10)
                breached.set()
            finally:
                stopped.set()
                thread.join()
        (out / (prefix + '.exit')).write_text(str(exit_code) + '\n')
        write(out / (prefix + '.disk-monitor.json'), {'samples': samples, 'floor_bytes': 350 * 1024 * 1024,
                                                    'resource_stop': breached.is_set()})
        # Each test log identifies the actual executed binary. Copy exact bytes now,
        # before later profile/toolchain commands can overwrite Cargo outputs.
        logs = (out / (prefix + '.stdout')).read_text(errors='replace') + (out / (prefix + '.stderr')).read_text(errors='replace')
        binaries = re.findall(r'Running [^\n]*\((/[^\n()]+)\)', logs)
        retained = {}
        for binary in sorted(set(binaries)):
            path = Path(binary)
            assert path.is_file() and path.read_bytes().startswith(b'\x7fELF'), binary
            data = path.read_bytes()
            destination = out / 'executed-elfs' / (sha(data) + '.elf.gz')
            destination.parent.mkdir(mode=0o700, exist_ok=True)
            if not destination.exists():
                with destination.open('wb') as sink:
                    with gzip.GzipFile(fileobj=sink, mode='wb', filename='', mtime=0) as gz:
                        gz.write(data)
            assert gzip.decompress(destination.read_bytes()) == data
            retained[binary] = {'sha256': sha(data), 'bytes': len(data),
                                'mode': stat.S_IMODE(path.stat().st_mode),
                                'archive': str(destination), 'archive_sha256': sha(destination.read_bytes())}
        write(out / (prefix + '.executed-elfs.json'), retained)
        guard(prefix + '-after')
        if exit_code != 0 or breached.is_set():
            raise SystemExit(f'Actual command stopped/failed: {prefix}; preserve logs/source/ELFs; no candidate pass')
        if label == 'force-owned-package-recompile':
            leftovers = [str(path) for pattern in ['**/libpartitionline-*.rlib', '**/libpartitionline-*.rmeta', '**/.fingerprint/partitionline-*'] for path in target.glob(pattern)]
            assert not leftovers, ('Owned package output survived mandatory clean', leftovers)
        if arguments[0] == 'test':
            passed = [int(value) for value in re.findall(r'test result: ok\. (\d+) passed;', logs)]
            assert passed and all(value > 0 for value in passed), 'Actual nonzero test results required'
    write(out / 'receipt.json', {'pin': args.pin, 'toolchain': args.toolchain,
                                'feature_graph': args.features, 'commands': len(commands),
                                'disposition': 'focused executed pass only; not full client qualification or performance',
                                'full_cache_inventory_and_verified_retention_required_before_any_clean': True})


if __name__ == '__main__':
    main()
