#!/usr/bin/env python3
"""Run the frozen AddPartitions qualification on a clean x86_64 checkout.

This runner never invokes the separate v18 or ReadCommitted validations.
Every command has a process-group timeout and retains its raw combined output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import types
import time
import traceback

BASE_COMMIT = '45cbff24de76490594970fc962cd04e7be9fafb1'
BASE_SOURCE = '4f461c00864e3da5e20925796c1f489580a4046202cffd86fefeb64cbd145a32'
POST_SOURCE = '9154ec1b502bc4c639b6a1f6cb30568f72a91f442123a7f6d1378e46ea2e93ff'
TXN_SOURCE = '4e5a224f22301d2c9fa6b388e94871223e300869facef5b28b63bfc73691d949'
ERROR_SOURCE = '014c33e7716b004aa22ec46b4b7e0e9e67b54d982b69b8f152ee02eb421d45f2'
PATCH = '5e356bd2d7616354de4a3e60ebd23bd12104bc4a9d8a29cc65cbd258f38d8cfe'
WITNESS = '539c1df76bc8114792699c09d65ff0986b1afddfbcc2803ec3cf115e783f4a6c'
OWNERSHIP = '34ddbf04a4bbead974fad4a451d30c7a56818248bd5b58c88c87bdfc41896da9'
MODULE = (
    b'\n#[cfg(test)]\n'
    b'#[path = "producer/add_partitions_regression_witness.rs"]\n'
    b'mod add_partitions_regression_witness;\n'
)
MIN_FREE = 1536 * 1024 * 1024  # 1.5 GiB on every monitored filesystem
WITNESSES = (
    ('missing_response_must_not_mark_partitions_added', 'WITNESS_MISSING_EXPECTS_PROTOCOL'),
    ('mixed_load_and_fence_must_record_fence', 'WITNESS_FENCE_EXPECTS_BROKER'),
)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pinned(path: Path, expected: str) -> bytes:
    data = path.read_bytes()
    actual = digest(data)
    if actual != expected:
        raise RuntimeError(f'{path}: expected SHA-256 {expected}, found {actual}')
    return data


class DiskFloorError(RuntimeError):
    pass


def existing_parent(path: Path) -> Path:
    path = path.resolve()
    while not path.exists():
        parent = path.parent
        if parent == path:
            raise RuntimeError(f'no existing parent for {path}')
        path = parent
    return path


def hosted_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('qualification may run only in GitHub Actions')
    workspace = Path(os.environ['GITHUB_WORKSPACE']).resolve(strict=True)
    runner_temp = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
    checkout = workspace / 'base'
    output = runner_temp / 'add-partitions-qualification'
    if (Path(os.path.abspath(args.checkout)) != checkout or checkout.is_symlink()
            or checkout.resolve(strict=True) != checkout):
        raise RuntimeError('checkout must be the non-symlink GITHUB_WORKSPACE/base')
    if (Path(os.path.abspath(args.output)) != output or output.is_symlink()
            or output.resolve(strict=True) != output):
        raise RuntimeError('output must be RUNNER_TEMP/add-partitions-qualification')
    for name in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'):
        if re.fullmatch(r'[0-9]+', os.environ.get(name, '')) is None:
            raise RuntimeError(f'{name} must be a GitHub numeric run identifier')
    return checkout, output, runner_temp


class Runner:
    def __init__(self, checkout: Path, output: Path, runner_temp: Path):
        self.checkout = checkout
        self.output = output
        self.runner_temp = runner_temp
        self.target: Path | None = None
        self.target_fd: int | None = None
        self.environment = os.environ.copy()
        self.ownership = None
        self.output.mkdir(parents=True, exist_ok=True)
        self.steps: list[dict[str, object]] = []
        roots = (
            checkout,
            output,
            runner_temp,
            Path(tempfile.gettempdir()),
            Path(os.environ.get('CARGO_HOME', str(Path.home() / '.cargo'))),
            Path(os.environ.get('RUSTUP_HOME', str(Path.home() / '.rustup'))),
        )
        self.mounts: dict[int, Path] = {}
        for root in roots:
            existing = existing_parent(root)
            self.mounts.setdefault(existing.stat().st_dev, existing)
        self.summary: dict[str, object] = {
            'base_commit': BASE_COMMIT,
            'patch_sha256': PATCH,
            'witness_sha256': WITNESS,
            'ownership_helper_sha256': OWNERSHIP,
            'architecture': platform.machine(),
            'disk_minimum_free_bytes': MIN_FREE,
            'monitored_filesystems': [str(path) for path in self.mounts.values()],
            'platform': platform.platform(),
            'trigger_sha': os.environ.get('GITHUB_SHA'),
            'run_id': os.environ.get('GITHUB_RUN_ID'),
            'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
            'runner_arch': os.environ.get('RUNNER_ARCH'),
            'scope': 'AddPartitions unit and mock-socket controls; not broker interop or performance',
            'steps': self.steps,
            'success': False,
        }
        self.save()

    def save(self) -> None:
        tmp = self.output / 'summary.json.tmp'
        tmp.write_text(json.dumps(self.summary, indent=2) + '\n')
        tmp.replace(self.output / 'summary.json')

    def capacity_sample(self, context: str) -> None:
        samples = {str(path): shutil.disk_usage(path).free for path in self.mounts.values()}
        sample = {'unix_seconds': time.time(), 'context': context, 'free_bytes': samples}
        with (self.output / 'capacity.jsonl').open('a') as stream:
            stream.write(json.dumps(sample, sort_keys=True) + '\n')
        if min(samples.values()) < MIN_FREE:
            raise DiskFloorError(f'free space below {MIN_FREE} bytes: {samples}')

    def prepare(self) -> None:
        helper = Path(__file__).with_name('candidate-command-ownership.py')
        code = pinned(helper, OWNERSHIP)
        module = types.ModuleType('pinned_add_partitions_ownership')
        module.__file__ = str(helper)
        exec(compile(code, str(helper), 'exec'), module.__dict__)
        self.ownership = module
        run_id = os.environ['GITHUB_RUN_ID']
        run_attempt = os.environ['GITHUB_RUN_ATTEMPT']
        target = self.runner_temp / f'add-partitions-cargo-{run_id}-{run_attempt}'
        # mkdir is exclusive, and therefore refuses a pre-existing file,
        # directory or symlink. Keep a directory fd as the ownership anchor.
        os.mkdir(target, 0o700)
        self.target = target
        self.target_fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        identity = os.fstat(self.target_fd)
        current = os.stat(target, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise RuntimeError('owned target changed during initialization')
        self.environment['CARGO_TARGET_DIR'] = str(target)
        self.summary['cargo_target'] = str(target)
        self.summary['cargo_target_device'] = identity.st_dev
        self.summary['cargo_target_inode'] = identity.st_ino
        self.save()

    def cleanup_owned_target(self) -> None:
        if self.target is None or self.target_fd is None:
            return
        if self.target.parent != self.runner_temp or self.target.is_symlink():
            raise RuntimeError('owned target path changed; refusing cleanup')
        try:
            current = os.stat(self.target, follow_symlinks=False)
        except FileNotFoundError:
            raise RuntimeError('owned target disappeared; refusing cleanup') from None
        identity = os.fstat(self.target_fd)
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise RuntimeError('owned target identity changed; refusing cleanup')
        shutil.rmtree(self.target)
        self.summary['owned_target_removed_after_disk_floor'] = True

    def close_target(self) -> None:
        if self.target_fd is not None:
            os.close(self.target_fd)
            self.target_fd = None

    def command(self, name: str, argv: list[str], seconds: int = 1800) -> tuple[int | None, str, bool]:
        self.capacity_sample(f'{name}:before')
        if self.ownership is None or self.target_fd is None:
            raise RuntimeError('command ownership or target was not prepared')
        # The helper is verified on every command and holds the leader PID
        # unreaped until its private session has been cleaned up.
        pinned(Path(__file__).with_name('candidate-command-ownership.py'), OWNERSHIP)
        log = self.output / f'{name}.log'
        started = time.monotonic()
        next_sample = started
        code: int | None = None
        timed_out = False
        disk_floor_breached = False
        interrupted = False
        error = None
        owner = None
        with log.open('wb') as stream:
            stream.write(('$ ' + ' '.join(argv) + '\n').encode())
            stream.flush()
            try:
                with self.ownership.owned_command(
                    argv, cwd=self.checkout, stdout=stream, stderr=subprocess.STDOUT,
                    env=self.environment,
                ) as owner:
                    while not owner.finished():
                        now = time.monotonic()
                        if now - started >= seconds:
                            timed_out = True
                            break
                        if now >= next_sample:
                            try:
                                self.capacity_sample(f'{name}:running')
                            except DiskFloorError:
                                disk_floor_breached = True
                                break
                            next_sample = now + 1.0
                        time.sleep(0.05)
            except Exception as exc:  # retained even when an executable is absent
                error = f'{type(exc).__name__}: {exc}'
                interrupted = isinstance(exc, InterruptedError)
                stream.write((error + '\n').encode())
        receipt = owner.cleanup_receipt if owner is not None else None
        if receipt is not None:
            code = receipt.get('leader_exit')
        try:
            self.capacity_sample(f'{name}:after')
        except DiskFloorError:
            disk_floor_breached = True
        result = {
            'name': name,
            'argv': argv,
            'exit_code': code,
            'timed_out': timed_out,
            'disk_floor_breached': disk_floor_breached,
            'spawn_or_wait_error': error,
            'ownership_cleanup': receipt,
            'unexpected_descendants': bool(receipt and receipt.get('descendants_present_at_close')),
            'duration_seconds': round(time.monotonic() - started, 3),
            'log': log.name,
        }
        self.steps.append(result)
        self.save()
        if interrupted:
            raise InterruptedError(f'{name} interrupted after command cleanup')
        if disk_floor_breached:
            raise DiskFloorError(f'{name} stopped after disk space fell below the floor')
        # The raw log is retained in full; keep parsing memory bounded.
        with log.open('rb') as stream:
            size = log.stat().st_size
            if size > 16 * 1024 * 1024:
                stream.seek(size - 16 * 1024 * 1024)
            contents = stream.read().decode(errors='replace')
        return code, contents, timed_out

    def prerequisite(self, name: str, argv: list[str]) -> str:
        code, output, timeout = self.command(name, argv, 120)
        if code != 0 or timeout or self.steps[-1]['unexpected_descendants'] or self.steps[-1]['spawn_or_wait_error']:
            raise RuntimeError(f'prerequisite {name} failed; see {name}.log')
        return output

    def baseline(self, features: str, test: str, marker: str) -> None:
        name = f'baseline-{features}-{test}'
        full = f'producer::add_partitions_regression_witness::{test}'
        argv = ['cargo', '+stable', 'test', '--locked', '--lib']
        if features == 'all-features':
            argv.append('--all-features')
        argv += [full, '--', '--exact', '--nocapture']
        code, output, timeout = self.command(name, argv)
        valid = (
            not timeout and code == 101
            and not self.steps[-1]['unexpected_descendants']
            and not self.steps[-1]['spawn_or_wait_error']
            and marker in output
            and re.search(r'(?m)^running 1 test$', output) is not None
            and re.search(r'(?m)^test result: FAILED\. 0 passed; 1 failed; 0 ignored;', output) is not None
            and re.search(r'(?m)^\s*' + re.escape(full) + r'\s*$', output) is not None
        )
        self.steps[-1]['expected_baseline_failure_observed'] = valid
        self.save()

    def candidate_test(self, features: str, test: str | None = None) -> None:
        name = f'candidate-{features}-' + (test if test else 'focused')
        if test:
            filter_name = f'producer::add_partitions_regression_witness::{test}'
            count = 1
        else:
            filter_name = 'producer::add_partitions_validation_tests::'
            count = 6
        argv = ['cargo', '+stable', 'test', '--locked', '--lib']
        if features == 'all-features':
            argv.append('--all-features')
        argv.append(filter_name)
        argv.append('--')
        if test:
            argv.append('--exact')
        argv.append('--nocapture')
        code, output, timeout = self.command(name, argv)
        valid = (
            not timeout and code == 0
            and not self.steps[-1]['unexpected_descendants']
            and not self.steps[-1]['spawn_or_wait_error']
            and re.search(r'(?m)^running ' + str(count) + r' tests?$', output) is not None
            and re.search(r'(?m)^test result: ok\. ' + str(count)
                          + r' passed; 0 failed; 0 ignored;', output) is not None
        )
        if test:
            valid = valid and re.search(r'(?m)^test ' + re.escape(filter_name) + r' \.\.\. ok$', output) is not None
        self.steps[-1]['candidate_tests_passed'] = valid
        self.save()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--patch', type=Path, required=True)
    parser.add_argument('--witness', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        checkout, output, runner_temp = hosted_paths(args)
    except Exception:
        sys.stderr.write(traceback.format_exc())
        return 1
    runner = Runner(checkout, output, runner_temp)
    try:
        runner.prepare()
        if platform.machine() != 'x86_64':
            raise RuntimeError('qualification requires an x86_64 host')
        head = runner.prerequisite('checkout-head', ['git', 'rev-parse', 'HEAD']).splitlines()[-1]
        if head != BASE_COMMIT:
            raise RuntimeError(f'checkout HEAD {head} differs from {BASE_COMMIT}')
        status = runner.prerequisite('checkout-status', ['git', 'status', '--porcelain=v1', '--untracked-files=all'])
        # The first line is the command header added by command().
        if status.splitlines()[1:]:
            raise RuntimeError('checkout has pre-existing modifications or untracked files')
        runner.prerequisite('rustc-version', ['rustc', '+stable', '--version', '--verbose'])
        runner.prerequisite('cargo-version', ['cargo', '+stable', '--version'])
        runner.prerequisite('rustfmt-version', ['rustfmt', '+stable', '--version'])
        runner.prerequisite('clippy-version', ['cargo', '+stable', 'clippy', '--version'])
        patch = pinned(args.patch, PATCH)
        witness = pinned(args.witness, WITNESS)
        source = checkout / 'src/producer.rs'
        base = pinned(source, BASE_SOURCE)
        pinned(checkout / 'src/protocol/txn.rs', TXN_SOURCE)
        pinned(checkout / 'src/error.rs', ERROR_SOURCE)
        runner.summary['cargo_lock_sha256'] = digest((checkout / 'Cargo.lock').read_bytes())
        runner.summary['witness_injection_sha256'] = digest(MODULE)
        runner.save()
        witness_path = checkout / 'src/producer/add_partitions_regression_witness.rs'
        if witness_path.exists():
            raise RuntimeError('witness path already exists in base')
        witness_path.parent.mkdir(parents=True, exist_ok=True)
        witness_path.write_bytes(witness)
        source.write_bytes(base + MODULE)
        runner.summary['baseline_source_with_witness_sha256'] = digest(source.read_bytes())
        runner.save()
        for features in ('default', 'all-features'):
            for name, marker in WITNESSES:
                runner.baseline(features, name, marker)
        source.write_bytes(base)
        # Recheck both frozen payloads immediately before applying them.
        if pinned(args.patch, PATCH) != patch or pinned(args.witness, WITNESS) != witness:
            raise RuntimeError('qualification payload changed')
        runner.prerequisite('patch-check', ['git', 'apply', '--check', str(args.patch.resolve())])
        runner.prerequisite('patch-apply', ['git', 'apply', str(args.patch.resolve())])
        candidate = pinned(source, POST_SOURCE)
        source.write_bytes(candidate + MODULE)
        runner.summary['candidate_source_with_witness_sha256'] = digest(source.read_bytes())
        runner.save()
        runner.command('candidate-rustfmt', ['rustfmt', '+stable', '--edition', '2021', '--check', 'src/producer.rs'])
        runner.command('candidate-clippy-all-features', ['cargo', '+stable', 'clippy', '--locked', '--lib', '--all-features', '--', '-D', 'warnings'])
        for features in ('default', 'all-features'):
            runner.candidate_test(features)
            for name, _marker in WITNESSES:
                runner.candidate_test(features, name)
        required_ok = True
        for step in runner.steps:
            if str(step['name']).startswith('baseline-'):
                required_ok = required_ok and step.get('expected_baseline_failure_observed') is True
            elif str(step['name']).startswith('candidate-') and step['name'] not in ('candidate-rustfmt', 'candidate-clippy-all-features'):
                required_ok = required_ok and step.get('candidate_tests_passed') is True
            else:
                required_ok = (required_ok and step.get('exit_code') == 0 and step.get('timed_out') is False
                               and not step.get('unexpected_descendants') and not step.get('spawn_or_wait_error'))
        runner.summary['success'] = required_ok
        runner.save()
        return 0 if required_ok else 1
    except Exception:
        detail = traceback.format_exc()
        (runner.output / 'runner-error.log').write_text(detail)
        runner.summary['error'] = detail
        if isinstance(sys.exc_info()[1], DiskFloorError):
            try:
                runner.cleanup_owned_target()
            except (OSError, RuntimeError) as cleanup_error:
                runner.summary['owned_target_cleanup_error'] = str(cleanup_error)
        runner.summary['success'] = False
        runner.save()
        sys.stderr.write(detail)
        return 1
    finally:
        runner.close_target()


if __name__ == '__main__':
    raise SystemExit(main())
