from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('stack_runner', HERE / 'stack_runner.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
PINS = json.loads((HERE / 'subject-pins.json').read_text())
SOURCE = Path(os.environ.get('PL_STACK_SOURCE_CHECKOUT', '/workspace/partitionline'))


class StackRunnerTest(unittest.TestCase):
    def test_workflow_starts_only_for_selected_main_paths_or_manual_dispatch(self):
        workflow = HERE.parents[3] / '.github/workflows/classic-group-stack-diagnostic.yml'
        text = workflow.read_text()
        trigger = text.split('on:\n', 1)[1].split('\npermissions:', 1)[0]
        self.assertEqual(trigger, """  push:
    branches: [main]
    paths:
      - .github/workflows/classic-group-stack-diagnostic.yml
      - docs/evidence/client/live-stack-hosted-45cb-20261103/**
  workflow_dispatch:
""")
        self.assertIn('max-parallel: 2', text)
        self.assertIn('if: always()', text)
        self.assertIn('retention-days: 30', text)
        self.assertIn('ref: ' + runner.COMMIT, text)

    def test_child_disables_core_and_bounds_single_file_size(self):
        with mock.patch.object(runner.resource, 'setrlimit') as set_limit:
            runner.limits()
        set_limit.assert_any_call(runner.resource.RLIMIT_CORE, (0, 0))
        set_limit.assert_any_call(runner.resource.RLIMIT_FSIZE, (512 * 1024 * 1024, 512 * 1024 * 1024))

    def test_environment_is_default_unless_stack_case_requests_override(self):
        hostile = {'PATH': '/usr/bin', 'RUST_MIN_STACK': '999', 'RUSTFLAGS': '-Cdebug-assertions=no',
                   'KAFKA_BOOTSTRAP': 'external', 'PL_COMPAT_RUN_VERIFIABLE': '1',
                   'CARGO_TARGET_DIR': '/bad', 'GITHUB_OUTPUT': '/bad', 'PL_STACK_RUN_TOKEN': 'evil'}
        with mock.patch.dict(os.environ, hostile, clear=True):
            normal = runner.child_environment('normal', Path('/tmp/pl-stack-normal'), 'a' * 32)
            stacked = runner.child_environment('stack_8m', Path('/tmp/pl-stack-stack_8m'), 'b' * 32)
        self.assertNotIn('RUST_MIN_STACK', normal)
        self.assertNotIn('RUSTFLAGS', normal)
        self.assertNotIn('KAFKA_BOOTSTRAP', normal)
        self.assertNotIn('GITHUB_OUTPUT', normal)
        self.assertEqual(stacked['RUST_MIN_STACK'], '8388608')
        self.assertEqual(normal['PL_COMPAT_RUN_VERIFIABLE'], '0')
        self.assertEqual(normal['PL_STACK_RUN_TOKEN'], 'a' * 32)
        self.assertEqual(normal['CARGO_TARGET_DIR'], '/tmp/pl-stack-target-normal')

    def test_success_requires_test_completion_and_exact_stop_marker(self):
        m = 'PL_STACK_CLASSIC_JOIN_COMPLETE'
        done = 'test result: ok. 1 passed; 0 failed'
        self.assertEqual(runner.classify(0, 'completed', m + '\n' + done), 'classic_join_returned')
        for code, reason, text in [(0, 'completed', m), (0, 'completed', done),
                                   (101, 'completed', m + done), (0, 'wall_timeout', m + done)]:
            self.assertNotEqual(runner.classify(code, reason, text), 'classic_join_returned')
        self.assertEqual(runner.classify(101, 'completed', "thread has overflowed its stack"),
                         'stack_overflow_reported')
        self.assertEqual(runner.classify(101, 'completed', ''), 'failed_or_inconclusive')

    def test_token_validation_happens_before_docker(self):
        with mock.patch.object(runner, 'run') as command:
            with self.assertRaisesRegex(RuntimeError, 'invalid'):
                runner.list_owned('bad')
            command.assert_not_called()

    def test_refuses_delete_without_exact_name_and_label(self):
        token = 'a' * 32
        ident = 'c' * 64
        item = {'Id': ident, 'Name': '/someone-elses-container',
                'Config': {'Labels': {'partitionline.stack.run': token}}}
        results = [mock.Mock(stdout=ident + '\n'), mock.Mock(stdout=json.dumps([item]))]
        with mock.patch.object(runner, 'run', side_effect=results) as command:
            with self.assertRaisesRegex(RuntimeError, 'refusing'):
                runner.cleanup_owned(token)
            self.assertEqual(command.call_count, 2)
            self.assertNotIn('rm', [arg for call in command.call_args_list for arg in call.args[0]])

    def test_cleanup_removes_only_exact_owned_container_then_checks(self):
        token = 'b' * 32
        ident = 'd' * 64
        item = {'Id': ident, 'Name': '/pl-stack-' + token,
                'Config': {'Labels': {'partitionline.stack.run': token}}}
        outputs = [mock.Mock(stdout=ident + '\n'), mock.Mock(stdout=json.dumps([item])),
                   mock.Mock(stdout=''), mock.Mock(stdout='')]
        with mock.patch.object(runner, 'run', side_effect=outputs) as command:
            self.assertEqual(runner.cleanup_owned(token), [ident])
            self.assertEqual(command.call_args_list[2].args[0], ['docker', 'rm', '-f', '--', ident])
            self.assertEqual(command.call_count, 4)

    def test_container_reappearance_and_inspect_failure_are_errors(self):
        token = 'e' * 32
        with mock.patch.object(runner, 'list_owned', side_effect=[['c' * 64], ['c' * 64]]), \
             mock.patch.object(runner, 'run'):
            with self.assertRaisesRegex(RuntimeError, 'remains'):
                runner.cleanup_owned(token)
        with mock.patch.object(runner, 'run', side_effect=subprocess.CalledProcessError(1, ['docker'])):
            with self.assertRaises(subprocess.CalledProcessError):
                runner.list_owned(token)

    def test_bad_checkout_does_not_write_receipt(self):
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(runner, 'validate_paths',
                side_effect=RuntimeError('not a dedicated checkout')):
            output = Path(temp)
            with self.assertRaisesRegex(RuntimeError, 'not a dedicated'):
                runner.execute(Path('/unsafe'), output, 'normal', PINS)
            self.assertFalse((output / 'receipt.json').exists())

    def test_low_space_prevents_preparation_and_broker(self):
        with tempfile.TemporaryDirectory() as temp, \
             mock.patch.object(runner, 'validate_paths'), \
             mock.patch.object(runner, 'check_space', side_effect=RuntimeError('low space')), \
             mock.patch.object(runner, 'prepare') as prepare, \
             mock.patch.object(runner, 'list_owned') as docker:
            output = Path(temp)
            result = runner.execute(Path('/irrelevant'), output, 'normal', PINS)
            self.assertEqual(result['outcome'], 'collector_error')
            self.assertIn('low space', result['error'])
            prepare.assert_not_called()
            docker.assert_not_called()

    def test_only_dedicated_clean_pinned_checkout_is_accepted(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            workspace, tempdir = base / 'workspace', base / 'temp'
            subject, output = workspace / 'subject', tempdir / 'pl-stack-normal'
            (subject / '.git').mkdir(parents=True)
            output.mkdir(parents=True)
            env = {'GITHUB_ACTIONS': 'true', 'GITHUB_WORKSPACE': str(workspace),
                   'RUNNER_TEMP': str(tempdir), 'STACK_CASE': 'normal'}
            def fake_git(path, *args):
                return {('rev-parse', '--show-toplevel'): str(subject),
                        ('rev-parse', 'HEAD'): runner.COMMIT,
                        ('status', '--porcelain=v1', '--untracked-files=normal'): ''}[args]
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(runner.platform, 'system', return_value='Linux'), \
                 mock.patch.object(runner.platform, 'machine', return_value='x86_64'), \
                 mock.patch.object(runner, 'git', side_effect=fake_git):
                runner.validate_paths(subject, output)
                with self.assertRaisesRegex(RuntimeError, 'subject must be'):
                    runner.validate_paths(workspace, output)
                link = tempdir / 'pl-stack-bad'
                link.symlink_to(output)
                with self.assertRaisesRegex(RuntimeError, 'output must be'):
                    runner.validate_paths(subject, link)
                def dirty(path, *args):
                    return ' M src/group.rs' if args[0] == 'status' else fake_git(path, *args)
                with mock.patch.object(runner, 'git', side_effect=dirty):
                    with self.assertRaisesRegex(RuntimeError, 'must be clean'):
                        runner.validate_paths(subject, output)
                def wrong(path, *args):
                    return 'different-commit' if args == ('rev-parse', 'HEAD') else fake_git(path, *args)
                with mock.patch.object(runner, 'git', side_effect=wrong):
                    with self.assertRaisesRegex(RuntimeError, 'unexpected subject commit'):
                        runner.validate_paths(subject, output)

    def test_interrupted_attempt_is_not_counted_as_completed(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'pl-stack-normal'
            (output / 'report').mkdir(parents=True)
            (output / 'report/runtime.log').write_text('PL_STACK_CLASSIC_JOIN_COMPLETE\n'
                                                       'test result: ok. 1 passed; 0 failed')
            owner = mock.Mock()
            owner.finished.return_value = False
            owner.process.returncode = 0
            owner.cleanup_receipt = {'leader_waited': True, 'remaining_descendants': []}
            @contextmanager
            def owned(*args, **kwargs):
                yield owner
            with mock.patch.object(runner, 'validate_paths'), \
                 mock.patch.object(runner, 'check_space', side_effect=[None, RuntimeError('low space')]), \
                 mock.patch.object(runner, 'list_owned', return_value=[]), \
                 mock.patch.object(runner, 'prepare', return_value='local'), \
                 mock.patch.object(runner, 'load_owner', return_value=owned), \
                 mock.patch.object(runner, 'cleanup_owned', return_value=[]), \
                 mock.patch.object(runner, 'verify_files'):
                result = runner.execute(Path('/irrelevant'), output, 'normal', PINS)
            self.assertEqual(result['stop_reason'], 'disk_floor')
            self.assertEqual(result['outcome'], 'interrupted_or_inconclusive')
            self.assertFalse(result['qualifying_full_compatibility'])

    def test_cleanup_failure_is_recorded_even_if_child_raises(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'pl-stack-normal'
            output.mkdir()
            owner = mock.Mock()
            owner.cleanup_receipt = {'remaining_descendants': []}
            @contextmanager
            def owned(*args, **kwargs):
                yield owner
                raise RuntimeError('child close failed')
            owner.finished.return_value = True
            with mock.patch.object(runner, 'validate_paths'), \
                 mock.patch.object(runner, 'check_space'), \
                 mock.patch.object(runner, 'list_owned', return_value=[]), \
                 mock.patch.object(runner, 'prepare', return_value='local'), \
                 mock.patch.object(runner, 'load_owner', return_value=owned), \
                 mock.patch.object(runner, 'cleanup_owned', side_effect=RuntimeError('docker offline')):
                result = runner.execute(Path('/irrelevant'), output, 'normal', PINS)
            self.assertEqual(result['outcome'], 'collector_error')
            self.assertIn('child close failed', result['error'])
            self.assertIn('docker offline', result['container_cleanup_error'])

    def test_patch_integrity_and_postimages(self):
        self.assertTrue(SOURCE.exists(), 'exact source checkout is required for patch tests')
        names = ['scripts/ci-broker-compatibility.sh', 'tests/broker_compatibility.rs', 'src/group.rs']
        cases = {'normal': ['classic-only.patch'], 'caller_box': ['classic-caller-box.patch'],
                 'library_box': ['classic-only.patch', 'library-box-experiment.patch'],
                 'phase_trace': ['classic-only.patch', 'phase-trace-diagnostic.patch']}
        for case, patches in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                for name in names:
                    data = subprocess.check_output(['git', 'show', PINS['commit'] + ':' + name], cwd=SOURCE)
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
                for patch in ['harness-classic-only.patch', *patches]:
                    self.assertEqual(runner.hash_file(HERE / patch), PINS['patches'][patch])
                    subprocess.run(['git', 'apply', '--check', str(HERE / patch)], cwd=root, check=True)
                    subprocess.run(['git', 'apply', str(HERE / patch)], cwd=root, check=True)
                for name, expected in PINS['known_postimages'][case].items():
                    self.assertEqual(runner.hash_file(root / name), expected)
                test = (root / 'tests/broker_compatibility.rs').read_text()
                join = test.index('let mut group = ', test.index('for kind in ["classic"'))
                guard = test.index('if kind == "classic" {', join)
                marker = test.index('PL_STACK_CLASSIC_JOIN_COMPLETE', guard)
                done = test.index('return Ok(());', marker)
                transaction = test.index('compatibility phase: transaction', done)
                self.assertLess(join, guard)
                self.assertLess(done, transaction)
                self.assertIn('timeout(Duration::from_secs(150), run())', test)
                self.assertIn('timeout(Duration::from_secs(20), async {', test)
                harness = (root / 'scripts/ci-broker-compatibility.sh').read_text()
                end_of_test = harness.index('PL_STACK_CLASSIC_TEST_PROCESS_RETURNED')
                self.assertLess(harness.index('exit 0', end_of_test),
                                harness.index('phase=independent-java-history-and-offsets'))


if __name__ == '__main__':
    unittest.main()
