"""Source-only safety and qualification controls; no Rust or broker execution."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SOURCE = Path(os.environ.get('PL_STACK_SOURCE_CHECKOUT', '/workspace/partitionline'))
spec = importlib.util.spec_from_file_location('caller_witness', HERE / 'stack_runner.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
PINS = json.loads((HERE / 'subject-pins.json').read_text())


class WitnessControls(unittest.TestCase):
    def test_source_pins_and_faithful_caller_for_both_cases(self):
        self.assertTrue(SOURCE.exists())
        names = ['src/group.rs', 'tests/broker_compatibility.rs', 'scripts/ci-broker-compatibility.sh']
        for case in driver.CASE_PATCHES:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                dest = Path(temp)
                for name in names:
                    path = dest / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(subprocess.check_output(['git', 'show', PINS['commit'] + ':' + name], cwd=SOURCE))
                for patch in ['harness-classic-only.patch', 'future-state-diagnostic.patch', *driver.CASE_PATCHES[case]]:
                    self.assertEqual(driver.hash_file(HERE / patch), PINS['patches'][patch])
                    subprocess.run(['git', 'apply', '--check', str(HERE / patch)], cwd=dest, check=True)
                    subprocess.run(['git', 'apply', str(HERE / patch)], cwd=dest, check=True)
                for name, value in PINS['known_postimages'][case + '_original_helper'].items():
                    self.assertEqual(driver.hash_file(dest / name), value)
                driver.verify_caller(dest, PINS)
                self.assertEqual(driver.hash_file(HERE / 'classic-helper-stop.patch'),
                                 PINS['patches']['classic-helper-stop.patch'])
                subprocess.run(['git', 'apply', '--check', str(HERE / 'classic-helper-stop.patch')], cwd=dest, check=True)
                subprocess.run(['git', 'apply', str(HERE / 'classic-helper-stop.patch')], cwd=dest, check=True)
                for name, value in PINS['known_postimages'][case + '_guarded_helper'].items():
                    self.assertEqual(driver.hash_file(dest / name), value)
                driver.verify_caller(dest, PINS)
                text = (dest / 'tests/broker_compatibility.rs').read_text()
                start = text.index('async fn join_group(')
                stop = text.index('async fn new_transactional(', start)
                join = text[start:stop]
                self.assertIn('if kind == "classic" {\n                        eprintln!("PL_CLASSIC_WITNESS_JOIN_SUCCEEDED");\n                        std::process::exit(0);\n', join)
                self.assertIn('timeout(Duration::from_secs(20), async {', join)
                self.assertIn('timeout(Duration::from_secs(150), run())', text)
                self.assertIn('compatibility phase: transaction', text)
                diagnostic = text[text.index('fn report_classic_witness_future_states()'):]
                self.assertNotIn('.await', diagnostic)
                self.assertNotIn('.poll(', diagnostic)
                self.assertIn('let caller = run();', diagnostic)
                self.assertIn('let join = join_group(', diagnostic)
                harness = (dest / 'scripts/ci-broker-compatibility.sh').read_text()
                marker = harness.index('PL_STACK_CLASSIC_TEST_PROCESS_RETURNED')
                self.assertLess(harness.index('exit 0', marker), harness.index('phase=independent-java-history-and-offsets'))
                subprocess.run(['bash', '-n', str(dest / 'scripts/ci-broker-compatibility.sh')], check=True)

    def test_workflow_has_only_the_selected_main_paths_or_manual_dispatch(self):
        text = (HERE / 'workflow.yml').read_text()
        trigger = text.split('on:\n', 1)[1].split('\npermissions:', 1)[0]
        self.assertEqual(trigger, """  push:
    branches: [main]
    paths:
      - .github/workflows/classic-original-caller-witness.yml
      - docs/evidence/client/classic-caller-witness-20261103/**
  workflow_dispatch:
""")
        self.assertIn('ref: ' + driver.COMMIT, text)
        self.assertIn('retention-days: 30', text)
        reviewed = ('stack_runner.py', 'test_stack_runner.py', 'subject-pins.json', 'source-pins.json',
                    'classic-helper-stop.patch', 'future-state-diagnostic.patch',
                    'harness-classic-only.patch', 'library-box-experiment.patch')
        for name in reviewed:
            digest = driver.hash_file(HERE / name)['sha256']
            self.assertIn("'" + name + "': '" + digest + "',", text)
        self.assertIn('timeout-minutes: 120', text)
        self.assertIn('timeout-minutes: 55', text)
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            active = HERE.parents[3] / '.github/workflows/classic-original-caller-witness.yml'
            self.assertEqual(active.read_bytes(), (HERE / 'workflow.yml').read_bytes())

    def test_prior_manual_fetch_uses_pinned_default_isolation(self):
        consumer = subprocess.check_output(['git', 'show', PINS['commit'] + ':src/consumer.rs'],
                                           cwd=SOURCE, text=True)
        test = subprocess.check_output(['git', 'show', PINS['commit'] + ':tests/broker_compatibility.rs'],
                                       cwd=SOURCE, text=True)
        self.assertIn('isolation_level: crate::IsolationLevel::ReadUncommitted,', consumer)
        config = test[test.index('fn consumer_config('):test.index('fn producer_config(')]
        self.assertNotIn('.isolation(', config)
        self.assertLess(test.index('compatibility phase: manual'), test.index('compatibility phase: groups'))
        self.assertLess(test.index('&manual.fetch().await?'), test.index('compatibility phase: groups'))

    def test_library_candidate_only_boxes_one_private_attempt(self):
        patch = (HERE / 'library-box-experiment.patch').read_text()
        removed = [line for line in patch.splitlines() if line.startswith('-') and not line.startswith('---')]
        added = [line for line in patch.splitlines() if line.startswith('+') and not line.startswith('+++')]
        self.assertEqual(removed, ['-            let revoked = self.rejoin_once(deadline).await?;'])
        self.assertEqual(added, ['+            let revoked = Box::pin(self.rejoin_once(deadline)).await?;'])

    def test_host_path_guards_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            workspace, runner_temp = root / 'workspace', root / 'temp'
            subject, output = workspace / 'subject', runner_temp / 'pl-caller-baseline'
            (subject / '.git').mkdir(parents=True)
            runner_temp.mkdir(parents=True)
            env = {'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'github-hosted',
                   'GITHUB_WORKSPACE': str(workspace), 'RUNNER_TEMP': str(runner_temp), 'STACK_CASE': 'baseline'}
            def fake_git(path, *args):
                return {('rev-parse', '--show-toplevel'): str(subject), ('rev-parse', 'HEAD'): driver.COMMIT,
                        ('status', '--porcelain=v1', '--untracked-files=normal'): ''}[args]
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(driver.platform, 'system', return_value='Linux'), \
                 mock.patch.object(driver.platform, 'machine', return_value='x86_64'), \
                 mock.patch.object(driver, 'git', side_effect=fake_git):
                driver.validate_paths(subject, output)
                driver.validate_checkout(subject)
                output.mkdir()
                (output / 'stale.log').write_text('old result')
                with self.assertRaisesRegex(RuntimeError, 'output must be a new'):
                    driver.validate_paths(subject, output)
                (output / 'stale.log').unlink()
                output.rmdir()
                with self.assertRaisesRegex(RuntimeError, 'dedicated'):
                    driver.validate_paths(workspace, output)
                target = runner_temp / 'pl-caller-target-baseline'
                target.symlink_to(root / 'missing')
                with self.assertRaisesRegex(RuntimeError, 'build target'):
                    driver.validate_paths(subject, output)
                target.unlink()
                os.environ['RUNNER_ENVIRONMENT'] = 'self-hosted'
                with self.assertRaisesRegex(RuntimeError, 'GitHub-hosted'):
                    driver.validate_paths(subject, output)

    def test_nonpolling_states_must_be_complete_and_unambiguous(self):
        text = ('test report_classic_witness_future_states ... PL_CLASSIC_FUTURE_STATE\tcaller\t1024\t8\n'
                'PL_CLASSIC_FUTURE_STATE\tjoin_helper\t512\t8\nPL_CLASSIC_FUTURE_STATES_COMPLETE\n'
                'test result: ok. 1 passed; 0 failed; 0 ignored;')
        result = driver.parse_future_states(text)
        self.assertEqual(result['caller'], {'size': 1024, 'align': 8})
        for bad in (text.replace('PL_CLASSIC_FUTURE_STATES_COMPLETE', ''),
                    text.replace('join_helper', 'caller'), text.replace('\t512\t8', '\t0\t8'),
                    text.replace('1 passed; 0 failed', '0 passed; 1 failed')):
            with self.assertRaises(RuntimeError):
                driver.parse_future_states(bad)

    def test_live_outcome_requires_exactly_one_marker_and_successful_exit(self):
        marker = 'PL_CLASSIC_WITNESS_JOIN_SUCCEEDED'
        self.assertEqual(driver.witness_outcome(0, marker), 'classic_join_returned_before_process_exit')
        for code, log in ((0, ''), (101, marker), (0, marker + marker),
                          (101, "thread 'rustc' (13) has overflowed its stack")):
            self.assertNotEqual(driver.witness_outcome(code, log), 'classic_join_returned_before_process_exit')
        self.assertEqual(driver.witness_outcome(101, "thread 'live_compatibility_required' (13) has overflowed its stack"),
                         'live_test_stack_overflow')

    def test_nonmatching_states_never_start_live_broker(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'new-output'
            first = {'caller': {'size': 1, 'align': 1}}
            second = {'caller': {'size': 2, 'align': 1}}
            def fake_command(subject, out, name, argv, env, owner):
                (out / (name + '.log')).write_text('placeholder')
                return {'exit_code': 0}
            fake_result = mock.Mock(stdout='fake version')
            with mock.patch.object(driver, 'validate_paths'), mock.patch.object(driver, 'validate_checkout'), mock.patch.object(driver, 'check_space'), \
                 mock.patch.object(driver, 'prepare', return_value='original'), \
                 mock.patch.object(driver, 'guard', return_value='guarded'), \
                 mock.patch.object(driver, 'load_owner'), mock.patch.object(driver, 'verify_files'), \
                 mock.patch.object(driver, 'verify_caller'), mock.patch.object(driver, 'test_binary', return_value={}), \
                 mock.patch.object(driver, 'run', return_value=fake_result), \
                 mock.patch.object(driver, 'git', return_value=''), \
                 mock.patch.object(driver, 'command', side_effect=fake_command) as command, \
                 mock.patch.object(driver, 'parse_future_states', side_effect=[first, second]), \
                 mock.patch.object(driver, 'list_owned') as docker:
                result = driver.execute(Path('/irrelevant'), output, 'baseline', PINS)
            self.assertEqual(result['outcome'], 'future_states_differ_live_witness_not_run')
            self.assertEqual(command.call_count, 2)
            for call in command.call_args_list:
                args = call.args[3]
                self.assertEqual(args[-4:], ['--ignored', '--exact', 'report_classic_witness_future_states', '--nocapture'])
            docker.assert_not_called()

    def test_matching_states_require_marker_exit_and_owned_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'new-output'
            state_log = ('PL_CLASSIC_FUTURE_STATE\tcaller\t1024\t8\n'
                         'PL_CLASSIC_FUTURE_STATE\tjoin_helper\t512\t8\n'
                         'PL_CLASSIC_FUTURE_STATES_COMPLETE\n'
                         'test result: ok. 1 passed; 0 failed;')
            def fake_command(subject, out, name, argv, env, owner):
                if name.startswith('future-'):
                    (out / (name + '.log')).write_text(state_log)
                else:
                    self.assertEqual(argv, ['bash', 'scripts/ci-broker-compatibility.sh', '4.1.2'])
                    (out / 'report').mkdir()
                    (out / 'report/runtime.log').write_text('PL_CLASSIC_WITNESS_JOIN_SUCCEEDED')
                return {'exit_code': 0}
            fake_version = mock.Mock(stdout='fake version')
            with mock.patch.object(driver, 'validate_paths'), mock.patch.object(driver, 'validate_checkout'), mock.patch.object(driver, 'check_space'), \
                 mock.patch.object(driver, 'prepare', return_value='original'), \
                 mock.patch.object(driver, 'guard', return_value='guarded'), \
                 mock.patch.object(driver, 'load_owner'), mock.patch.object(driver, 'verify_files'), \
                 mock.patch.object(driver, 'verify_caller'), mock.patch.object(driver, 'test_binary', return_value={'sha256': 'same'}), \
                 mock.patch.object(driver, 'run', return_value=fake_version) as version_probe, \
                 mock.patch.object(driver, 'git', return_value=''), \
                 mock.patch.object(driver, 'command', side_effect=fake_command) as command, \
                 mock.patch.object(driver, 'list_owned', return_value=[]), \
                 mock.patch.object(driver, 'cleanup_owned', return_value=[]) as cleanup:
                result = driver.execute(Path('/irrelevant'), output, 'baseline', PINS)
            self.assertEqual(result['outcome'], 'classic_join_returned_before_process_exit')
            self.assertFalse(result['full_compatibility_qualified'])
            self.assertTrue(result['future_state_size_and_alignment_match'])
            self.assertEqual(result['container_cleanup'], 'confirmed')
            self.assertEqual(command.call_count, 3)
            self.assertEqual(version_probe.call_args_list[0].args[0], ['rustc', '+stable', '-Vv'])
            self.assertEqual(version_probe.call_args_list[0].kwargs['cwd'], Path('/irrelevant'))
            self.assertEqual(version_probe.call_args_list[1].args[0], ['cargo', '+stable', '-V'])
            self.assertEqual(version_probe.call_args_list[1].kwargs['cwd'], Path('/irrelevant'))
            cleanup.assert_called_once()

    def test_checkout_probe_timeout_records_unverified_ownership(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'new-output'
            with mock.patch.object(driver, 'validate_paths'), mock.patch.object(driver, 'check_space'), \
                 mock.patch.object(driver, 'validate_checkout', side_effect=subprocess.TimeoutExpired(['git'], 30)), \
                 mock.patch.object(driver, 'prepare') as prepare:
                result = driver.execute(Path('/irrelevant'), output, 'baseline', PINS)
            self.assertEqual(result['outcome'], 'collector_error')
            self.assertEqual(result['process_ownership'], 'unverified_after_probe_timeout')
            self.assertEqual(json.loads((output / 'receipt.json').read_text())['outcome'], 'collector_error')
            prepare.assert_not_called()

    def test_cleanup_timeout_is_unconfirmed_and_never_success(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'new-output'
            state = {'caller': {'size': 1, 'align': 1}, 'join_helper': {'size': 1, 'align': 1}}
            def fake_command(subject, out, name, argv, env, owner):
                (out / (name + '.log')).write_text('placeholder')
                return {'exit_code': 0}
            with mock.patch.object(driver, 'validate_paths'), mock.patch.object(driver, 'validate_checkout'), mock.patch.object(driver, 'check_space'), \
                 mock.patch.object(driver, 'prepare', return_value='original'), \
                 mock.patch.object(driver, 'guard', return_value='guarded'), \
                 mock.patch.object(driver, 'load_owner'), mock.patch.object(driver, 'verify_files'), \
                 mock.patch.object(driver, 'verify_caller'), mock.patch.object(driver, 'test_binary', return_value={}), \
                 mock.patch.object(driver, 'run', return_value=mock.Mock(stdout='fake version')), \
                 mock.patch.object(driver, 'git', return_value=''), \
                 mock.patch.object(driver, 'command', side_effect=fake_command), \
                 mock.patch.object(driver, 'parse_future_states', return_value=state), \
                 mock.patch.object(driver, 'list_owned', return_value=[]), \
                 mock.patch.object(driver, 'cleanup_owned', side_effect=subprocess.TimeoutExpired(['docker'], 30)):
                result = driver.execute(Path('/irrelevant'), output, 'baseline', PINS)
            self.assertEqual(result['outcome'], 'collector_error')
            self.assertEqual(result['container_cleanup'], 'unconfirmed')
            self.assertEqual(result['process_ownership'], 'unverified_after_probe_timeout')

    def test_environment_never_passes_stack_override_to_cargo(self):
        with mock.patch.dict(os.environ, {'PATH': '/usr/bin', 'RUST_MIN_STACK': '999',
                                         'KAFKA_BOOTSTRAP': 'external', 'RUSTFLAGS': 'unsafe'}, clear=True):
            for case in driver.CASE_PATCHES:
                env = driver.child_environment(case, Path('/tmp/pl-caller-' + case), 'a' * 32)
                self.assertNotIn('RUST_MIN_STACK', env)
                self.assertNotIn('RUSTFLAGS', env)
                self.assertNotIn('KAFKA_BOOTSTRAP', env)
                self.assertNotIn('PL_STACK_RUNTIME_MIN', env)

    def test_docker_cleanup_rejects_unowned_or_ambiguous_containers(self):
        token = 'a' * 32
        ident = 'b' * 64
        wrong = {'Id': ident, 'Name': '/other', 'Config': {'Labels': {'partitionline.stack.run': token}}}
        outputs = [mock.Mock(stdout=ident + '\n'), mock.Mock(stdout=json.dumps([wrong]))]
        with mock.patch.object(driver, 'run', side_effect=outputs) as command:
            with self.assertRaisesRegex(RuntimeError, 'refusing'):
                driver.cleanup_owned(token)
            self.assertEqual(command.call_count, 2)
        with mock.patch.object(driver, 'run') as command:
            with self.assertRaisesRegex(RuntimeError, 'invalid'):
                driver.list_owned('bad')
            command.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
