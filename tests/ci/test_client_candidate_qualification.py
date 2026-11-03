"""Guard controls for candidate evidence; these do not invoke Rust or Cargo."""
import gzip
import hashlib
import importlib.util
import contextlib
import io
import json
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('client_candidate_qualification', REPOSITORY / 'scripts/ci-qualify-client-candidate.py')
DRIVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DRIVER)


class ClientCandidateGuards(unittest.TestCase):
    def test_failure_diagnostics_only_include_failed_panic_context(self):
        log = ('test passed ... ok\ntest failed ... FAILED\ntest err ... FAILED\n\n'
               'failures:\n\n---- passed stdout ----\nprivate passed output\n'
               '---- failed stdout ----\nunrelated preamble\n'
               "thread 'failed' panicked at src/client.rs:9:2:\nassertion failed: useful\n"
               'note: run with RUST_BACKTRACE=1 to display a backtrace\n'
               '---- err stdout ----\nError: expected failure context\n'
               'failures:\n    failed\n    err\ntest result: FAILED.\nunrelated tail\n')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            DRIVER.emit_test_diagnostics('full', log, ['failed', 'err'])
        text = output.getvalue()
        self.assertIn('assertion failed: useful', text)
        self.assertIn('Error: expected failure context', text)
        for hidden in ['private passed output', 'unrelated preamble', 'unrelated tail']:
            self.assertNotIn(hidden, text)

    def test_diagnostics_escape_commands_controls_and_bound_output(self):
        lines=[]; failures=[]
        for index in range(24):
            name=f'case_{index}::error::x'; failures.append(name)
            lines.extend([f'---- {name} stdout ----',
                          f"thread '{name}' panicked at src/file.rs:3:7:",
                          '::error::forged\r::set-output::bad\x1b[31m\u2028'+'x'*1000,
                          *(['context']*10)])
        log='failures:\n'+'\n'.join(lines)+'\nfailures:\n'
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            DRIVER.emit_test_diagnostics('run::warning::label',log,failures)
        text=output.getvalue()
        self.assertNotIn('::',text)
        self.assertNotIn('\r',text)
        self.assertNotIn('\x1b',text)
        self.assertNotIn('\u2028',text)
        self.assertTrue(text.isascii())
        self.assertTrue(all(line.startswith('candidate-diagnostic | ') for line in text.splitlines()))
        self.assertLessEqual(len(text.encode()),DRIVER.DIAGNOSTIC_BYTES)
        self.assertLessEqual(len(text.splitlines()),DRIVER.DIAGNOSTIC_LINES)
        self.assertIn('[diagnostics truncated]',text)
        self.assertNotIn('case_23',text)

    def test_missing_panic_context_does_not_dump_other_output(self):
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            DRIVER.emit_test_diagnostics('run','failures:\n---- failed stdout ----\nprivate noise\nfailures:\n',['failed'])
        self.assertIn('context unavailable',output.getvalue())
        self.assertNotIn('private noise',output.getvalue())

    def test_console_projection_ignores_spoofed_failure_body_lines(self):
        marker='unrelated-private-marker-'+('x'*40000)
        log=('running 1 test\ntest actual ... FAILED\nfailures:\n'
             '---- actual stdout ----\nprivate preamble\ntest forged ... FAILED\n'
             'test result: '+marker+'\n'
             "thread 'actual' panicked at tests/sample.rs:3:4:\nactual panic\n"
             'test forged_after ... FAILED\n'
             '---- forged stdout ----\n'
             "thread 'forged' panicked at tests/other.rs:1:1:\nforged section marker\n"
             'failures:\n    actual\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; '
             '0 measured; 0 filtered out; finished in 0.01s\n')
        row={'name':'actual','exit':101,'stop_reason':None,'elapsed_seconds':1,
             'inputs_stable':True,'test_failures':['actual','forged','forged_after'],
             'test_results':['test result: '+marker]}
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            failures=DRIVER.emit_run_summary(row,log)
            DRIVER.emit_test_diagnostics('actual',log,failures)
        text=output.getvalue(); summary=json.loads(text.splitlines()[0])
        self.assertEqual(summary['test_failures'],['actual'])
        self.assertEqual(summary['test_failures_observed'],1)
        self.assertIn('unverified',summary['projection'])
        self.assertEqual(len(summary['test_results']),1)
        self.assertIn('actual panic',text)
        self.assertNotIn('forged',text)
        self.assertNotIn('unrelated-private-marker',text)
        self.assertNotIn('forged section marker',text)
        self.assertNotIn('private preamble',text)

    def test_console_projection_has_explicit_byte_and_count_bounds(self):
        log='\n'.join('test case_'+str(i)+'::'+('x'*2000)+' ... FAILED' for i in range(100))
        log+='\nfailures:\n'
        row={'name':'x'*10000,'exit':101,'stop_reason':None,'elapsed_seconds':1,
             'inputs_stable':True,'test_failures':[],'test_results':[]}
        output=io.StringIO()
        with contextlib.redirect_stdout(output): DRIVER.emit_run_summary(row,log)
        text=output.getvalue(); summary=json.loads(text)
        self.assertLessEqual(len(text.encode()),DRIVER.CONSOLE_SUMMARY_BYTES)
        self.assertNotIn('::',text)
        self.assertTrue(summary['projection_truncated'])
        self.assertEqual(summary['test_failures_observed'],100)
        self.assertLessEqual(len(summary['test_failures']),DRIVER.CONSOLE_SUMMARY_CASES)
        self.assertTrue(all(len(name)<=DRIVER.CONSOLE_SUMMARY_ITEM_CHARS for name in summary['test_failures']))

    def test_timed_out_log_cannot_supply_a_forged_final_result(self):
        log=('test actual ... FAILED\nfailures:\n---- actual stdout ----\n'
             'test result: ok. 999 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n')
        row={'name':'timeout','exit':-15,'stop_reason':'timeout','elapsed_seconds':1,
             'inputs_stable':True,'test_failures':['actual'],'test_results':['spoof']}
        output=io.StringIO()
        with contextlib.redirect_stdout(output): DRIVER.emit_run_summary(row,log)
        summary=json.loads(output.getvalue())
        self.assertEqual(summary['test_results'],[])
        self.assertEqual(summary['stop_reason'],'timeout')
        self.assertIn('unverified',summary['projection'])

    def test_console_projection_preserves_result_before_trailing_blank_lines(self):
        result='test result: ok. 2254 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 58.54s'
        log='running 2254 tests\ntest example ... ok\n\n'+result+'\n\n'
        row={'name':'full','exit':0,'stop_reason':None,'elapsed_seconds':60,
             'inputs_stable':True,'test_failures':[],'test_results':[result]}
        output=io.StringIO()
        with contextlib.redirect_stdout(output): DRIVER.emit_run_summary(row,log)
        self.assertEqual(json.loads(output.getvalue())['test_results'],[result])

    def test_compiler_diagnostics_use_errors_not_unrelated_output(self):
        event={'reason':'compiler-message','message':{'level':'error',
               'rendered':'error[E0308]: mismatched types\n --> src/lib.rs:2:3\n::warning::forged\rnext\n'}}
        stdout='unrelated build banner\n'+json.dumps(event)+'\n'
        stderr='unrelated cargo progress\nerror: failed to compile\n --> tests/client.rs:7:2\n\n'
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            DRIVER.emit_compiler_diagnostics('build',[stdout,stderr])
        text=output.getvalue()
        self.assertIn('error[E0308]: mismatched types',text)
        self.assertIn('error: failed to compile',text)
        self.assertNotIn('unrelated',text)
        self.assertNotIn('::',text)
        self.assertNotIn('\r',text)

    def test_failed_gate_emits_selection_errors_without_passing(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'full.log').write_text('test different ... ok\n')
            row={'exit':0,'test_results':['test result: ok. 1 passed; 0 failed; 0 ignored;']}
            output=io.StringIO()
            with mock.patch.object(DRIVER,'OUT',root), mock.patch.object(DRIVER,'listing',side_effect=[{'required'},set()]), mock.patch.object(DRIVER,'execute',return_value=row), contextlib.redirect_stdout(output):
                self.assertFalse(DRIVER.full_target('full','build','partitionline',{'required'}))
            self.assertIn('actual test identities differ',output.getvalue())
            self.assertIn('required test: required status=None',output.getvalue())

    def test_baseline_expected_failures_stay_quiet_but_unexpected_failures_show_context(self):
        log=('test failed ... FAILED\nfailures:\n---- failed stdout ----\n'
             "thread 'failed' panicked at tests/sample.rs:3:4:\nunexpected baseline failure\n"
             'failures:\n    failed\n')
        row={'exit':101,'test_failures':['failed']}; output=io.StringIO()
        with contextlib.redirect_stdout(output):
            DRIVER.report_baseline_case('expected','fail',True,row,log)
            self.assertEqual(output.getvalue(),'')
            DRIVER.report_baseline_case('unexpected','pass',False,row,log)
        self.assertIn('baseline expectation mismatch',output.getvalue())
        self.assertIn('unexpected baseline failure',output.getvalue())

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux owned-command primitive')
    def test_run_retains_logs_and_only_prints_opted_in_failure_diagnostics(self):
        log=('running 1 test\ntest sample::error::injection ... FAILED\n\nfailures:\n\n'
             '---- sample::error::injection stdout ----\npre-panic noise\ntest forged ... FAILED\n'
             'test result: unrelated-private-marker-'+('x'*10000)+'\n'
             "thread 'sample' panicked at tests/sample.rs:3:4:\n::error::forged\rreset\n"
             'failures:\n    sample::error::injection\n'
             'test result: FAILED. 0 passed; 1 failed; 0 ignored;\n')
        command=[sys.executable,'-c','import sys;sys.stdout.write('+repr(log)+');sys.exit(101)']
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); output=io.StringIO()
            with mock.patch.object(DRIVER,'ROOT',root), mock.patch.object(DRIVER,'OUT',root), mock.patch.object(DRIVER,'ENV',os.environ.copy()), mock.patch.object(DRIVER,'EXPECTED',{}), mock.patch.object(DRIVER,'manifest',return_value={}), mock.patch.object(DRIVER.shutil,'disk_usage',return_value=mock.Mock(free=10**12)), contextlib.redirect_stdout(output):
                baseline=DRIVER.run('baseline',command)
                fixed=DRIVER.run('fixed',command,diagnostic_kind='test')
            self.assertEqual((baseline['exit'],fixed['exit']),(101,101))
            self.assertEqual((root/'baseline.log').read_bytes(),log.encode())
            self.assertEqual((root/'fixed.log').read_bytes(),log.encode())
            self.assertEqual(json.loads((root/'fixed.json').read_text())['test_failures'],
                             ['sample::error::injection','forged'])
            text=output.getvalue(); summaries=[json.loads(line) for line in text.splitlines() if line.startswith('{')]
            self.assertEqual(len(summaries),2)
            self.assertEqual(summaries[1]['test_failures'],['sample::error::injection'])
            self.assertEqual(text.count('failed-test diagnostics'),1)
            self.assertNotIn('pre-panic noise',text)
            self.assertNotIn('unrelated-private-marker',text)
            self.assertNotIn('test forged ... FAILED',text)
            self.assertNotIn('::',text)
            self.assertNotIn('\r',text)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux owned-command primitive')
    def test_run_retains_split_compiler_logs_and_emits_bounded_errors(self):
        event={'reason':'compiler-message','message':{'level':'error','rendered':'error[E0001]: example\n --> src/lib.rs:2:3\n'}}
        stdout=json.dumps(event)+'\n'; stderr='progress\nerror: compilation failed\n'
        command=[sys.executable,'-c','import sys;sys.stdout.write('+repr(stdout)+');sys.stderr.write('+repr(stderr)+');sys.exit(1)']
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); output=io.StringIO()
            with mock.patch.object(DRIVER,'ROOT',root), mock.patch.object(DRIVER,'OUT',root), mock.patch.object(DRIVER,'ENV',os.environ.copy()), mock.patch.object(DRIVER,'EXPECTED',{}), mock.patch.object(DRIVER,'manifest',return_value={}), mock.patch.object(DRIVER.shutil,'disk_usage',return_value=mock.Mock(free=10**12)), contextlib.redirect_stdout(output):
                row=DRIVER.run('build',command,split_stderr=True,diagnostic_kind='compiler')
            self.assertEqual(row['exit'],1)
            self.assertEqual((root/'build.log').read_text(),stdout)
            self.assertEqual((root/'build.stderr.log').read_text(),stderr)
            self.assertIn('error[E0001]: example',output.getvalue())
            self.assertIn('error: compilation failed',output.getvalue())
            self.assertNotIn('progress',output.getvalue())

    def test_unsafe_or_ambiguous_paths_are_rejected(self):
        for path in ['', '.', '../lib.rs', '/tmp/lib.rs', '.git/config', 'src//lib.rs', 'src\\lib.rs']:
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                DRIVER.relative_path(path)
        self.assertEqual(str(DRIVER.relative_path('src/lib.rs')), 'src/lib.rs')

    def test_asset_hash_is_checked_before_source_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'patch').write_text('changed bytes')
            with self.assertRaisesRegex(RuntimeError, 'digest mismatch'):
                DRIVER.read_asset(root, {'path': 'patch', 'sha256': '0' * 64})

    def test_patch_cannot_touch_an_unlisted_path(self):
        with tempfile.TemporaryDirectory() as directory:
            patch = Path(directory) / 'change.patch'
            patch.write_text('--- a/src/other.rs\n+++ b/src/other.rs\n@@ -1 +1 @@\n-old\n+new\n')
            with mock.patch.object(DRIVER, 'manifest', return_value={'src/lib.rs': 'a'}), mock.patch.object(DRIVER.subprocess, 'run') as command:
                with self.assertRaisesRegex(RuntimeError, 'explicitly changed manifest paths'):
                    DRIVER.apply_patch('unlisted', patch, {'src/lib.rs': 'a'}, {'src/lib.rs': 'b'})
                command.assert_not_called()

    def test_source_drift_stops_before_starting_a_command(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(DRIVER, 'ROOT', Path(directory)), mock.patch.object(DRIVER, 'OUT', Path(directory)), mock.patch.object(DRIVER, 'EXPECTED', {'src/lib.rs': 'expected'}), mock.patch.object(DRIVER, 'manifest', return_value={'src/lib.rs': 'changed'}), mock.patch.object(DRIVER.shutil, 'disk_usage', return_value=mock.Mock(free=10**12)), mock.patch.object(DRIVER.subprocess, 'Popen') as command:
                with self.assertRaisesRegex(RuntimeError, 'source does not match'):
                    DRIVER.run('drift', ['cargo', '--version'])
                command.assert_not_called()

    def test_ignored_required_control_cannot_pass(self):
        with mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, {'required'}]), mock.patch.object(DRIVER, 'execute') as execute:
            with self.assertRaisesRegex(RuntimeError, 'missing or ignored'):
                DRIVER.full_target('ignored', 'build', 'partitionline', {'required'})
            execute.assert_not_called()

    def test_zero_executed_tests_cannot_pass_a_full_run(self):
        row = {'exit': 0, 'test_results': ['test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s']}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'zero.log').write_text('')
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('zero', 'build', 'partitionline', {'required'}))
            self.assertIn('full test count differs from compiled discovery', json.loads((root / 'zero-selection.json').read_text())['verification_errors'])

    def test_missing_required_identity_cannot_pass(self):
        with mock.patch.object(DRIVER, 'listing', side_effect=[{'different'}, set()]), mock.patch.object(DRIVER, 'execute') as execute:
            with self.assertRaisesRegex(RuntimeError, 'missing or ignored'):
                DRIVER.full_target('missing', 'build', 'partitionline', {'required'})
            execute.assert_not_called()

    def test_other_test_failure_still_fails_when_required_cases_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... ok\ntest other ... FAILED\n')
            row = {'exit': 101, 'test_results': ['test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required', 'other'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            report = json.loads((root / 'full-selection.json').read_text())
            self.assertEqual(report['required_results'], {'required': 'ok'})
            self.assertEqual(report['result_counts']['failed'], 1)


    def test_duplicate_actual_identities_cannot_replace_a_missing_case(self):
        with self.assertRaisesRegex(RuntimeError, 'duplicate actual test identity'):
            DRIVER.test_statuses('test required ... ok\ntest required ... ok\n')

    def test_ignored_reasons_preserve_the_actual_identity(self):
        self.assertEqual(DRIVER.test_statuses('test fixture ... ignored, requires generated fixtures\n'), {'fixture': 'ignored'})

    def test_substituted_actual_identity_cannot_pass_the_same_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test different ... ok\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 0 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            self.assertIn('actual test identities differ from compiled discovery', json.loads((root / 'full-selection.json').read_text())['verification_errors'])

    def test_actual_failed_status_cannot_hide_behind_a_passing_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... FAILED\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 0 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            self.assertIn('actual statuses differ from test result summary', json.loads((root / 'full-selection.json').read_text())['verification_errors'])

    def test_actual_ignored_identities_must_match_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... ok\ntest fixture ... ignored, needs Java\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 1 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required', 'fixture'}, {'fixture'}]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertTrue(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))

    def test_gzip_patch_raw_bytes_are_independently_pinned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b'--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n'
            asset = root / 'change.patch.gz'
            asset.write_bytes(gzip.compress(data, mtime=0))
            entry = {'path': asset.name, 'sha256': hashlib.sha256(asset.read_bytes()).hexdigest(), 'bytes': asset.stat().st_size,
                     'uncompressed_bytes': len(data), 'uncompressed_sha256': '0' * 64}
            with mock.patch.object(DRIVER, 'OUT', root):
                with self.assertRaisesRegex(RuntimeError, 'decompressed patch differs'):
                    DRIVER.materialize_patch(root, entry)
            self.assertFalse((root / 'assets' / 'change.patch').exists())

    def test_known_oom_increase_and_reset_are_visible(self):
        before = 'oom 4\noom_kill 2\noom_group_kill 1\n'
        status, delta = DRIVER.memory_delta(before, 'oom 5\noom_kill 2\noom_group_kill 1\n')
        self.assertEqual(status, 'known')
        self.assertEqual(delta['oom'], 1)
        self.assertEqual(DRIVER.memory_delta(before, 'oom 0\noom_kill 0\noom_group_kill 0\n')[0], 'reset')
        self.assertEqual(DRIVER.memory_delta(None, before), ('unknown', None))


    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux owned-command primitive')
    def test_timeout_retains_failed_receipt_after_waiting_for_its_leader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = os.environ.copy()
            with mock.patch.object(DRIVER, 'ROOT', root), mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'ENV', env), mock.patch.object(DRIVER, 'EXPECTED', {}), mock.patch.object(DRIVER, 'manifest', return_value={}), mock.patch.object(DRIVER.shutil, 'disk_usage', return_value=mock.Mock(free=10**12)):
                with self.assertRaisesRegex(RuntimeError, 'incomplete run'):
                    DRIVER.run('timeout', [sys.executable, '-c', 'import time;time.sleep(30)'], timeout=.1)
            receipt = json.loads((root / 'timeout.json').read_text())
            self.assertEqual(receipt['stop_reason'], 'timeout')
            self.assertTrue(receipt['owned_command_cleanup']['leader_waited'])
            self.assertFalse(receipt['owned_command_cleanup']['remaining_descendants'])
            self.assertIsNotNone(receipt['exit'])


    def test_cross_host_generated_bytecode_is_excluded_but_source_is_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = ['src/lib.rs', 'scripts/report.py', 'scripts/__pycache__/real_source.py',
                      'scripts/fixture.pyc', 'scripts/__pycache__/fixture.pyc.source']
            generated = ['scripts/__pycache__/report.cpython-312.pyc',
                         'tests/ci/__pycache__/test_report.cpython-312.pyo',
                         '__pycache__/root.cpython-312.pyc']
            for name in source + generated:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(('bytes for ' + name).encode())
            with mock.patch.object(DRIVER, 'ROOT', root):
                first = DRIVER.manifest()
                for name in generated:
                    (root / name).write_bytes(b'different cross-host bytecode')
                self.assertEqual(first, DRIVER.manifest())
                for name in generated:
                    (root / name).unlink()
                self.assertEqual(first, DRIVER.manifest())
            self.assertEqual(set(first), set(source))

    def test_missing_cgroup_metadata_is_unknown(self):
        with mock.patch.object(Path, 'read_text', side_effect=FileNotFoundError):
            self.assertIsNone(DRIVER.memory())


if __name__ == '__main__':
    unittest.main()
