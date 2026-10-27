"""Exercise the campaign driver with actual instrumented broker executables."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/broker-fuzz-campaign.py'
spec = importlib.util.spec_from_file_location('broker_campaign', SCRIPT)
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


class BrokerCampaign(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get('PL_BROKER_FUZZ_BUILD')
        if not configured:
            raise RuntimeError('Build real instrumented targets and set PL_BROKER_FUZZ_BUILD; no skipped/fake pass')
        cls.build = Path(configured).resolve()
        cls.directory = Path(tempfile.mkdtemp(prefix='broker-campaign-', dir=os.environ.get('PL_BROKER_FUZZ_TEST_OUTPUT')))
        cls.nominal = cls.directory / 'nominal'
        cls.call('run', '--build', cls.build, '--output', cls.nominal, '--kind', 'smoke', '--seconds', '2')

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('PL_BROKER_FUZZ_TEST_OUTPUT'):
            shutil.rmtree(cls.directory)

    @classmethod
    def call(cls, *args, expected=0):
        proc = subprocess.run([sys.executable, '-B', str(SCRIPT), *map(str, args)], capture_output=True, text=True, timeout=45)
        if proc.returncode != expected:
            raise AssertionError(f'{proc.returncode} != {expected}: {proc.stdout}\n{proc.stderr}')
        return proc

    def copied(self, name):
        path = self.directory / name
        shutil.copytree(self.nominal, path)
        return path

    def test_real_short_run_has_coverage_and_never_qualifies_a_campaign(self):
        verdict = campaign.audit(self.nominal)
        self.assertEqual(verdict['state'], 'completed')
        self.assertFalse(verdict['campaign_qualified'])
        self.assertFalse(verdict['production_qualified'])
        self.assertEqual(set(verdict['targets']), set(campaign.TARGETS))
        for values in verdict['targets'].values():
            self.assertGreater(values['coverage'], 0)
            self.assertGreater(values['executions'], 0)
            self.assertGreaterEqual(values['engine_elapsed_seconds'], 2)

    def test_mutated_real_log_is_refused(self):
        output = self.copied('changed-log')
        with (output / 'parse_header/fuzzer.log').open('ab') as log:
            log.write(b'changed\n')
        with self.assertRaisesRegex(ValueError, 'artifact changed'):
            campaign.audit(output)

    def test_launched_job_without_final_receipt_is_incomplete(self):
        output = self.copied('missing-result')
        (output / 'result.json').unlink()
        verdict = campaign.audit(output)
        self.assertEqual(verdict['state'], 'incomplete')
        self.assertFalse(verdict['campaign_qualified'])

    def test_short_campaign_request_refused_before_launch(self):
        output = self.directory / 'short-campaign'
        self.call('run', '--build', self.build, '--output', output, '--kind', 'campaign', '--seconds', 15, expected=1)
        self.assertFalse(output.exists())

    def test_unjoined_worker_refused(self):
        output = self.copied('unjoined')
        result = campaign.read(output / 'result.json')
        result['jobs'][0]['parent_waited'] = False
        campaign.write(output / 'result.json', result)
        with self.assertRaisesRegex(ValueError, 'unjoined child'):
            campaign.audit(output)

    def test_recovery_fixture_io_failure_cannot_pass_as_fuzz_work(self):
        invalid_tmp = self.directory / 'tmp-is-a-file'
        invalid_tmp.write_text('A deliberately unusable temporary directory.\n')
        receipt = campaign.read(self.build / 'build.json')
        for name in ('recover_journal', 'recover_epoch'):
            with self.subTest(target=name):
                probe = self.directory / ('invalid-tmp-' + name)
                probe.mkdir()
                result = subprocess.run([receipt['binaries'][name]['path'], '-runs=1',
                                         '-artifact_prefix=' + str(probe) + '/',
                                         str(ROOT / 'fuzz/broker/seeds' / name)],
                                        env=dict(os.environ, TMPDIR=str(invalid_tmp)),
                                        cwd=probe, capture_output=True, text=True, timeout=10)
                (probe / 'control.log').write_text(result.stdout + result.stderr)
                campaign.write(probe / 'control.json', {'kind': 'deliberate invalid TMPDIR',
                               'expected_failure': True, 'returncode': result.returncode})
                self.assertNotEqual(result.returncode, 0, 'Fixture creation failed, so recovery was never exercised')
                self.assertIn('fuzz infrastructure:', result.stderr)

    def test_short_real_run_cannot_be_relabelled_as_twenty_four_hours(self):
        output = self.copied('false-duration')
        manifest = campaign.read(output / 'manifest.json')
        manifest.update(kind='campaign', seconds_per_target=86400)
        campaign.write(output / 'manifest.json', manifest)
        campaign.write(output / 'frozen.json', {'manifest_sha256': campaign.sha(output / 'manifest.json')})
        result = campaign.read(output / 'result.json')
        for name in ('manifest.json', 'frozen.json'):
            result['artifacts'][name] = campaign.sha(output / name)
        # Even a forged parent-side elapsed value cannot replace the engine's
        # actual duration in its retained final statistics.
        result['elapsed_seconds'] = 86400
        for job in result['jobs']:
            job['elapsed_seconds'] = 86400
        campaign.write(output / 'result.json', result)
        verdict = campaign.audit(output)
        self.assertEqual(verdict['state'], 'failed')
        self.assertFalse(verdict['campaign_qualified'])

    def test_changed_executable_refused_before_any_worker_starts(self):
        build = self.directory / 'changed-build'
        build.mkdir()
        receipt = copy.deepcopy(campaign.read(self.build / 'build.json'))
        path = build / 'parse_header'
        shutil.copyfile(receipt['binaries']['parse_header']['path'], path)
        with path.open('ab') as stream:
            stream.write(b'changed')
        receipt['binaries']['parse_header']['path'] = str(path)
        campaign.write(build / 'build.json', receipt)
        output = self.directory / 'refused'
        self.call('run', '--build', build, '--output', output, '--kind', 'smoke', '--seconds', 2, expected=1)
        self.assertFalse(output.exists())

    def test_interruption_joins_all_children_and_resume_keeps_the_prior_attempt(self):
        output = self.directory / 'interrupted'
        child = subprocess.Popen([sys.executable, '-B', str(SCRIPT), 'run', '--build', str(self.build),
                                  '--output', str(output), '--kind', 'smoke', '--seconds', '20'],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if (output / 'jobs.json').exists() and (output / 'telemetry.jsonl').stat().st_size > 0:
                    break
                if child.poll() is not None:
                    self.fail('Supervisor exited before interruption')
                time.sleep(.05)
            else:
                self.fail('No live workers before deadline')
            child.send_signal(signal.SIGTERM)
            stdout, stderr = child.communicate(timeout=12)
            self.assertEqual(child.returncode, 0, stdout + stderr)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            if child.stdout:
                child.stdout.close()
            if child.stderr:
                child.stderr.close()
        self.assertEqual(campaign.audit(output)['state'], 'interrupted')
        result = campaign.read(output / 'result.json')
        self.assertEqual(len(result['jobs']), 5)
        for job in result['jobs']:
            self.assertTrue(job['parent_waited'])
            self.assertFalse(Path(f'/proc/{job["pid"]}').exists())
        pins = {name: campaign.sha(output / name) for name in ('manifest.json', 'frozen.json', 'result.json')}
        resumed = self.directory / 'resumed'
        self.call('run', '--build', self.build, '--output', resumed, '--kind', 'smoke', '--seconds', 2, '--resume-from', output)
        self.assertEqual(campaign.audit(resumed)['state'], 'completed')
        self.assertFalse(campaign.audit(resumed)['campaign_qualified'])
        for name, digest in pins.items():
            self.assertEqual(campaign.sha(output / name), digest)


if __name__ == '__main__':
    unittest.main()
