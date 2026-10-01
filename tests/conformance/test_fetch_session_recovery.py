"""Negative qualification tests use fabricated histories only inside this fixture."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('fetch_recovery', ROOT / 'scripts/report-fetch-session-recovery.py')
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
SOURCE = 'a' * 40
ATTEMPT = 'b' * 32
TOPIC = 'plfetch-recovery-' + ATTEMPT


def fixture(directory):
    identity = {'source_sha': SOURCE, 'broker_reference': REPORT.REFERENCE, 'actual_reference': REPORT.REFERENCE,
        'container': 'pl-fetch-recovery-' + ATTEMPT, 'topic': TOPIC,
        'backend': '127.0.0.1:19192', 'proxy': '127.0.0.1:19193', 'mapped_backend': '127.0.0.1:19192',
        'advertised_listeners': 'PLAINTEXT://127.0.0.1:19193,INTERNAL://localhost:9094',
        'container_image_id': 'sha256:' + 'c'*64, 'inspected_image_id': 'sha256:' + 'c'*64,
        'repo_digests': [REPORT.REFERENCE], 'java_cli_version': '4.1.2', 'host_os': 'Linux', 'host_arch': 'x86_64',
        'exit_codes': {name:0 for name in ['create','start','readiness','create-topic','build','runtime','java-records','broker-logs','cleanup']}}
    rows = []
    java = []
    for p in range(32):
        for offset in range(2):
            value = f'KL05-07/{p}/{offset}'
            phase = 0 if offset == 0 else 3 if p < 16 else 4
            rows += [f'PL_FETCH_ACK\t{p}\t{offset}\t{p}\t{value}', f'PL_FETCH_RECORD\t{phase}\t{p}\t{offset}\t{p}\t{value}']
            java.append(f'Partition:{p}\tOffset:{offset}\t{p}\t{value}')
    rows += ['PL_FETCH_ACK\t0\t2\t0\tKL05-07/0/2', 'PL_FETCH_RECORD\t5\t0\t2\t0\tKL05-07/0/2']
    java += ['Partition:0\tOffset:2\t0\tKL05-07/0/2']
    rows += [f'PL_FETCH_POSITION\t{p}\t{3 if p==0 else 2}' for p in range(32)]
    def offsets(start, stop, offset):
        return ','.join(f'{p}:{offset}' for p in range(start, stop))
    rows += [
        'PL_FETCH_WIRE\t0\t17\t0\t0\t21\t0\t1100\t'+offsets(0,32,0)+'\t',
        'PL_FETCH_WIRE\t1\t17\t21\t1\t21\t0\t1100\t'+offsets(0,32,1)+'\t',
        'PL_FETCH_WIRE\t2\t17\t21\t2\t21\t0\t25\t\t',
        'PL_FETCH_WIRE\t3\t17\t21\t3\t21\t0\t80\t\t'+','.join(map(str,range(16,32))),
        'PL_FETCH_WIRE\t4\t17\t21\t4\t21\t0\t1100\t'+offsets(0,16,2)+','+offsets(16,32,1)+'\t',
        'PL_FETCH_WIRE\t5\t17\t21\t0\t22\t0\t1100\t'+offsets(0,32,2)+'\t',
        'PL_FETCH_WIRE\t6\t17\t22\t-1\t0\t0\t25\t\t',
        'PL_FETCH_SOURCE\t'+SOURCE, 'PL_FETCH_TOPIC\t'+TOPIC, 'PL_FETCH_COMPLETE',
        'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1s']
    (directory/'identity.json').write_text(json.dumps(identity))
    (directory/'runtime.stdout.log').write_text('\n'.join(rows)+'\n')
    (directory/'java-records.stdout.log').write_text('\n'.join(java)+'\n')


class FetchSessionRecoveryReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        fixture(self.directory)

    def identity(self, mutate):
        path = self.directory/'identity.json'
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))

    def replace(self, old, new, filename='runtime.stdout.log'):
        path = self.directory/filename
        self.assertIn(old, path.read_text())
        path.write_text(path.read_text().replace(old,new,1))

    def rejected(self):
        with self.assertRaises((ValueError, KeyError)):
            REPORT.finish(self.directory, SOURCE)
        self.assertFalse((self.directory/'report.json').exists())

    def test_complete_fixture_passes_and_retains_full_history(self):
        result = REPORT.finish(self.directory, SOURCE)
        self.assertEqual(len(result['runtime']['wire']), 7)
        self.assertEqual(len(result['java_records']), 65)
        self.assertEqual(result['status'], 'passed')

    def test_wrong_source_rejected(self):
        self.identity(lambda x:x.update(source_sha='d'*40))
        self.rejected()

    def test_wrong_actual_digest_rejected(self):
        self.identity(lambda x:x.update(actual_reference='apache/kafka:latest'))
        self.rejected()

    def test_missing_process_status_rejected(self):
        self.identity(lambda x:x['exit_codes'].pop('runtime'))
        self.rejected()

    def test_failed_or_boolean_process_status_rejected(self):
        for status in [1,False]:
            with self.subTest(status=status):
                self.identity(lambda x:x['exit_codes'].update(runtime=status))
                self.rejected()

    def test_bypassed_observer_rejected(self):
        self.identity(lambda x:x.update(advertised_listeners='PLAINTEXT://127.0.0.1:19192,INTERNAL://localhost:9094'))
        self.rejected()

    def test_missing_completion_rejected(self):
        self.replace('PL_FETCH_COMPLETE','')
        self.rejected()

    def test_duplicate_acknowledgement_rejected(self):
        self.replace('PL_FETCH_ACK\t0\t2\t0\tKL05-07/0/2','PL_FETCH_ACK\t0\t1\t0\tKL05-07/0/1')
        self.rejected()

    def test_stale_paused_delivery_rejected(self):
        self.replace('PL_FETCH_RECORD\t4\t16\t1', 'PL_FETCH_RECORD\t3\t16\t1')
        self.rejected()

    def test_reset_must_be_observed_with_old_id_and_new_positive_id(self):
        self.replace('PL_FETCH_WIRE\t5\t17\t21\t0\t22', 'PL_FETCH_WIRE\t5\t17\t21\t5\t21')
        self.rejected()

    def test_epoch_discontinuity_rejected(self):
        self.replace('PL_FETCH_WIRE\t2\t17\t21\t2', 'PL_FETCH_WIRE\t2\t17\t21\t3')
        self.rejected()

    def test_missing_forgotten_partition_rejected(self):
        self.replace('\t16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31', '\t16,17,18')
        self.rejected()

    def test_reset_changed_offset_rejected(self):
        self.replace('PL_FETCH_WIRE\t5\t17\t21\t0\t22\t0\t1100\t0:2', 'PL_FETCH_WIRE\t5\t17\t21\t0\t22\t0\t1100\t0:1')
        self.rejected()

    def test_missing_terminal_close_rejected(self):
        self.replace('PL_FETCH_WIRE\t6\t17\t22\t-1\t0\t0\t25\t\t', '')
        self.rejected()

    def test_java_duplicate_or_wrong_payload_rejected(self):
        self.replace('Partition:0\tOffset:2\t0\tKL05-07/0/2', 'Partition:0\tOffset:1\t0\tKL05-07/0/1', 'java-records.stdout.log')
        self.rejected()

    def test_skipped_required_runtime_rejected(self):
        self.replace('1 passed; 0 failed; 0 ignored;', '0 passed; 0 failed; 1 ignored;')
        self.rejected()


if __name__=='__main__': unittest.main()
