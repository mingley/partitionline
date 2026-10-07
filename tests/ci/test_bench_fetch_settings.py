"""Fetch benchmark limits must be visible and fail before connection on error."""
import json
import os
from pathlib import Path
import socket
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]
BINARY = Path(os.environ.get('PL_BENCH_FETCH_BINARY', ROOT / 'target/release/examples/bench_fetch'))


class FetchBenchmarkSettings(unittest.TestCase):
    def invoke(self, values, config=True):
        prefixes = ('FETCH_', 'GROUP_ID', 'COUNT', 'WARMUP', 'MAX_', 'MIN_', 'VERIFY',
                    'RECORD_', 'SEED', 'PAYLOAD_', 'ISOLATION', 'RUN_TIMEOUT', 'SASL_', 'TLS_')
        env = {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}
        env.update(KAFKA_BOOTSTRAP='127.0.0.1:1')
        env.update(values)
        return subprocess.run([str(BINARY)] + (['--print-config'] if config else []),
                              cwd=ROOT, env=env, capture_output=True, text=True, timeout=5)

    def test_independent_response_and_partition_limits_are_reported(self):
        process = self.invoke({'COUNT': '100', 'WARMUP': '17', 'MAX_BYTES': '8192',
                               'MAX_PARTITION_BYTES': '1024', 'MAX_POLL_RECORDS': '7',
                               'FETCH_BUFFER_MEMORY': '16384', 'FETCH_MODE': 'group',
                               'GROUP_ID': 'settings-test', 'ISOLATION': 'read_committed'})
        self.assertEqual(process.returncode, 0, process.stderr)
        row = json.loads(process.stdout)['effective_settings']
        self.assertEqual((row['count'], row['warmup_records']), (100, 17))
        self.assertEqual((row['max_bytes'], row['max_partition_bytes']), (8192, 1024))
        self.assertEqual(row['max_poll_records'], 7)
        self.assertEqual(row['buffer_memory_bytes'], 16384)
        self.assertEqual((row['mode'], row['group_id']), ('group', 'settings-test'))
        self.assertFalse(row['auto_commit'])
        self.assertEqual(row['isolation'], 'read_committed')
        self.assertEqual(row['seed'], 1592590337)
        self.assertEqual(row['payload_bytes'], 100)

    def test_bad_limits_and_modes_fail_before_network(self):
        for knobs in ({'COUNT': '0'}, {'COUNT': '10', 'WARMUP': '10'}, {'WARMUP': 'bad'},
                      {'MAX_PARTITION_BYTES': '0'}, {'MAX_BYTES': '-1'}, {'MAX_POLL_RECORDS': '0'},
                      {'MAX_WAIT_MS': '-1'}, {'FETCH_BUFFER_MEMORY': '0'}, {'RUN_TIMEOUT_MS': '0'},
                      {'FETCH_MODE': 'unknown'}, {'GROUP_ID': ''}, {'ISOLATION': 'unknown'},
                      {'PAYLOAD_BYTES': '10000001'}, {'VERIFY_HEADERS': '129'}):
            with self.subTest(knobs=knobs), socket.socket() as listener:
                listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(0.02)
                process = self.invoke(knobs | {'KAFKA_BOOTSTRAP': f'127.0.0.1:{listener.getsockname()[1]}'}, config=False)
                self.assertNotEqual(process.returncode, 0)
                self.assertIn('Protocol', process.stderr)
                with self.assertRaises(TimeoutError): listener.accept()

    def test_verification_formats_are_named_and_separate(self):
        for knobs, expected in (({}, 'none'), ({'VERIFY': '1'}, 'null-broker-v1'),
                                ({'RECORD_HISTORY': 'new-journal.jsonl'}, 'history-v1')):
            process = self.invoke(knobs)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(json.loads(process.stdout)['effective_settings']['verification'], expected)
        conflict = self.invoke({'VERIFY': '1', 'RECORD_HISTORY': 'new-journal.jsonl'})
        self.assertNotEqual(conflict.returncode, 0)


if __name__ == '__main__':
    unittest.main()
