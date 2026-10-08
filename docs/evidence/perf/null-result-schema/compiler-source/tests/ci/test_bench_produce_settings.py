"""The standalone producer benchmark must report and validate effective knobs."""
import json
import os
from pathlib import Path
import socket
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]
BINARY = Path(os.environ.get('PL_BENCH_PRODUCE_BINARY', ROOT / 'target/release/examples/bench_produce'))


class ProducerBenchmarkSettings(unittest.TestCase):
    def invoke(self, values, config=True):
        # Preserve runtime/toolchain setup while removing benchmark inputs.
        prefixes = ('BENCH_', 'PAYLOAD_', 'WARMUP', 'MEASURE_', 'BATCH_', 'KEY_', 'RECORD_',
                    'QUEUE_', 'MAX_IN_FLIGHT', 'IDEMPOTENT', 'CONNECTIONS', 'LINGER_',
                    'BUFFER_MEMORY', 'ACKS', 'DELIVERY_TIMEOUT', 'RUN_TIMEOUT', 'MAX_REQUEST_SIZE',
                    'SASL_', 'TLS_', 'SEED', 'COUNT', 'PARTITIONS', 'COMPRESSION', 'KAFKA_TOPIC')
        env = {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}
        env.update(KAFKA_BOOTSTRAP='127.0.0.1:1')
        env.update(values)
        return subprocess.run([str(BINARY)] + (['--print-config'] if config else []),
                              cwd=ROOT, env=env, capture_output=True, text=True, timeout=5)

    def test_report_contains_schema_required_effective_knobs(self):
        process = self.invoke({'BATCH_SIZE': '8192', 'BATCH_RECORDS': '7', 'WARMUP': '17',
                               'COUNT': '100', 'SEED': '0xffff', 'ACKS': '-1', 'IDEMPOTENT': '1',
                               'MAX_IN_FLIGHT': '3', 'CONNECTIONS': '2', 'QUEUE_KBYTES': '1024'})
        self.assertEqual(process.returncode, 0, process.stderr)
        row = json.loads(process.stdout)['effective_settings']
        required = json.loads((ROOT / 'benchmarks/result-schema.json').read_text())['definitions']['kafka_result']['properties']['provenance']['properties']['config']['properties']['effective_settings']['required']
        self.assertTrue(set(required) <= set(row))
        self.assertEqual(row['batch_size_bytes'], 8192)
        self.assertEqual(row['batch_records'], 7)
        self.assertEqual(row['warmup_requested_records'], 17)
        self.assertEqual(row['record_seed'], 65535)
        self.assertEqual(row['max_in_flight'], 3)
        self.assertTrue(row['idempotence'])
        self.assertEqual(row['buffer_memory_bytes'], 1024 * 1024)

    def test_unknown_knobs_alias_conflicts_and_clamping_fail_before_network(self):
        values = ({'BATCH_RECODS': '7'}, {'BENCH_UNKNOWN': '1'}, {'WARMUP_RECORDS': '7'},
                  {'BATCH_SIZE': '8192', 'BATCH_BYTES': '4096'},
                  {'RECORD_SEED': '1', 'SEED': '2'},
                  {'IDEMPOTENT': '1', 'ACKS': '-1', 'MAX_IN_FLIGHT': '6'},
                  {'BATCH_BYTES': '2097152'}, {'COUNT': '0'})
        for knobs in values:
            with self.subTest(knobs=knobs), socket.socket() as listener:
                listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(0.02)
                process = self.invoke(knobs | {'KAFKA_BOOTSTRAP': f'127.0.0.1:{listener.getsockname()[1]}'}, config=False)
                self.assertNotEqual(process.returncode, 0)
                self.assertIn('Protocol', process.stderr)
                with self.assertRaises(TimeoutError): listener.accept()

    def test_seeded_is_default_and_legacy_bytes_are_explicit(self):
        default = self.invoke({})
        self.assertEqual(default.returncode, 0, default.stderr)
        row = json.loads(default.stdout)['effective_settings']
        self.assertEqual((row['payload_mode'], row['key_mode']), ('seeded', 'id'))
        legacy = self.invoke({'PAYLOAD_MODE': 'constant-x', 'KEY_MODE': 'none'})
        self.assertEqual(legacy.returncode, 0, legacy.stderr)
        row = json.loads(legacy.stdout)['effective_settings']
        self.assertEqual((row['payload_mode'], row['key_mode']), ('constant-x', 'none'))

    def test_bad_command_line_cannot_open_a_connection(self):
        process = subprocess.run([str(BINARY), '--unknown-knob'], cwd=ROOT,
                                 capture_output=True, text=True, timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn('usage:', process.stderr)


if __name__ == '__main__':
    unittest.main()
