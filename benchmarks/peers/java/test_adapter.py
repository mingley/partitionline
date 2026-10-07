"""Actual pinned JVM/SDK config and byte-vector tests; no broker required."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('java_peer', HERE / 'run.py')
peer = importlib.util.module_from_spec(spec); spec.loader.exec_module(peer)

class PeerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Missing prerequisites are a failure, rather than a skipped qualification.
        cls.binary = Path(os.environ['JAVA_PEER_BUILD_DIR']) / 'java-peer.jar'
        cls.manifest = json.loads((cls.binary.parent / 'build-manifest.json').read_text())
        cls.pin = json.loads((HERE / 'source-pin.json').read_text())
        cls.command = peer.verified_command(cls.binary, cls.manifest, cls.pin)

    def java(self, command='emit-config', settings=None, code=0):
        with patch.dict(os.environ, settings or {}, clear=True):
            result = subprocess.run(self.command + [command], env=peer.build.environment(),
                                    text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, code, result.stderr)
        return json.loads(result.stdout) if code == 0 else result

    def test_actual_resolved_sdk_defaults_and_caps(self):
        result = self.java(settings={'ACKS': '-1', 'IDEMPOTENT': '1', 'MAX_IN_FLIGHT': '3',
            'LINGER_MS': '7', 'BATCH_BYTES': '65536', 'QUEUE_KBYTES': '1024', 'ISOLATION': 'read_committed'})
        p, c = result['producer'], result['consumer']
        self.assertEqual((str(p['acks']), p['enable.idempotence'], p['max.in.flight.requests.per.connection']), ('-1', True, 3))
        self.assertEqual((p['linger.ms'], p['batch.size'], p['buffer.memory']), (7, 65536, 1048576))
        self.assertFalse(c['enable.auto.commit']); self.assertFalse(c['allow.auto.create.topics'])
        self.assertEqual(c['isolation.level'], 'read_committed')
        self.assertEqual((result['sdk_version'], result['java_version']), ('4.3.1', '21.0.12.1'))

    def test_all_codecs_and_zstd_explicit_level(self):
        for codec in ('none', 'gzip', 'snappy', 'lz4', 'zstd'):
            with self.subTest(codec=codec):
                result = self.java(settings={'COMPRESSION': codec})
                self.assertEqual(result['producer']['compression.type'], codec)
                if codec == 'zstd':
                    self.assertEqual(result['producer']['compression.zstd.level'], 3)
                    self.assertEqual(result['zstd_backend']['version'], '1.5.6')

    def test_full_unsigned_seed_and_rust_c_record_vectors(self):
        mask = 2**64-1
        def mix(value):
            value = (value + 0x9e3779b97f4a7c15) & mask
            value = ((value ^ (value >> 30)) * 0xbf58476d1ce4e5b9) & mask
            value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & mask
            return value ^ (value >> 31)
        for seed in (0x5eed0001, mask):
            rows = self.java('vectors', {'RECORD_SEED': hex(seed)})
            self.assertEqual(len(rows), 24)
            for row in rows:
                record_id = row['id']
                expected_key = record_id.to_bytes(8, 'big') + mix(seed ^ record_id).to_bytes(8, 'big')
                state = seed ^ ((record_id * 0x9e3779b97f4a7c15) & mask)
                payload = b''
                while len(payload) < row['bytes']:
                    state = mix(state); payload += state.to_bytes(8, 'big')
                self.assertEqual(row['key'], expected_key.hex())
                self.assertEqual(row['value'], payload[:row['bytes']].hex())

    def test_actual_sdk_refuses_implicit_idempotence_adjustments(self):
        for settings in ({'IDEMPOTENT': '1'}, {'IDEMPOTENT': '1', 'ACKS': '-1', 'MAX_IN_FLIGHT': '6'},
                         {'DELIVERY_TIMEOUT_MS': '5', 'LINGER_MS': '5'}):
            with self.subTest(settings=settings): self.java(settings=settings, code=1)

    def test_unsupported_settings_reject_without_secret_echo(self):
        for name, value in {'BATCH_RECORDS': '32768', 'TLS_CA_PEM': 'private.pem',
            'SASL_PASSWORD': 'do-not-emit-secret', 'GROUP_ID': 'group', 'OPEN_LOOP_RATE': '100',
            'TRANSACTIONAL_ID': 'transaction', 'KEY_MODE': 'none', 'SECURITY_PROTOCOL': 'SSL'}.items():
            with self.subTest(name=name):
                result = self.java(settings={name: value}, code=1)
                self.assertNotIn('do-not-emit-secret', result.stderr)

    def test_adapter_rejects_same_unsupported_settings_before_jvm(self):
        for settings in ({'BATCH_RECORDS': '1'}, {'TRANSACTIONAL_ID': 'x'}, {'GROUP_ID': 'x'},
                         {'OPEN_LOOP_RATE': '1'}, {'SASL_MECHANISM': 'PLAIN', 'SASL_USERNAME': 'u', 'SASL_PASSWORD': 'p'},
                         {'COUNT': '10000001'}, {'KEY_MODE': 'none', 'PAYLOAD_MODE': 'constant-x'}):
            with patch.dict(os.environ, settings, clear=True), self.assertRaises(ValueError): peer.settings()

    def test_jvm_and_native_injection_options_removed(self):
        settings = {key: 'injected' for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
                                               'CLASSPATH', 'LD_PRELOAD', 'LD_LIBRARY_PATH')}
        with patch.dict(os.environ, settings, clear=True):
            env = peer.process_env(peer.settings())
            self.assertFalse(set(settings) & set(env))

    def test_byte_pin_tampering_fails_before_client(self):
        with tempfile.TemporaryDirectory() as directory:
            copied = Path(directory) / 'build'
            shutil.copytree(self.binary.parent, copied)
            jar = copied / 'java-peer.jar'
            with jar.open('ab') as file: file.write(b'changed')
            with self.assertRaises(ValueError): peer.verified_command(jar, self.manifest, self.pin)
        changed = dict(self.manifest, sources={})
        with self.assertRaises(ValueError): peer.verified_command(self.binary, changed, self.pin)

    def test_result_exclusive_creation_keeps_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempt.json'
            peer.write(path, {'failed': True})
            with self.assertRaises(FileExistsError): peer.write(path, {'failed': False})
            self.assertTrue(json.loads(path.read_text())['failed'])

    def test_failed_offline_build_retains_actual_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'failed-build'
            result = subprocess.run(['python3', '-B', str(HERE / 'build.py'), '--output', str(output),
                '--cache', str(Path(directory) / 'empty-cache'), '--offline'], capture_output=True, text=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            inputs = json.loads((output / 'build-inputs.json').read_text())
            self.assertEqual(inputs['sources'], peer.build.sources())
            for name, checksum in inputs['sources'].items():
                self.assertEqual(peer.sha(output / 'executed-source' / name), checksum)
            self.assertIn('missing offline jar', result.stderr)
            self.assertFalse((output / 'build-manifest.json').exists())

if __name__ == '__main__':
    unittest.main()
