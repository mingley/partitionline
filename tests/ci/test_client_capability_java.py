"""Pure metadata/command controls. Synthetic bytes do not qualify Kafka codecs."""
import ast
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

REPO = Path(os.environ.get('CAPABILITY_JAVA_REPO', Path(__file__).resolve().parents[2]))
HELPER = Path(os.environ.get('CAPABILITY_JAVA_HELPER', REPO / 'scripts/client-capability-java.py'))
SPEC = importlib.util.spec_from_file_location('capability_java', HELPER)
JAVA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(JAVA)


class JavaControls(unittest.TestCase):
    def setUp(self):
        self.previous_umask = os.umask(0o077)
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        for name in JAVA.SOURCE_PINS:
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((REPO / name).read_bytes())
        self.evidence = self.root / 'target/evidence'
        self.evidence.mkdir(parents=True)
        self.calls = []
        self.saved = {}

    def tearDown(self):
        self.temporary.cleanup()
        os.umask(self.previous_umask)

    def command(self, name, argv, **options):
        self.calls.append((name, argv, options))
        return dict(exit_code=0, trigger=None), b'', b''

    def save(self, name, data):
        self.assertNotIn(name, self.saved)
        self.saved[name] = data

    def proof(self):
        return JAVA.JavaFixtureProof(self.root, self.evidence, self.command, self.save)

    def fixture(self, suite='write_txn_markers_v2', release='4.1.2'):
        directory = self.root / 'fixtures' / suite / release
        directory.mkdir(parents=True)
        cases = []
        lines = []
        for name, (api, version, status) in sorted(JAVA.expected_cases(suite, release).items()):
            lines.append(f'{name}\t{api}\t{version}\t{status}')
            case = dict(name=name, api_key=api, api_version=version)
            if status == 'unsupported':
                case.update(status='actual-official-unsupported', exception_type='org.apache.kafka.common.errors.UnsupportedVersionException')
            else:
                case['status'] = 'actual-official-serialized-and-parsed'
                for side in ('request', 'response'):
                    # Deliberately synthetic fixture bytes; this tests the file
                    # inventory guard, not any SDK parser or Kafka semantics.
                    data = (name + ':' + side).encode()
                    (directory / (name + '.' + side + '.bin')).write_bytes(data)
                    case[side + '_hex'] = data.hex()
                    case[side + '_sha256'] = JAVA.digest(data)
            cases.append(case)
        (directory / 'cases.tsv').write_text('\n'.join(lines) + '\n')
        (directory / 'goldens.json').write_text(json.dumps(dict(schema_version=1, cases=cases)))
        nulls = []
        for name, (side, version, field) in JAVA.expected_nulls(suite, release).items():
            (directory / (name + '.bin')).write_bytes(('null:' + name).encode())
            nulls.append('\t'.join((name, side, version, field, 'java.lang.RuntimeException')))
        (directory / 'null-controls.tsv').write_text('\n'.join(nulls) + '\n')
        return directory

    def export(self, directory, suite, release):
        lines = []
        for name, (api, version, status) in sorted(JAVA.expected_cases(suite, release).items()):
            if api != JAVA.SUITES[suite]['api'] or status != 'supported':
                continue
            pair = [(directory / (name + '.' + side + '.bin')).read_bytes().hex() for side in ('request', 'response')]
            lines.append('\t'.join((name, str(api), str(version), *pair)))
        (directory / 'rust-emitted.tsv').write_text('\n'.join(lines) + '\n')

    def test_pins_match_both_existing_peer_runners(self):
        for name in ('run-write-txn-markers-v2.py', 'run-share-offsets-v1.py'):
            parsed = ast.parse((REPO / 'tests/conformance' / name).read_text())
            values = {node.targets[0].id: ast.literal_eval(node.value) for node in parsed.body
                      if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                      and node.targets[0].id in ('PINS', 'SLF4J')}
            self.assertEqual(values['PINS'], JAVA.PINS)
            self.assertEqual(values['SLF4J'], JAVA.SLF4J)
        for name, expected in JAVA.SOURCE_PINS.items():
            self.assertEqual(JAVA.digest((REPO / name).read_bytes()), expected)

    def test_explicit_generation_and_reverse_totals(self):
        marker = []
        share = []
        for release in JAVA.RELEASES:
            rows = JAVA.expected_cases('write_txn_markers_v2', release)
            self.assertEqual(len(rows), 97)
            marker.extend(value for value in rows.values() if value[0] == 27)
            share.extend(JAVA.expected_cases('share_offsets_v1', release).values())
        self.assertEqual((sum(r[2] == 'supported' for r in marker), sum(r[2] == 'unsupported' for r in marker)), (125, 100))
        self.assertEqual((sum(r[2] == 'supported' for r in share), sum(r[2] == 'unsupported' for r in share)), (100, 20))
        self.assertEqual(sum(len(JAVA.expected_nulls('write_txn_markers_v2', r)) for r in JAVA.RELEASES), 40)
        self.assertEqual(sum(len(JAVA.expected_nulls('share_offsets_v1', r)) for r in JAVA.RELEASES), 45)

    def test_source_mismatch_fails_before_any_command(self):
        proof = self.proof()
        first = next(iter(JAVA.SOURCE_PINS))
        (self.root / first).write_bytes(b'changed source')
        with self.assertRaisesRegex(JAVA.FixtureError, 'pinned Java input differs'):
            proof.prepare()
        self.assertEqual(self.calls, [])
        self.assertFalse(proof.report()['passed'])
        self.assertIn('prepare_failure', proof.report())
        self.assertIn('capability-java/inputs-preparation-after.json', self.saved)

    def test_command_adapter_strips_all_java_injection_options(self):
        proof = self.proof()
        proof.checked('identity', ['java', '-version'], java=True)
        self.assertEqual(self.calls[0][2]['remove_env'], JAVA.JAVA_ENV_KEYS)
        self.assertEqual(self.calls[0][2]['timeout'], 120)

    def test_plan_uses_pinned_jars_and_two_argument_reverse_mode(self):
        proof = self.proof()
        for suite, spec in JAVA.SUITES.items():
            for release in JAVA.RELEASES:
                generation = proof.java(suite, release)
                null = proof.java(suite, release, null=True)
                reverse = proof.java(suite, release, reverse=True)
                self.assertEqual(generation[:2], ['java', '--class-path'])
                self.assertIn('kafka-clients-' + release + '.jar', generation[2])
                self.assertIn('slf4j-api-1.7.36.jar', generation[2])
                self.assertTrue(generation[3].endswith('/' + spec['oracle']))
                self.assertTrue(null[3].endswith('/' + spec['null']))
                self.assertEqual(reverse[-1], generation[-1])
                self.assertNotEqual(reverse[-2], generation[-1])
                self.assertTrue(Path(generation[-1]).is_relative_to(self.evidence))
                self.assertTrue(Path(reverse[-2]).is_relative_to(self.evidence))
                self.assertEqual(len(reverse), len(generation) + 1)

    def test_outputs_cannot_escape_target(self):
        with self.assertRaisesRegex(JAVA.FixtureError, 'under target'):
            JAVA.JavaFixtureProof(self.root, self.root / 'outside', self.command, self.save)
        self.assertFalse((self.root / 'outside').exists())

    def test_all_generated_sets_have_exact_member_counts(self):
        observed = {}
        for suite in JAVA.SUITES:
            counts = []
            for release in JAVA.RELEASES:
                result = JAVA.validate_fixtures(self.fixture(suite, release), suite, release)
                counts.append(len(result['files']))
            observed[suite] = counts
        self.assertEqual(observed, {'write_txn_markers_v2': [83, 163, 163], 'share_offsets_v1': [52, 101, 101]})

    def test_same_count_renamed_case_is_rejected(self):
        directory = self.fixture()
        path = directory / 'cases.tsv'
        rows = path.read_text().splitlines()
        rows[0] = 'unrelated-case\t' + rows[0].split('\t', 1)[1]
        path.write_text('\n'.join(rows) + '\n')
        with self.assertRaisesRegex(JAVA.FixtureError, 'exact official case'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_same_count_duplicate_case_is_rejected(self):
        directory = self.fixture()
        path = directory / 'cases.tsv'
        rows = path.read_text().splitlines()
        rows[-1] = rows[0]
        path.write_text('\n'.join(rows) + '\n')
        with self.assertRaisesRegex(JAVA.FixtureError, 'duplicate table case'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_changed_official_frame_is_rejected(self):
        directory = self.fixture()
        path = next(directory.glob('api-27-*.request.bin'))
        path.write_bytes(b'changed bytes')
        with self.assertRaisesRegex(JAVA.FixtureError, 'binary/JSON pin'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_missing_null_rejection_is_rejected(self):
        directory = self.fixture()
        path = directory / 'null-controls.tsv'
        path.write_text('\n'.join(path.read_text().splitlines()[1:]) + '\n')
        with self.assertRaisesRegex(JAVA.FixtureError, 'null rejection inventory'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_unsupported_cell_cannot_leave_a_payload(self):
        directory = self.fixture()
        (directory / 'api-27-v0-tv-0.request.bin').write_bytes(b'unexpected')
        with self.assertRaisesRegex(JAVA.FixtureError, 'member inventory'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_symlink_cannot_enter_generated_members(self):
        directory = self.fixture()
        (directory / 'extra').symlink_to(directory / 'cases.tsv')
        with self.assertRaisesRegex(JAVA.FixtureError, 'non-private/non-regular'):
            JAVA.validate_fixtures(directory, 'write_txn_markers_v2', '4.1.2')

    def test_rust_reverse_exports_cover_only_the_target_supported_cases(self):
        for suite, counts in [('write_txn_markers_v2', [25, 50, 50]), ('share_offsets_v1', [20, 40, 40])]:
            for release, expected in zip(JAVA.RELEASES, counts):
                directory = self.fixture(suite, release)
                self.export(directory, suite, release)
                JAVA.validate_fixtures(directory, suite, release, with_rust=True)
                result = JAVA.validate_rust_export(directory, suite, release)
                self.assertEqual((result['pair_count'], result['expected_java_parse_checks']), (expected, 2 * expected))

    def test_final_report_rechecks_completed_fixture_members(self):
        proof = self.proof()
        # Synthetic completed metadata isolates final tamper detection; no Java
        # command or real codec qualification is claimed by this unit control.
        proof.state['prepared'] = True
        proof.verify_inputs = lambda: None
        changed = None
        for suite, spec in JAVA.SUITES.items():
            generated = {}
            reversed_rows = {}
            for release in JAVA.RELEASES:
                directory = self.fixture(suite, release)
                target = proof.directory / 'fixtures' / spec['directory'] / release
                target.parent.mkdir(parents=True, exist_ok=True)
                directory.rename(target)
                generated[release] = JAVA.validate_fixtures(target, suite, release)
                self.export(target, suite, release)
                reversed_rows[release] = JAVA.validate_rust_export(target, suite, release)
                changed = next(target.glob('api-*.request.bin'))
            proof.state['suites'][suite] = dict(passed=True, generated=generated, reverse=reversed_rows)
        self.assertTrue(proof.report()['passed'])
        changed.write_bytes(b'changed after Java reverse')
        report = proof.report()
        self.assertFalse(report['passed'])
        self.assertIsNotNone(report['final_failure'])
        self.assertEqual(self.calls, [])

    def test_empty_rust_reverse_table_cannot_pass(self):
        directory = self.fixture()
        (directory / 'rust-emitted.tsv').write_text('')
        with self.assertRaisesRegex(JAVA.FixtureError, 'exact Rust reverse'):
            JAVA.validate_rust_export(directory, 'write_txn_markers_v2', '4.1.2')

    def test_renamed_reverse_case_cannot_keep_only_the_count(self):
        directory = self.fixture()
        self.export(directory, 'write_txn_markers_v2', '4.1.2')
        path = directory / 'rust-emitted.tsv'
        rows = path.read_text().splitlines()
        rows[0] = 'unrelated-case\t' + rows[0].split('\t', 1)[1]
        path.write_text('\n'.join(rows) + '\n')
        with self.assertRaisesRegex(JAVA.FixtureError, 'exact Rust reverse'):
            JAVA.validate_rust_export(directory, 'write_txn_markers_v2', '4.1.2')


if __name__ == '__main__':
    unittest.main()
