"""A live adapter claim requires complete Rust and independent Java histories."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('verifiable_report', ROOT / 'scripts/report-verifiable-scenario.py')
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)
PROFILE = json.loads(report.PROFILE.read_text())
SOURCE = '1' * 40
TOPIC = 'plverifiable-4-1-2-' + '2' * 32
GROUP = TOPIC + '-group'


def fixture():
    stamp = 1001
    def event(name, **fields):
        nonlocal stamp
        row = {'timestamp': stamp, 'name': name, **fields}
        stamp += 1
        return row
    def record(offset):
        return {'topic': TOPIC, 'partition': 0, 'offset': offset, 'key': None, 'value': str(offset)}
    partition = {'topic': TOPIC, 'partition': 0}
    producer = [event('startup_complete')]
    producer += [event('producer_send_success', **record(offset)) for offset in range(25)]
    producer += [event('shutdown_complete'), event('tool_data', sent=25, acked=25,
                  target_throughput=-1, avg_throughput=10.5)]
    consumer = [event('startup_complete'), event('partitions_assigned', partitions=[partition])]
    # Two polls verify per-poll summary/commit bookkeeping, not just final totals.
    for first, end in [(0, 10), (10, 25)]:
        consumer += [event('record_data', **record(offset)) for offset in range(first, end)]
        consumer += [event('records_consumed', count=end-first, partitions=[partition |
                      {'count': end-first, 'minOffset': first, 'maxOffset': end-1}]),
                     event('offsets_committed', success=True, offsets=[partition | {'offset': end}])]
    consumer += [event('partitions_revoked', partitions=[partition]), event('shutdown_complete')]
    identity = {'candidate_source_sha': SOURCE, 'profile_sha256': hashlib.sha256(report.PROFILE.read_bytes()).hexdigest(),
                'broker_reference': PROFILE['broker_reference'], 'broker_version': '4.1.2',
                'container_image_id': 'sha256:' + '3'*64, 'inspected_image_id': 'sha256:' + '3'*64,
                'repo_digests': ['apache/kafka@' + PROFILE['broker_reference'].split('@')[1]],
                'kafka_cli_version': '4.1.2',
                'prerequisite_exit_codes': {'build': 0, 'create-topic': 0},
                'topic': TOPIC, 'group': GROUP, 'started_ms': 1000, 'ended_ms': stamp,
                'exit_codes': dict.fromkeys(['producer', 'consumer', 'java-records', 'java-offsets'], 0)}
    return identity, producer, consumer


def write_fixture(directory, identity, producer, consumer):
    (directory / 'identity.json').write_text(json.dumps(identity))
    for name, rows in [('producer', producer), ('consumer', consumer)]:
        (directory / f'{name}.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
        (directory / f'{name}.stderr.log').write_text('')
    (directory / 'java-records.log').write_text(''.join(f'Partition:0\tOffset:{i}\tnull\t{i}\n' for i in range(25)))
    (directory / 'java-offsets.log').write_text(f"Consumer group '{GROUP}' has no active members.\n\n"
        'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n'
        f'{GROUP} {TOPIC} 0 25 25 0 - - -\n')


class VerifiableScenario(unittest.TestCase):
    def test_actual_cli_bare_version_or_commit_suffix_qualifies(self):
        for version in ['4.1.2', '4.1.2 (Commit:fixture)']:
            with self.subTest(version=version), tempfile.TemporaryDirectory() as temporary:
                identity, producer, consumer = fixture()
                identity['kafka_cli_version'] = version
                directory = Path(temporary)
                write_fixture(directory, identity, producer, consumer)
                report.validate(directory, SOURCE)

    def test_complete_history_qualifies_exactly_one_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_fixture(directory, *fixture())
            result = report.validate(directory, SOURCE)
            self.assertEqual(result['case_summary']['denominator_cases'], 1)
            self.assertEqual(result['consumer']['commits'], 2)
            self.assertEqual(result['java']['lag'], 0)
            self.assertEqual(result['cases'][0]['source_pin'], PROFILE['contract_source_pin'])
            self.assertEqual(result['candidate_source_sha'], SOURCE)

    def test_wrong_identity_or_process_cannot_qualify(self):
        edits = [lambda i:i.update(candidate_source_sha='3'*40),
                 lambda i:i.update(profile_sha256='0'*64),
                 lambda i:i.update(broker_reference='apache/kafka:4.1.2'),
                 lambda i:i.update(broker_version='4.1.0'),
                 lambda i:i.update(inspected_image_id='sha256:'+'4'*64),
                 lambda i:i.update(repo_digests=[]), lambda i:i.update(kafka_cli_version='4.1.0 (Commit:old)'),
                 lambda i:i.update(prerequisite_exit_codes={'build':False, 'create-topic':0}),
                 lambda i:i.update(topic='unowned-topic'), lambda i:i.update(group='unowned-group'),
                 lambda i:i['exit_codes'].pop('consumer'),
                 lambda i:i['exit_codes'].update(consumer=124),
                 lambda i:i['exit_codes'].update(producer=False),
                 lambda i:i.update(started_ms=True), lambda i:i.update(ended_ms=1000)]
        for edit in edits:
            with self.subTest(edit=edit), tempfile.TemporaryDirectory() as temporary:
                identity, producer, consumer = fixture()
                edit(identity)
                directory = Path(temporary)
                write_fixture(directory, identity, producer, consumer)
                with self.assertRaises(ValueError):
                    report.validate(directory, SOURCE)

    def test_broken_producer_history_fails(self):
        edits = [lambda p:p.pop(5), lambda p:p.insert(5, copy.deepcopy(p[5])),
                 lambda p:p[5].update(offset=0), lambda p:p[5].update(value='wrong'),
                 lambda p:p[5].update(partition=False), lambda p:p[5].update(key='key'),
                 lambda p:p[-1].update(acked=24), lambda p:p[-1].update(sent=True),
                 lambda p:p[-1].update(avg_throughput=float('nan')),
                 lambda p:p[-2].update(name='startup_complete'),
                 lambda p:p[4].update(timestamp=999), lambda p:p[4].update(extra=0),
                 lambda p:p[4].update(name='producer_send_error')]
        for edit in edits:
            with self.subTest(edit=edit):
                identity, producer, _ = fixture()
                edit(producer)
                text = '\n'.join(map(json.dumps, producer))
                with self.assertRaises(ValueError):
                    report.producer_history(text, TOPIC, 25, identity['started_ms'], identity['ended_ms'])

    def test_broken_consumer_poll_or_lifecycle_fails(self):
        edits = [lambda c:c.pop(5), lambda c:c.insert(5, copy.deepcopy(c[5])),
                 lambda c:c[5].update(offset=0), lambda c:c[5].update(value='wrong'),
                 lambda c:c[1]['partitions'][0].update(partition=False),
                 lambda c:c[12]['partitions'][0].update(minOffset=False),
                 lambda c:c[12]['partitions'][0].update(count=9),
                 lambda c:c[13]['offsets'][0].update(offset=9),
                 lambda c:c[13]['offsets'][0].update(partition=False),
                 lambda c:c[13].update(success=False), lambda c:c.pop(13),
                 lambda c:c[-2].update(name='partitions_assigned'),
                 lambda c:c.pop(), lambda c:c[5].update(timestamp=999)]
        for edit in edits:
            with self.subTest(edit=edit):
                identity, _, consumer = fixture()
                edit(consumer)
                with self.assertRaises(ValueError):
                    report.consumer_history('\n'.join(map(json.dumps, consumer)), TOPIC, 25,
                                            identity['started_ms'], identity['ended_ms'])

    def test_duplicate_keys_blank_and_nonfinite_events_fail(self):
        for text in ['{"timestamp":1001,"name":"startup_complete","name":"startup_complete"}',
                     '\n', '{"timestamp":NaN,"name":"startup_complete"}']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                report.events(text, {'startup_complete':set()}, 1000, 2000)

    def test_incomplete_or_corrupted_java_history_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_fixture(directory, *fixture())
            records = (directory / 'java-records.log').read_text()
            offsets = (directory / 'java-offsets.log').read_text()
            for bad_records, bad_offsets in [('\n'.join(records.splitlines()[:-1]), offsets),
                    (records + records.splitlines()[0] + '\n', offsets),
                    (records.replace('Offset:5', 'Offset:4'), offsets),
                    (records.replace('\tnull\t5', '\tnull\twrong'), offsets),
                    (records, offsets.replace('25 25 0', '24 25 1')),
                    (records, offsets + offsets.splitlines()[-1] + '\n'),
                    (records, offsets.replace('- - -', 'member host client')),
                    (records, offsets.replace("has no active members.", "unexpected warning"))]:
                with self.subTest(records=bad_records[-80:], offsets=bad_offsets[-80:]), self.assertRaises(ValueError):
                    report.java_history(bad_records, bad_offsets, TOPIC, GROUP, 25)

    def test_rust_stderr_or_missing_artifact_fails(self):
        for file, content in [('producer.stderr.log', 'warning'), ('consumer.stderr.log', 'error'),
                              ('consumer.jsonl', None)]:
            with self.subTest(file=file), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                write_fixture(directory, *fixture())
                path = directory / file
                path.unlink() if content is None else path.write_text(content)
                with self.assertRaises((ValueError, OSError)):
                    report.validate(directory, SOURCE)

    def test_single_case_cannot_pass_full_primary_registry(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_fixture(directory, *fixture())
            result = report.validate(directory, SOURCE)
            spec = importlib.util.spec_from_file_location('primary_report', ROOT / 'scripts/conformance-report.py')
            primary = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(primary)
            registry = json.loads((ROOT / 'tests/conformance/cases.json').read_text())
            with self.assertRaisesRegex(primary.ConformanceValidationError, 'Missing 105 required'):
                primary.validate_and_aggregate_reports(registry, [('single live scenario', result)],
                    require_independent_pass=True, repo_root=ROOT)
            self.assertEqual(sum(case['denominator'] for case in registry['cases']), 86)


if __name__ == '__main__':
    unittest.main()
