#!/usr/bin/env python3
"""Check fresh public-client receipts and independently reverse-decode durable bytes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True
TIME = 1_700_000_000_000
RELEASES = ('4.1.2', '4.2.1', '4.3.1')
PROFILE = {0: (3, 13), 1: (4, 6), 2: (1, 3), 3: (0, 13), 18: (0, 4), 19: (2, 4), 20: (1, 6)}
FIELDS = ('topic', 'partition', 'offset', 'timestamp', 'key_hex', 'value_hex', 'headers')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    assert path.stat().st_size <= 4 * 1024 * 1024, path
    return json.loads(path.read_text())


def receipt(record):
    record = {key: record[key] for key in FIELDS}
    content = json.dumps(record, ensure_ascii=False, separators=(',', ':')).encode()
    return {'sha256': hashlib.sha256(content).hexdigest(), 'record': record}


def expected_record(topic, partition, index, raw=False, old=False):
    identity = f'{topic}:{partition}:{index}'
    if raw:
        prefix = topic.removesuffix('-raw')
        key, value = f'raw-key:{index}'.encode().hex(), f'raw-value:{index}'.encode().hex()
        headers = [{'key': 'receipt', 'value_hex': f'{prefix}:raw:{index}'.encode().hex()}]
    else:
        key = None if index % 3 == 0 else ('key:' + identity).encode().hex()
        value = None if index % 4 == 0 else '' if index % 4 == 1 else ('value:' + identity).encode().hex()
        if old:
            value = b'deleted-old-identity'.hex()
        headers = [{'key': 'receipt', 'value_hex': identity.encode().hex()},
                   {'key': 'dup', 'value_hex': '61'}, {'key': 'dup', 'value_hex': None}]
    return receipt({'topic': topic, 'partition': partition, 'offset': index,
                    'timestamp': TIME + (0 if raw else partition * 100) + index,
                    'key_hex': key, 'value_hex': value, 'headers': headers})


def expected_history(restart):
    result = {}
    for release in RELEASES:
        prefix = 'ordinary-' + release.replace('.', '-')
        for ack in ('1', '-1', '0'):
            topic = prefix + '-acks' + ack
            for partition in range(2 if ack == '1' else 1):
                result[topic, partition] = [expected_record(topic, partition, n) for n in range(12)]
        topic = prefix + '-raw'
        result[topic, 0] = [expected_record(topic, 0, n, raw=True) for n in range(33)]
    for ack in ('1', '-1', '0'):
        topic = 'ordinary-rust-acks' + ack
        for partition in range(2):
            result[topic, partition] = [expected_record(topic, partition, n) for n in range(13 if restart else 12)]
    for partition in range(2):
        result['ordinary-native', partition] = [expected_record('ordinary-native', partition, n) for n in range(12)]
    topic = 'ordinary-rust-lifecycle'
    result[topic, 0] = [expected_record(topic, 0, n) for n in range(2 if restart else 1)]
    assert len(result) == 24 and sum(map(len, result.values())) == (347 if restart else 340)
    return result


class Reader:
    def __init__(self, data):
        assert 4 <= len(data) <= 128 * 1024
        self.data, self.position = data, 0

    def take(self, length):
        assert 0 <= length <= len(self.data) - self.position
        result = self.data[self.position:self.position + length]
        self.position += length
        return result

    def integer(self, format):
        return struct.unpack(format, self.take(struct.calcsize(format)))[0]

    def varint(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.integer('>B')
            value |= (byte & 127) << shift
            if byte < 128:
                assert value <= 2**32 - 1
                return value
        raise AssertionError('bounded unsigned varint')

    def tags(self):
        count = self.varint()
        assert count <= 16
        previous = -1
        for _ in range(count):
            tag = self.varint()
            assert tag > previous
            previous = tag
            self.take(self.varint())


def check_profile(event):
    request, response = Reader(bytes.fromhex(event['request_hex'])), Reader(bytes.fromhex(event['response_hex']))
    assert request.integer('>h') == 18 and request.integer('>h') == 4
    correlation = request.integer('>i')
    client_length = request.integer('>h')
    assert -1 <= client_length <= 128
    if client_length >= 0:
        request.take(client_length)
    request.tags()
    for _ in range(2):
        length = request.varint()
        assert 1 <= length <= 129
        request.take(length - 1)
    request.tags()
    assert request.position == len(request.data)
    assert response.integer('>i') == correlation and response.integer('>h') == 0
    count = response.varint() - 1
    assert count == 7
    actual = {}
    for _ in range(count):
        key, low, high = (response.integer('>h') for _ in range(3))
        assert key not in actual
        actual[key] = (low, high)
        response.tags()
    assert response.integer('>i') == 0
    response.tags()
    assert response.position == len(response.data) and actual == PROFILE


def check_receipts(events, expected, label=None):
    observed = {}
    count = 0
    for event in events:
        if label is not None and event.get('label') != label:
            continue
        item = event.get('receipt') or event.get('expected_receipt')
        if item is None:
            continue
        assert item == receipt(item['record'])
        record = item['record']
        key = record['topic'], record['partition']
        offset = record['offset']
        assert key in expected and 0 <= offset < len(expected[key])
        assert item == expected[key][offset]
        identity = (*key, offset)
        assert observed.setdefault(identity, item['sha256']) == item['sha256']
        count += 1
    return observed, count


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def native_events(log):
    assert log.stat().st_size <= 4 * 1024 * 1024
    values = []
    for line in log.read_text().splitlines():
        if line.startswith('{'):
            event = json.loads(line)
            if event.get('operation') in ('consume', 'delivery'):
                values.append({'receipt': receipt(event)})
    assert values and len(values) <= 26
    return values


def decode_journals(run, clients, expected, build, driver, decoder, source, output):
    output.mkdir(parents=True, exist_ok=False)
    rows, comparisons = [], 0
    all_expected = dict(expected)
    old_topic = 'ordinary-rust-lifecycle'
    all_expected[old_topic + ':deleted', 0] = [expected_record(old_topic, 0, 0, old=True)]
    for (selector, partition), wanted in sorted(all_expected.items()):
        old = selector.endswith(':deleted')
        topic = selector.removesuffix(':deleted')
        uuid = (clients / ('lifecycle-deleted-uuid' if old else topic + '.rust-uuid')).read_text()
        assert len(uuid) == 32 and bytes.fromhex(uuid) != bytes(16)
        journal = run / 'journal-snapshot/partitions' / (uuid + '-' + str(partition) + '.journal')
        directory = output / (selector.replace(':', '-') + '-' + str(partition))
        framing = decoder.extract(journal, directory)
        assert framing['next_offset'] == len(wanted)
        readers = []
        for release in RELEASES:
            destination = directory / ('decoded-' + release + '.json')
            command = driver.java_command(build, 'JournalPeer', release, 0, '', clients)
            # JournalPeer has a distinct offline CLI; use the pinned classpath only.
            command = command[:command.index('JournalPeer') + 1] + [topic, str(partition), str(directory), str(destination)]
            log = directory / ('decode-' + release + '.log')
            result, _ = driver.process(command, source, driver.environment(Path(build['target'])), log, 30)
            assert result['exit_code'] == 0
            decoded = load(destination)
            assert decoded['receipts'] == wanted and decoded['records'] == len(wanted)
            readers.append({'release': release, **result, 'decoded_sha256': sha(destination), 'records': len(wanted)})
            comparisons += len(wanted)
        rows.append({'topic': topic, 'partition': partition, 'deleted_identity': old,
                     'uuid_hex': uuid, 'framing': framing, 'readers': readers})
    assert len(rows) == 25
    journal_files = list((run / 'journal-snapshot').rglob('*.journal'))
    assert len(journal_files) == 26, 'all durable partition journals and catalog retained'
    result = {'passed': True, 'topics_partitions_including_deleted_identity': len(rows),
              'decoded_record_comparisons': comparisons, 'rows': rows}
    (output / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser()
    for name in ('source', 'integrity', 'preparation', 'runs', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    driver = module(args.source / 'scripts/broker-interop.py', 'interop_driver')
    source_sha, files = driver.verify_source(args.source, args.integrity)
    build = load(args.preparation)
    assert build['passed'] and build['source_sha'] == source_sha
    for row in [*build['rust'].values(), *build['server'].values(), build['native']]:
        assert sha(Path(row['binary'])) == row['binary_sha256']
    for name in ('header', 'library'):
        assert sha(Path(build['native'][name])) == build['native']['pins'][name + '_sha256']
    decoder_path = args.source / 'docs/evidence/broker/KL11-68/decode-produce-journals.py'
    assert sha(decoder_path) == 'f0482624c6b14958200cc65fef975b5c3f836901708441ec513a6910e480efad'
    decoder = module(decoder_path, 'independent_journal_decoder')
    assert decoder.crc32c(b'123456789') == 0xe3069283
    lanes = load(args.runs)['lanes']
    assert {(row['toolchain'], row['features']) for row in lanes} == {
        (tc, feature) for tc in ('stable', '1.85.0') for feature in ('default', 'all-features')}
    assert len(lanes) == 4
    report = {'schema_version': 1, 'card': 'KL11-08', 'source_sha': source_sha, 'passed': False,
              'verified_git_files': files, 'preparation_sha256': sha(args.preparation), 'lanes': [],
              'actual_peer_executions': 0, 'record_receipt_comparisons': 0,
              'independent_apache_journal_record_comparisons': 0, 'actual_seven_api_wire_profiles': 0}
    try:
        for lane in lanes:
            toolchain, features = lane['toolchain'], lane['features']
            lane_result = {'toolchain': toolchain, 'features': features, 'phases': []}
            seed_uuids = None
            for phase in ('seed', 'restart'):
                run = Path(lane[phase])
                result = load(run / 'validation.json')
                restart = phase == 'restart'
                assert result['passed'] and result['source_sha'] == source_sha
                assert result['toolchain'] == toolchain and result['features'] == features and result['restart'] == restart
                assert result['preparation_sha256'] == sha(args.preparation) and result['server_exit_code'] == 0
                assert result['rust'] == build['rust'][toolchain + '/' + features]
                assert result['server'] == build['server'][toolchain]
                assert result['verified_git_files'] == result['verified_git_files_after'] == files
                assert len(result['peers']) == result['planned_peer_count'] == (24 if restart else 27)
                for peer in result['peers']:
                    assert peer['exit_code'] == 0 and not peer['timed_out']
                    log = next(path for path in run.glob('*-' + peer['name'] + '.log'))
                    assert sha(log) == peer['log_sha256']
                if restart:
                    assert result['complete_journal_hashes_unchanged_on_recovery']
                    assert result['complete_journal_hashes_unchanged_after_recovery_read']
                clients = run / 'client-snapshot'
                expected = expected_history(restart)
                all_ids = {(*key, n) for key, values in expected.items() for n in range(len(values))}
                rust = load(clients / ('rust-' + phase + '.json'))
                assert rust['passed'] and rust['records'] == (373 if restart else 364)
                observed, checks = check_receipts(rust['history'], expected)
                assert set(observed) == all_ids
                public_profiles = [event for event in rust['history'] if event.get('label') == 'public-ApiVersions']
                assert len(public_profiles) == 3
                if restart:
                    recovery = load(clients / 'rust-recovery-read.json')
                    assert recovery['passed'] and recovery['records'] == 364
                    recovered, count = check_receipts(recovery['history'], expected_history(False))
                    assert len(recovered) == 340
                    checks += count
                deleted = [event for event in rust['history'] if event.get('label') == 'public-admin-deleted-identity']
                assert len(deleted) == 1 and deleted[0]['error_code'] == 100
                uuids = {path.name: path.read_text() for path in clients.glob('*.rust-uuid')}
                assert len(uuids) == 17
                if seed_uuids is None:
                    seed_uuids = uuids
                else:
                    assert uuids == seed_uuids
                for name, value in result['journal_hashes'].items():
                    assert sha(run / 'journal-snapshot' / name) == value
                profiles = 0
                for release in RELEASES:
                    own = load(clients / (release + ('-restart-history.json' if restart else '-fetch-history.json')))
                    assert own['passed']
                    _, count = check_receipts(own['history'], expected)
                    assert count == 147
                    checks += count
                    histories = [own]
                    if not restart:
                        append = load(clients / (release + '-append-all-history.json'))
                        assert append['passed']
                        _, count = check_receipts(append['history'], expected)
                        assert count == 48
                        checks += count
                        histories.append(append)
                        crc_cases = [event for event in append['history'] if event.get('api_key') == 0 and event['label'].endswith('-bad-crc')]
                        assert {event['api_version'] for event in crc_cases} == set(range(3, 14))
                        assert all('errorCode=2' in event['apache_data'] and 'baseOffset=-1' in event['apache_data'] for event in crc_cases)
                        eof_cases = [event for event in append['history'] if event.get('label') == 'acks0-error-close']
                        assert len(eof_cases) == 11 and all(event['outcome'] == 'EOF' for event in eof_cases)
                    for history in histories:
                        for event in history['history']:
                            if event.get('api_key') == 18:
                                check_profile(event)
                                profiles += 1
                    for topic in ('ordinary-rust-acks1', 'ordinary-rust-acks-1', 'ordinary-rust-acks0', 'ordinary-native'):
                        cross = load(clients / (release + '-cross-read-' + ('restart' if restart else 'initial') + '-' + topic + '.json'))
                        assert cross['passed']
                        _, count = check_receipts(cross['history'], expected)
                        assert count == (26 if restart and topic != 'ordinary-native' else 24)
                        checks += count
                assert profiles == (3 if restart else 39)
                for peer in result['peers']:
                    if peer['name'].startswith('native-'):
                        log = next(path for path in run.glob('*-' + peer['name'] + '.log'))
                        _, count = check_receipts(native_events(log), expected)
                        checks += count
                decode = decode_journals(run, clients, expected, build, driver, decoder, args.source,
                                         args.output / (toolchain + '-' + features + '-' + phase))
                phase_result = {'phase': phase, 'run': str(run), 'run_sha256': sha(run / 'validation.json'),
                                'rust_records_checked': rust['records'], 'unique_live_records': len(all_ids),
                                'record_receipt_comparisons': checks, 'stable_topic_identities': len(uuids),
                                'actual_peer_executions': len(result['peers']), 'actual_seven_api_wire_profiles': profiles,
                                'independent_apache_journal_record_comparisons': decode['decoded_record_comparisons']}
                lane_result['phases'].append(phase_result)
                for key in ('actual_peer_executions', 'record_receipt_comparisons', 'actual_seven_api_wire_profiles',
                            'independent_apache_journal_record_comparisons'):
                    report[key] += phase_result[key]
                print('SEALED', toolchain, features, phase, flush=True)
            report['lanes'].append(lane_result)
        report['verified_git_files_after'] = driver.verify_source(args.source, args.integrity)[1]
        report['limitations'] = [
            'Fresh loopback single-node ordinary uncompressed histories; no replication, group coordination, transaction, idempotence, retention or production-readiness claim.',
            'acks1 and acks-1 are local sync/declared single-node ISR; acks0 has no durable client acknowledgment, verified by subsequent real reads.',
            'Manual assignment and auto.commit=false. read_committed equals ordinary log HW only because this broker rejects transactional/control/idempotent writes.',
            'Both client feature variants use the real default-feature server binary for their toolchain; no optional server-codec claim.',
            'Deliberately corrupt wire CRC is tested in every Produce3–13 version; physical journal corruption recovery has separate storage-card evidence.'
        ]
        report['passed'] = True
    finally:
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'lanes'}))


if __name__ == '__main__':
    main()
