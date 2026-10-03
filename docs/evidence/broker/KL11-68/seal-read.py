#!/usr/bin/env python3
"""Verify actual wire/receipt histories and seal the accepted ordinary read runs."""
import hashlib
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
SOURCE = '58810f2ed90ac9643d59a60098a29f5a2f87a8d0'
INPUTS = [('stable', 'seed', 'stable/attempt-4'),
          ('stable', 'restart', 'stable/attempt-5-restart'),
          ('1-85-0', 'seed', '1-85-0/attempt-1'),
          ('1-85-0', 'restart', '1-85-0/attempt-2-restart')]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text())


def resolve(path):
    path = Path(path)
    return path if path.is_absolute() else REPO / path


def receipt_hash(record):
    data = json.dumps(record, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(data).hexdigest()


def verify_receipt(receipt):
    assert receipt_hash(receipt['record']) == receipt['sha256']
    return (receipt['record']['topic'], receipt['record']['partition'], receipt['record']['offset']), receipt['sha256']


def main():
    output = ROOT / 'live-read'
    counts = {'peer_executions': 0, 'assertions': 0, 'ordinary_hash_comparisons': 0,
              'codec_hash_comparisons': 0, 'native_java_hash_comparisons': 0,
              'native_consumed_receipts': 0, 'native_delivery_receipts': 0,
              'live_read_cases': 0, 'positive_read_responses': 0, 'structural_eofs': 0}
    cases = []
    lanes = []
    native_receipts = []
    sources = []
    native_deliveries = {}
    known_java = {}
    journal_hashes = {}
    identities = {}
    for lane, phase, relative in INPUTS:
        directory = output / relative
        validation_path = directory / 'validation.json'
        report = load(validation_path)
        assert report['source_sha'] == SOURCE and report['passed'] and report['server_exit_code'] == 0
        assert len(report['peers']) == (17 if phase == 'seed' else 16)
        assert all(peer['exit_code'] == 0 for peer in report['peers'])
        assert report['build']['reused_exact_source_binary']
        runtime = {'lane': lane, 'phase': phase, 'validation': str(validation_path.relative_to(REPO)),
                   'validation_sha256': sha(validation_path), 'binary_sha256': report['binary_sha256'],
                   'peer_executions': len(report['peers']), 'server_exit_code': report['server_exit_code']}
        lanes.append(runtime)
        journal = {name: digest for name, digest in report['journal_hashes'].items() if name.endswith('.journal')}
        if phase == 'seed':
            journal_hashes[lane] = journal
        else:
            assert journal == journal_hashes[lane]
            runtime['all_journal_hashes_unchanged_after_process_restart'] = True
        snapshots = list(report['peer_state_snapshots'].values())
        assert len(snapshots) == 1
        state = resolve(snapshots[0])
        ids = {path.name: path.read_text() for path in state.glob('*.uuid')}
        assert len(ids) == 22
        if phase == 'seed':
            identities[lane] = ids
        else:
            assert identities[lane] == ids
            runtime['all_22_allocated_topic_uuids_retained'] = True
        ordinary_hashes = {}
        for ordinal, peer in enumerate(report['peers'], 1):
            counts['peer_executions'] += 1
            job = peer['job']
            kind = job['peer']
            stdout = [json.loads(line) for line in peer['stdout'].splitlines() if line.startswith('{')]
            final = stdout[-1]
            assert final.get('passed') is True or final.get('status') == 'pass'
            counts['assertions'] += final['assertions']
            if kind == 'native':
                rows = [row for row in stdout if 'operation' in row]
                assert len(rows) == (26 if 'topic' in job else 24)
                for row in rows:
                    record = {name: row[name] for name in ('topic', 'partition', 'offset', 'timestamp', 'key_hex', 'value_hex', 'headers')}
                    identity = (record['topic'], record['partition'], record['offset'])
                    digest = receipt_hash(record)
                    assert bytes.fromhex(record['headers'][0]['value_hex']).decode() == row['id']
                    entry = {'lane': lane, 'phase': phase, 'operation': row['operation'],
                             'receipt': {'sha256': digest, 'record': record},
                             'source_log': str(resolve(peer['log']).relative_to(REPO))}
                    native_receipts.append(entry)
                    if row['operation'] == 'delivery':
                        assert identity not in native_deliveries.setdefault(lane, {})
                        native_deliveries[lane][identity] = digest
                        counts['native_delivery_receipts'] += 1
                    else:
                        counts['native_consumed_receipts'] += 1
                        if record['topic'] == 'ordinary-native':
                            assert native_deliveries[lane][identity] == digest
                        else:
                            assert ordinary_hashes[identity] == digest
                continue
            release = job['release']
            if kind == 'ordinary':
                history_path = state / f'{release}-{job["phase"]}-history.json'
            elif kind == 'read_control':
                history_path = state / f'{release}-read-control-{job["phase"]}.json'
            elif kind == 'native_read':
                history_path = state / f'{release}-native-read-{job["phase"]}.json'
            else:
                assert kind == 'codec_read'
                history_path = directory / f'codec-peer-{ordinal:02}-{job["phase"]}.json'
            history = load(history_path)
            assert history['passed']
            sources.append({'path': str(history_path.relative_to(REPO)), 'sha256': sha(history_path),
                            'lane': lane, 'phase': phase, 'release': release, 'peer': kind})
            for item in history['history']:
                if 'receipt' in item:
                    identity, digest = verify_receipt(item['receipt'])
                    if kind == 'ordinary':
                        counts['ordinary_hash_comparisons'] += 1
                        if item['label'] == 'actual-Consumer':
                            assert identity not in ordinary_hashes
                            ordinary_hashes[identity] = digest
                            previous = known_java.setdefault(lane, {}).setdefault(identity, digest)
                            assert previous == digest
                    elif kind == 'codec_read':
                        counts['codec_hash_comparisons'] += 1
                    else:
                        assert kind == 'native_read'
                        counts['native_java_hash_comparisons'] += 1
                        assert native_deliveries[lane][identity] == digest
                if kind == 'read_control':
                    if item['name'].startswith('seven-entry-profile-v'):
                        assert item['api_key'] == 18 and item['response_header_version'] == 0
                        request = bytes.fromhex(item['request_hex'])
                        response = bytes.fromhex(item['response_hex'])
                        assert len(request) <= 128 * 1024 and len(response) <= 128 * 1024
                        assert struct.unpack_from('>hh', request) == (18, item['api_version'])
                        assert struct.unpack_from('>i', request, 4)[0] == item['correlation_id']
                        assert struct.unpack_from('>i', response)[0] == item['correlation_id']
                        case = {key: item[key] for key in ('api_key', 'api_version', 'correlation_id', 'request_header_version', 'response_header_version', 'request_hex', 'response_hex')}
                        case.update(toolchain=lane, phase=phase, release=release,
                                    source_history=str(history_path.relative_to(REPO)), source_history_sha256=sha(history_path))
                        cases.append(case)
                    elif item['name'].startswith(('fetch-v', 'list-offsets-v')):
                        counts['live_read_cases'] += 1
                        if item['observed_clean_eof']:
                            assert item['response_hex'] is None
                            counts['structural_eofs'] += 1
                        else:
                            assert item['response_hex'] is not None
                            counts['positive_read_responses'] += 1
    assert len(cases) == 60
    assert len({(x['toolchain'], x['phase'], x['release'], x['api_version']) for x in cases}) == 60
    assert counts['peer_executions'] == 66 and counts['live_read_cases'] == 1464
    assert counts['positive_read_responses'] == 1320 and counts['structural_eofs'] == 144
    assert counts['ordinary_hash_comparisons'] == 1848 and counts['codec_hash_comparisons'] == 8784
    assert counts['native_java_hash_comparisons'] == 288 and counts['native_consumed_receipts'] == 408
    assert counts['native_delivery_receipts'] == 48
    for lane in native_deliveries:
        assert len(native_deliveries[lane]) == 24
    supplement = {'schema_version': 1, 'source_sha': SOURCE, 'passed': True, 'actual_exchanges': len(cases),
                  'scope': 'Actual TCP API18v0–4 seven-entry ordinary read/write profile; source/runtime commands and hashes in accepted live-read reports.', 'cases': cases}
    (output / 'api-versions.json').write_text(json.dumps(supplement, indent=2) + '\n')
    (output / 'native-receipts.json').write_text(json.dumps({'source_sha': SOURCE, 'passed': True,
                                                          'receipts': native_receipts}, indent=2) + '\n')
    counts['explicit_record_hash_comparisons'] = sum(counts[key] for key in ('ordinary_hash_comparisons', 'codec_hash_comparisons', 'native_java_hash_comparisons', 'native_consumed_receipts'))
    aggregate = {'schema_version': 1, 'source_sha': SOURCE, 'passed': True,
                 'initial_produce_source_sha': 'c34bdfd3fbba65492ea49b3804dd0ae5311364b0',
                 'actual_api_versions_exchanges': 60, 'counts': counts, 'accepted_runs': lanes,
                 'actual_history_sources': sources, 'allocated_topic_uuids': identities,
                 'restart_journal_hashes': journal_hashes,
                 'actual_native_all_topics_queries': 18,
                 'limitations': ['Ordinary single-node durable logs only; local acks-1/fsync does not establish replication.',
                                 'No transaction, idempotent, control-batch, group coordination, retention or incremental Fetch-session implementation claimed.',
                                 'Live expected seeded batches use official setPartitionLeaderEpoch(0); direct-Partition fixtures retain-1 outside the protectedCRC.',
                                 'Failed stable attempts1–3, peer configuration failures and partial-source initial behavior remain retained as development history.'],
                 'failure_paths': ['live-read/stable/attempt-1', 'live-read/stable/attempt-2', 'live-read/stable/attempt-3',
                                   'failures/read-control-attempt-1', 'failures/native-read-config-attempt-1', 'failures/native-read-config-attempt-2'],
                 'api_versions_report': {'path': str((output / 'api-versions.json').relative_to(REPO)), 'sha256': sha(output / 'api-versions.json')},
                 'native_receipts_report': {'path': str((output / 'native-receipts.json').relative_to(REPO)), 'sha256': sha(output / 'native-receipts.json')}}
    (output / 'validation.json').write_text(json.dumps(aggregate, indent=2) + '\n')
    print(json.dumps({'source_sha': SOURCE, 'passed': True, 'counts': counts, 'api_versions_exchanges': 60}, indent=2))


if __name__ == '__main__':
    main()
