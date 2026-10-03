#!/usr/bin/env python3
"""Describe independently generated request/response bytes and profile differences."""
import csv
import hashlib
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
FIXTURES = ROOT / 'partitionline-broker/tests/fixtures/raft-protocol'
POLICIES = {
    'actual-handler': 'Actual KafkaRaftClient private handler output, using its actual QuorumState and explicit synthetic dependencies.',
    'controller-profile-advertisement': 'Apache-generated ApiVersions bytes for this local four-API profile; not the full Apache controller inventory or an executed ApiVersions handler.',
    'fixed-membership': 'Local fixed trusted membership rejects unconfigured IDs before mutation with partition error42. Actual Java Vote may grant; Begin/End may throw for missing leader endpoints.',
    'reserved-empty-log-coordinate': 'Local empty offset0 requires lastEpoch0. Actual Java handler accepts lastEpoch1/offset0 at candidateEpoch2 on the empty synthetic log.',
    'leader-conflict-error': 'Local semantic partition error42, preserving known same-epoch leader; actual Java private handler throws IllegalStateException.',
    'remote-end-leader-required': 'Local End rejects its own leader ID; actual Java transition to follower rejects self leader.',
    'unique-fixed-successors': 'Local End requires unique configured successors and returns partition42 before mutation. Actual Java accepts duplicates/nonmembers and computes their ranking.',
    'close-successor-bound': 'Local structural cap31 closes before state mutation; actual Java rank arithmetic uses Java masked shift counts, observable at counts32/33.',
    'close-modern-profile': 'Local higher controller API versions close before mutation and remain unadvertised. Actual Java v1 handler behavior is preserved separately, not locally implemented.'
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Do not disable assertions.')
    manifest = {'schema_version': 1, 'frame_scope': 'Primary *.bin payloads contain request/response header and body without the four-byte TCP length; *.frame.bin adds the signed big-endian length.',
        'configuration': {'cluster_id': 'cluster-fixed', 'topic': '__cluster_metadata', 'partition': 0,
            'local_voter': 1, 'voters': [1, 2, 3], 'initial_wire_epoch': 0, 'initial_core_term': 1,
            'initial_log_end': 0, 'initial_log_epoch': 0, 'now_ms': 0, 'seed': 7,
            'local_election_timeout_min_ms': 5, 'local_election_timeout_max_ms': 5,
            'apache_election_timeout_ms': 5, 'apache_follower_fetch_timeout_ms': 5, 'max_end_backoff_ms': 1000},
        'scope': 'Fixed trusted membership, one metadata partition, Vote52/Begin53/End54 version0 and ApiVersions18 versions0–4. No full KRaft/network/replication/storage interoperability qualification.',
        'policy_notes': POLICIES, 'releases': []}
    baseline = None
    for version in ['4.1.2', '4.2.1', '4.3.1']:
        directory = FIXTURES / version
        observation = json.loads((directory / 'observations.json').read_text())
        rows = list(csv.DictReader((directory / 'cases.tsv').open(), delimiter='\t'))
        observations = {item['name']: item for item in observation['cases']}
        assert len(rows) == observation['case_count'] == len(observations)
        cases = []
        for row in rows:
            actual = observations[row['name']]
            assert int(row['key']) == actual['key'] and int(row['version']) == actual['version']
            assert actual['policy'] in POLICIES
            request = directory / row['request_file']
            assert sha(request) == actual['request_sha256']
            response = None if row['response_file'] == '-' else directory / row['response_file']
            if response:
                assert sha(response) == actual['response_sha256']
            for payload in [request] + ([response] if response else []):
                frame = payload.with_name(payload.name.replace('.bin', '.frame.bin'))
                raw = frame.read_bytes()
                assert struct.unpack('>i', raw[:4])[0] == len(raw) - 4 and raw[4:] == payload.read_bytes()
            setup = [] if row['setup'] == 'fresh' else row['setup'].split('|')
            cases.append({**row, 'setup_actions': setup, 'policy': actual['policy'],
                'request_sha256': sha(request), 'response_sha256': sha(response) if response else None,
                'actual_apache_response_file': row['name'] + '.apache-response.bin' if actual['actual_apache_response'] is not None else None,
                'actual_apache_exception': actual['actual_apache_exception'],
                'actual_apache_response': actual['actual_apache_response'],
                'actual_apache_remaining_fetch_ms': actual['apache_remaining_fetch_ms']})
        file_pins = {path.name: sha(path) for path in sorted(directory.iterdir()) if path.name != 'observations.json'}
        if baseline is None:
            baseline = file_pins
        assert baseline == file_pins
        manifest['releases'].append({'release': version, 'case_count': len(cases), 'cases': cases,
            'case_table_sha256': sha(directory / 'cases.tsv'), 'observations_sha256': sha(directory / 'observations.json'),
            'file_sha256': file_pins})
    (FIXTURES / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('PASS manifest:', sum(row['case_count'] for row in manifest['releases']), 'cases')


if __name__ == '__main__':
    main()
