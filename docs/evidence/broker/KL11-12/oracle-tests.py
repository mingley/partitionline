#!/usr/bin/env python3
"""Retain minimized corruptions of actual Rust histories and check rejection."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct

SPEC = importlib.util.spec_from_file_location('election_oracle', Path(__file__).with_name('oracle.py'))
ORACLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ORACLE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('history', type=Path)
    parser.add_argument('output_directory', type=Path)
    args = parser.parse_args()
    args.output_directory.mkdir()
    lines = [json.loads(line) for line in args.history.read_text().splitlines()]
    end = next(index for index, row in enumerate(lines[1:], 1) if row['kind'] == 'config')
    original = lines[:end]
    baseline = ORACLE.verify(args.history)
    assert baseline['verdict'] == 'passed', baseline
    reports = []

    def case(name, mutate, file_mutation=None, expected_error=None):
        directory = args.output_directory / name
        directory.mkdir()
        selected = copy.deepcopy(original)
        description = mutate(selected)
        for row in selected:
            if row['kind'] == 'journal':
                shutil.copyfile(args.history.parent / row['path'], directory / row['path'])
        if file_mutation:
            file_mutation(directory)
        history = directory / 'history.jsonl'
        history.write_text(''.join(json.dumps(row, separators=(',', ':')) + '\n' for row in selected))
        verdict = ORACLE.verify(history)
        assert verdict['verdict'] == 'failed', (name, verdict)
        if expected_error:
            assert expected_error in verdict['error'], (name, verdict)
        (directory / 'verdict.json').write_text(json.dumps(verdict, indent=2) + '\n')
        manifest = {'kind': 'synthetic counterexample derived from actual Rust seed history',
                    'source_history': str(args.history), 'source_sha256': hashlib.sha256(args.history.read_bytes()).hexdigest(),
                    'seed': original[0]['seed'], 'mutation': description, 'raw_failed_fixture_retained': True,
                    'production_qualification': False}
        (directory / 'mutation.json').write_text(json.dumps(manifest, indent=2) + '\n')
        reports.append({'case': name, 'verdict': verdict['verdict'], 'line': verdict['line'], 'error': verdict['error']})

    def find(rows, predicate):
        return next(row for row in rows if predicate(row))

    def double_vote(rows):
        row = find(rows, lambda x: x['kind'] == 'request_vote' and not x['response']['granted']
                   and x['request']['term'] == x['state']['term'] and x['state']['vote'] is not None
                   and x['state']['vote'] != x['request']['candidate'])
        row['response']['granted'] = True
        row['state']['vote'] = row['request']['candidate']
        return 'turn a same-term alternate-candidate denial into a second affirmative vote'
    case('double-vote', double_vote)

    def stale_log(rows):
        row = find(rows, lambda x: x['kind'] == 'request_vote' and not x['response']['granted']
                   and x['request']['term'] == x['state']['term']
                   and (x['request']['log_term'], x['request']['log_index']) < (x['state']['log_term'], x['state']['log_index']))
        row['response']['granted'] = True
        row['state']['vote'] = row['request']['candidate']
        return 'grant a candidate whose log is behind the voter log summary'
    case('stale-log-grant', stale_log)

    def stale_leader(rows):
        row = find(rows, lambda x: x['kind'] == 'observe_leader' and not x['accepted'])
        row['accepted'] = True
        return 'accept an older-term leader assertion'
    case('stale-leader', stale_leader)

    def duplicate_vote(rows):
        row = find(rows, lambda x: x['kind'] == 'receive_vote' and x['outcome'] == 'Ignored' and x['response']['granted'])
        row['outcome'] = 'Elected'
        return 'turn an ignored duplicate/stale vote into another successful election'
    case('duplicate-election', duplicate_vote)

    def unsynchronized(rows):
        row = find(rows, lambda x: x['kind'] == 'tick' and x['outcome'] == 'campaign')
        row['state']['states'] -= 1
        return 'emit a self-candidacy without the new synchronized journal state'
    case('campaign-before-persistence', unsynchronized)

    def grant_without_vote(rows):
        at = next(index for index, row in enumerate(rows) if row['kind'] == 'tick' and row['outcome'] == 'campaign')
        start = rows[at]
        voter = next(node for node in original[0]['members'] if node != start['node'])
        state = copy.deepcopy(start['state'])
        state.update(role='Leader', leader=start['node'], grants=2)
        rows.insert(at + 1, {'kind': 'receive_vote', 'node': start['node'], 'now': start['now'],
                            'response': {'term': state['term'], 'candidate': start['node'], 'voter': voter, 'granted': True},
                            'outcome': 'Elected', 'state': state})
        return 'inject a quorum-forming grant without any preceding voter decision'
    case('forged-majority', grant_without_vote)

    def restart_rollback(rows):
        row = find(rows, lambda x: x['kind'] == 'restart' and x['state']['term'] > 0)
        row['state']['term'] -= 1
        row['state']['vote'] = None
        return 'discard a persisted term/vote during restart'
    case('restart-rollback', restart_rollback)

    def log_rollback(rows):
        row = find(rows, lambda x: x['kind'] == 'advance_log')
        row['log_index'] = 0
        row['state']['log_index'] = 0
        return 'publish a nonempty log term with an empty/regressed log index'
    case('log-rollback', log_rollback)

    def bad_timeout(rows):
        row = find(rows, lambda x: x['kind'] == 'open')
        row['state']['deadline'] = row['now'] + original[0]['timeouts'][1] + 1
        return 'choose a timeout above the configured inclusive maximum'
    case('unbounded-timeout', bad_timeout)

    def wrong_leader(rows):
        row = find(rows, lambda x: x['kind'] == 'observe_leader' and x['accepted'])
        row['leader'] = next(node for node in original[0]['members'] if node != row['leader'])
        return 'assert a member as leader without an actual majority win for that term'
    case('leader-without-election', wrong_leader)

    def crc_damage(directory):
        path = sorted(directory.glob('*.journal'))[0]
        data = bytearray(path.read_bytes()); data[-1] ^= 1; path.write_bytes(data)
    case('actual-journal-crc', lambda _: 'flip an actual retained journal payload byte', crc_damage)

    def durable_second_vote(directory):
        path = sorted(directory.glob('*.journal'))[0]
        data = bytearray(path.read_bytes())
        payload = bytearray(data[-60:])
        term = struct.unpack_from('>Q', payload, 16)[0]
        vote = struct.unpack_from('>I', payload, 24)[0]
        chosen = next(node for node in original[0]['members'] if node != vote)
        # Keep CRCs valid while appending a second vote/erase in the same term.
        if payload[28] == 0:
            payload[28] = 1
            struct.pack_into('>I', payload, 24, chosen)
            first = b'PLENTRY1' + struct.pack('>IQII', len(payload), (len(data) - 24) // 92, 1, ORACLE.CRC.crc32c(payload))
            data += first + struct.pack('>I', ORACLE.CRC.crc32c(first)) + payload
        current = chosen if payload[28] == 0 else struct.unpack_from('>I', payload, 24)[0]
        payload[28] = 1
        struct.pack_into('>I', payload, 24, next(node for node in original[0]['members'] if node != current))
        first = b'PLENTRY1' + struct.pack('>IQII', len(payload), (len(data) - 24) // 92, 1, ORACLE.CRC.crc32c(payload))
        data += first + struct.pack('>I', ORACLE.CRC.crc32c(first)) + payload
        path.write_bytes(data)
        assert term > 0
    case('checksummed-durable-double-vote', lambda _: 'append checksum-valid conflicting same-term durable vote records', durable_second_vote, 'durable double vote')

    output = {'positive_actual_history': baseline, 'negative_cases': len(reports), 'expected_rejections': len(reports),
              'observed_rejections': len(reports), 'counterexamples': reports, 'production_qualification': False}
    (args.output_directory / 'results.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
