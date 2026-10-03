#!/usr/bin/env python3
"""Independently decode the actual post-Install/pre-summary/pre-ACK owner cuts.

Expected canonical contents, prior commit and receiver identity are fixed probe
inputs. An image or emitted cut verdict is never an election term/vote source.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct

from election_oracle import read_election
from snapshot_oracle import SnapshotReplay, read_journal
from wal_oracle import Record, require

GROUP = {'cluster_id': 'snapshot-cut', 'topic': '__cluster_metadata',
         'partition': 0, 'voters': [1, 2, 3]}
RECORDS = [Record(1, 2, 1, b''), Record(2, 2, 0, b'retained-prefix'),
           Record(3, 2, 0, b'snapshot-catchup')]


def prior_state(wal):
    # The full independent reader already checked every frame and checksum.
    # Replaying the strict prefix recovers the authoritative old commit floor.
    data, offset, rows = wal.read_bytes(), 24, []
    while offset < len(data):
        size = struct.unpack_from('>I', data, offset + 8)[0]
        rows.append(data[offset + 32:offset + 32 + size])
        offset += 32 + size
    require(rows and rows[-1][:9] == b'PLREPL01\x05', 'last operation is not selecting Install')
    replay = SnapshotReplay(wal.parent / 'images', 2, GROUP)
    for row in rows[:-1]:
        replay.apply(row)
    return replay


def verify(root, source_sha):
    states = []
    for phase in ['receipt', 'summary']:
        case = root / ('install-cut-' + phase)
        require(case.is_dir(), 'actual Install cut missing')
        metadata = json.loads((case / 'cut.json').read_text())
        require(metadata['profile'] == 'actual-owner-install-process-cut' and
                metadata['phase'] == phase and metadata['exit_code'] == 87 and
                metadata['source_sha'] == source_sha, 'Install cut identity/source/exit receipt')
        pairs = []
        for stage in ['before-reopen', 'after-reopen']:
            directory = case / stage
            wal = directory / 'metadata.wal'
            replay = read_journal(wal, 2, GROUP)
            prior = prior_state(wal)
            election = read_election(directory / 'election.wal', 2, [1, 2, 3])
            require(prior.records == RECORDS[:2] and prior.committed == 1 and
                    prior.selected['generation'] == '01' * 16 and
                    prior.selected['base'] == {'term': 2, 'index': 1}, 'prior confirmed Install floor/tail')
            require(replay.records == RECORDS and replay.committed == 3 and
                    replay.selected['generation'] == '02' * 16 and
                    replay.selected['base'] == {'term': 2, 'index': 3}, 'selected canonical Install prefix')
            operation = replay.operations[-1]
            require(operation['authority'] == 1 and operation['leader'] == 1 and operation['peer'] == 2 and
                    operation['term'] == 2 and operation['sequence'] > 0, 'configured receiving Install authority')
            expected_tail = (2, 2) if phase == 'receipt' and stage == 'before-reopen' else (2, 3)
            require(election['final']['term'] == 2 and election['final']['voted_for'] == 2 and
                    election['final']['log'] == expected_tail, 'receiver election term/vote/summary boundary')
            # The old election tail is checked against its own preceding WAL
            # commit floor (1), not against the newly selected commit (3).
            require(prior.committed <= expected_tail[1] <= len(replay.records),
                    'election reconciliation cannot preserve the preceding confirmed floor')
            state = {'phase': phase, 'stage': stage, 'wal_sha256': replay.journal_sha256,
                     'election_sha256': election['sha256'], 'receiver_term': 2, 'receiver_vote': 2,
                     'election_tail': list(expected_tail), 'prior_commit': prior.committed,
                     'prior_tail': [2, 2], 'committed_end': 3, 'selected': replay.selected}
            states.append(state)
            pairs.append(replay)
        require(pairs[0].journal_sha256 == pairs[1].journal_sha256 and
                pairs[0].selected == pairs[1].selected and pairs[0].records == pairs[1].records,
                'reopening changed the synchronized authoritative Install WAL')
    return {'source_sha': source_sha, 'passed': True, 'actual_cuts': 2, 'raw_states': states,
            'scope': 'Two actual local owner process cuts; configured typed leader input, no election-majority or physical powerloss inference',
            'raw_input_files_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                       for p in sorted(root.glob('install-cut-*/*/*')) if p.is_file()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    inputs = [p for d in args.root.glob('install-cut-*') for p in d.rglob('*') if p.is_file()]
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    result = verify(args.root, args.source_sha)
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    require(before == after, 'Install cut inputs changed')
    result['raw_input_files_sha256'] = before
    result['inputs_unchanged'] = True
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'cuts': 2, 'raw_states': 4}))


if __name__ == '__main__':
    main()
