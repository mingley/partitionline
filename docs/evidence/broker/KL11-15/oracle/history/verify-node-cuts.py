#!/usr/bin/env python3
"""Decode exact one-voter Node image-publication cuts and preserve local authority.

Expected inputs are declared fixture constants, not extracted from the emitted
history verdict. Multi-node causality and the separate Install cuts have other
checkers; this probe covers these six publication cuts and their reopened states.
"""
import argparse
import hashlib
import json
from pathlib import Path

from election_oracle import read_election
from snapshot_oracle import read_journal, read_image
from wal_oracle import Record, require

GROUP = {'cluster_id': 'snapshot-inner', 'topic': '__cluster_metadata',
         'partition': 0, 'voters': [0]}
RECORDS = [Record(1, 2, 1, b''), Record(2, 2, 0, b'alpha=a'), Record(3, 2, 0, b'beta=b')]


def verify(root):
    cases = [(mode, phase) for mode in ['io', 'exit'] for phase in range(3)]
    states = []
    for mode, phase in cases:
        case = root / (mode + '-' + str(phase))
        require(case.is_dir(), 'actual Node cut case missing')
        history = json.loads((case / 'history.json').read_text())
        require(history['mode'] == mode and history['publication_phase'] == phase,
                'cut case metadata binding')
        for stage in ['interrupted', 'recovered']:
            directory = case / stage
            wal = directory / 'node.wal'
            replay = read_journal(wal, 0, GROUP)
            election = read_election(directory / 'node.election', 0, [0])
            require(replay.records == RECORDS and replay.committed == 3,
                    'Node cut lost or changed confirmed prefix/suffix')
            require(replay.selected is not None and replay.selected['generation'] == '01' * 16 and
                    replay.selected['base'] == {'term': 2, 'index': 2},
                    'unreceipted candidate selected or prior selection lost')
            require(election['final']['term'] == 2 and election['final']['voted_for'] == 0 and
                    election['final']['log'] == (2, 3), 'receiver local term/vote/tail changed')
            inactive = directory / 'images' / ('snapshot-' + '02' * 16 + '.image')
            if inactive.exists():
                image = read_image(inactive, GROUP)
                require(image['records'] == RECORDS and image['descriptor']['base'] == {'term': 2, 'index': 3},
                        'complete inactive candidate content differs')
            states.append({'mode': mode, 'phase': phase, 'stage': stage,
                           'wal_sha256': replay.journal_sha256, 'election_sha256': election['sha256'],
                           'receiver_term': 2, 'receiver_vote': 0, 'committed_end': 3,
                           'selected': replay.selected, 'inactive_complete_image_present': inactive.exists()})
    require(len(states) == 12, 'Node cut state count')
    return {'scope': 'Six actual one-voter image publication cuts and twelve raw interrupted/recovered states; fixture-specific finite checks',
            'passed': True, 'states': states, 'cases': 6,
            'raw_input_files_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                       for p in sorted(root.rglob('*')) if p.is_file()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in args.root.rglob('*') if p.is_file()}
    result = verify(args.root)
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in args.root.rglob('*') if p.is_file()}
    require(before == after, 'Node cut input changed')
    result['inputs_unchanged'] = True
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'cases': 6, 'raw_states': 12}))


if __name__ == '__main__':
    main()
