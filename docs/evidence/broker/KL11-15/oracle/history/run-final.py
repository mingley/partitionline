#!/usr/bin/env python3
"""Run frozen independent checkers on all immutable snapshot capture cells."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def hashes(root):
    return {str(p.relative_to(root)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
                                      'mode': p.stat().st_mode & 0o777}
            for p in sorted(root.rglob('*')) if p.is_file() and '__pycache__' not in p.parts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.source_sha)
    source = args.source.resolve()
    script = source / 'docs/evidence/broker/KL11-15/oracle/history'
    freeze = json.loads((script / 'source-freeze.json').read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    commands, counts = [], {'histories': 0, 'events': 0, 'raw_checkpoints': 0,
                            'actual_inner_cut_states': 0, 'checker_control_cases': 0}

    def verify_source():
        for name, expected in freeze['source_files_sha256'].items():
            assert hashlib.sha256((source / name).read_bytes()).hexdigest() == expected, name

    def run(argv, name, raw_root):
        verify_source()
        before = hashes(raw_root)
        (args.output / (name + '-inputs-before.json')).write_text(json.dumps(before, indent=2) + '\n')
        log = args.output / (name + '.log')
        with log.open('wb') as stream:
            process = subprocess.run(['taskset', '-c', '0-2,4'] + argv, stdout=stream, stderr=subprocess.STDOUT,
                                     env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'})
        after = hashes(raw_root)
        (args.output / (name + '-inputs-after.json')).write_text(json.dumps(after, indent=2) + '\n')
        commands.append({'command': ['taskset', '-c', '0-2,4'] + argv, 'exit_code': process.returncode,
                         'log': log.name, 'sha256': hashlib.sha256(log.read_bytes()).hexdigest(),
                         'raw_inputs_unchanged': before == after, 'raw_input_files': len(before)})
        (args.output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        verify_source()
        assert before == after, 'raw input changed: ' + name
        assert process.returncode == 0, 'checker failed; authentic outcome retained: ' + name

    for toolchain in ['stable', '1.85.0']:
        for feature in ['default', 'all-features']:
            cell_name = toolchain + '-' + feature
            cell = args.runtime / cell_name
            assert cell.is_dir(), cell
            for voters in [3, 5]:
                raw = cell / 'snapshot' / ('history-' + str(voters))
                trace_path = raw / 'trace.json'
                trace = json.loads(trace_path.read_text())
                assert trace['source_sha'] == args.source_sha
                output = args.output / (cell_name + '-history-' + str(voters) + '.json')
                run(['python3', str(script / 'history_oracle.py'), str(trace_path), '--output', str(output)],
                    cell_name + '-history-' + str(voters), raw)
                result = json.loads(output.read_text())
                counts['histories'] += 1
                counts['events'] += result['events']
                counts['raw_checkpoints'] += len(result['raw_journal_checkpoints'])
                if voters == 3:
                    seed = next(c for c in trace['final_journals'] if c['phase'] == 'after-checkpoint')
                    controls = args.output / (cell_name + '-controls')
                    run(['python3', str(script / 'check-counterexamples.py'), '--wal', str(raw / seed['wal_path']),
                         '--trace', str(trace_path), '--output', str(controls)], cell_name + '-controls', raw)
                    counts['checker_control_cases'] += len(json.loads((controls / 'results.json').read_text())['results'])
            cuts = cell / 'snapshot-inner'
            cut_files = sorted(cuts.rglob('node.wal'))
            assert len(cut_files) == 12, (cell_name, len(cut_files))
            for index, wal in enumerate(cut_files):
                result = args.output / (cell_name + '-inner-' + str(index) + '.json')
                run(['python3', str(script / 'snapshot_oracle.py'), str(wal), '--output', str(result)],
                    cell_name + '-inner-' + str(index), cuts)
                state = json.loads(result.read_text())
                assert state['committed_end'] == state['last_index'] == 3
                assert state['selected_snapshot']['base'] == {'term': 2, 'index': 2}
                counts['actual_inner_cut_states'] += 1
    manifest = hashes(args.output)
    receipt = {'schema_version': 1, 'source_sha': args.source_sha, 'commands': commands, 'counts': counts,
               'passed': True, 'source_files_sha256': freeze['source_files_sha256'], 'artifacts': manifest,
               'scope': 'Finite independent raw-byte/causal snapshot checks and deliberate checker controls; full source QA and compiled guard proofs are separate',
               'limitations': freeze['limitations']}
    (args.output / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(counts))


if __name__ == '__main__':
    main()
