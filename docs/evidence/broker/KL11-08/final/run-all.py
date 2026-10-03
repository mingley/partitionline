#!/usr/bin/env python3
"""Run four fresh accepted-source lanes, preserving every initial attempt."""
import json
from pathlib import Path
import subprocess

REPO = Path('/workspace/partitionline')
SOURCE = Path('/workspace/work/broker-interop-final/source')
FINAL = REPO / 'docs/evidence/broker/KL11-08/final'
SCRATCH = Path('/workspace/work/broker-interop-final')
TARGET = Path('/workspace/work/target-client-capabilities')


def main():
    lanes = []
    report = {'source_sha': '45582234de9a40620f038ba6b68ab57d2616beb2', 'passed': False, 'commands': []}
    try:
        for toolchain in ('stable', '1.85.0'):
            for features in ('default', 'all-features'):
                lane = {'toolchain': toolchain, 'features': features}
                state = SCRATCH / 'lanes' / toolchain / features / 'state'
                clients = state.parent / 'clients'
                for phase in ('seed', 'restart'):
                    output = FINAL / 'runtime' / toolchain / features / (phase + '-attempt-1')
                    command = ['python3', str(SOURCE / 'scripts/broker-interop.py'), 'run',
                               '--source', str(SOURCE), '--integrity', str(FINAL / 'source-integrity.json'),
                               '--scratch', str(SCRATCH / 'peers'), '--target', str(TARGET),
                               '--evidence', str(output), '--preparation', str(FINAL / 'build-attempt-1/preparation.json'),
                               '--toolchain', toolchain, '--features', features, '--port', '19135',
                               '--state', str(state), '--clients', str(clients)]
                    if phase == 'restart':
                        command.append('--restart')
                    print('LANE', toolchain, features, phase, flush=True)
                    result = subprocess.run(command, cwd=REPO, timeout=300)
                    report['commands'].append({'toolchain': toolchain, 'features': features, 'phase': phase,
                                               'command': command, 'exit_code': result.returncode})
                    assert result.returncode == 0
                    lane[phase] = str(output)
                lanes.append(lane)
        (FINAL / 'accepted-runs.json').write_text(json.dumps({'lanes': lanes}, indent=2) + '\n')
        report['passed'] = True
    finally:
        (FINAL / 'run-attempt-1.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
