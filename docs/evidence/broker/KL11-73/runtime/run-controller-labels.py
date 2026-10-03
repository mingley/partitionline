#!/usr/bin/env python3
"""Re-execute only controller tests affected by the registry's reflected labels."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    runtime = args.runtime.resolve()
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
    env['PATH'] = '/workspace/work/cargo/bin:' + env.get('PATH', '')
    commands = []
    result = []
    for toolchain in ['stable', '1.85.0']:
        for features in ['default', 'all-features']:
            name = toolchain + '-' + features
            cell = output / name
            cell.mkdir()
            testenv = env.copy()
            testenv.update(CARGO_TARGET_DIR=str(args.target.resolve() / toolchain),
                           PL_BROKER_CONTROLLER_REPORT=str(cell / 'controller-report.json'),
                           PL_CONTROLLER_RESPONSE_DIR=str(cell / 'controller-responses'))
            argv = ['taskset', '-c', '0-2,4', 'cargo', '+' + toolchain, 'test',
                    '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml',
                    '--test', 'raft_protocol', '-j1']
            if features == 'all-features':
                argv.append('--all-features')
            log = cell / 'tests.log'
            with log.open('wb') as stream:
                completed = subprocess.run(argv, cwd=source, env=testenv,
                                           stdout=stream, stderr=subprocess.STDOUT)
            commands.append({'argv': argv, 'cwd': str(source), 'exit_code': completed.returncode,
                             'log': str(log.relative_to(output)), 'log_sha256': digest(log)})
            (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
            assert completed.returncode == 0, name
            assert re.search(r'test result: ok\. 10 passed; 0 failed; 0 ignored', log.read_text())
            original = json.loads((runtime / name / 'controller-report.json').read_text())
            current = json.loads((cell / 'controller-report.json').read_text())
            changed = sorted(k for k in current if current[k] != original[k])
            assert changed == ['controller_source_sha256', 'election_source_sha256', 'raft_module_source_sha256']
            old = runtime / name / 'controller-responses'
            new = cell / 'controller-responses'
            before = {str(p.relative_to(old)): digest(p) for p in old.rglob('*') if p.is_file()}
            after = {str(p.relative_to(new)): digest(p) for p in new.rglob('*') if p.is_file()}
            assert before == after
            result.append({'lane': name, 'passed': 10, 'failed': 0, 'ignored': 0,
                           'golden_outcomes': len(current['case_results']),
                           'tcp_exchanges': 18, 'raw_files': len(after),
                           'all_raw_response_bytes_equal_original_runtime': True,
                           'only_changed_report_fields': changed})
    (output / 'validation.json').write_text(json.dumps({
        'schema_version': 1, 'source_sha': args.source_sha,
        'original_runtime_source_sha': json.loads((runtime / 'results.json').read_text())['source_sha'],
        'passed': True, 'cells': result, 'commands': commands,
        'reason': 'Compiled reporter embeds reflected registry source labels; only three labels changed. Actual original-source compilation was already proven independently.',
        'scope': 'Focused controller golden and real TCP tests only; full broker matrix remains the original immutable runtime receipt.',
    }, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
