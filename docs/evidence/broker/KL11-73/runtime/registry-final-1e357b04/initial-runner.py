#!/usr/bin/env python3
"""Run immutable registry/static/compiled-report checks without rebuilding Rust."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--runtime-source-sha', required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--data-api-versions', type=Path, required=True)
    parser.add_argument('--read-api-versions', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    runtime = args.runtime.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = json.loads((runtime / 'results.json').read_text())
    assert result['source_sha'] == args.runtime_source_sha
    frozen_broker = Path(__file__).parent / 'final-source' / 'source-integrity-before.json'
    before = json.loads(frozen_broker.read_text())
    broker_binding = []
    for row in before['files']:
        if row['path'].startswith('partitionline-broker/'):
            assert digest(source / row['path']) == row['sha256'], row['path']
            broker_binding.append({'path': row['path'], 'sha256': row['sha256']})
    inputs = [args.data_api_versions.resolve(), args.read_api_versions.resolve()]
    for cell in result['cells']:
        root = runtime / (cell['toolchain'] + '-' + cell['features'])
        inputs.extend(root.glob('*-report.json'))
        inputs.extend(p for p in (root / 'controller-responses').rglob('*') if p.is_file())
    input_hashes = {str(p): digest(p) for p in sorted(inputs)}
    source_hashes = {str(p.relative_to(source)): digest(p) for p in source.rglob('*') if p.is_file()}
    commands = []

    def run(argv, name):
        log = output / (name + '.log')
        start = time.monotonic()
        with log.open('wb') as stream:
            completed = subprocess.run(['taskset', '-c', '0-2,4'] + argv, cwd=source,
                                       stdout=stream, stderr=subprocess.STDOUT)
        row = {'name': name, 'argv': ['taskset', '-c', '0-2,4'] + argv,
               'cwd': str(source), 'exit_code': completed.returncode,
               'elapsed_seconds': time.monotonic() - start,
               'log': str(log.relative_to(output)), 'log_sha256': digest(log)}
        commands.append(row)
        (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        assert completed.returncode == 0, 'retained failed gate: ' + name
        return log.read_text()

    base = ['python3', '-B', 'scripts/check-broker-api-matrix.py']
    run(base + ['--report', str(output / 'static.json')], 'static')
    for cell in result['cells']:
        name = cell['toolchain'] + '-' + cell['features']
        root = runtime / name
        run(base + ['--handler-report', str(root / 'protocol-report.json'),
                    '--metadata-handler-report', str(root / 'metadata-report.json'),
                    '--produce-handler-report', str(root / 'produce-report.json'),
                    '--read-write-handler-report', str(root / 'fetch-report.json'),
                    '--controller-handler-report', str(root / 'controller-report.json'),
                    '--controller-tcp-responses', str(root / 'controller-responses' / 'transport'),
                    '--data-api-versions-report', str(args.data_api_versions.resolve()),
                    '--read-write-api-versions-report', str(args.read_api_versions.resolve()),
                    '--report', str(output / (name + '.json'))], name)
    text = run(['python3', '-B', '-m', 'unittest', 'discover', '-s', 'tests/ci',
                '-p', 'test_broker_api_matrix.py', '-v'], 'baseline-and-mutations')
    match = re.search(r'Ran (\d+) tests', text)
    assert match and '\nOK\n' in text
    assert source_hashes == {str(p.relative_to(source)): digest(p) for p in source.rglob('*') if p.is_file()}
    assert all(digest(Path(p)) == value for p, value in input_hashes.items())
    (output / 'validation.json').write_text(json.dumps({
        'schema_version': 1, 'gate_source_sha': args.source_sha,
        'runtime_source_sha': args.runtime_source_sha, 'passed': True,
        'compiled_report_cells': 4, 'baseline_mutation_tests': int(match[1]),
        'all_archived_broker_files_equal_runtime_archive': broker_binding,
        'all_archived_source_files_unchanged': True, 'source_files_checked': len(source_hashes),
        'all_frozen_inputs_unchanged': True, 'input_files_sha256': input_hashes,
        'commands': commands,
        'scope': 'Exact-source source/fixture registry, compiled outcome bytes and retained TCP frames; execution and independent peer provenance remain separate.',
        'limitations': ['Retained KL11-68 API18 exchanges are a frozen supplementary input, not a new live execution in this gate.',
                       'No Rust source differs between gate archive and tested runtime archive.',
                       'Static/compiled registry pass is not production qualification.'],
    }, indent=2) + '\n')
    print(json.dumps({'passed': True, 'gate_source_sha': args.source_sha,
                      'baseline_mutation_tests': int(match[1]), 'compiled_report_cells': 4}))


if __name__ == '__main__':
    main()
