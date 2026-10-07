#!/usr/bin/env python3
"""Reverse-check actual Rust generic-header output with the pinned Java vectors."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / 'tests/fixtures/generic_schema_header'
SOURCE = ROOT / 'tests/conformance/java/ConformanceGenericSchemaHeader.java'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rust-output', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        raise FileExistsError('retain the previous report; choose a new path')
    report = dict(status='fail', process_started=False, process_exited=False, profile='generic-five-byte-header',
                  scope='Unsigned 32-bit wire field and opaque payload; no schema, Protobuf index, or live Registry qualification.')
    started = time.monotonic()
    try:
        manifest = json.loads((FIXTURES / 'manifest.json').read_text())
        if sha(SOURCE) != manifest['java_source_sha256']:
            raise ValueError('Java source pin differs')
        for filename, expected in manifest['artifact_sha256'].items():
            if sha(FIXTURES / filename) != expected:
                raise ValueError('fixture hash differs: ' + filename)
        report['rust_artifact_sha256'] = {}
        for filename in manifest['artifact_sha256']:
            if filename.endswith('.bin'):
                report['rust_artifact_sha256'][filename] = sha(args.rust_output / filename)
        report['java_version'] = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True, timeout=10)
        command = ['java', '-Xmx64m', str(SOURCE), 'verify', str(args.rust_output.resolve())]
        report['command'] = command
        report['process_started'] = True
        try:
            process = subprocess.run(command, capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired:
            # subprocess.run kills and waits for its child before raising.
            report.update(process_exited=True, timed_out=True, exit_code=None)
            raise
        report.update(process_exited=True, exit_code=process.returncode, stdout=process.stdout, stderr=process.stderr)
        if process.returncode != 0:
            raise ValueError('Java reverse-check failed')
        row = json.loads(process.stdout)
        if row != dict(status='pass', valid=7, invalid=7, profile='generic-five-byte-header'):
            raise ValueError('unexpected peer case count or disposition')
        report.update(status='pass', cases=row)
    except Exception as error:
        report['failure'] = f'{type(error).__name__}: {error}'
    report['elapsed_seconds'] = time.monotonic()-started
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open('x') as output:
        json.dump(report, output, indent=2); output.write('\n')
    print(json.dumps({'status': report['status'], 'report': str(args.report)}))
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
