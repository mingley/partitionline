#!/usr/bin/env python3
"""Confirm the independently generated wire corpus rejects concrete corruption."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('retention_wire_oracle', HERE / 'prepare-and-run.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    checker.check_corpus(args.corpus)
    names = ['missing-case', 'duplicate-case', 'path-traversal', 'wrong-api-key', 'forged-header-version',
             'forged-trailing-consumption', 'rewritten-response-file', 'response-for-reject', 'missing-tsv-row']
    results = []
    for name in names:
        directory = args.work / name
        shutil.copytree(args.corpus, directory)
        manifest_path = directory / 'goldens.json'
        manifest = json.loads(manifest_path.read_text())
        if name == 'missing-case':
            manifest['cases'].pop()
        elif name == 'duplicate-case':
            manifest['cases'][-1] = manifest['cases'][0]
        elif name == 'path-traversal':
            manifest['cases'][0]['name'] = '../outside'
        elif name == 'wrong-api-key':
            manifest['cases'][0]['api_key'] = 20
        elif name == 'forged-header-version':
            manifest['cases'][0]['request_header_version'] = 2
        elif name == 'forged-trailing-consumption':
            row = next(row for row in manifest['cases'] if row['name'].endswith('trailing-zero'))
            row['apache_request_parse']['remaining_bytes'] = 0
        elif name == 'rewritten-response-file':
            row = next(row for row in manifest['cases'] if row['response_hex'] is not None)
            path = directory / (row['name'] + '.response.bin')
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 1
            path.write_bytes(raw)
        elif name == 'response-for-reject':
            row = next(row for row in manifest['cases'] if row['response_hex'] is None)
            (directory / (row['name'] + '.response.bin')).write_bytes(b'\0')
        elif name == 'missing-tsv-row':
            path = directory / 'cases.tsv'
            path.write_text('\n'.join(path.read_text().splitlines()[:-1]) + '\n')
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
        try:
            checker.check_corpus(directory)
        except (ValueError, FileNotFoundError) as failure:
            results.append({'name': name, 'rejected': True, 'exception_class': type(failure).__name__, 'reason': str(failure)})
        else:
            raise AssertionError('Corrupt corpus accepted: ' + name)
    report = {'schema_version': 1, 'passed': True, 'positive_baseline_passed': True,
              'scope': 'Corruption rejection of retained SDK wire corpus; no local broker or claims-policy implementation test.',
              'counterexamples_rejected': len(results), 'controls': results}
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': True, 'counterexamples_rejected': len(results)}))


if __name__ == '__main__':
    main()
