#!/usr/bin/env python3
"""Verify frozen QA receipts/captures, including matching-count corruptions."""
import copy
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
PIN = '58810f2ed90ac9643d59a60098a29f5a2f87a8d0'
APIS = [(0, 3, 13), (1, 4, 6), (2, 1, 3), (3, 0, 13), (18, 0, 4), (19, 2, 4), (20, 1, 6)]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_report(report, fixture_root, capture_root):
    require(report['schema_version'] == 1, 'report schema')
    apis = [(r['api_key'], r['min_version'], r['max_version']) for r in report['read_write_api_versions']]
    require(apis == APIS, 'read/write advertisement')
    rows = report['case_results']
    require(len(rows) == 366, 'outcome count')
    found = {(r['release'], r['case']): r for r in rows}
    require(len(found) == 366, 'duplicate outcomes')
    expected = {}
    for release in ['4.1.2', '4.2.1', '4.3.1']:
        for line in (fixture_root / release / 'cases.tsv').read_text().splitlines():
            if not line or line.startswith('#'):
                continue
            name, key, version, seed, outcome = line.split('\t')
            expected[(release, name)] = outcome
    require(set(found) == set(expected), 'missing/unexpected cases')
    responses = 0
    for identity, outcome in expected.items():
        row = found[identity]
        require(row['outcome'] == outcome, 'outcome mismatch')
        release, name = identity
        if outcome == 'response':
            actual = bytes.fromhex(row['response_hex'])
            require(actual == (fixture_root / release / (name + '.response.bin')).read_bytes(), 'response differs from authentic fixture')
            require(actual == (capture_root / release / (name + '.actual-response.bin')).read_bytes(), 'raw response differs from compiled report')
            responses += 1
        else:
            require(row['response_hex'] is None, 'structural close has response bytes')
    require(responses == 330, 'raw response coverage')
    api_rows = report['api_versions_cases']
    require(len(api_rows) == 15, 'ApiVersions coverage')
    require({(r['release'], r['version']) for r in api_rows} == {(release, v) for release in ['4.1.2', '4.2.1', '4.3.1'] for v in range(5)}, 'ApiVersions unique layouts')
    for row in api_rows:
        require(row['header_version'] == 0, 'ApiVersions response header')
        response = bytes.fromhex(row['response_hex'])
        require(response[:4] == row['correlation_id'].to_bytes(4, 'big', signed=True), 'ApiVersions correlation')
        require(response[4:6] == b'\0\0', 'ApiVersions error')
    return responses


def main():
    final = ROOT / 'final-qualified'
    source = json.loads((final / 'source-integrity.json').read_text())
    require(source['source_pin'] == PIN, 'source pin')
    require(source['archive_scope'] == 'complete committed Git tree', 'archive scope')
    require(source['file_count'] == 17370, 'whole source file count')
    require(set(source['source_sha256']) == set(source['git_blob_identities']), 'source identities')
    fixture_root = ROOT.parents[3] / 'partitionline-broker/tests/fixtures/fetch'
    matrix = json.loads((final / 'matrix-results.json').read_text())
    require(matrix['source_pin'] == PIN and len(matrix['matrix']) == 2, 'matrix source')
    canonical = None
    rows = []
    for lane in matrix['matrix']:
        tool = lane['toolchain']
        require(tool in ['stable', '1.85.0'] and lane['source_unchanged'], 'toolchain/source')
        require(len(lane['commands']) == 10, 'all QA commands')
        for command in lane['commands']:
            require(command['exit_code'] == 0, 'command failed')
            require(command['full_git_blob_identities_match'] and command['source_file_count'] == 17370, 'full Git source check')
            require(command['before_source_manifest_sha256'] == source['source_manifest_sha256'] == command['after_source_manifest_sha256'], 'source changed')
            require(command['log_sha256'] == sha(final / tool / (command['name'] + '.log')), 'log hash')
            require(all(t['failed'] == 0 and t['ignored'] == 0 for t in command['test_summaries']), 'test failures or skipped assertions')
        for feature, tests in [('default', 151), ('all-features', 225)]:
            command = next(c for c in lane['commands'] if c['name'] == feature + '-all-targets')
            require(sum(t['passed'] for t in command['test_summaries']) == tests, 'behavioral test count')
            directory = final / tool / (feature + '-all-targets')
            report = json.loads((directory / 'compiled-report.json').read_text())
            validate_report(report, fixture_root, directory / 'responses')
            hashes = {name: sha(directory / name) for name in ['compiled-report.json', 'wire-report.json', 'metadata-report.json', 'produce-report.json']}
            if canonical is None:
                canonical = hashes
            require(hashes == canonical, 'report bytes changed across toolchains/features')
            for name, count in [('wire-report.json', 99), ('metadata-report.json', 555), ('produce-report.json', 708)]:
                require(len(json.loads((directory / name).read_text())['case_results']) == count, 'other API report coverage')
            rows.append({'toolchain': tool, 'features': feature, 'behavioral_tests_passed': tests, 'fetch_cases': 366, 'raw_fetch_responses': 330, 'api_versions_cases': 15, 'reports': hashes})
    directory = final / 'stable/default-all-targets'
    report = json.loads((directory / 'compiled-report.json').read_text())
    mutants = []
    for name in ['matching-count-corrupt-response', 'duplicate-case', 'false-structural-response', 'missing-advertised-range']:
        bad = copy.deepcopy(report)
        if name == 'matching-count-corrupt-response':
            row = next(r for r in bad['case_results'] if r['outcome'] == 'response')
            raw = bytearray.fromhex(row['response_hex']); raw[-1] ^= 1; row['response_hex'] = raw.hex()
        elif name == 'duplicate-case':
            bad['case_results'][-1] = bad['case_results'][0]
        elif name == 'false-structural-response':
            next(r for r in bad['case_results'] if r['outcome'] == 'structural_reject')['response_hex'] = '00'
        else:
            bad['read_write_api_versions'][1]['min_version'] = 3
        try:
            validate_report(bad, fixture_root, directory / 'responses')
        except ValueError as error:
            mutants.append({'name': name, 'rejected': True, 'reason': str(error)})
        else:
            raise ValueError('counterexample accepted: ' + name)
    review = json.loads((ROOT / 'independent-actor-review.json').read_text())
    require(review['final_source_sha'] == PIN and not review['remaining_review_blockers'], 'independent static review')
    for entry in review['reviewed_inputs']:
        require(source['source_sha256'][entry['path']] == entry['sha256'], 'review source mismatch')
    output = {'source_pin': PIN, 'whole_git_tree_files': 17370, 'matrix': rows, 'counterexamples': mutants,
              'independent_static_review_sha256': sha(ROOT / 'independent-actor-review.json'),
              'qualification': 'Local behavior/lint/doc and exact direct Apache-fixture outcomes. Independent live Java/native acceptance is recorded separately.'}
    (ROOT / 'final-validation.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps({'source_pin': PIN, 'matrix_cells': len(rows), 'counterexamples_rejected': len(mutants), 'status': 'pass'}))


if __name__ == '__main__':
    main()
