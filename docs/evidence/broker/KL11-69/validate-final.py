#!/usr/bin/env python3
"""Bind final independent replay, actual Rust captures and review to Git blobs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
PRODUCTION = {
    'controller_source_sha256': 'partitionline-broker/src/raft/protocol.rs',
    'controller_test_source_sha256': 'partitionline-broker/tests/raft_protocol.rs',
    'election_source_sha256': 'partitionline-broker/src/raft/election.rs',
    'raft_module_source_sha256': 'partitionline-broker/src/raft/mod.rs',
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Pin assertions require Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--matrix', type=Path, required=True)
    parser.add_argument('--captures', type=Path, required=True)
    parser.add_argument('--runtime-report', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    for name in ['matrix', 'captures', 'runtime_report', 'output']:
        setattr(args, name, getattr(args, name).resolve())
    assert len(args.source_sha) == 40 and all(c in '0123456789abcdef' for c in args.source_sha)
    matrix = json.loads((args.matrix / 'results.json').read_text())
    assert matrix['source_sha'] == args.source_sha and matrix['case_total'] == 201
    runtime = json.loads(args.runtime_report.read_text())
    assert runtime['profile'] == 'fixed-controller-v0' and runtime['independent_fixture_cases'] == 201
    assert runtime['implemented_api_versions'] == [
        {'api_key': key, 'min_version': 0, 'max_version': 4 if key == 18 else 0}
        for key in [18, 52, 53, 54]]
    assert len(runtime['case_results']) == 201
    freeze = json.loads((HERE / 'source-freeze.json').read_text())
    paths = dict(freeze['paths_sha256'])
    for field, relative in PRODUCTION.items():
        assert field in runtime
        paths[relative] = runtime[field]
    checks = {}
    for relative, expected in sorted(paths.items()):
        blob = subprocess.run(['git', 'show', args.source_sha + ':' + relative], cwd=ROOT,
            capture_output=True, check=True).stdout
        assert sha(blob) == expected and sha((ROOT / relative).read_bytes()) == expected, relative
        checks[relative] = expected
    fixture = ROOT / 'partitionline-broker/tests/fixtures/raft-protocol'
    observations = {(item['release'], case['name']): case for item in json.loads((fixture / 'manifest.json').read_text())['releases']
        for case in item['cases']}
    for case in runtime['case_results']:
        expected = observations[(case['release'], case['case'])]
        response = case['response_hex']
        if expected['expected_disposition'] == 'close':
            assert response is None
        else:
            assert bytes.fromhex(response) == (fixture / case['release'] / expected['response_file']).read_bytes()
    command_pins = {}
    for release in matrix['releases']:
        assert release['case_count'] == 67 and release['decoded_actual_rust_responses'] == 62
        assert release['decoded_actual_tcp_responses'] == 6 and len(release['negative_decoder_checks']) == 2
        for command in release['commands'] + release['negative_decoder_checks']:
            assert command['exit_code'] == command['expected_exit_code']
            for stream in ['stdout', 'stderr']:
                path = args.matrix / command[stream]
                assert sha(path.read_bytes()) == command[stream + '_sha256']
                command_pins[path.relative_to(ROOT).as_posix()] = sha(path.read_bytes())
    captures = {path.relative_to(args.captures).as_posix(): sha(path.read_bytes())
        for path in sorted(args.captures.rglob('*')) if path.is_file()}
    assert len(captures) == 222, 'Expected186 direct payloads,18TCP payloads and18TCP frames.'
    review = json.loads((HERE / 'development/source-review.json').read_text())
    for relative, expected in review['paths_sha256'].items():
        assert checks[relative] == expected, 'Reviewed production source changed: ' + relative
    review['source_sha'] = args.source_sha
    review['preliminary_review_head'] = review.pop('head')
    review['kind'] = 'independent frozen-source review'
    review['reviewed_source_matches_git_blobs'] = True
    (HERE / 'independent-source-review.json').write_text(json.dumps(review, indent=2) + '\n')
    result = {'schema_version': 1, 'source_sha': args.source_sha,
        'source_file_count': len(checks), 'source_git_blobs_and_worktree_sha256': checks,
        'java_matrix_sha256': sha((args.matrix / 'results.json').read_bytes()),
        'runtime_report_sha256': sha(args.runtime_report.read_bytes()),
        'independent_review_sha256': sha((HERE / 'independent-source-review.json').read_bytes()),
        'actual_rust_capture_sha256': captures, 'command_log_sha256': command_pins,
        'oracle_case_count': 201, 'actual_rust_direct_response_count': 186,
        'actual_tcp_response_count': 18, 'expected_negative_decoder_failure_count': 6,
        'unchanged_source_and_fixture_receipt': True,
        'rust_execution_attribution': 'rpc_reuse immutable-source direct/TCP fixture driver; no duplicate Rust build by oracle worker.',
        'full_kraft_qualification': False}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print('PASS', len(checks), 'source blobs,201cases,186direct/18TCP responses,6negative decoders')


if __name__ == '__main__':
    main()
