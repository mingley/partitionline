#!/usr/bin/env python3
"""Verify and seal the immutable KL05-28 client matrix and retained failures."""
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
SOURCE_SHA = '6d6ca9aeed263f24870c4b06531d801be655294e'


def read(path):
    return json.loads(path.read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    final = ROOT / 'final/attempt-2'
    receipt = read(final / 'results.json')
    integrity = read(ROOT / 'final/source-integrity.json')
    assert receipt['source_sha'] == SOURCE_SHA and receipt['passed']
    assert integrity['source_sha'] == SOURCE_SHA and len(integrity['files']) == 20193
    for step in receipt['steps']:
        assert step['exit_code'] == 0
        assert step['git_files_verified_before'] == step['git_files_verified_after'] == 20193
    pattern = r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out'
    matrix = []
    for toolchain in ('stable', '1.85.0'):
        for features in ('default', 'all-features'):
            stem = toolchain + '-' + features
            suites = [tuple(map(int, row)) for row in re.findall(pattern, (final / (stem + '-behavior.log')).read_text())]
            totals = tuple(map(sum, zip(*suites)))
            expected = 1814 if features == 'default' else 1815
            assert len(suites) == 49 and totals == (expected, 0, 4, 0, 0)
            docs = [tuple(map(int, row)) for row in re.findall(pattern, (final / (stem + '-doctests.log')).read_text())]
            assert docs == [(4, 0, 0, 0, 0)]
            matrix.append({'toolchain': toolchain, 'features': features, 'behavior_passed': expected,
                           'behavior_ignored': 4, 'behavior_suites': 49, 'doctests_passed': 4,
                           'strict_all_target_clippy': True, 'strict_rustdoc': True,
                           'standalone_cli_examples_built_before_all_target_tests': True})
    failed = read(ROOT / 'final/attempt-1/results.json')
    assert not failed['passed']
    text = (ROOT / 'final/attempt-1/stable-default-behavior.log').read_text()
    assert 'missing example binary' in text and 'run `cargo build --locked --examples` first' in text
    report = {
        'schema_version': 1, 'card': 'KL05-28', 'source_sha': SOURCE_SHA, 'passed': True,
        'verified_git_files_before_and_after_every_final_command': 20193,
        'whole_client_matrix': matrix, 'focused_public_socket_cases_per_cell': 10,
        'checks': {'format': True, 'public_socket_capability_regressions': True,
                   'core_protocol_and_resource_behavior_retained': True},
        'scope': 'Operation-specific capability selection for ordinary Producer and basic Admin; public signatures and dependency defaults unchanged.',
        'limitations': [
            'This prerequisite proves capability behavior against bounded mock TCP peers; real persisted broker interop is a separate KL11-08 lane.',
            'Transactional Producer still requires FindCoordinator and InitProducerId; idempotent nontransactional Producer still requires InitProducerId.',
            'Existing documented empty Admin inputs remain no-ops; nonempty unavailable operations return Unsupported before unrelated network work.',
            'Four existing ignored tests remain ignored; no integration broker availability claim is inferred from this client matrix.'
        ],
        'retained_failures': [
            {'path': 'development/stable-default-attempt-1.log', 'classification': 'Draft test imports/results lint errors; corrected before source freeze.'},
            {'path': 'development/stable-default-attempt-2.log', 'classification': 'Draft mock assumed broker node0 rather than fixture node1; corrected before source freeze.'},
            {'path': 'development/focused-attempt-1', 'classification': 'Draft focused test Clippy style errors; corrected before source freeze.'},
            {'path': 'final/attempt-1', 'classification': 'Exact-source QA preparation omitted standalone CLI examples required by existing CI; all ten verifiable_contract cases failed. Attempt2 built examples first with identical source and all four cells passed.'}
        ],
        'final_receipt': 'final/attempt-2/results.json', 'source_integrity': 'final/source-integrity.json'
    }
    report['closure_dependency'] = 'KL11-08 fresh public Rust-client persisted broker interop; this matrix/socket constituent is complete, but card28 closes together with08 after that independent runtime qualification.'
    (ROOT / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    paths = [p for p in sorted(ROOT.rglob('*')) if p.is_file() and '__pycache__' not in p.parts and p.name != 'SHA256SUMS']
    (ROOT / 'SHA256SUMS').write_text(''.join(digest(p) + '  ' + p.relative_to(ROOT).as_posix() + '\n' for p in paths))
    print(json.dumps({'passed': True, 'source_sha': SOURCE_SHA, 'matrix': matrix, 'artifacts': len(paths),
                      'validation_sha256': digest(ROOT / 'validation.json')}))


if __name__ == '__main__':
    main()
