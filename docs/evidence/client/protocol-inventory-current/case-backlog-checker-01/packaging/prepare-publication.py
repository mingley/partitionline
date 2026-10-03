#!/usr/bin/env python3
"""Inventory existing static proof bytes; do not copy or alter sealed inputs."""
import hashlib
import json
import os
import stat
from pathlib import Path

WORK = Path('/workspace/work')
P = {i: WORK / f'consumer-protocol-checker-implementation-{i:02d}' for i in (1, 2, 3)}
OUT = WORK / 'consumer-protocol-checker-publication-01'
PREFIX = 'docs/evidence/client/protocol-inventory-current/case-backlog-checker-01'
PIN = '5cdbe674430cdbcb6d1ff99f7a992bc2d30132ae'
ROWS = []


def identity(path):
    path = Path(path)
    s = path.lstat()
    assert stat.S_ISREG(s.st_mode) and not path.is_symlink(), path
    data = path.read_bytes()
    return {'source': str(path), 'bytes': len(data), 'full_mode': oct(stat.S_IMODE(s.st_mode)),
            'sha256': hashlib.sha256(data).hexdigest()}


def save(name, obj):
    path = OUT / name
    assert not path.exists(), path
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + '\n')
    os.chmod(path, 0o644)
    return path


def add(source, target, role):
    ROWS.append({**identity(source), 'target': PREFIX + '/' + target, 'role': role})


def directory(source, target, role):
    for path in sorted(source.rglob('*')):
        if path.is_file() or path.is_symlink():
            add(path, target + '/' + str(path.relative_to(source)), role)


def verify_source(p):
    m = json.loads((p / 'immutable-inputs.json').read_text())
    f = json.loads((p / 'candidate-source-freeze-work.json').read_text())
    expected = {r['path']: r for r in m['rows']}
    expected.update({r['path']: r for r in f['rows']})
    actual = {str(x.relative_to(p / 'source')) for x in (p / 'source').rglob('*')
              if x.is_file() or x.is_symlink()}
    assert actual == set(expected)
    for n, row in expected.items():
        actual_id = identity(p / 'source' / n)
        assert all(actual_id[k] == row[k] for k in ('bytes', 'full_mode', 'sha256')), n
    return {'files': len(actual), 'byte_full_mode_pathset_equal': True,
            'scope': '83 minimal static inputs, not whole product archive'}


source_guard = verify_source(P[3])
frozen = json.loads((P[3] / 'candidate-source-freeze-work.json').read_text())
assert frozen['source_sha'] == PIN
canonical = []
for row in frozen['rows']:
    before = identity(Path('/workspace/partitionline') / row['path'])
    baseline = next(r for r in json.loads((P[3] / 'immutable-inputs.json').read_text())['rows']
                    if r['path'] == row['path'])
    assert before['sha256'] == baseline['sha256']
    canonical.append({'target': row['path'], 'before': before,
                      'candidate': identity(P[3] / 'source' / row['path'])})

# Full six-command final receipts, without copying their data.
directory(P[3] / 'guarded-final-work', 'final/commands', 'actual final six-command raw receipt')
for name in ('FINAL-WORK-HANDOFF.json', 'authoritative-install-map.json',
             'candidate-source-freeze-work.json', 'immutable-inputs.json',
             'prior-cohort-preservation.json', 'run-guarded-work.py'):
    add(P[3] / name, 'final/' + name, 'final source/input/installation contract')

# Full unassigned six-command cohort is required: it records honest failing gates.
directory(P[1] / 'guarded-final-work', 'prior/unassigned/commands', 'actual unassigned six-command raw receipt')
for name in ('candidate-source-freeze-work.json', 'immutable-inputs.json', 'run-guarded-work.py'):
    add(P[1] / name, 'prior/unassigned/' + name, 'unassigned source/input contract')
for row in json.loads((P[1] / 'candidate-source-freeze-work.json').read_text())['rows']:
    add(P[1] / 'source' / row['path'], 'prior/unassigned/source/' + row['path'],
        'four-file source-only unassigned snapshot; no 83-input duplicate')

# Preserve the actual assigned stale-test failure and exact source-only snapshot.
directory(P[2] / 'guarded-assigned-first-failure', 'prior/assigned-first-failure/commands',
          'actual first assigned stale-expectation failure')
directory(P[2] / 'retained-assigned-first', 'prior/assigned-first-failure/source-snapshot',
          'four candidate files and three metadata files only')
add(P[2] / 'assigned-first-failure-reason.json', 'prior/assigned-first-failure/reason.json',
    'explicit failure disposition; not product/runtime failure')

# Meaningful first failing controls and initial baseline; small intermediate outputs.
for name in ('baseline-existing-suite.log', 'baseline-existing-suite.exit',
             'baseline-new-controls.log', 'baseline-new-controls.exit',
             'baseline-preparation-first-error.json', 'new-tests.txt',
             'candidate-controls-01.log', 'candidate-controls-01.exit',
             'candidate-full-suite-02.log', 'candidate-full-suite-02.exit',
             'candidate-annotated-suite-03.log', 'candidate-annotated-suite-03.exit',
             'candidate-full-suite-04.log', 'candidate-full-suite-04.exit'):
    add(P[1] / name, 'prior/baseline-controls/' + name, 'actual static control/baseline history')
for name in ('unassigned-preservation.json', 'taskbook-evidence-only-bridge.json'):
    add(P[2] / name, 'prior/' + name, 'earlier cohort identity/provenance bridge')

# Intermediate successful raw runs stay intact in WORK. Publish precise references
# and their validation records instead of another ~4.8 MB of duplicate JSON output.
intermediate = []
for cohort in ('guarded-assigned-dc5-pass', 'guarded-final-work'):
    q = P[2] / cohort
    intermediate.append({'cohort': cohort, 'retained_in_place': str(q),
                         'files': [identity(x) for x in sorted(q.rglob('*')) if x.is_file()],
                         'publication_policy': 'validation copied; complete raw original retained in WORK'})
    add(q / 'validation.json', 'prior/intermediate-success/' + cohort + '/validation.json',
        'intermediate assigned validation; raw originals retained by checksum reference')
for name in ('candidate-source-freeze-work.json', 'authoritative-install-map.json',
             'immutable-inputs.json'):
    add(P[2] / name, 'prior/intermediate-success/' + name, 'prior assigned input/source/install contract')
for name in ('candidate-source-freeze-work.json', 'authoritative-install-map.json',
             'immutable-inputs.json'):
    add(P[2] / 'retained-assigned-dc5-pass' / name,
        'prior/intermediate-success/dc5-source/' + name, 'dc5 taskbook provenance retained')
intermediate.append({'cohort': 'dc5-taskbook-snapshot',
                     'retained_raw_original': identity(P[2] / 'retained-assigned-dc5-pass' / 'taskbook.json'),
                     'publication_policy': 'no duplicate taskbook copied; exact original and source refs retained'})
add(save('retained-intermediate-references.json', {'cohorts': intermediate}),
    'prior/retained-intermediate-references.json', 'all earlier raw successes and taskbook retention hashes')

restore = save('restore-contract.json', {
    'source_sha': PIN, 'scope': 'Static conformance checker qualification only',
    'default_classification_compatibility_retained': True,
    'restore_final_candidate': canonical,
    'restore_originals': 'Git source_sha plus final/immutable-inputs.json; no 83-file tree copies published',
    'restore_prior_candidates': 'Use prior source-only snapshots and each 83-input manifest; original supporting files come from the named Git base',
    'intermediate_taskbooks': 'Exact WORK originals remain intact and hash-bound; final taskbook is Git5cd cb464cc48b434379b90e4ca7638010ec2d0d4b0c',
    'final_commands': 6, 'python_tests': 53,
    'expected_exits': {'unit-tests': 0, 'self-test': 0, 'classification': 0, 'backlog': 0, 'core': 1, 'full': 1},
    'registered_cases': 184, 'required_cases': 164, 'excluded_cases': 20,
    'independent_cases': 16, 'unqualified_cases': 148,
    'core_protocol_complete': False, 'full_current_protocol_complete': False,
    'upstream_applicability_complete': False,
    'no_sdk_cargo_jvm_or_product_runtime': True,
    'task14_must_remain_in_progress': True,
    'coordinator_only_canonical_install_commit_push': True,
    'no_original_source_log_or_mode_mutations': True,
})
add(restore, 'restore-contract.json', 'restoration scope and non-promotion policy')

assert len({r['target'] for r in ROWS}) == len(ROWS)
logical = sum(r['bytes'] for r in ROWS)
max_blob = max(r['bytes'] for r in ROWS)
candidate_bytes = sum(row['candidate']['bytes'] for row in canonical)
assert logical + candidate_bytes < 7_000_000, (logical, candidate_bytes)
assert max_blob < 12_000_000
assert source_guard == verify_source(P[3])
for row in canonical:
    assert identity(row['before']['source']) == row['before']
for row in ROWS:
    assert identity(row['source']) == {k: row[k] for k in ('source', 'bytes', 'full_mode', 'sha256')}
inventory = save('publication-inventory.json', {
    'schema': 1, 'zero_copy_existing_artifact_inventory': True,
    'proof_prefix': PREFIX, 'source_sha': PIN, 'rows': ROWS,
    'proof_file_count': len(ROWS), 'logical_proof_bytes': logical,
    'candidate_source_bytes_separately_installed': candidate_bytes,
    'total_proof_plus_four_candidate_sources': logical + candidate_bytes,
    'max_proof_blob_bytes': max_blob, 'all_blobs_below_12MB': True,
    'logical_proof_budget_7MB_passed': True,
    'excluded_from_publication': ['83-input source copies', 'duplicate intermediate full raw JSON outputs',
                                  'duplicate taskbooks', 'ELF/JAR/class/cache/archive payloads'],
    'source_guard_before_and_after': source_guard,
    'canonical_files_unchanged': True,
    'canonical_candidate_install_rows': canonical,
})
validation = save('publication-validation.json', {
    'passed': True, 'inventory': identity(inventory), 'proof_files': len(ROWS),
    'proof_bytes': logical, 'proof_plus_candidate_bytes': logical + candidate_bytes,
    'source_guard_before_and_after': source_guard, 'canonical_four_paths_unchanged': True,
    'all_selected_artifacts_bytes_full_modes_verified': True,
    'no_test_or_runtime_rerun': True,
})
print(json.dumps({'inventory': identity(inventory), 'validation': identity(validation),
                  'proof_files': len(ROWS), 'proof_bytes': logical,
                  'total_with_sources': logical + candidate_bytes}, indent=2))
