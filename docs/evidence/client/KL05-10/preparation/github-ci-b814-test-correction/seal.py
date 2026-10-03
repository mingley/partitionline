"""Seal only the two authorized source edits and their additive preparation proof."""
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import stat
import subprocess

OUT = Path(__file__).resolve().parent
REPO = Path('/workspace/partitionline')
STAGE = OUT / 'stage'
PREFIX = 'docs/evidence/client/KL05-10/preparation/github-ci-b814-test-correction'
TARGETS = ['tests/sticky_partitioner.rs', 'tests/conformance/test_verifiable_scenario.py']
CI = Path('/workspace/work/github-ci-b814-review-01')

def info(path):
    raw = path.read_bytes()
    return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'mode_07777': oct(stat.S_IMODE(path.stat().st_mode))}

def put_json(path, value):
    assert not path.exists(), str(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)

def copy(source, target):
    assert not target.exists(), str(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    target.chmod(stat.S_IMODE(source.stat().st_mode))
    assert info(source) == info(target)

assert not STAGE.exists()
original = json.loads((OUT / 'repository-before.json').read_text())
after = [{'path': row['path'], **info(REPO / row['path'])} for row in original['files']]
assert after == original['files'], 'Repository target/production/held source guard changed.'
put_json(OUT / 'repository-after.json', {'observed_head': subprocess.check_output(
    ['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
    'scope': original['scope'], 'files': after, 'all_guarded_bytes_full_modes_unchanged': True})

ci_files = []
for path in sorted(CI.glob('*.decoded.log')):
    ci_files.append({'path': str(path), **info(path)})
assert len(ci_files) == 13
put_json(OUT / 'ci-evidence-reference.json', {
    'ci_source_sha': 'b8148087f08b205d7d19fd0be10994ebcd1fcc3a',
    'scope': 'All 13 exact tool-decoded UTF-8 logs, including original BOM/newlines; not original HTTP/GitHub archive byte claims.',
    'raw_logs_retained_in': str(CI), 'raw_publication_owner': 'codex-c-peer',
    'compiled_rust_fix_not_yet_executed': True,
    'files': ci_files,
    'retrieval_and_diagnostics': {'path': str(CI / 'retrieval-and-diagnostics.json'), **info(CI / 'retrieval-and-diagnostics.json')},
    'publication_inventory': {'path': str(CI / 'publication-inventory.json'), **info(CI / 'publication-inventory.json')},
    'source_provenance': {'path': str(CI / 'source-provenance.json'), **info(CI / 'source-provenance.json')}})

diff = ''
for path in TARGETS:
    before = (OUT / 'original-b814' / path).read_text().splitlines(keepends=True)
    corrected = (OUT / 'candidate' / path).read_text().splitlines(keepends=True)
    diff += ''.join(difflib.unified_diff(before, corrected, fromfile='a/' + path, tofile='b/' + path))
patch = OUT / 'two-test-source.patch'
patch.write_text(diff)
patch.chmod(0o600)
registry = json.loads((OUT / 'candidate/tests/conformance/cases.json').read_text())
contract = registry['coverage_contract']
profile = json.loads((OUT / 'candidate/tests/conformance/verifiable-live-profile.json').read_text())
missing = sorted(set(contract['required_case_ids']) - {profile['case_id']})
assert len(missing) == 183 and len(contract['required_case_ids']) == 184
assert len(contract['denominator_case_ids']) == 164
expected = f"Missing {len(missing)} required conformance case(s) (showing 5 of {len(missing)}: {', '.join(missing[:5])})"
stderr = (OUT / 'checks/corrected-cohort-complete-dependencies/stderr.log').read_text()
assert '\nRan 113 tests ' in stderr and stderr.endswith('\nOK\n')
checks = {name: json.loads((OUT / 'checks' / name / 'receipt.json').read_text())
          for name in ['failed-first', 'corrected-cohort', 'corrected-cohort-complete-dependencies',
                       'stable-format', 'stable-format-complete-dependencies', 'msrv-format']}
put_json(OUT / 'validation.json', {
    'source_sha': 'a85b241765981a21cf2b3161ae6eac155200a441',
    'source_scope': 'Exact Git dependency subset with only two authorized test overlays; not full client qualification.',
    'candidate_sources': [{'path': path, **info(OUT / 'candidate' / path)} for path in TARGETS],
    'production_changed': False, 'registry_or_checker_changed': False,
    'rust_fix': 'Public Producer clone retains a handle after consuming close; all shared clones are closed; unchanged zero tuple is still asserted after awaited close.',
    'rust_compiled_or_executed': False,
    'python_fix': 'Audit declared unique required and denominator ID lists against actual rows; reject the exact independently derived missing-case count and first five sorted IDs.',
    'python_registry': {'all_report_rows': 184, 'qualified_denominator': 164, 'excluded_rows': 20,
                        'reported_fixture_case': profile['case_id'], 'exact_missing_count': len(missing),
                        'ordered_missing_ids': missing, 'expected_diagnostic': expected},
    'actual_python_cohort': {'test_methods': 113, 'failures': 0, 'errors': 0, 'skipped': 0,
                             'scope': 'Offline synthetic qualification and negative fixtures; no actual broker/SDK histories newly executed.'},
    'format': {'stable': 'passed', 'rust_1_85': 'passed', 'no_Cargo': True},
    'first_attempts_retained': {
        'ci': 'Actual b814 E0382 and stale Missing 105 versus Missing 168, preserved by c-peer references.',
        'local_stale_case': 'Actual exit1: canonical current registry Missing 183 does not match Missing 105.',
        'initial_full_cohort': '112 passing methods; one failure because isolated preparation closure omitted docs/plan/evidence/KL05-15.json. Exact dependency added without changing its bytes.',
        'initial_format': 'Missing tests/common module in isolated preparation closure; exact Git dependency added.'},
    'checks': checks,
    'limits': ['No Cargo, Rust test execution, JVM, SDK, broker, performance, or general CI-green claim.',
               'Complete qualified Rust/client matrix must use a future actual pushed source pin.',
               'The original registry fields and production Producer/partitioner/held codecs are unchanged.']})

proofs = ['prepare.py', 'apply_python.py', 'run_checks.py', 'seal.py',
          'dependency-source-before.json', 'dependency-closure-addition.json', 'prepared-source-scope.json',
          'original-two-sources.json', 'repository-before.json', 'repository-after.json',
          'ci-evidence-reference.json', 'validation.json', 'two-test-source.patch']
proofs += [str(path.relative_to(OUT)) for group in ['original-b814', 'checks', 'history']
           for path in sorted((OUT / group).rglob('*')) if path.is_file()]
for path in TARGETS:
    copy(OUT / 'candidate' / path, STAGE / path)
for path in proofs:
    copy(OUT / path, STAGE / PREFIX / path)
payloads = [{'path': str(path.relative_to(STAGE)), 'source': str(path), **info(path)}
            for path in sorted(STAGE.rglob('*')) if path.is_file()]
put_json(OUT / 'stage-handoff.json', {
    'safe_to_stage': True, 'safe_to_claim_qualified': False, 'write_paths': TARGETS,
    'proof_prefix': PREFIX, 'payload_count': len(payloads),
    'payload_bytes': sum(row['bytes'] for row in payloads), 'files': payloads,
    'directories': [{'path': str(path.relative_to(STAGE)), 'mode_07777': oct(stat.S_IMODE(path.stat().st_mode))}
                    for path in sorted(STAGE.rglob('*')) if path.is_dir()],
    'ci_raw_logs': 'Retained unchanged in c-peer packet; exact13 SHA/fullmode binding in staged reference.',
    'dependency_copy': '53 exact input paths retained WORK-only, with original/actual command before/after full byte and07777 guards.',
    'source_installation': 'Root only: overlay the two named test files; production and registry unchanged.',
    'executed': '113 offline Python unit methods and stable/MSRV source-only format checks.',
    'not_executed': ['Cargo', 'Rust sticky test', 'JVM', 'SDK', 'broker', 'benchmark']})
print(json.dumps({'stage_handoff': str(OUT / 'stage-handoff.json'),
                  **info(OUT / 'stage-handoff.json'), 'files': len(payloads),
                  'payload_bytes': sum(row['bytes'] for row in payloads),
                  'candidate_sources': {p: info(OUT / 'candidate' / p) for p in TARGETS}}))
