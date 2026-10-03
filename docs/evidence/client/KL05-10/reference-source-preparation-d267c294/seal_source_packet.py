import ast
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import stat

ROOT = Path(__file__).resolve().parent
PRIOR = Path('/workspace/work/client-sticky-qualification-prep-9f5a22cb')
REPO = Path('/workspace/partitionline')
PREFIX = Path('docs/evidence/client/KL05-10/reference-source-preparation-d267c294')
FIXTURES = Path('tests/fixtures/sticky-partitioner')


def info(path):
    data = path.read_bytes()
    return {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
            'mode': oct(stat.S_IMODE(path.stat().st_mode))}


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


prior_manifest = json.loads((PRIOR / 'manifest.json').read_text())
assert info(PRIOR / 'manifest.json')['sha256'] == '4b0da607033163571aaed894a9a367b8827f1a497cf9dcb1ad0f9208d30fff97'
for relative, expected in prior_manifest['files'].items():
    assert info(PRIOR / relative) == expected, relative
prior_directories = {str(path.relative_to(PRIOR)): oct(stat.S_IMODE(path.stat().st_mode))
                     for path in [PRIOR] + sorted(PRIOR.rglob('*')) if path.is_dir()}
identity = json.loads((ROOT / 'candidate-source-identity.json').read_text())
for relative, expected in identity['files'].items():
    assert info(REPO / relative) == expected, relative
assert identity['files']['src/partitioner.rs']['sha256'] == '40a756c2f12f65c1f53cc44a42f604619156ab760321745f4d0899fb30dbdc9a'
assert identity['files']['src/producer.rs']['sha256'] == '37098eee8da69a2e8c39b24cd88358447d55a8992d3ef6b21e9097fabcfebc10'
assert identity['files']['tests/sticky_partitioner.rs']['sha256'] == '959361daf4d29ba1615fa33ea97f0a06e7992044ea7d5fb10ffe57b2f3b9808f'
for name in ['GenerateUniformStickyFixture.java', 'uniform-input.tsv']:
    assert info(ROOT / name) == info(PRIOR / name), name
for name in ['run-java-reference.py', 'run-focused.py', 'prepare_inputs.py', 'seal_source_packet.py']:
    ast.parse((ROOT / name).read_text())

provenance = json.loads((ROOT / 'input-provenance.json').read_text())
provenance.update({'status': 'Source-only reference input checkpoint; caller/input uncompiled and unexecuted',
                   'candidate_source_identity': identity,
                   'input_file': info(ROOT / 'uniform-input.tsv'),
                   'genuine_caller_file': info(ROOT / 'GenerateUniformStickyFixture.java'),
                   'prepared_cases': 12, 'prepared_events': 72,
                   'expected_outputs_present': False, 'generated_classes_present': False})
write(ROOT / 'uniform-input-provenance.json', provenance)
reference_plan = json.loads((ROOT / 'java-reference-plan.json').read_text())
reference_plan.update({'status': 'Source-only caller/input checkpoint. No javac/java/Rust/reference test/runtime acceptance',
                       'candidate_source_identity': identity,
                       'genuine_runner_source': info(ROOT / 'run-java-reference.py'),
                       'supplemental_resource_guards': {
                           'whole_source': 'Actual Git blob/path set and full600/700 modes verified initially, complete SHA/bytes/full file and directory modes before/after every child command',
                           'owned_process_group': 'start_new_session; /proc pid/group/session membership histories, only own-session signals, parent reaped and zero membership required',
                           'disk_monitor_period_seconds': 0.2, 'physical_disk_floor_mib': 350,
                           'total_runtime_seconds': 300, 'per_child_runtime_seconds_maximum': 120,
                           'kernel_per_captured_file_cap_bytes': 262144,
                           'reference_stdout_validation_cap_bytes': 32768,
                           'invalid_control_exit': 'Exactly1 with genuine IllegalArgumentException; resource/time/signal/classpath failures cannot count as expected invalid input',
                           'compiled_artifact_scope': 'Only two caller class files, WORK-only. No compiled artifacts or expected output installed by this checkpoint'},
                       'Rust_package_reuse_guard': 'All future qualification must retain existing own package/cached ELFs, format FIRST, then cargo clean --package partitionline in the exclusively owned target before strict/build/tests. Dependencies may remain; confirm no libpartitionline rlib/rmeta or package fingerprint leftovers. Recompile actual package for each candidate/profile when mtimes can hide changed bytes.',
                       'actual_commands_executed_in_this_stage': ['Python source AST parse, immutable prior-preparation hash/full-mode verification, source-only fixture/evidence installation'],
                       'Cargo_JVM_benchmark_executed': False})
write(ROOT / 'candidate-reference-plan.json', reference_plan)
focused_plan = json.loads((ROOT / 'focused-qa-plan.json').read_text())
focused_plan.update({'status': 'Amended WORK source-only runner; not executed',
                    'candidate_source_identity': identity,
                    'runner': info(ROOT / 'run-focused.py'),
                    'force_package_recompile': 'Format FIRST, verified own-cache ELF retention receipt for an existing target, cargo clean --package partitionline, assert no own library rlib/rmeta or .fingerprint leftovers before strict/build/tests. Shared dependency cache may remain; no inference that prior mtime-based library reuse validated this pin.',
                    'performance_proposal': 'Unchanged WORK-only prior phase; no benchmark sources installed and no CPU3 use'})
write(ROOT / 'candidate-focused-qa-plan.json', focused_plan)
write(ROOT / 'resource-forecast.json', {
    'schema': 1, 'status': 'Conservative future forecast, not a measurement or execution lease',
    'CPU': 'Parent and SDK children0,1 only',
    'JVM_heap_mib': 128, 'JVM_max_metaspace_mib': 128, 'runtime_code_cache_mib': 64,
    'memory_forecast_mib': 'Up to768 for Python whole-source dictionaries/audit plus one JVM; not a hard total RSS cap',
    'disk': 'Official JARs already retained, no download/container/image. Reuse root materialized whole source, no additional tree extraction. Complete source audit gzip and10 child command capture sets forecast<=16MiB output, caller classes<=1MiB; source manifest memory/raw encoding transient is in RAM.',
    'disk_initial_minimum_mib': 400, 'physical_floor_mib': 350, 'monitor_seconds': 0.2,
    'runtime_total_seconds': 300, 'runtime_child_seconds': 120,
    'bounds': 'Input<=32KiB, positive/negative reference stdout<=32KiB each, each captured file kernel<=256KiB, each run exact2 caller classes, invalid inputs tiny<=32769bytes',
    'failures': 'Stop only owned child group, retain all raw captured output/exit/source/resource/membership observations, require zero membership. Resource failure is not Java invalid-control proof and never fixture success.',
    'Rust_focus': 'Separate later lease; one exclusively owned cache, no stale own-package reuse, initial>=1150MiB and350MiB live floor as prior plan. No Cargo launched here.',
    'performance': 'Proposal remains WORK-only; no build/JVM/broker/performance lease or CPU3 use implied.'})
write(ROOT / 'prior-prep-preservation.json', {
    'schema': 1, 'prior_root': str(PRIOR), 'prior_manifest': info(PRIOR / 'manifest.json'),
    'files': 33, 'full_directory_modes': prior_directories,
    'exact_bytes_full_modes_verified_before_new_phase': True,
    'original_snapshot_89dd_status': 'Historical pre-unused-results source only; current production identity40a756 is separately observed',
    'scope': 'Prior33 files and their manifest remain untouched in WORK; copies in this new phase are independent files'})
(ROOT / 'runner-amendment.patch').write_text(''.join(difflib.unified_diff(
    (PRIOR / 'run-java-reference.py').read_text().splitlines(keepends=True),
    (ROOT / 'run-java-reference.py').read_text().splitlines(keepends=True),
    fromfile='prior-prepared/run-java-reference.py', tofile='amended/run-java-reference.py')))
(ROOT / 'Rust-fixture-integration-pending.md').write_text(
    'The prepared Rust consumer remains WORK/evidence source only. Do not install a cfg(test) include or expected outputs before actual genuine JVM positive and independent negative outputs have been captured, frozen and reviewed. The later minimum source stage adds the exact outputs, provenance, consumer fixture and cfg(test) include on a new actual pin; all actual Rust tests then force own-package recompilation. No Java accumulator/compression/RNG or whole-producer history equivalence is claimed.\n')

fixture_files = {
    'GenerateUniformStickyFixture.java': ROOT / 'GenerateUniformStickyFixture.java',
    'uniform-input.tsv': ROOT / 'uniform-input.tsv',
    'uniform-input-provenance.json': ROOT / 'uniform-input-provenance.json',
}
evidence_names = ['candidate-source-identity.json', 'candidate-reference-plan.json',
                  'candidate-focused-qa-plan.json', 'run-java-reference.py', 'run-focused.py',
                  'resource-forecast.json', 'prior-prep-manifest.json', 'prior-prep-preservation.json',
                  'genuine-artifact-read-identity.json', 'prepare_inputs.py', 'runner-amendment.patch',
                  'java-reference-tests.rs', 'Rust-fixture-integration-pending.md', 'seal_source_packet.py']
write(ROOT / 'source-freeze.json', {'schema': 1,
                                  'status': 'Source-only reference checkpoint, uncompiled/unexecuted',
                                  'candidate_source_identity': identity,
                                  'fixture_files': {str(FIXTURES / name): info(path) for name, path in fixture_files.items()},
                                  'evidence_files': {str(PREFIX / name): info(ROOT / name) for name in evidence_names},
                                  'expected_outputs_or_generated_classes_installed': False,
                                  'cfg_test_include_installed': False,
                                  'Cargo_JVM_benchmark_executed': False})
evidence_names.append('source-freeze.json')
stage = {}
for relative, path in [(FIXTURES / name, path) for name, path in fixture_files.items()] + [(PREFIX / name, ROOT / name) for name in evidence_names]:
    destination = REPO / relative
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    assert not destination.exists(), str(destination)
    shutil.copy2(path, destination)
    assert info(path) == info(destination)
    stage[str(relative)] = info(destination)
for relative, expected in prior_manifest['files'].items():
    assert info(PRIOR / relative) == expected, relative
assert {str(path.relative_to(PRIOR)): oct(stat.S_IMODE(path.stat().st_mode))
        for path in [PRIOR] + sorted(PRIOR.rglob('*')) if path.is_dir()} == prior_directories
for relative, expected in identity['files'].items():
    assert info(REPO / relative) == expected, relative
write(ROOT / 'stage-handoff.json', {'schema': 1, 'status': 'safe_to_stage source-only checkpoint; root commits/pushes',
                                   'stage_files': stage, 'source_freeze': info(ROOT / 'source-freeze.json'),
                                   'prepared_reference_cases': 12, 'prepared_reference_events': 72,
                                   'actual_Rust_JVM_performance_execution': False,
                                   'source_prod_and_held_files_unchanged': True,
                                   'prior33_exact_bytes_full_modes_unchanged_after': True})
print(json.dumps({'stage_files': len(stage), 'stage_bytes': sum(v['bytes'] for v in stage.values()),
                  'handoff': info(ROOT / 'stage-handoff.json'), 'source_freeze': info(ROOT / 'source-freeze.json'),
                  'runner': info(ROOT / 'run-java-reference.py')}, indent=2))
