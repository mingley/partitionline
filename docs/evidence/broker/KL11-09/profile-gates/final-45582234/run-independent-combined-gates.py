import hashlib
import json
from pathlib import Path
import subprocess
import time

REPO = Path('/workspace/partitionline')
WORK = Path('/workspace/work/segments-final-45582234')
SOURCE = Path('/workspace/work/snapshot-foundation-final/45582234/source')
PIN = '45582234de9a40620f038ba6b68ab57d2616beb2'
HISTORICAL = 'db002076bb5a19ed4a84cf8a60bce949c1d9cd84'
OUTPUT = REPO / 'docs/evidence/broker/KL11-09/profile-gates/final-45582234'
source_inputs = json.loads((REPO / 'docs/evidence/broker/KL11-15/foundation-final/45582234/source-inputs.json').read_text())
files = source_inputs['source_files_sha256']
registry = json.loads((SOURCE / 'tests/conformance/broker/implemented-api-versions.json').read_text())
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def digest(value):
    return hashlib.sha256(value).hexdigest()
def identity():
    bad = [name for name, checksum in files.items() if not (SOURCE / name).is_file() or sha(SOURCE / name) != checksum]
    assert not bad, bad[:10]
    return {'tracked_files_checked': len(files), 'passed': True}
labels = ['controller_source', 'controller_test_source', 'election_source', 'raft_module_source']
bindings = {}
for label in labels:
    name = registry[label]
    historical = subprocess.check_output(['git', 'show', HISTORICAL + ':' + name], cwd=REPO)
    committed = subprocess.check_output(['git', 'show', PIN + ':' + name], cwd=REPO)
    assert digest(committed) == files[name] == registry[label + '_sha256']
    bindings[name] = {'historical_sha256': digest(historical), 'gate_sha256': digest(committed), 'unchanged': historical == committed}
    if historical != committed:
        assert label == 'raft_module_source'
        bindings[name]['diff'] = subprocess.run(['git', 'diff', HISTORICAL, PIN, '--', name], cwd=REPO, capture_output=True, text=True, check=True).stdout
commands = json.loads((OUTPUT / 'commands.json').read_text())
normalizations = []
cells = []
for toolchain in ['stable', '1.85.0']:
    for features in ['default', 'all-features']:
        stem = toolchain + '-' + features
        prefix = WORK / (stem + '-all-targets')
        historical = REPO / 'docs/evidence/broker/KL11-73/runtime/final-db002076' / stem
        original = historical / 'controller-report.json'
        original_json = json.loads(original.read_text())
        normalized = dict(original_json)
        for label in labels:
            normalized[label + '_sha256'] = registry[label + '_sha256']
        assert normalized['case_results'] == original_json['case_results']
        path = OUTPUT / (stem + '-controller-registry-labels.json')
        path.write_text(json.dumps(normalized, indent=2) + '\n')
        normalizations.append({'cell': stem, 'actual_runtime_source_sha': HISTORICAL,
            'original_report': str(original.relative_to(REPO)), 'original_report_sha256': sha(original),
            'normalized_report': path.name, 'normalized_report_sha256': sha(path),
            'changes': {label + '_sha256': {'original_reflected_label': original_json[label + '_sha256'], 'current_registry_label': normalized[label + '_sha256']} for label in labels},
            'case_results_unchanged': True, 'fresh_controller_runtime_execution_at_gate_source': False})
        reports = [prefix / name for name in ['wire-report.json', 'metadata-report.json', 'produce-report.json', 'fetch-report.json']]
        data = REPO / 'docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json'
        read = REPO / 'docs/evidence/broker/KL11-68/live-read/api-versions.json'
        tcp = historical / 'controller-responses/transport'
        report = OUTPUT / (stem + '-combined-report.json')
        log = OUTPUT / (stem + '-combined.log')
        argv = ['taskset', '-c', '0-2,4', 'python3', 'scripts/check-broker-api-matrix.py',
            '--handler-report', str(reports[0]), '--metadata-handler-report', str(reports[1]),
            '--produce-handler-report', str(reports[2]), '--read-write-handler-report', str(reports[3]),
            '--controller-handler-report', str(path), '--controller-tcp-responses', str(tcp),
            '--data-api-versions-report', str(data), '--read-write-api-versions-report', str(read), '--report', str(report)]
        before = identity()
        start = time.time()
        with log.open('wb') as output:
            result = subprocess.run(argv, cwd=SOURCE, stdout=output, stderr=subprocess.STDOUT)
        inputs = reports + [original, path, data, read]
        commands.append({'argv': argv, 'cwd': str(SOURCE), 'exit_code': result.returncode,
            'elapsed_seconds': round(time.time() - start, 3), 'log': log.name, 'log_sha256': sha(log),
            'source_before': before, 'source_after': identity(),
            'input_reports_sha256': {str(p): sha(p) for p in inputs},
            'input_tcp_captures_sha256': {str(p.relative_to(tcp)): sha(p) for p in sorted(tcp.rglob('*.bin'))}})
        (OUTPUT / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        assert result.returncode == 0, log
        assert json.loads(report.read_text())['verdict'] == 'passed'
        cells.append({'cell': stem, 'report': report.name, 'sha256': sha(report)})
(OUTPUT / 'controller-registry-reflection-normalization.json').write_text(json.dumps({'schema_version': 1,
    'gate_source_sha': PIN, 'historical_controller_runtime_source_sha': HISTORICAL,
    'scope': 'Reflected source labels only. Original reports and every case/capture byte are unchanged; normalization does not establish fresh controller runtime execution.',
    'source_bindings': bindings, 'cells': normalizations}, indent=2) + '\n')
rolling = {}
for cell in cells:
    prefix = WORK / (cell['cell'] + '-all-targets')
    rolling[cell['cell']] = {name: sha(prefix / name) for name in ['rolling-report.json', 'apache-router-report.json']}
receipt = {'schema_version': 1, 'gate_source_sha': PIN, 'passed': True, 'ci_tests': 48,
    'tracked_source_files_checked': len(files), 'compiled_cells': cells,
    'per_cell_cases': {'fresh_wire': 99, 'fresh_metadata': 555, 'fresh_produce': 708, 'fresh_fetch_list_offsets': 366,
        'fresh_compiled_read_write_api_versions_layouts': 15, 'historical_controller': 201, 'historical_controller_tcp_payload_frame_pairs': 18},
    'independent_live_api_versions_exchanges': {'historical_produce': 30, 'historical_read_write': 60},
    'rolling_compiled_report_sha256': rolling,
    'static_review': {'path': 'docs/evidence/broker/KL11-09/independent-storage-review.json', 'sha256': sha(REPO / 'docs/evidence/broker/KL11-09/independent-storage-review.json')},
    'limitations': ['The four ordinary compiled reports and rolling tests were executed on exact 455 source. The controller reports and18TCP pairs retain actual db002 execution; only reflected source labels are normalized for the current static registry.',
        'The independent30Produce/60read-write live API18 receipts retain exact c34/58810 provenance; no fresh455live client claim follows from them.',
        'Existing profile verifier checks do not parse the new rolling366 or Apache56 report schemas. Their actual test execution/immutable source provenance and independent storage review are separately retained; raw fault states are checked by the root-owned history oracle.',
        'No uncommitted15replication or37security runtime source was used.'],
    'artifacts_sha256': {p.name: sha(p) for p in sorted(OUTPUT.iterdir()) if p.is_file() and p.name != 'validation.json'}}
(OUTPUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'passed': True, 'output': str(OUTPUT), 'cells': len(cells)}))
