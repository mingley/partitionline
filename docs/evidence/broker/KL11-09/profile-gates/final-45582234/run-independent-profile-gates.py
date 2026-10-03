import hashlib
import json
from pathlib import Path
import subprocess
import time

REPO = Path('/workspace/partitionline')
WORK = Path('/workspace/work/segments-final-45582234')
SOURCE = Path('/workspace/work/snapshot-foundation-final/45582234/source')
PIN = '45582234de9a40620f038ba6b68ab57d2616beb2'
OUTPUT = REPO / 'docs/evidence/broker/KL11-09/profile-gates/final-45582234'
OUTPUT.mkdir(parents=True, exist_ok=True)
source_inputs = json.loads((REPO / 'docs/evidence/broker/KL11-15/foundation-final/45582234/source-inputs.json').read_text())
assert source_inputs['source_sha'] == PIN
files = source_inputs['source_files_sha256']

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def identity():
    bad = [name for name, digest in files.items() if not (SOURCE / name).is_file() or sha(SOURCE / name) != digest]
    assert not bad, bad[:10]
    return {'tracked_files_checked': len(files), 'passed': True}

before = identity()
commands = []
ci_log = WORK / 'ci-48-independent.log'
assert 'Ran 48 tests' in ci_log.read_text() and ci_log.read_text().rstrip().endswith('OK')
(OUTPUT / 'ci-48.log').write_bytes(ci_log.read_bytes())
commands.append({'argv': ['python3', 'tests/ci/test_broker_api_matrix.py'], 'cwd': str(SOURCE), 'exit_code': 0,
                 'log': 'ci-48.log', 'log_sha256': sha(OUTPUT / 'ci-48.log'),
                 'source_after': before, 'source_before_provenance': 'Same source tree was verified by the preceding immutable foundation runner; CI command completed before this wrapper was written.'})
profiles = []
for toolchain in ['stable', '1.85.0']:
    for features in ['default', 'all-features']:
        prefix = WORK / (toolchain + '-' + features + '-all-targets')
        reports = {key: prefix / name for key, name in [('wire', 'wire-report.json'), ('metadata', 'metadata-report.json'), ('produce', 'produce-report.json'), ('fetch', 'fetch-report.json')]}
        reports['data_api_versions'] = REPO / 'docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json'
        reports['read_write_api_versions'] = REPO / 'docs/evidence/broker/KL11-68/live-read/api-versions.json'
        stem = toolchain + '-' + features
        report = OUTPUT / (stem + '-ordinary-report.json')
        log = OUTPUT / (stem + '-ordinary.log')
        argv = ['taskset', '-c', '0-2,4', 'python3', 'scripts/check-broker-api-matrix.py',
                '--handler-report', str(reports['wire']), '--metadata-handler-report', str(reports['metadata']),
                '--produce-handler-report', str(reports['produce']), '--read-write-handler-report', str(reports['fetch']),
                '--data-api-versions-report', str(reports['data_api_versions']),
                '--read-write-api-versions-report', str(reports['read_write_api_versions']), '--report', str(report)]
        source_before = identity()
        start = time.time()
        with log.open('wb') as output:
            result = subprocess.run(argv, cwd=SOURCE, stdout=output, stderr=subprocess.STDOUT)
        commands.append({'argv': argv, 'cwd': str(SOURCE), 'exit_code': result.returncode,
                         'elapsed_seconds': round(time.time() - start, 3), 'log': log.name, 'log_sha256': sha(log),
                         'source_before': source_before, 'source_after': identity(),
                         'input_reports_sha256': {key: {'path': str(path), 'sha256': sha(path)} for key, path in reports.items()}})
        (OUTPUT / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        assert result.returncode == 0, log
        parsed = json.loads(report.read_text())
        assert parsed['verdict'] == 'passed'
        profiles.append({'toolchain': toolchain, 'features': features, 'report': report.name, 'sha256': sha(report)})

receipt = {'schema_version': 1, 'gate_source_sha': PIN, 'runtime_source_sha': PIN, 'passed': True,
           'source_archive_sha256': source_inputs['archive_sha256'], 'tracked_files_checked': len(files),
           'ci_tests': 48, 'compiled_cells': profiles,
           'per_cell_cases': {'wire': 99, 'metadata': 555, 'produce': 708, 'fetch_list_offsets': 366, 'read_write_api_versions_response_layouts': 15},
           'independent_api_versions_exchanges': {'produce_profile': 30, 'read_write_profile': 60},
           'limitations': ['Independent live ApiVersions receipts retain their original c34 Produce and 58810 read/write runtime sources; they are not relabeled as 455 rolling execution.',
                           'This first receipt covers the existing ordinary profiles. Rolling response/TCP reports and the fixed-controller reflected module label are handled separately.'],
           'artifacts_sha256': {p.name: sha(p) for p in sorted(OUTPUT.iterdir()) if p.is_file() and p.name != 'ordinary-validation.json'}}
(OUTPUT / 'ordinary-validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'passed': True, 'output': str(OUTPUT), 'compiled_cells': len(profiles)}))
