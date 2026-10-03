#!/usr/bin/env python3
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil

repo = Path('/workspace/partitionline')
raw = Path('/workspace/work/retention-final-d147bcf1')
out = repo / 'docs/evidence/broker/KL11-10/storage/final-d147bcf1'
source_sha = 'd147bcf1c0164778bdbad625842363f3721bc10e'
qa_bytes = (raw / 'validation.json').read_bytes()
assert hashlib.sha256(qa_bytes).hexdigest() == '94e6f2b35d73ba1f70775cadc6de41635e9ee8741d30b3a9e019f0d380bd0bfa'
qa = json.loads(qa_bytes)
assert qa['source_commit'] == source_sha and len(qa['commands']) == 19
assert all(c['exit_code'] == 0 for c in qa['commands'])
assert qa['final_source']['file_count'] == 44019
assert all(c['source_before'] == c['source_after'] == qa['final_source'] for c in qa['commands'])
source_bytes = (raw / 'source-integrity.json').read_bytes()
assert hashlib.sha256(source_bytes).hexdigest() == '17b1a87c5ee3230f1cffd5422c5eacd082ac554de19b1795e879d0e983b1e00e'
source = json.loads(source_bytes)
assert source['file_count'] == 44019 and source['source_commit'] == source_sha
out.mkdir(parents=True, exist_ok=False)
(out / 'validation.json').write_bytes(qa_bytes)
(out / 'source-integrity.json.gz').write_bytes(gzip.compress(source_bytes, compresslevel=6, mtime=0))
assert gzip.decompress((out / 'source-integrity.json.gz').read_bytes()) == source_bytes
shutil.copy2(raw / 'source/docs/evidence/broker/KL11-10/storage/run-final.py', out / 'run-final.py')
for command in qa['commands']:
    shutil.copytree(raw / command['name'], out / command['name'])
cells = []
for toolchain in ('stable', '1.85.0'):
    for features in ('default', 'all-features'):
        name = toolchain + '-' + features
        destination = out / name
        destination.mkdir()
        for path in (raw / name).iterdir():
            if path.name in ('snapshot', 'snapshot-inner', 'replication'):
                continue
            if path.is_dir():
                shutil.copytree(path, destination / path.name)
            else:
                shutil.copy2(path, destination / path.name)
        text = (raw / (name + '-all-targets') / 'command.log').read_text()
        suites = [list(map(int, row)) for row in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', text)]
        assert len(suites) == 22 and all(row[1:] == [0, 0] for row in suites)
        retention = json.loads((destination / 'retention-report.json').read_text())
        assert len(retention['case_results']) == 363 and len(retention['api_versions_cases']) == 15
        fault_root = destination / 'retention-fault-histories'
        assert len(list(fault_root.glob('*/*/*/case.json'))) == 46
        cells.append({'toolchain': toolchain, 'features': features, 'passed': sum(row[0] for row in suites),
                      'failed': 0, 'ignored': 0, 'suites': len(suites), 'retention_cases': 363,
                      'api_versions_cases': 15, 'retention_histories': 46, 'retention_states': 92,
                      'additional_getter_scenarios': 23, 'legacy_rolling_histories': 33,
                      'legacy_rolling_states': 66})
owned = ('partitionline-broker/src/segments.rs', 'partitionline-broker/src/partition.rs',
         'partitionline-broker/tests/segments.rs', 'partitionline-broker/tests/partition.rs')
bindings = []
for name in owned:
    data = (raw / 'source' / name).read_bytes()
    assert hashlib.sha256(data).hexdigest() == source['files'][name]['sha256']
    path = out / 'sources' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    bindings.append({'path': name, **source['files'][name]})
summary = {'schema': 1, 'source_commit': source_sha, 'original_execution_root': str(raw),
           'qa_receipt_sha256': hashlib.sha256(qa_bytes).hexdigest(),
           'uncompressed_full_source_manifest_sha256': hashlib.sha256(source_bytes).hexdigest(),
           'compressed_full_source_manifest_sha256': hashlib.sha256((out / 'source-integrity.json.gz').read_bytes()).hexdigest(),
           'full_source_files': 44019, 'commands_passed': 19, 'test_passes': sum(c['passed'] for c in cells),
           'cells': cells, 'owned_source_bindings': bindings,
           'retained_binaries': {'unchanged_scratch_root': str(raw / 'bin'), 'count': 18,
                                 'git_stage': False, 'receipt': 'validation.json'},
           'snapshot_and_replication_captures': 'Kept in original execution root for separately owned KL11-15 sealing and durable receipts.',
           'scope': 'Exact-source local behavioral/fault/strict qualification; independent upstream, live peers and byte-oracle receipts are separate.',
           'limitations': ['No physical power-loss, multi-replica retention, transaction safety, exhaustive crash scheduling or production qualification claim.',
                           'Negative/unknown timestamp segments are excluded from age retention; Apache mutable-file-time fallback is a documented local difference.',
                           'Monolithic/default APIs retain their prior contract; retention is explicitly enabled with rolling storage.']}
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
artifacts = {str(p.relative_to(out)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': p.stat().st_size}
             for p in sorted(out.rglob('*')) if p.is_file()}
(out / 'artifacts.json').write_text(json.dumps({'schema': 1, 'source_commit': source_sha, 'artifacts': artifacts}, indent=2) + '\n')
print(json.dumps({'root': str(out), 'files': len(artifacts) + 1, 'passes': summary['test_passes'],
                  'summary_sha256': hashlib.sha256((out / 'summary.json').read_bytes()).hexdigest(),
                  'artifact_manifest_sha256': hashlib.sha256((out / 'artifacts.json').read_bytes()).hexdigest()}))
