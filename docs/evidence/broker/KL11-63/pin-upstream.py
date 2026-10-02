#!/usr/bin/env python3
"""Fetch exact official Apache implementation/schema snapshots for audit."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import urllib.request
ROOT = Path(__file__).resolve().parent
PREVIOUS = ROOT.parent / 'KL11-57/upstream-java-oracle.json'
PATHS = [
    'core/src/main/scala/kafka/server/ControllerApis.scala',
    'core/src/main/scala/kafka/server/KafkaApis.scala',
    'core/src/main/scala/kafka/server/AuthHelper.scala',
    'metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java',
    *[f'clients/src/main/java/org/apache/kafka/common/requests/{name}Request.java' for name in ['Metadata', 'CreateTopics', 'DeleteTopics']],
    *[f'clients/src/main/resources/common/message/{name}{direction}.json' for name in ['Metadata', 'CreateTopics', 'DeleteTopics', 'ApiVersions'] for direction in ['Request', 'Response']],
]
def fetch(job):
    version, source_sha, path = job
    url = f'https://raw.githubusercontent.com/apache/kafka/{source_sha}/{path}'
    with urllib.request.urlopen(url, timeout=60) as response: data = response.read()
    dest = ROOT / 'upstream' / version / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return {'version': version, 'source_sha': source_sha, 'path': str(dest.relative_to(ROOT)), 'upstream_path': path, 'url': url, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
previous = json.loads(PREVIOUS.read_text())
releases = [r['distribution_provenance'] for r in previous['releases']]
jobs = [(r['version'], r['source_sha'], path) for r in releases for path in PATHS]
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: files = list(pool.map(fetch, jobs))
report = {'distribution_provenance': releases, 'snapshots': files, 'scope': 'Exact source/schema snapshots. ControllerApis/KafkaApis/ReplicationControlManager are inspected; this evidence does not assert their execution.'}
(ROOT / 'upstream-pins.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'files': len(files), 'bytes': sum(f['bytes'] for f in files)}))
