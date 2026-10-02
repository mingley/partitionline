#!/usr/bin/env python3
"""Retain independent Kafka log API schemas and the exact upstream semantic sources."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
PROVENANCE = REPO / 'docs/evidence/broker/KL11-57/upstream-java-oracle.json'
SOURCE_PATHS = [
    *[f'clients/src/main/resources/common/message/{api}{direction}.json'
      for api in ['Produce', 'Fetch', 'ListOffsets'] for direction in ['Request', 'Response']],
    *[f'clients/src/main/java/org/apache/kafka/common/requests/{api}{direction}.java'
      for api in ['Produce', 'Fetch', 'ListOffsets'] for direction in ['Request', 'Response']],
    'clients/src/main/java/org/apache/kafka/common/requests/ApiError.java',
    'clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java',
    'clients/src/main/java/org/apache/kafka/common/protocol/Errors.java',
    'clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java',
    'core/src/main/scala/kafka/server/KafkaApis.scala',
    'core/src/main/scala/kafka/server/ReplicaManager.scala',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java',
    'storage/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java',
]
NATIVE_SHA = '9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab'

def digest(data):
    return hashlib.sha256(data).hexdigest()

def retain(item):
    version, commit, path = item
    url = f'https://raw.githubusercontent.com/apache/kafka/{commit}/{path}'
    with urllib.request.urlopen(url, timeout=45) as response:
        data = response.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise ValueError('bounded source download exceeded')
    target = ROOT / 'upstream' / version / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return {'release': version, 'source_sha': commit, 'path': str(target.relative_to(ROOT)),
            'upstream_path': path, 'url': url, 'bytes': len(data), 'sha256': digest(data)}

def main():
    provenance = json.loads(PROVENANCE.read_text())
    releases = [row['distribution_provenance'] for row in provenance['releases']]
    items = [(row['version'], row['source_sha'], path) for row in releases for path in SOURCE_PATHS]
    for row in releases:
        namespace = 'record/internal' if row['version'] == '4.3.1' else 'record'
        items.extend((row['version'], row['source_sha'],
                      f'clients/src/main/java/org/apache/kafka/common/{namespace}/{name}.java')
                     for name in ['FileRecords', 'MemoryRecords', 'MemoryRecordsBuilder'])
    outcomes = []
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        pending = {executor.submit(retain, item): item for item in items}
        for future in concurrent.futures.as_completed(pending):
            try:
                outcomes.append(future.result())
            except Exception as failure:
                failures.append({'input': pending[future], 'exception': type(failure).__name__,
                                 'message': str(failure)})
    if failures:
        index = 1
        while (ROOT / f'upstream-failed-attempt-{index}.json').exists():
            index += 1
        (ROOT / f'upstream-failed-attempt-{index}.json').write_text(json.dumps(failures, indent=2) + '\n')
        raise RuntimeError(f'{len(failures)} source downloads failed; retained attempt {index}')
    native_path = 'src/rdkafka_feature.c'
    native_data = subprocess.check_output(['git', '-C', '/workspace/work/c-peer/source',
                                           'show', f'{NATIVE_SHA}:{native_path}'])
    target = ROOT / 'upstream/librdkafka-2.15.0' / native_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(native_data)
    report = {'scope': 'Exact source/schema evidence, not an executed Apache broker qualification.',
              'provenance_source': str(PROVENANCE.relative_to(REPO)),
              'provenance_sha256': digest(PROVENANCE.read_bytes()), 'releases': releases,
              'files': sorted(outcomes, key=lambda row: (row['release'], row['upstream_path'])),
              'native': {'version': '2.15.0', 'source_sha': NATIVE_SHA,
                         'path': str(target.relative_to(ROOT)), 'bytes': len(native_data),
                         'sha256': digest(native_data),
                         'url': f'https://github.com/confluentinc/librdkafka/blob/{NATIVE_SHA}/{native_path}'}}
    (ROOT / 'upstream-pins.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'apache_source_files': len(outcomes), 'native_source_files': 1}))

if __name__ == '__main__':
    main()
