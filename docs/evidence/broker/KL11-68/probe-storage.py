#!/usr/bin/env python3
"""Replay authentic bounded Apache storage component methods, using official jars only."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(command, log):
    result = subprocess.run(command, cwd=REPO, capture_output=True, text=True, check=False)
    log.write_text('$ ' + ' '.join(map(str, command)) + '\n' + result.stdout + result.stderr
                   + f'\nexit_code={result.returncode}\n')
    if result.returncode:
        raise RuntimeError(f'failed: {log}')
    return {'command': list(map(str, command)), 'exit_code': result.returncode,
            'log': str(log.relative_to(ROOT))}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jars', type=Path, required=True)
    parser.add_argument('--distributions', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    args = parser.parse_args()
    releases = json.loads((ROOT / 'upstream-pins.json').read_text())['releases']
    source = (ROOT / 'LogStorageProbe.java').read_text()
    args.scratch.mkdir(parents=True, exist_ok=True)
    report = {'source_sha256': sha(ROOT / 'LogStorageProbe.java'),
              'scope': 'Actual LogSegment/FileRecords methods. Component acceptance is not full broker acceptance.',
              'releases': []}
    outcomes = []
    for release in releases:
        version = release['version']
        scratch = args.scratch / version
        scratch.mkdir(exist_ok=True)
        distro = args.distributions / f'kafka_2.13-{version}.tgz'
        assert sha(distro) == release['distribution_sha256']
        jar = args.jars / f'kafka-clients-{version}.jar'
        assert sha(jar) == release['jar_sha256']
        jars = [jar, args.jars / 'slf4j-api-1.7.36.jar']
        assert sha(jars[1]) == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
        with tarfile.open(distro) as archive:
            for basename in [f'kafka-storage-{version}.jar', f'kafka-storage-api-{version}.jar',
                             f'kafka-server-common-{version}.jar', 'metrics-core-2.2.0.jar']:
                member = archive.getmember(f'kafka_2.13-{version}/libs/{basename}')
                assert member.isfile() and member.size < 2 * 1024 * 1024
                extracted = archive.extractfile(member)
                assert extracted is not None
                path = scratch / basename
                path.write_bytes(extracted.read())
                jars.append(path)
        with zipfile.ZipFile(jar) as archive:
            namespace = 'org.apache.kafka.common.record.internal' if 'org/apache/kafka/common/record/internal/MemoryRecords.class' in archive.namelist() else 'org.apache.kafka.common.record'
        adapted = scratch / 'LogStorageProbe.java'
        adapted.write_text(source.replace('org.apache.kafka.common.record.internal.', namespace + '.'))
        classes = scratch / 'classes'
        classes.mkdir(exist_ok=True)
        cp = ':'.join(map(str, jars))
        attempt = 1
        while (ROOT / f'logs/storage-{version}-attempt-{attempt}-compile.txt').exists():
            attempt += 1
        prefix = ROOT / f'logs/storage-{version}-attempt-{attempt}'
        compiled = run(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
                        'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp,
                        '-d', str(classes), str(adapted)], Path(str(prefix) + '-compile.txt'))
        with tempfile.TemporaryDirectory(dir=scratch, prefix='segments-') as temporary:
            tmp = Path(temporary)
            result = tmp / 'first.json'
            command = ['taskset', '-c', '0-2,4', 'java', '-cp', f'{classes}:{cp}',
                       'LogStorageProbe', str(tmp / 'first'), str(result)]
            first = run(command, Path(str(prefix) + '-runtime.txt'))
            replay = tmp / 'replay.json'
            second = run(command[:-2] + [str(tmp / 'replay'), str(replay)], Path(str(prefix) + '-replay.txt'))
            assert result.read_bytes() == replay.read_bytes()
            destination = ROOT / f'storage-component-{version}.json'
            destination.write_bytes(result.read_bytes())
        outcome = json.loads(destination.read_text())
        outcomes.append(outcome)
        assert len(outcome['reads']) == 12 and len(outcome['timestamp_searches']) == 17
        report['releases'].append({'release': version, 'distribution_sha256': sha(distro),
                                   'record_package': namespace, 'adapted_source_sha256': sha(adapted),
                                   'classpath': [{'name': path.name, 'sha256': sha(path)} for path in jars],
                                   'compile': compiled, 'runtime': first, 'replay': second,
                                   'class_sha256': sha(classes / 'LogStorageProbe.class'),
                                   'result': str(destination.relative_to(ROOT)),
                                   'result_sha256': sha(destination), 'reads': 12, 'timestamp_searches': 17})
    assert outcomes[0] == outcomes[1] == outcomes[2]
    report['cross_release_outcomes_identical'] = True
    report['total_reads'] = 36
    report['total_timestamp_searches'] = 51
    (ROOT / 'storage-probes.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'reads': 36, 'timestamp_searches': 51, 'cross_release_identical': True}))

if __name__ == '__main__':
    main()
