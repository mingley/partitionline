#!/usr/bin/env python3
"""Reproduce bounded component assertions against independently pinned Apache jars."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import time
import zipfile

HERE = Path(__file__).resolve().parent
SELECTED = [
    'LICENSE', 'NOTICE',
    'raft/src/main/java/org/apache/kafka/raft/LeaderState.java',
    'raft/src/main/java/org/apache/kafka/raft/FollowerState.java',
    'raft/src/main/java/org/apache/kafka/raft/MetadataLogConfig.java',
    'raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java',
    'raft/src/main/java/org/apache/kafka/raft/VoterSet.java',
    'raft/src/main/java/org/apache/kafka/raft/internals/BatchAccumulator.java',
    'raft/src/main/java/org/apache/kafka/raft/internals/IdentitySerde.java',
    'raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftMetrics.java',
    'raft/src/test/java/org/apache/kafka/raft/LeaderStateTest.java',
    'raft/src/test/java/org/apache/kafka/raft/FollowerStateTest.java',
]

def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()

def retained_source(original, destination, version):
    selected = SELECTED + (['raft/src/main/java/org/apache/kafka/raft/RaftLog.java',
                           'raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java',
                           'raft/src/test/java/org/apache/kafka/raft/internals/KafkaRaftLogTest.java']
                          if version == '4.3.1' else
                          ['raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java',
                           'core/src/main/scala/kafka/raft/KafkaMetadataLog.scala',
                           'core/src/test/scala/kafka/raft/KafkaMetadataLogTest.scala'])
    contents = {}
    with tarfile.open(original, 'r:gz') as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                if name in contents or not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicated selected source member: ' + name)
                contents[name] = archive.extractfile(member).read()
    if set(contents) != set(selected):
        raise ValueError('Missing selected sources: ' + str(set(selected) - set(contents)))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(contents.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode, member.mtime = len(data), 0o644, 0
            archive.addfile(member, io.BytesIO(data))
    destination.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(contents.items())}

def extract_jars(distribution, version, directory):
    names = ['kafka-clients-' + version + '.jar', 'kafka-raft-' + version + '.jar',
             'kafka-server-common-' + version + '.jar', 'kafka-storage-' + version + '.jar',
             'kafka-storage-api-' + version + '.jar', 'metrics-core-2.2.0.jar', 'slf4j-api-1.7.36.jar']
    if version != '4.3.1':
        names += ['kafka_2.13-' + version + '.jar',
                  'kafka-server-' + version + '.jar',
                  'log4j-api-2.25.' + ('3' if version == '4.1.2' else '4') + '.jar',
                  'log4j-core-2.25.' + ('3' if version == '4.1.2' else '4') + '.jar',
                  'commons-validator-1.10.1.jar',
                  'scala-library-2.13.' + ('16' if version == '4.1.2' else '17') + '.jar',
                  'scala-logging_2.13-' + ('3.9.5' if version == '4.1.2' else '3.9.6') + '.jar']
    directory.mkdir(parents=True, exist_ok=True)
    found = {}
    with tarfile.open(distribution, 'r:gz') as archive:
        for member in archive:
            if member.name == 'kafka_2.13-' + version + '/libs/' + Path(member.name).name and Path(member.name).name in names:
                filename = Path(member.name).name
                if filename in found or not member.isfile() or member.size > 32 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicated jar: ' + filename)
                target = directory / filename
                target.write_bytes(archive.extractfile(member).read())
                found[filename] = {'distribution_member': member.name, 'bytes': member.size, 'sha256': digest(target)}
    if set(found) != set(names):
        raise ValueError('Missing official jars: ' + str(set(names) - set(found)))
    return found

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    pins = json.loads((HERE / 'pins.json').read_text())
    report = {'schema_version': 1, 'scope': 'Official Apache LeaderState/FollowerState and real local KafkaRaftLog components; no partitionline73 execution, multi-node durable-history or Kafka network claim', 'passed': False, 'probe_sha256': digest(HERE / 'ApacheQuorumProbe.java'), 'runner_sha256': digest(Path(__file__)), 'pins_sha256': digest(HERE / 'pins.json'), 'releases': [], 'commands': []}
    def checkpoint():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    def execute(argv, label, expected=0):
        started = time.time()
        command = ['taskset', '-c', '0-2,4'] + argv
        stdout, stderr = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        with stdout.open('wb') as out, stderr.open('wb') as err:
            completed = subprocess.run(command, cwd=args.work, stdout=out, stderr=err, timeout=60)
        accepted = completed.returncode == 0 if expected == 0 else completed.returncode != 0
        report['commands'].append({'argv': command, 'cwd': str(args.work), 'exit_code': completed.returncode, 'expected': 'zero' if expected == 0 else 'nonzero deliberate failing assertion', 'accepted': accepted, 'elapsed_seconds': round(time.time() - started, 3), 'stdout': stdout.name, 'stdout_sha256': digest(stdout), 'stderr': stderr.name, 'stderr_sha256': digest(stderr)})
        checkpoint()
        if not accepted:
            raise RuntimeError('Unexpected command result: ' + label)
        return stdout, stderr
    try:
        java_version, _ = execute(['java', '-version'], 'java-version')
        canonical = None
        for pin in pins['releases']:
            version = pin['release']
            original = args.source_archives / (version + '.tar.gz')
            distribution = args.distributions / ('kafka_2.13-' + version + '.tgz')
            assert digest(original) == pin['source_archive_sha256'], 'Source archive mismatch'
            assert digest(distribution) == pin['distribution_sha256'], 'Distribution SHA256 mismatch'
            assert digest(distribution, 'sha512') == pin['distribution_sha512'], 'Official distribution SHA512 mismatch'
            checksum_text = (args.distributions / (distribution.name + '.sha512')).read_text()
            official_checksum = ''.join(checksum_text.split(':', 1)[-1].split()).lower()
            assert official_checksum == pin['distribution_sha512'], 'Official checksum-file mismatch'
            retained = args.output / ('apache-components-' + version + '.tar.gz')
            selected = retained_source(original, retained, version)
            jar_dir = args.work / 'jars' / version
            jars = extract_jars(distribution, version, jar_dir)
            assert jars['kafka-clients-' + version + '.jar']['sha256'] == pin['client_jar_sha256']
            with zipfile.ZipFile(jar_dir / ('kafka-clients-' + version + '.jar')) as jar:
                properties = jar.read('kafka/kafka-version.properties').decode()
                assert ('version=' + version) in properties and ('commitId=' + pin['source_sha'][:16]) in properties
            row = {'release': version, 'source_sha': pin['source_sha'], 'source_archive_sha256': digest(original), 'distribution_sha256': digest(distribution), 'distribution_sha512': digest(distribution, 'sha512'), 'embedded_client_version_properties': properties, 'retained_archive': retained.name, 'retained_archive_sha256': digest(retained), 'retained_files_sha256': selected, 'jars': jars}
            report['releases'].append(row)
            classes = args.work / 'classes' / version
            classes.mkdir(parents=True, exist_ok=True)
            classpath = ':'.join(str(jar_dir / name) for name in sorted(jars))
            adapted = args.work / 'adapted' / version / 'ApacheQuorumProbe.java'
            adapted.parent.mkdir(parents=True, exist_ok=True)
            java_source = (HERE / 'ApacheQuorumProbe.java').read_text()
            adaptations = []
            if version != '4.3.1':
                changes = [('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.'),
                           ('org.apache.kafka.raft.internals.KafkaRaftLog', 'kafka.raft.KafkaMetadataLog'),
                           ('KafkaRaftLog', 'KafkaMetadataLog'),
                           ('KafkaMetadataLog.createLog(', 'KafkaMetadataLog.apply('),
                           ('log.read(0, isolation, 4096)', 'log.read(0, isolation)')]
                for before, after in changes:
                    count = java_source.count(before)
                    if not count:
                        raise ValueError('Missing reviewed API adaptation: ' + before)
                    java_source = java_source.replace(before, after)
                    adaptations.append({'from': before, 'to': after, 'occurrences': count})
            adapted.write_text(java_source)
            row['api_adaptations'] = adaptations
            row['adapted_source_sha256'] = digest(adapted)
            retained_adapter = args.output / ('ApacheQuorumProbe-' + version + '.adapted.java')
            retained_adapter.write_text(java_source)
            row['adapted_source_file'] = retained_adapter.name
            execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', classpath, '-d', str(classes), str(adapted)], version + '-compile')
            row['compiled_class_sha256'] = {str(p.relative_to(classes)): digest(p) for p in sorted(classes.rglob('*.class'))}
            histories = []
            for replay in [1, 2]:
                work = args.work / ('log-' + version + '-replay-' + str(replay))
                stdout, _ = execute(['java', '-cp', str(classes) + ':' + classpath, 'org.apache.kafka.raft.ApacheQuorumProbe', str(work)], version + '-replay-' + str(replay))
                observed = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
                assert observed[-1]['summary'] and observed[-1]['passed']
                assert all(item['passed'] for item in observed)
                assert observed[-1]['checks'] == len(observed) - 1
                histories.append(observed)
            assert histories[0] == histories[1], 'Same-release assertions did not replay identically'
            if canonical is None:
                canonical = histories[0]
            assert canonical == histories[0], 'Cross-release component outcomes differ'
            stdout, stderr = execute(['java', '-cp', str(classes) + ':' + classpath, 'org.apache.kafka.raft.ApacheQuorumProbe', str(args.work / ('negative-' + version)), '--wrong-barrier'], version + '-deliberate-wrong-barrier', expected=1)
            negative = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
            assert negative[-1] == {'case': 'one.current-epoch-barrier', 'actual': -1, 'expected': 10, 'passed': False}
            assert 'AssertionError' in stderr.read_text()
            row['positive_checks_per_replay'] = len(histories[0]) - 1
            row['positive_replays'] = 2
            row['deliberate_failing_assertion_detected'] = True
            checkpoint()
        report['passed'] = True
        report['positive_component_assertions'] = sum(row['positive_checks_per_replay'] * 2 for row in report['releases'])
        report['deliberate_negative_executions'] = 3
        report['limitations'] = ['LeaderState component inputs are not typed request/ACK transport histories;73 runtime must independently reject unmatched, stale, future and over-end ACKs.', 'Follower high-watermark monotonicity and real local truncation are component behavior. Apache local log reopening resets this component high-watermark; replicated durable commit needs separate causal/WAL evidence.', 'No official Apache broker/quorum network process or partitionline73 runtime is exercised.', 'Intentional wrong-barrier variant changes only probe expectation, leaving Apache libraries untouched.']
        checkpoint()
        print(json.dumps({'passed': True, 'component_assertions': report['positive_component_assertions'], 'negative_executions': 3}))
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
        checkpoint()
        raise

if __name__ == '__main__':
    main()
