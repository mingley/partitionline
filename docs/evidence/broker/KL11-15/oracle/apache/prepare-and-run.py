#!/usr/bin/env python3
"""Run independently pinned Apache snapshot components with retained provenance."""
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


def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


def selected_archive(original, output):
    selected = set(json.loads((HERE / 'selected-source-files.json').read_text())['files'])
    contents = {}
    with tarfile.open(original, 'r:gz') as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                if name in contents or not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicate retained source: ' + name)
                contents[name] = archive.extractfile(member).read()
    if set(contents) != selected:
        raise ValueError('Missing retained source: ' + str(selected - set(contents)))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(contents.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode, member.mtime = len(data), 0o644, 0
            archive.addfile(member, io.BytesIO(data))
    output.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(contents.items())}


def extract_jars(distribution, version, directory):
    names = {'kafka-clients-' + version + '.jar', 'kafka-raft-' + version + '.jar',
             'kafka-server-common-' + version + '.jar', 'slf4j-api-1.7.36.jar'}
    directory.mkdir(parents=True)
    found = {}
    with tarfile.open(distribution, 'r:gz') as archive:
        for member in archive:
            name = Path(member.name).name
            if name in names and member.name == 'kafka_2.13-' + version + '/libs/' + name:
                if name in found or not member.isfile() or member.size > 32 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicate official jar: ' + name)
                path = directory / name
                path.write_bytes(archive.extractfile(member).read())
                found[name] = {'distribution_member': member.name, 'bytes': member.size,
                               'sha256': digest(path)}
    if set(found) != names:
        raise ValueError('Missing official jar: ' + str(names - set(found)))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    args.output.mkdir(parents=True)
    report = {'schema_version': 1, 'scope': 'Official Apache snapshot components; no partitionline, durable quorum or wire interoperability claim',
              'passed': False, 'probe_sha256': digest(HERE / 'ApacheSnapshotProbe.java'),
              'runner_sha256': digest(Path(__file__)), 'pins_sha256': digest(HERE / 'pins.json'),
              'source_selection_sha256': digest(HERE / 'selected-source-files.json'),
              'commands': [], 'releases': []}

    def checkpoint():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')

    def execute(argv, label, negative=False):
        started = time.time()
        command = ['taskset', '-c', '0-2,4'] + argv
        stdout, stderr = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        with stdout.open('wb') as out, stderr.open('wb') as err:
            completed = subprocess.run(command, cwd=args.work, stdout=out, stderr=err, timeout=60)
        accepted = completed.returncode != 0 if negative else completed.returncode == 0
        report['commands'].append({'argv': command, 'cwd': str(args.work), 'exit_code': completed.returncode,
                                   'expected': 'deliberate nonzero assertion' if negative else 'zero',
                                   'accepted': accepted, 'elapsed_seconds': round(time.time() - started, 3),
                                   'stdout': stdout.name, 'stdout_sha256': digest(stdout),
                                   'stderr': stderr.name, 'stderr_sha256': digest(stderr)})
        checkpoint()
        if not accepted:
            raise RuntimeError('Unexpected command outcome: ' + label)
        return stdout, stderr

    try:
        execute(['java', '-version'], 'java-version')
        canonical = None
        for pin in json.loads((HERE / 'pins.json').read_text())['releases']:
            version = pin['release']
            original = args.source_archives / (version + '.tar.gz')
            distribution = args.distributions / ('kafka_2.13-' + version + '.tgz')
            if digest(original) != pin['source_archive_sha256']:
                raise ValueError('Source archive mismatch')
            checksum = ''.join((args.distributions / (distribution.name + '.sha512')).read_text().split(':', 1)[-1].split()).lower()
            if (digest(distribution) != pin['distribution_sha256']
                    or digest(distribution, 'sha512') != pin['distribution_sha512']
                    or checksum != pin['distribution_sha512']):
                raise ValueError('Official Apache distribution mismatch')
            retained = args.output / ('apache-components-' + version + '.tar.gz')
            source_files = selected_archive(original, retained)
            jar_dir = args.work / 'jars' / version
            jars = extract_jars(distribution, version, jar_dir)
            if jars['kafka-clients-' + version + '.jar']['sha256'] != pin['client_jar_sha256']:
                raise ValueError('Client JAR mismatch')
            with zipfile.ZipFile(jar_dir / ('kafka-clients-' + version + '.jar')) as jar:
                properties = jar.read('kafka/kafka-version.properties').decode()
                if ('version=' + version) not in properties or ('commitId=' + pin['source_sha'][:16]) not in properties:
                    raise ValueError('Embedded source/version mismatch')
            row = {'release': version, 'source_sha': pin['source_sha'],
                   'source_archive_sha256': digest(original), 'distribution_sha256': digest(distribution),
                   'distribution_sha512': digest(distribution, 'sha512'), 'embedded_client_version_properties': properties,
                   'retained_archive': retained.name, 'retained_archive_sha256': digest(retained),
                   'retained_files_sha256': source_files, 'jars': jars}
            report['releases'].append(row)
            source = (HERE / 'ApacheSnapshotProbe.java').read_text()
            changes = []
            if version != '4.3.1':
                changes.append(('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.'))
            if version == '4.1.2':
                changes.append(('.setMaxBatchSizeBytes(', '.setMaxBatchSize('))
            row['api_adaptations'] = []
            for before, after in changes:
                occurrences = source.count(before)
                if not occurrences:
                    raise ValueError('Missing reviewed API adaptation: ' + before)
                source = source.replace(before, after)
                row['api_adaptations'].append({'from': before, 'to': after, 'occurrences': occurrences})
            adapted = args.work / 'adapted' / version / 'ApacheSnapshotProbe.java'
            adapted.parent.mkdir(parents=True)
            adapted.write_text(source)
            retained_adapted = args.output / ('ApacheSnapshotProbe-' + version + '.adapted.java')
            retained_adapted.write_text(source)
            row['adapted_source_file'] = retained_adapted.name
            row['adapted_source_sha256'] = digest(retained_adapted)
            classes = args.work / 'classes' / version
            classes.mkdir(parents=True)
            classpath = ':'.join(str(jar_dir / name) for name in sorted(jars))
            execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror',
                     '-cp', classpath, '-d', str(classes), str(adapted)], version + '-compile')
            row['compiled_class_sha256'] = {str(p.relative_to(classes)): digest(p) for p in sorted(classes.rglob('*.class'))}
            histories = []
            for replay in [1, 2]:
                history_dir = args.work / ('snapshot-' + version + '-replay-' + str(replay))
                stdout, _ = execute(['java', '-cp', str(classes) + ':' + classpath, 'ApacheSnapshotProbe', str(history_dir)],
                                    version + '-replay-' + str(replay))
                observed = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
                summary = observed[-1]
                assertions = [item for item in observed if 'passed' in item and not item.get('summary')]
                if not summary.get('summary') or not summary['passed'] or summary['checks'] != len(assertions) or not all(item['passed'] for item in assertions):
                    raise ValueError('Invalid component assertions')
                histories.append(observed)
                snapshot = history_dir / '00000000000000000003-0000000005.checkpoint'
                snapshot_copy = args.output / (version + '-replay-' + str(replay) + '.snapshot.bin')
                snapshot_copy.write_bytes(snapshot.read_bytes())
                row.setdefault('snapshot_files', []).append({'file': snapshot_copy.name, 'bytes': snapshot_copy.stat().st_size,
                                                             'sha256': digest(snapshot_copy)})
            if histories[0] != histories[1] or canonical is not None and canonical != histories[0]:
                raise ValueError('Component outcomes differ between identical replays/releases')
            canonical = histories[0]
            row['canonical_observation'] = next(item for item in histories[0] if item.get('observation'))
            stdout, stderr = execute(['java', '-cp', str(classes) + ':' + classpath, 'ApacheSnapshotProbe',
                                      str(args.work / ('negative-' + version)), '--wrong-offset'],
                                     version + '-deliberate-wrong-offset', negative=True)
            negative = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
            if negative[-1] != {'case': 'writer.exclusive-end-maps-inclusive-offset', 'actual': 2, 'expected': 3, 'passed': False} or 'AssertionError' not in stderr.read_text():
                raise ValueError('Deliberate incorrect assertion was not identified')
            row['positive_checks_per_replay'] = len(assertions)
            row['positive_replays'] = 2
            row['deliberate_failing_assertion_detected'] = True
        report['positive_checks'] = sum(row['positive_checks_per_replay'] * row['positive_replays'] for row in report['releases'])
        report['deliberate_failures_detected'] = len(report['releases'])
        observation = next(item for item in canonical if item.get('observation'))
        contained = next(item['actual'] for item in canonical if item.get('case') == 'reader.last-contained-offset')
        lines = ['exclusive_offset\t' + str(observation['exclusive_offset']),
                 'epoch\t' + str(observation['epoch']), 'last_contained_offset\t' + str(contained)]
        lines += ['record_hex\t' + record.encode().hex() for record in observation['records_utf8'].split(';') if record]
        fixture = args.output / 'opaque-state.tsv'
        fixture.write_text('\n'.join(lines) + '\n')
        report['opaque_state_fixture'] = {'file': fixture.name, 'sha256': digest(fixture),
                                        'scope': 'actual opaque record order and component boundary mapping, not equivalent serialization or replicated application state'}
        report['passed'] = True
    except Exception as error:
        report['failure'] = {'class': type(error).__name__, 'message': str(error)}
        checkpoint()
        raise
    checkpoint()


if __name__ == '__main__':
    main()
