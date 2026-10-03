#!/usr/bin/env python3
"""Run pinned Apache UnifiedLog components and preserve independent outcomes."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import zipfile

HERE = Path(__file__).resolve().parent
CONTROL_CASES = {
    '--wrong-floor': ('floor.logical', '2', '1'),
    '--wrong-age': ('age.strict-equality-keeps', '0', '1'),
    '--wrong-size': ('size.target-79-whole-removal', '0', '1'),
    '--wrong-unknown-age': ('unknown.apache-mtime-one-ms-over-deletes', '1', '0'),
}


def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


def write_archive(output, contents):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(contents.items()):
            item = tarfile.TarInfo(name)
            item.size, item.mode, item.mtime = len(data), 0o644, 0
            archive.addfile(item, io.BytesIO(data))
    output.write_bytes(gzip.compress(raw.getvalue(), mtime=0))


def selected_archive(original, version, output):
    selection = json.loads((HERE / 'selected-source-files.json').read_text())
    prefix = selection['record_path_4_3' if version == '4.3.1' else 'record_path_older']
    selected = set(selection['files']) | {prefix + name + '.java' for name in selection['record_files']}
    contents = {}
    with tarfile.open(original, 'r:gz') as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                if name in contents or not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicate source: ' + name)
                contents[name] = archive.extractfile(member).read()
    if set(contents) != selected:
        raise ValueError('Missing source: ' + str(selected - set(contents)))
    write_archive(output, contents)
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(contents.items())}


def extract_jars(distribution, version, directory):
    names = {'kafka-' + name + '-' + version + '.jar'
             for name in ['clients', 'storage', 'storage-api', 'server-common']}
    names |= {'metrics-core-2.2.0.jar', 'slf4j-api-1.7.36.jar'}
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


def capture_history(directory, output):
    contents, observations = {}, {}
    for path in sorted(directory.rglob('*')):
        if path.is_dir():
            continue
        if not path.is_file() or path.is_symlink() or len(contents) >= 192 or path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError('Unbounded component history')
        name = str(path.relative_to(directory))
        contents[name] = path.read_bytes()
        observations[name] = {'bytes': path.stat().st_size, 'sha256': digest(path)}
    write_archive(output, contents)
    return observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seed-root', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    args.output.mkdir(parents=True)
    report = {
        'schema_version': 1, 'passed': False,
        'scope': 'Actual official UnifiedLog/LogSegment components; no full Kafka broker or partitionline durability qualification',
        'probe_sha256': digest(HERE / 'ApacheRetentionProbe.java'),
        'runner_sha256': digest(Path(__file__)), 'pins_sha256': digest(HERE / 'pins.json'),
        'source_selection_sha256': digest(HERE / 'selected-source-files.json'),
        'commands': [], 'releases': [],
        'known_policy_differences': [
            'Apache does not write the logical-floor checkpoint immediately; supplied checkpoint replay is distinct from local synchronous V2 manifest durability.',
            'Apache NO_TIMESTAMP age falls back to mutable file mtime; local policy retains all-negative/unknown-time segments under age.',
            'Apache timestamp search stops at the first historically qualifying segment even when its qualifying record is below floor; local bounded full scan can find a later retained match.',
        ],
    }

    def checkpoint():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')

    def execute(argv, label, negative=False):
        started = time.time()
        command = ['taskset', '-c', '0-2,4'] + argv
        stdout, stderr = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        with stdout.open('wb') as out, stderr.open('wb') as err:
            completed = subprocess.run(command, cwd=args.work, stdout=out, stderr=err, timeout=60)
        accepted = completed.returncode != 0 if negative else completed.returncode == 0
        report['commands'].append({
            'argv': command, 'cwd': str(args.work), 'exit_code': completed.returncode,
            'expected': 'named deliberate assertion failure' if negative else 'zero', 'accepted': accepted,
            'elapsed_seconds': round(time.time() - started, 3),
            'stdout': stdout.name, 'stdout_sha256': digest(stdout),
            'stderr': stderr.name, 'stderr_sha256': digest(stderr),
        })
        checkpoint()
        if not accepted:
            raise RuntimeError('Unexpected command outcome: ' + label)
        return stdout, stderr

    try:
        execute(['java', '-version'], 'java-version')
        canonical = None
        canonical_seeds = None
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
            source_files = selected_archive(original, version, retained)
            jar_dir = args.work / 'jars' / version
            jars = extract_jars(distribution, version, jar_dir)
            if jars['kafka-clients-' + version + '.jar']['sha256'] != pin['client_jar_sha256']:
                raise ValueError('Client JAR mismatch')
            with zipfile.ZipFile(jar_dir / ('kafka-clients-' + version + '.jar')) as jar:
                properties = jar.read('kafka/kafka-version.properties').decode()
                if ('version=' + version) not in properties or ('commitId=' + pin['source_sha'][:16]) not in properties:
                    raise ValueError('Embedded source/version mismatch')
            seeds = args.work / 'seeds' / version
            seeds.mkdir(parents=True)
            seed_files = {}
            for name, expected_bytes in [('log-batch-0.bin', 104), ('log-batch-3.bin', 78)]:
                source = args.seed_root / version / name
                if source.stat().st_size != expected_bytes:
                    raise ValueError('Unexpected independently produced batch size')
                shutil.copyfile(source, seeds / name)
                shutil.copyfile(source, args.output / (version + '-' + name))
                seed_files[name] = digest(source)
            if canonical_seeds is not None and seed_files != canonical_seeds:
                raise ValueError('Input batches differ across releases')
            canonical_seeds = seed_files
            row = {'release': version, 'source_sha': pin['source_sha'],
                   'source_archive_sha256': digest(original), 'distribution_sha256': digest(distribution),
                   'distribution_sha512': digest(distribution, 'sha512'), 'embedded_client_version_properties': properties,
                   'retained_archive': retained.name, 'retained_archive_sha256': digest(retained),
                   'retained_files_sha256': source_files, 'jars': jars, 'seeds_sha256': seed_files}
            report['releases'].append(row)
            source = (HERE / 'ApacheRetentionProbe.java').read_text()
            row['api_adaptations'] = []
            if version != '4.3.1':
                before, after = 'org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.'
                occurrences = source.count(before)
                if occurrences != 3:
                    raise ValueError('Unexpected reviewed record-package adaptation count')
                source = source.replace(before, after)
                row['api_adaptations'].append({'from': before, 'to': after, 'occurrences': occurrences})
            adapted = args.work / 'adapted' / version / 'ApacheRetentionProbe.java'
            adapted.parent.mkdir(parents=True)
            adapted.write_text(source)
            retained_adapted = args.output / ('ApacheRetentionProbe-' + version + '.adapted.java')
            retained_adapted.write_text(source)
            row['adapted_source_file'] = retained_adapted.name
            row['adapted_source_sha256'] = digest(retained_adapted)
            classes = args.work / 'classes' / version
            classes.mkdir(parents=True)
            classpath = ':'.join(str(jar_dir / name) for name in sorted(jars))
            execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror',
                     '-cp', classpath, '-d', str(classes), str(adapted)], version + '-compile')
            row['compiled_class_sha256'] = {str(path.relative_to(classes)): digest(path)
                                            for path in sorted(classes.rglob('*.class'))}
            histories = []
            for replay in [1, 2]:
                directory = args.work / ('retention-' + version + '-replay-' + str(replay))
                stdout, _ = execute(['java', '-Xmx128m', '-cp', str(classes) + ':' + classpath,
                                     'ApacheRetentionProbe', str(directory), str(seeds)],
                                    version + '-replay-' + str(replay))
                observed = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
                summary, assertions = observed[-1], observed[:-1]
                if (summary != {'summary': True, 'passed': True, 'checks': 67}
                        or len(assertions) != 67 or len({item['case'] for item in assertions}) != 67
                        or not all(item['passed'] is True for item in assertions)):
                    raise ValueError('Incomplete or invalid actual component assertions')
                histories.append(observed)
                capture = args.output / (version + '-replay-' + str(replay) + '.history.tar.gz')
                files = capture_history(directory, capture)
                row.setdefault('raw_histories', []).append({'file': capture.name, 'sha256': digest(capture),
                                                           'files': files})
            if histories[0] != histories[1] or canonical is not None and canonical != histories[0]:
                raise ValueError('Actual component results differ across identical replays/releases')
            canonical = histories[0]
            row['deliberate_failures'] = []
            for option, (case, actual, expected) in CONTROL_CASES.items():
                stdout, stderr = execute(['java', '-Xmx128m', '-cp', str(classes) + ':' + classpath,
                                          'ApacheRetentionProbe', str(args.work / ('negative-' + version + '-' + option[2:])),
                                          str(seeds), option], version + '-' + option[2:], negative=True)
                observed = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
                if (observed[-1] != {'case': case, 'actual': actual, 'expected': expected, 'passed': False}
                        or any(item['passed'] is not True for item in observed[:-1])
                        or 'java.lang.AssertionError: ' + case not in stderr.read_text()):
                    raise ValueError('Incorrect or unrelated deliberate failing assertion')
                row['deliberate_failures'].append({'option': option, 'case': case, 'actual': actual, 'incorrect_expected': expected})
            row['positive_checks_per_replay'], row['positive_replays'] = 67, 2
        report['positive_checks'] = sum(row['positive_checks_per_replay'] * row['positive_replays'] for row in report['releases'])
        report['deliberate_failures_detected'] = sum(len(row['deliberate_failures']) for row in report['releases'])
        report['assertions_identical_across_replays_and_releases'] = True
        report['passed'] = True
    except Exception as error:
        report['failure'] = {'class': type(error).__name__, 'message': str(error)}
        checkpoint()
        raise
    checkpoint()


if __name__ == '__main__':
    main()
