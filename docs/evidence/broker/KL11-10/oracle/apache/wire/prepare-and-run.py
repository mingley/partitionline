#!/usr/bin/env python3
"""Independently generate and replay pinned Apache DeleteRecords wire frames."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import time
import zipfile

HERE = Path(__file__).resolve().parent


def sha(path, algorithm='sha256'):
    value = hashlib.new(algorithm)
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def archive_sources(original, output):
    selected = set(json.loads((HERE / 'reference-files.json').read_text())['files'])
    contents = {}
    with tarfile.open(original, 'r:gz') as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                if name in contents or not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise ValueError('Unsafe source member')
                contents[name] = archive.extractfile(member).read()
    if set(contents) != selected:
        raise ValueError('Missing source: ' + str(selected - set(contents)))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(contents.items()):
            item = tarfile.TarInfo(name)
            item.size, item.mode, item.mtime = len(data), 0o644, 0
            archive.addfile(item, io.BytesIO(data))
    output.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(contents.items())}


def jars(distribution, version, directory):
    directory.mkdir(parents=True)
    wanted = {'kafka-clients-' + version + '.jar', 'slf4j-api-1.7.36.jar'}
    result = {}
    with tarfile.open(distribution, 'r:gz') as archive:
        for member in archive:
            name = Path(member.name).name
            if name in wanted and member.name == 'kafka_2.13-' + version + '/libs/' + name:
                if not member.isfile() or member.size > 32 * 1024 * 1024 or name in result:
                    raise ValueError('Unsafe official JAR')
                path = directory / name
                path.write_bytes(archive.extractfile(member).read())
                result[name] = {'member': member.name, 'bytes': member.size, 'sha256': sha(path)}
    if set(result) != wanted:
        raise ValueError('Missing official JAR')
    return result


def file_hashes(directory):
    return {str(path.relative_to(directory)): sha(path) for path in sorted(directory.rglob('*')) if path.is_file()}


def check_corpus(directory):
    manifest = json.loads((directory / 'goldens.json').read_text())
    cases = manifest['cases']
    if len(cases) != 121 or len({row['name'] for row in cases}) != 121:
        raise ValueError('Wrong case identities')
    table = ''.join('\t'.join([row['name'], '21', str(row['api_version']), row['seed'], row['expected_outcome']]) + '\n'
                    for row in cases)
    if table != (directory / 'cases.tsv').read_text():
        raise ValueError('TSV/manifest mismatch')
    if sum(row['expected_outcome'] == 'response' for row in cases) != 115:
        raise ValueError('Wrong response count')
    for row in cases:
        if (not re.fullmatch(r'delete-records-v[012]-[a-z0-9-]+', row['name'])
                or row['api_key'] != 21 or row['api_version'] not in [0, 1, 2]
                or not row['name'].startswith('delete-records-v' + str(row['api_version']) + '-')
                or row['seed'] not in ['fixture', 'fixture_floor_2', 'fixture_empty']
                or row['expected_outcome'] not in ['response', 'reject']):
            raise ValueError('Unsafe or mismatched case identity')
        for role in ['request', 'response']:
            raw = row[role + '_hex']
            path = directory / (row['name'] + '.' + role + '.bin')
            if raw is None:
                if path.exists() or row[role + '_sha256'] is not None:
                    raise ValueError('Unexpected rejection response')
            else:
                if bytes.fromhex(raw) != path.read_bytes() or sha(path) != row[role + '_sha256'] or path.stat().st_size > 128 * 1024:
                    raise ValueError('Raw frame mismatch or budget')
        if row['request_header_version'] != (2 if row['api_version'] == 2 else 1):
            raise ValueError('Incorrect request header layout')
        if row['response_header_version'] != (1 if row['api_version'] == 2 else 0):
            raise ValueError('Incorrect response header layout')
        parsed = row['apache_request_parse']
        if row['expected_outcome'] == 'response':
            if not parsed['accepted'] or parsed['remaining_bytes'] != 0 or parsed['correlation_id'] != 7:
                raise ValueError('Positive official parse incomplete')
        elif row['name'].endswith('trailing-zero'):
            if not parsed['accepted'] or parsed['remaining_bytes'] != 1:
                raise ValueError('Missing retained Apache trailing remainder')
        elif row['name'].endswith('truncated'):
            if parsed['accepted']:
                raise ValueError('Truncated official parser unexpectedly accepted')
        else:
            raise ValueError('Unreviewed negative case')
    if len(manifest['apache_global_error_helpers']) != 12 or manifest['actual_serializer_parser_checks'] != 248:
        raise ValueError('Incomplete actual helper/parser execution')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    args.output.mkdir(parents=True)
    report = {'schema_version': 1, 'passed': False, 'generator_sha256': sha(HERE / 'DeleteRecordsGoldens.java'),
              'runner_sha256': sha(Path(__file__)), 'pins_sha256': sha(HERE / 'pins.json'),
              'scope': 'Actual official serializers/parsers and error helpers; independently declared local ordinary RF1 success/error policy, no full Apache controller or partitionline runtime.',
              'commands': [], 'releases': []}

    def save():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')

    def run(argv, label):
        command = ['taskset', '-c', '0-2,4'] + argv
        started = time.time()
        out, err = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        with out.open('wb') as stdout, err.open('wb') as stderr:
            result = subprocess.run(command, cwd=args.work, stdout=stdout, stderr=stderr, timeout=60)
        report['commands'].append({'argv': command, 'cwd': str(args.work), 'exit_code': result.returncode,
                                   'elapsed_seconds': round(time.time() - started, 3),
                                   'stdout': out.name, 'stdout_sha256': sha(out), 'stderr': err.name, 'stderr_sha256': sha(err)})
        save()
        if result.returncode:
            raise RuntimeError('Unexpected command failure: ' + label)
        return out

    try:
        run(['java', '-version'], 'java-version')
        canonical = None
        for pin in json.loads((HERE / 'pins.json').read_text())['releases']:
            version = pin['release']
            original = args.source_archives / (version + '.tar.gz')
            distribution = args.distributions / ('kafka_2.13-' + version + '.tgz')
            checksum = ''.join((args.distributions / (distribution.name + '.sha512')).read_text().split(':', 1)[-1].split()).lower()
            if (sha(original) != pin['source_archive_sha256'] or sha(distribution) != pin['distribution_sha256']
                    or sha(distribution, 'sha512') != pin['distribution_sha512'] or checksum != pin['distribution_sha512']):
                raise ValueError('Pinned official source/distribution mismatch')
            retained = args.output / ('apache-wire-sources-' + version + '.tar.gz')
            references = archive_sources(original, retained)
            jar_dir = args.work / 'jars' / version
            jar_map = jars(distribution, version, jar_dir)
            if jar_map['kafka-clients-' + version + '.jar']['sha256'] != pin['client_jar_sha256']:
                raise ValueError('Client JAR pin mismatch')
            with zipfile.ZipFile(jar_dir / ('kafka-clients-' + version + '.jar')) as archive:
                properties = archive.read('kafka/kafka-version.properties').decode()
                if ('version=' + version) not in properties or ('commitId=' + pin['source_sha'][:16]) not in properties:
                    raise ValueError('Embedded source version mismatch')
            classes = args.work / 'classes' / version
            classes.mkdir(parents=True)
            classpath = ':'.join(str(jar_dir / name) for name in sorted(jar_map))
            run(['java', '-Xmx128m', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror',
                 '-cp', classpath, '-d', str(classes), str(HERE / 'DeleteRecordsGoldens.java')], version + '-compile')
            row = {'release': version, 'source_sha': pin['source_sha'], 'jars': jar_map,
                   'embedded_client_version_properties': properties, 'source_files_sha256': references,
                   'retained_source_archive': retained.name, 'retained_source_archive_sha256': sha(retained),
                   'compiled_classes_sha256': file_hashes(classes), 'api_adaptations': [], 'replays': []}
            report['releases'].append(row)
            replays = []
            for replay in [1, 2]:
                directory = args.work / ('replay-' + str(replay)) / version
                stdout = run(['java', '-Xmx128m', '-cp', str(classes) + ':' + classpath, 'DeleteRecordsGoldens',
                              str(directory), version], version + '-replay-' + str(replay))
                expected = {'passed': True, 'cases': 121, 'checks': 248, 'global_error_helpers': 12}
                if json.loads(stdout.read_text()) != expected:
                    raise ValueError('Unexpected actual generator counts')
                manifest = check_corpus(directory)
                hashes = file_hashes(directory)
                replays.append(hashes)
                row['replays'].append({'replay': replay, 'files_sha256': hashes, 'actual_checks': 248})
                if canonical is not None and manifest['cases'] != canonical:
                    raise ValueError('Case frames/parser observations differ across SDKs/replays')
                canonical = manifest['cases']
            if replays[0] != replays[1]:
                raise ValueError('Identical history replay output changed')
            shutil.copytree(args.work / 'replay-1' / version, args.output / version)
            row['corpus'] = {'directory': version, 'cases': 121, 'responses': 115, 'structural_rejections': 6,
                             'actual_global_error_helpers': 12}
        report['declared_cases'] = 363
        report['response_goldens'] = 345
        report['local_structural_rejections'] = 18
        report['actual_serializer_parser_checks'] = 1488
        report['actual_global_error_helper_executions'] = 72
        report['identical_case_bytes_and_parser_outcomes_across_releases'] = True
        report['passed'] = True
    except Exception as failure:
        report['failure'] = {'class': type(failure).__name__, 'message': str(failure)}
        save()
        raise
    save()


if __name__ == '__main__':
    main()
