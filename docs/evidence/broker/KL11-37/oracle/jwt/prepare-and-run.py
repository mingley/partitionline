#!/usr/bin/env python3
"""Run public synthetic JWTs through three pinned official Apache validators."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import time
import zipfile

HERE = Path(__file__).resolve().parent

def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with path.open('rb') as stream:
        while value := stream.read(1024 * 1024):
            result.update(value)
    return result.hexdigest()

def retain_source(original, output):
    selected = set(json.loads((HERE / 'selected-source-files.json').read_text())['files'])
    content = {}
    with tarfile.open(original) as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                assert member.isfile() and member.size <= 1024 * 1024 and name not in content
                content[name] = archive.extractfile(member).read()
    assert set(content) == selected
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(content.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode, member.mtime = len(data), 0o644, 0
            archive.addfile(member, io.BytesIO(data))
    output.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(content.items())}

def extract_jars(distribution, version, target):
    target.mkdir(parents=True)
    patterns = [r'kafka-clients-' + re.escape(version) + r'\.jar', r'jose4j-[0-9.]+\.jar',
        r'jackson-databind-[0-9.]+\.jar', r'jackson-core-[0-9.]+\.jar', r'jackson-annotations-[0-9.]+\.jar', r'slf4j-api-[0-9.]+\.jar']
    found = {}
    with tarfile.open(distribution) as archive:
        for member in archive:
            name = Path(member.name).name
            matched = [i for i, pattern in enumerate(patterns) if re.fullmatch(pattern, name)]
            if matched:
                assert member.name == 'kafka_2.13-' + version + '/libs/' + name
                assert member.isfile() and member.size <= 32 * 1024 * 1024 and matched[0] not in found
                path = target / name
                path.write_bytes(archive.extractfile(member).read())
                found[matched[0]] = {'file': name, 'distribution_member': member.name, 'bytes': member.size, 'sha256': digest(path)}
    assert len(found) == len(patterns), found
    return list(found.values())

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--private-work', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    args.output.mkdir(parents=True)
    report = {'schema_version': 1, 'passed': False,
        'scope': 'Actual public configured BrokerJwtValidator and DefaultJwtValidator using the official VerificationKeyResolverFactory/file resolver; no adapted validator or private clock, no live IdP/socket claim.',
        'source_files_sha256': {name: digest(HERE / name) for name in ['ApacheJwtProbe.java', 'prepare-and-run.py', 'generate-fixtures.py', 'pins.json', 'selected-source-files.json']},
        'commands': [], 'releases': []}
    def save():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    def execute(argv, label, negative=False):
        start = time.time()
        command = ['taskset', '-c', '0-2,4'] + argv
        out, err = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        with out.open('wb') as stdout, err.open('wb') as stderr:
            result = subprocess.run(command, cwd=args.work, stdout=stdout, stderr=stderr, timeout=60)
        accepted = result.returncode != 0 if negative else result.returncode == 0
        report['commands'].append({'argv': command, 'cwd': str(args.work), 'exit_code': result.returncode,
            'expected': 'deliberate named assertion failure' if negative else 'zero', 'accepted': accepted,
            'elapsed_seconds': round(time.time() - start, 3), 'stdout': out.name, 'stdout_sha256': digest(out),
            'stderr': err.name, 'stderr_sha256': digest(err)})
        save()
        assert accepted, label
        if negative:
            assert 'controlled_wrong_valid_token_verdict' in err.read_text(), label
    try:
        execute(['java', '-version'], 'java-version')
        fresh = args.output / 'fresh-real-clock-fixtures'
        epoch = int(time.time())
        execute(['python3', str(HERE / 'generate-fixtures.py'), '--work', str(args.private_work),
            '--output', str(fresh), '--epoch', str(epoch)], 'generate-fresh-real-clock-fixtures')
        report['actual_epoch_anchor'] = epoch
        report['fresh_manifest_sha256'] = digest(fresh / 'fixtures.json')
        for pin in json.loads((HERE / 'pins.json').read_text())['releases']:
            version = pin['release']
            original = args.source_archives / (version + '.tar.gz')
            distribution = args.distributions / ('kafka_2.13-' + version + '.tgz')
            checksum = ''.join((args.distributions / (distribution.name + '.sha512')).read_text().split(':', 1)[-1].split()).lower()
            assert digest(original) == pin['source_archive_sha256']
            assert digest(distribution) == pin['distribution_sha256']
            assert digest(distribution, 'sha512') == checksum == pin['distribution_sha512']
            retained = args.output / ('apache-jwt-source-' + version + '.tar.gz')
            source_map = retain_source(original, retained)
            jars_dir = args.work / 'jars' / version
            jars = extract_jars(distribution, version, jars_dir)
            client = jars_dir / ('kafka-clients-' + version + '.jar')
            assert digest(client) == pin['client_jar_sha256']
            with zipfile.ZipFile(client) as jar:
                props = jar.read('kafka/kafka-version.properties').decode()
                assert 'version=' + version in props and 'commitId=' + pin['source_sha'][:16] in props
            row = {'release': version, 'source_sha': pin['source_sha'], 'source_archive_sha256': digest(original),
                'distribution_sha256': digest(distribution), 'distribution_sha512': digest(distribution, 'sha512'),
                'embedded_client_version_properties': props, 'retained_archive': retained.name,
                'retained_archive_sha256': digest(retained), 'retained_source_files_sha256': source_map,
                'jars': jars, 'api_adaptations': []}
            report['releases'].append(row)
            classes = args.work / 'classes' / version
            classes.mkdir(parents=True)
            classpath = ':'.join(str(jars_dir / jar['file']) for jar in jars)
            execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror',
                '-cp', classpath, '-d', str(classes), str(HERE / 'ApacheJwtProbe.java')], version + '-compile')
            row['compiled_classes_sha256'] = {str(path.relative_to(classes)): digest(path) for path in sorted(classes.rglob('*.class'))}
            observations = args.output / (version + '-actual-outcomes.json')
            argv = ['java', '-cp', str(classes) + ':' + classpath, 'ApacheJwtProbe', version, str(fresh), str(observations)]
            execute(argv, version + '-actual-validator-matrix')
            actual = json.loads(observations.read_text())
            assert actual['passed'] and actual['actual_validator_executions'] == 140
            assert actual['epoch_anchor'] == epoch and actual['finished_epoch'] - epoch < 240
            row['observations_file'] = observations.name
            row['observations_sha256'] = digest(observations)
            row['actual_validator_executions'] = actual['actual_validator_executions']
            row['required_component_assertions'] = actual['required_component_assertions']
            row['policy_differences'] = [{'id': result['id'], 'validator': result['validator'], 'actual_accepted': result['accepted'],
                'strict_local_expected': result['strict_local_expected'], 'actual_epoch': result['observed_epoch'],
                'local_controlled_epoch': result['local_validation_epoch']} for result in actual['cases']
                if result['accepted'] != (result['strict_local_expected'] == 'accept')]
            execute(argv + ['wrong-valid'], version + '-controlled-wrong-valid-verdict', negative=True)
            # A deliberate-failure invocation never overwrites the accepted outcomes.
            assert digest(observations) == row['observations_sha256']
        report['passed'] = True
        report['actual_validator_executions'] = sum(row['actual_validator_executions'] for row in report['releases'])
        report['required_component_assertions'] = sum(row['required_component_assertions'] for row in report['releases'])
        report['controlled_named_failures'] = 3
        report['limitations'] = ['Strict local default access type, duplicate JSON, size/lifetime and NumericDate policies exceed these Apache component validators; every actual difference is retained, not counted as equivalence.',
            'Real JVM clock is used. Fixture-controlled validation epochs do not move the official validators clock; near-boundary verdicts depend on recorded real execution epoch.',
            'Explicitly allowlisted local file JWKS validates the factory/component path; HTTPS discovery, refresh/revocation, socket sessions and authorization remain separate37evidence.',
            'Only synthetic public test tokens and public keys are retained. No production credential or private signing key is in the artifacts.']
    except Exception as error:
        report['failure_class'] = type(error).__name__
        report['failure_context'] = str(error)
        save()
        raise
    save()
    print(json.dumps({'passed': report['passed'], 'executions': report['actual_validator_executions'],
        'component_assertions': report['required_component_assertions'], 'controlled_failures': report['controlled_named_failures']}))

if __name__ == '__main__':
    main()
