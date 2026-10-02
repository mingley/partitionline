#!/usr/bin/env python3
"""Run pinned authentic Apache mechanisms; source and synthetic entropy only."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[5]
ORACLE = Path(__file__).resolve().parent
FILES = ['apache-scram.tsv', 'apache-messages.tsv', 'apache-plain.tsv', 'rfc-scram-sha256.tsv', 'rfc-scram-extensions.tsv']
JAR_PROVENANCE = ROOT / 'docs/evidence/broker/KL11-04'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Use normal Python, without -O, to enforce all pin checks.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jars', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--classes', type=Path, required=True)
    parser.add_argument('--fixtures', type=Path)
    parser.add_argument('--source-sha')
    args = parser.parse_args()
    args.jars, args.output, args.classes = (path.resolve() for path in [args.jars, args.output, args.classes])
    args.output.mkdir(parents=True)
    args.classes.mkdir(parents=True)
    java = ORACLE / 'SaslMechanismOracle.java'
    matrix = json.loads((ROOT / 'tests/conformance/broker/api-matrix.json').read_text())
    slf = args.jars / 'slf4j-api-1.7.36.jar'
    assert sha(slf) == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
    versions = subprocess.run(['java', '-version'], capture_output=True, text=True, check=True)
    report = {'scope': 'Actual Java SASL mechanism objects/formatters/parsers; no Kafka framing, TLS, broker network or session interoperability.',
        'fixture_entropy': 'Public deterministic SecureRandom replacement through Java reflection in fixture generator only; no production RNG claim.',
        'source_sha': args.source_sha, 'generator_sha256': sha(java), 'runner_sha256': sha(Path(__file__)),
        'java_version': versions.stdout + versions.stderr, 'slf4j_sha256': sha(slf), 'releases': [],
        'production_qualification': False}
    if args.source_sha:
        assert len(args.source_sha) == 40 and all(c in '0123456789abcdef' for c in args.source_sha)
    else:
        report['source_context'] = {'kind': 'precommit generator development, not final frozen validation',
            'head': subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip(),
            'owned_status': subprocess.run(['git', 'status', '--short', '--', str(ORACLE), 'partitionline-broker/tests/fixtures/sasl'], cwd=ROOT, capture_output=True, text=True, check=True).stdout}
    baseline = None
    rfc_text = (ORACLE / 'references/rfc/rfc7677.txt').read_text()
    rfc_lines = rfc_text.splitlines()
    published = []
    for index, line in enumerate(rfc_lines):
        match = re.fullmatch(r'\s+[CS]: (.*)', line)
        if match:
            message = match.group(1)
            while message.endswith(','):
                index += 1
                message += rfc_lines[index].strip()
            published.append(message)
    assert len(published) == 4

    def execute(argv, name, expected):
        stdout = args.output / (name + '.stdout.log')
        stderr = args.output / (name + '.stderr.log')
        with stdout.open('xb') as out, stderr.open('xb') as err:
            completed = subprocess.run(argv, cwd=ROOT, stdout=out, stderr=err)
        row = {'argv': argv, 'exit_code': completed.returncode, 'expected_exit_code': expected,
            'stdout': stdout.name, 'stderr': stderr.name,
            'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr)}
        assert completed.returncode == expected, row
        return row

    for release in matrix['releases']:
        version = release['version']
        jar = args.jars / ('kafka-clients-' + version + '.jar')
        provenance = json.loads((JAR_PROVENANCE / ('apache-' + version + '.provenance.json')).read_text())
        assert sha(jar) == provenance['jar_sha256'] and provenance['source_sha'] == release['commit']
        with zipfile.ZipFile(jar) as archive:
            names = [name for name in archive.namelist() if name.endswith('kafka-version.properties')]
            assert len(names) == 1
            properties = dict(line.split('=', 1) for line in archive.read(names[0]).decode().splitlines() if '=' in line and not line.startswith('#'))
            assert properties['version'] == version and properties['commitId'] == release['commit'][:16]
        classes = args.classes / version
        classes.mkdir()
        compile_command = ['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
            'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', str(jar), '-d', str(classes), str(java)]
        compile_result = execute(compile_command, version + '-compile', 0)
        classpath = ':'.join([str(classes), str(jar), str(slf)])
        command = ['taskset', '-c', '0-2,4', 'java', '-cp', classpath, 'SaslMechanismOracle', str(args.output / version)]
        positive = execute(command, version + '-mechanisms', 0)
        negative_command = command[:-1] + [str(args.output / (version + '-rfc-mutant')), 'mutate-rfc']
        negative = execute(negative_command, version + '-rfc-mutant', 1)
        assert 'RFC 7677 literal client proof differs' in (args.output / negative['stderr']).read_text()
        assert (args.output / positive['stdout']).read_text().strip() == 'PASS server_scram=48 parser=36 plain=12 rfc=1 rfc_extensions=2 client_proof=12'
        rfc_row = (args.output / version / 'rfc-scram-sha256.tsv').read_text().splitlines()[1].split('\t')
        assert [bytes.fromhex(rfc_row[index]).decode() for index in [5, 6, 7, 8]] == published, 'Literal transcript differs from retained RFC text.'
        vectors = {name: sha(args.output / version / name) for name in FILES + ['outcomes.tsv']}
        if baseline is None:
            baseline = vectors
        assert baseline == vectors, 'Release mechanism results differ; preserve raw files and investigate before exporting.'
        report['releases'].append({'version': version, 'upstream_commit': release['commit'],
            'distribution_provenance': provenance, 'jar_sha256': sha(jar), 'jar_properties': properties,
            'commands': [compile_result, positive, negative], 'output_files_sha256': vectors,
            'compiled_class_sha256': {path.name: sha(path) for path in sorted(classes.glob('*.class'))},
            'server_scram_cases': 48, 'parser_cases': 36, 'plain_cases': 12, 'literal_rfc7677_cases': 1,
            'rfc_raw_optional_extension_cases': 2, 'actual_client_proof_cases': 12, 'mechanism_case_total': 111})
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    if args.fixtures:
        args.fixtures.mkdir(parents=True, exist_ok=True)
        for name in FILES:
            target = args.fixtures / name
            raw = (args.output / '4.3.1' / name).read_bytes()
            if target.exists():
                assert target.read_bytes() == raw, 'Published frozen fixture differs: ' + str(target)
            else:
                with target.open('xb') as output:
                    output.write(raw)
        report['published_fixture_sha256'] = {name: sha(args.fixtures / name) for name in FILES}
    report.update(verdict='passed', release_outputs_byte_identical=True,
        total_case_executions=333, expected_failing_literal_rfc_mutants=3,
        literal_transcript_matches_retained_rfc_text=True,
        rfc_mirror_text_sha256=sha(ORACLE / 'references/rfc/rfc7677.txt'))
    (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'verdict': 'passed', 'release_cases': 333, 'expected_rfc_mutant_failures': 3,
        'release_outputs_byte_identical': True}))


if __name__ == '__main__':
    main()
