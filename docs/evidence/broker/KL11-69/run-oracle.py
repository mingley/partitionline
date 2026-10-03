#!/usr/bin/env python3
"""Strictly compile and replay independently pinned Apache controller oracles."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
RELEASES = ['4.1.2', '4.2.1', '4.3.1']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Python -O disables required pin assertions.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clients-jars', type=Path, required=True)
    parser.add_argument('--helper-jars', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--classes', type=Path, required=True)
    parser.add_argument('--fixtures', type=Path)
    parser.add_argument('--verify-fixtures', type=Path)
    parser.add_argument('--actual-responses', type=Path)
    parser.add_argument('--negative-decode-tests', action='store_true')
    parser.add_argument('--source-sha')
    args = parser.parse_args()
    assert not args.negative_decode_tests or args.actual_responses, 'Negative decoder checks require actual Rust responses.'
    for key in ['clients_jars', 'helper_jars', 'output', 'classes', 'fixtures', 'verify_fixtures', 'actual_responses']:
        value = getattr(args, key)
        if value is not None:
            setattr(args, key, value.resolve())
    args.output.mkdir(parents=True, exist_ok=False)
    args.classes.mkdir(parents=True, exist_ok=False)
    source = HERE / 'ControllerOracle.java'
    reference_pins = json.loads((HERE / 'reference-pins.json').read_text())
    for release in reference_pins['releases']:
        for name, expected in release['retained_file_sha256'].items():
            assert sha(HERE / 'references' / release['release'] / name) == expected
    helpers = json.loads((HERE / 'helper-jars.json').read_text())
    slf = args.clients_jars / 'slf4j-api-1.7.36.jar'
    assert sha(slf) == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
    matrix = json.loads((ROOT / 'tests/conformance/broker/api-matrix.json').read_text())
    commits = {item['version']: item['commit'] for item in matrix['releases']}
    report = {'schema_version': 1, 'source_sha': args.source_sha,
        'generator_sha256': sha(source), 'runner_sha256': sha(Path(__file__)),
        'reference_pins_sha256': sha(HERE / 'reference-pins.json'),
        'java_version': subprocess.run(['java', '-version'], capture_output=True, text=True, check=True).stderr,
        'scope': 'Actual Apache generated schemas, KafkaRaftClient handlers and QuorumState using explicit synthetic empty log, mutable log-end summaries, in-memory state store and no network/poll.',
        'real_broker_or_full_kraft_qualification': False, 'releases': []}
    if args.source_sha:
        assert len(args.source_sha) == 40 and all(c in '0123456789abcdef' for c in args.source_sha)
        for owned in [source, Path(__file__), HERE / 'helper-jars.json']:
            relative = owned.relative_to(ROOT).as_posix()
            expected = subprocess.run(['git', 'show', args.source_sha + ':' + relative], cwd=ROOT, capture_output=True, check=True).stdout
            assert expected == owned.read_bytes(), 'Source does not match immutable pin: ' + relative
    else:
        report['source_context'] = {'kind': 'precommit dirty generator development',
            'head': subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip(),
            'owned_status': subprocess.run(['git', 'status', '--short', '--', str(HERE), 'partitionline-broker/tests/fixtures/raft-protocol'], cwd=ROOT, capture_output=True, text=True, check=True).stdout}

    def execute(argv, name, expected=0):
        out, err = args.output / (name + '.stdout.log'), args.output / (name + '.stderr.log')
        with out.open('xb') as stdout, err.open('xb') as stderr:
            result = subprocess.run(argv, cwd=ROOT, stdout=stdout, stderr=stderr)
        row = {'argv': argv, 'exit_code': result.returncode, 'expected_exit_code': expected,
            'stdout': out.name, 'stderr': err.name, 'stdout_sha256': sha(out), 'stderr_sha256': sha(err)}
        report.setdefault('all_commands', []).append(row)
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
        assert result.returncode == expected, row
        return row

    baseline = None
    for version in RELEASES:
        client = args.clients_jars / ('kafka-clients-' + version + '.jar')
        provenance = json.loads((ROOT / 'docs/evidence/broker/KL11-04' / ('apache-' + version + '.provenance.json')).read_text())
        assert provenance['source_sha'] == commits[version] and sha(client) == provenance['jar_sha256']
        with zipfile.ZipFile(client) as archive:
            names = [name for name in archive.namelist() if name.endswith('kafka-version.properties')]
            assert len(names) == 1
            properties = dict(line.split('=', 1) for line in archive.read(names[0]).decode().splitlines() if '=' in line and not line.startswith('#'))
            assert properties['version'] == version and properties['commitId'] == commits[version][:16]
        jars = [client]
        for helper in helpers:
            if helper['release'] != version:
                continue
            path = args.helper_jars / helper['jar']
            assert path.stat().st_size == helper['bytes'] and sha(path) == helper['sha256']
            assert helper['distribution_provenance'] == provenance
            jars.append(path)
        assert len(jars) == 3
        jars.append(slf)
        classes = args.classes / version
        classes.mkdir()
        cp = ':'.join(map(str, jars))
        compiled = execute(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
            'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(source)], version + '-compile')
        output = args.output / version
        command = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '-cp', str(classes) + ':' + cp,
            'ControllerOracle', str(output), version]
        if args.actual_responses:
            command.append(str(args.actual_responses))
        replay = execute(command, version + '-replay')
        negatives = []
        if args.negative_decode_tests:
            for kind in ['correlation', 'trailing']:
                base = args.output / ('negative-' + version + '-' + kind)
                mutated = base / 'input' / version
                mutated.mkdir(parents=True)
                data = bytearray((args.actual_responses / version / 'api-versions-0.actual-response.bin').read_bytes())
                if kind == 'correlation':
                    data[3] ^= 1
                else:
                    data.append(0)
                (mutated / 'api-versions-0.actual-response.bin').write_bytes(data)
                negative_command = command[:-3] + [str(base / 'output'), version, str(base / 'input')]
                result = execute(negative_command, version + '-negative-' + kind, 1)
                expected_message = 'AssertionError: correlation' if kind == 'correlation' else 'AssertionError: trailing response bytes'
                assert expected_message in (args.output / result['stderr']).read_text(), 'Negative failed for another reason.'
                negatives.append(result)
        observations = json.loads((output / 'observations.json').read_text())
        files = {path.name: sha(path) for path in sorted(output.iterdir()) if path.name != 'observations.json'}
        if args.verify_fixtures:
            retained = args.verify_fixtures / version
            expected_files = {path.name: sha(path) for path in sorted(retained.iterdir()) if path.name != 'observations.json'}
            assert expected_files == files, 'Retained fixtures differ from independent regeneration.'
            report.setdefault('unchanged_fixture_receipts', {})[version] = expected_files
        if baseline is None:
            baseline = files
        assert files == baseline, 'Release fixture bytes differ; retain and inspect before exporting.'
        report['releases'].append({'release': version, 'upstream_commit': commits[version],
            'client_provenance': provenance, 'jar_sha256': {path.name: sha(path) for path in jars},
            'jar_properties': properties, 'commands': [compiled, replay], 'case_count': observations['case_count'],
            'decoded_actual_rust_responses': observations['decoded_actual_rust_responses'],
            'decoded_actual_tcp_responses': observations['decoded_actual_tcp_responses'], 'negative_decoder_checks': negatives,
            'compiled_class_sha256': {path.name: sha(path) for path in sorted(classes.glob('*.class'))},
            'fixture_sha256': files, 'observations_sha256': sha(output / 'observations.json')})
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    if args.fixtures:
        for version in RELEASES:
            directory = args.fixtures / version
            directory.mkdir(parents=True, exist_ok=False)
            for path in sorted((args.output / version).iterdir()):
                shutil.copyfile(path, directory / path.name)
    report['fixture_files_identical_all_releases'] = True
    report['case_total'] = sum(row['case_count'] for row in report['releases'])
    (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print('PASS', report['case_total'], 'controller cases across three pinned releases')


if __name__ == '__main__':
    main()
