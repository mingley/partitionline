#!/usr/bin/env python3
"""Reproduce pinned Apache-built bytes and actual positive/negative parser outcomes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PINS = {
    'kafka-clients-4.3.1.jar': 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e',
    'slf4j-api-1.7.36.jar': 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0',
    'snappy-java-1.1.10.5.jar': '0f3f1857ed33116583f480b4df5c0218836c47bfbc9c6221c0d73f356decf37b',
    'lz4-java-1.8.0.jar': 'd74a3334fb35195009b338a951f918203d6bbca3d1d359033dc33edd1cadc9ef',
    'zstd-jni-1.5.6-4.jar': '793ca8734aa15687e7e64564eab8b6ae9ee2720eae27aa663074682144b1c386',
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('run without -O so validation assertions remain enabled')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-jar', type=Path, required=True)
    parser.add_argument('--slf4j-jar', type=Path, required=True)
    parser.add_argument('--codec-dir', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    args = parser.parse_args()
    repo = args.repo.resolve()
    evidence = repo / 'docs/evidence/broker/KL11-60'
    fixtures = repo / 'partitionline-broker/tests/fixtures/records'
    source = evidence / 'RecordsOracle.java'
    jars = [args.kafka_jar, args.slf4j_jar] + [args.codec_dir / name for name in list(PINS)[2:]]
    classpath = []
    for path, name in zip(jars, PINS):
        assert sha(path) == PINS[name], f'wrong pinned {name}'
        classpath.append({'name': name, 'path': str(path.resolve()), 'sha256': sha(path)})
    cp = ':'.join(item['path'] for item in classpath)
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    classes = work / 'classes'
    classes.mkdir(exist_ok=True)
    commands = []

    def run(command, log):
        result = subprocess.run(command, capture_output=True, text=True, timeout=45, cwd=repo)
        (evidence / log).write_text(result.stdout + result.stderr)
        commands.append({'command': command, 'exit_code': result.returncode, 'log': str((evidence / log).relative_to(repo))})
        assert result.returncode == 0, f'{log}: {result.stderr}'
        return result.stdout

    run(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
         'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp,
         '-d', str(classes), str(source)], 'apache-compile.log')
    executions = []
    for phase in ['execute', 'reproduce']:
        generated = work / phase
        stdout = run(['taskset', '-c', '0-2,4', 'java', '-Xms16m', '-Xmx64m', '-cp',
                      str(classes) + ':' + cp, 'RecordsOracle', str(generated)], 'apache-' + phase + '.log')
        rows = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
        assert len(rows) == 58 and len({row['fixture'] for row in rows}) == 58
        for row in rows:
            expected = fixtures / (row['fixture'] + '.bin')
            fresh = generated / expected.name
            assert expected.read_bytes() == fresh.read_bytes(), f"fixture changed: {row['fixture']}"
        executions.append(rows)
    assert executions[0] == executions[1], 'upstream outcomes differ between executions'
    rows = executions[0]
    table = '\n'.join(row['fixture'] + '\t' + row['expected_rust'] for row in rows) + '\n'
    assert table == (fixtures / 'expectations.tsv').read_text(), 'Rust fixture expectation manifest changed'
    for row in rows:
        path = fixtures / (row['fixture'] + '.bin')
        row['fixture_path'] = str(path.relative_to(repo))
        row['sha256'] = sha(path)
        if row['expected_rust'] == 'accepted':
            assert row['upstream']['status'] == 'accepted' and row['upstream']['full_input_consumed']
            row['admission_relation'] = 'supported-and-accepted'
        elif row['fixture'].startswith('feature-'):
            assert row['upstream']['status'] == 'accepted' and row['upstream']['full_input_consumed']
            row['admission_relation'] = 'unsupported-feature'
        elif row['upstream']['status'] == 'accepted':
            row['admission_relation'] = 'strict-admission-rejection-upstream-accepts'
        else:
            row['admission_relation'] = 'both-reject'
    counts = {'fixtures': len(rows), 'supported_and_accepted': sum(row['expected_rust'] == 'accepted' for row in rows),
              'unsupported_features': sum(row['fixture'].startswith('feature-') for row in rows),
              'upstream_rejected': sum(row['upstream']['status'] == 'rejected' for row in rows),
              'upstream_accepted': sum(row['upstream']['status'] == 'accepted' for row in rows),
              'stricter_admission_rejections': sum(row['admission_relation'] == 'strict-admission-rejection-upstream-accepts' for row in rows)}
    assert counts == {'fixtures': 58, 'supported_and_accepted': 6, 'unsupported_features': 11,
                      'upstream_rejected': 22, 'upstream_accepted': 36, 'stricter_admission_rejections': 19}
    upstream = json.loads((repo / 'docs/evidence/broker/KL11-57/upstream-java-oracle.json').read_text())
    distribution = next(item['distribution_provenance'] for item in upstream['releases'] if item['version'] == '4.3.1')
    result = {'schema_version': 1, 'task_id': 'KL11-60', 'apache_version': '4.3.1',
              'distribution_provenance': distribution, 'classpath': classpath,
              'java_version': subprocess.run(['java', '-version'], capture_output=True, text=True).stderr,
              'oracle_source': str(source.relative_to(repo)), 'oracle_source_sha256': sha(source),
              'oracle_class_sha256': sha(classes / 'RecordsOracle.class'),
              'runner': str(Path(__file__).resolve().relative_to(repo)), 'runner_sha256': sha(Path(__file__)),
              'commands': commands, 'jvm_heap_max_bytes': 67108864, 'execution_timeout_seconds': 45,
              'fixtures': rows, 'counts': counts,
              'reproduction': 'Two fresh executions reproduced all 58 retained fixture bytes and identical parser outcomes.',
              'scope': 'Official Apache MemoryRecords builders, validBytes, batch.ensureValid and complete record iteration/record.ensureValid. No running broker, LogValidator or Produce handler is exercised.'}
    (evidence / 'apache-oracle.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(counts, indent=2))


if __name__ == '__main__':
    main()
