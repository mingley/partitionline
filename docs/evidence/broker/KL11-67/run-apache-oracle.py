#!/usr/bin/env python3
"""Generate and independently reproduce bounded Apache/JNI codec fixtures."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('assertions must be enabled')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--install-fixtures', action='store_true')
    args = parser.parse_args()
    repo = args.repo.resolve()
    evidence = repo / 'docs/evidence/broker/KL11-67'
    fixtures = repo / 'partitionline-broker/tests/fixtures/codecs'
    prior = json.loads((repo / 'docs/evidence/broker/KL11-60/apache-oracle.json').read_text())
    jars = prior['classpath']
    for jar in jars:
        assert sha(Path(jar['path'])) == jar['sha256'], jar['name']
    cp = ':'.join(jar['path'] for jar in jars)
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=False)
    classes = work / 'classes'
    classes.mkdir()
    commands = []

    def run(command, name):
        result = subprocess.run(['taskset', '-c', '0-2,4'] + command,
                                capture_output=True, text=True, timeout=45)
        log = evidence / name
        log.write_text(result.stdout + result.stderr)
        commands.append(dict(command=['taskset', '-c', '0-2,4'] + command,
                             exit_code=result.returncode, log=name, log_sha256=sha(log)))
        (evidence / 'apache-commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        assert result.returncode == 0, name
        return result.stdout

    source = evidence / 'CodecsOracle.java'
    run(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main',
         '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(source)], 'apache-compile.log')
    runs = []
    for phase in ['execute', 'reproduce']:
        generated = work / phase
        stdout = run(['java', '-Xms16m', '-Xmx64m', '-cp', str(classes) + ':' + cp,
                      'CodecsOracle', str(generated)], 'apache-' + phase + '.log')
        rows = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
        assert len(rows) == 44 and len({r['fixture'] for r in rows}) == 44
        for row in rows:
            fresh = generated / (row['fixture'] + '.bin')
            assert fresh.stat().st_size <= 65536
            if phase == 'execute' and args.install_fixtures:
                fixtures.mkdir(parents=True, exist_ok=True)
                (fixtures / fresh.name).write_bytes(fresh.read_bytes())
            retained = fixtures / fresh.name
            assert retained.read_bytes() == fresh.read_bytes(), row['fixture']
            row['sha256'] = sha(fresh)
        runs.append(rows)
    assert runs[0] == runs[1], 'independent reproduction changed bytes/parser outcomes'
    rows = runs[0]
    plain = next(r['upstream']['history'] for r in rows if r['fixture'] == 'plain')
    expanded = next(r['upstream']['history'] for r in rows if r['fixture'] == 'expanded-plain')
    for row in rows:
        if row['fixture'] in ['gzip', 'snappy', 'lz4', 'zstd', 'gzip-concatenated', 'zstd-concatenated']:
            assert row['upstream']['status'] == 'accepted'
            assert row['upstream']['full_input_consumed'] and row['upstream']['history'] == plain
        if row['fixture'] in ['multiple', 'multiple-plain']:
            assert row['upstream']['status'] == 'accepted' and row['upstream']['history'] == plain * 5
        if row['fixture'].endswith('-expanded'):
            assert row['upstream']['status'] == 'accepted' and row['upstream']['history'] == expanded
    table = ''.join(row['fixture'] + '\t' + row['expected_rust'] + '\n' for row in rows)
    if args.install_fixtures:
        (fixtures / 'expectations.tsv').write_text(table)
    assert (fixtures / 'expectations.tsv').read_text() == table
    result = dict(task='KL11-67', apache_version='4.3.1',
                  distribution_provenance=prior['distribution_provenance'], classpath=jars,
                  source='CodecsOracle.java', source_sha256=sha(source),
                  class_sha256=sha(classes / 'CodecsOracle.class'), commands=commands,
                  java_version=subprocess.run(['java', '-version'], capture_output=True, text=True).stderr,
                  timeout_seconds=45, heap_max_bytes=64 * 1024 * 1024,
                  fixture_bytes_limit=65536, fixtures=rows, fixture_count=len(rows),
                  reproduction='Two actual Apache executions produce identical fixtures and parser histories.',
                  scope='Official Apache builders and codec output wrappers with JDK gzip, Snappy JNI, lz4-java and Zstd JNI; native dependencies are external oracle tools only. No broker or Produce policy/performance was executed.')
    (evidence / 'apache-oracle.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(fixtures=len(rows), upstream_accepted=sum(r['upstream']['status'] == 'accepted' for r in rows)), indent=2))


if __name__ == '__main__':
    main()
