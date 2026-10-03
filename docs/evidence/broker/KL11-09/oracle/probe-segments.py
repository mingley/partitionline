#!/usr/bin/env python3
"""Execute authentic, bounded Apache index/segment methods with pinned jars."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[4]
PINS = REPO / 'docs/evidence/broker/KL11-68/upstream-pins.json'


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def run(command, log):
    result = subprocess.run(command, cwd=REPO, capture_output=True, text=True,
                            timeout=90, check=False)
    log.write_text('$ ' + ' '.join(map(str, command)) + '\n' + result.stdout + result.stderr
                   + f'\nexit_code={result.returncode}\n')
    receipt = {'argv': list(map(str, command)), 'exit_code': result.returncode,
               'log': log.name, 'log_sha256': sha(log)}
    if result.returncode:
        raise RuntimeError(f'failed: {log}')
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jars', type=Path, required=True)
    parser.add_argument('--distributions', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir(parents=True, exist_ok=True)
    pins = json.loads(PINS.read_text())
    source = (ROOT / 'SegmentIndexProbe.java').read_text()
    report = {'source_sha256': sha(ROOT / 'SegmentIndexProbe.java'),
              'driver_sha256': sha(Path(__file__)), 'pins_sha256': sha(PINS),
              'scope': 'Apache components only; manual segment boundaries; no Rust or wire broker/performance claim.',
              'releases': []}
    outcomes = []
    for release in pins['releases']:
        version = release['version']
        scratch = args.scratch / version
        scratch.mkdir(exist_ok=True)
        distro = args.distributions / f'kafka_2.13-{version}.tgz'
        if sha(distro) != release['distribution_sha256']:
            raise ValueError('distribution pin mismatch')
        jar = args.jars / f'kafka-clients-{version}.jar'
        if sha(jar) != release['jar_sha256']:
            raise ValueError('client pin mismatch')
        jars = [jar, args.jars / 'slf4j-api-1.7.36.jar']
        if sha(jars[1]) != 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0':
            raise ValueError('logging pin mismatch')
        with tarfile.open(distro) as archive:
            for basename in [f'kafka-storage-{version}.jar', f'kafka-storage-api-{version}.jar',
                             f'kafka-server-common-{version}.jar', 'metrics-core-2.2.0.jar']:
                member = archive.getmember(f'kafka_2.13-{version}/libs/{basename}')
                if not member.isfile() or member.size >= 2 * 1024 * 1024:
                    raise ValueError('unexpected bounded archive member')
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError('missing archive member')
                path = scratch / basename
                path.write_bytes(stream.read())
                jars.append(path)
        with zipfile.ZipFile(jar) as archive:
            namespace = ('org.apache.kafka.common.record.internal'
                         if 'org/apache/kafka/common/record/internal/MemoryRecords.class' in archive.namelist()
                         else 'org.apache.kafka.common.record')
        adapted = scratch / 'SegmentIndexProbe.java'
        adapted_source = source.replace('org.apache.kafka.common.record.internal.', namespace + '.')
        # Apache4.2 migrated these immutable carriers from public fields to
        # Java records. Adapt access syntax only; all storage calls are actual.
        if version == '4.1.2':
            for field in ('offset', 'position', 'timestamp'):
                adapted_source = adapted_source.replace(f'hint.{field}()', f'hint.{field}')
        adapted.write_text(adapted_source)
        classes = scratch / 'classes'
        classes.mkdir(exist_ok=True)
        cp = ':'.join(map(str, jars))
        compiled = run(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
                        'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp,
                        '-d', str(classes), str(adapted)], args.output / f'{version}-compile.log')
        retained = args.output / version
        retained.mkdir(exist_ok=True)
        (retained / adapted.name).write_bytes(adapted.read_bytes())
        (retained / 'SegmentIndexProbe.class').write_bytes((classes / 'SegmentIndexProbe.class').read_bytes())
        executions = []
        for repetition in (1, 2):
            directory = scratch / f'segments-{repetition}'
            if directory.exists():
                raise ValueError('fresh segment directory required; preserve prior attempts')
            destination = args.output / f'{version}-run-{repetition}.json'
            executions.append(run(['taskset', '-c', '0-2,4', 'java', '-cp', f'{classes}:{cp}',
                                   'SegmentIndexProbe', str(directory), str(destination)],
                                  args.output / f'{version}-run-{repetition}.log'))
        first = args.output / f'{version}-run-1.json'
        second = args.output / f'{version}-run-2.json'
        if first.read_bytes() != second.read_bytes():
            raise ValueError('fresh execution disagreement')
        outcome = json.loads(first.read_text())
        if not outcome['close_reopen_equal'] or outcome['segments'] != 3:
            raise ValueError('component invariants missing')
        if len(outcome['before']['reads']) != 21 or len(outcome['before']['timestamp_searches']) != 160:
            raise ValueError('unexpected case denominator')
        outcomes.append(outcome)
        mutant_dir = scratch / 'mutant'
        mutant_dir.mkdir(exist_ok=True)
        mutant = mutant_dir / 'SegmentIndexProbe.java'
        mutation = 'actual.get().offset == expectedOffset'
        if adapted_source.count(mutation) != 1:
            raise ValueError('controlled counterexample source did not match once')
        mutant.write_text(adapted_source.replace(mutation, mutation + ' + 1'))
        mutant_classes = mutant_dir / 'classes'
        mutant_classes.mkdir(exist_ok=True)
        mutant_compile = run(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
                              'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp,
                              '-d', str(mutant_classes), str(mutant)],
                             args.output / f'{version}-counterexample-compile.log')
        command = ['taskset', '-c', '0-2,4', 'java', '-cp', f'{mutant_classes}:{cp}',
                   'SegmentIndexProbe', str(mutant_dir / 'segments'), str(mutant_dir / 'result.json')]
        failed = subprocess.run(command, cwd=REPO, capture_output=True, text=True, timeout=90, check=False)
        failure_log = args.output / f'{version}-counterexample-runtime.log'
        failure_log.write_text('$ ' + ' '.join(command) + '\n' + failed.stdout + failed.stderr
                               + f'\nexit_code={failed.returncode}\n')
        if failed.returncode != 1 or 'AssertionError: equal/regressing timestamp first-match offset' not in failed.stderr:
            raise ValueError('controlled expected-offset counterexample did not fail at intended assertion')
        (retained / 'counterexample.java').write_bytes(mutant.read_bytes())
        (retained / 'counterexample.class').write_bytes((mutant_classes / 'SegmentIndexProbe.class').read_bytes())
        report['releases'].append({'version': version, 'record_namespace': namespace,
            'classpath': [{'name': path.name, 'sha256': sha(path)} for path in jars],
            'adapted_source_sha256': sha(adapted), 'class_sha256': sha(classes / 'SegmentIndexProbe.class'),
            'compile': compiled, 'executions': executions, 'result_sha256': sha(first),
            'controlled_counterexample': {'compile': mutant_compile, 'argv': command,
                'exit_code': failed.returncode, 'expected_assertion_detected': True,
                'log': failure_log.name, 'log_sha256': sha(failure_log),
                'source_sha256': sha(mutant), 'class_sha256': sha(mutant_classes / 'SegmentIndexProbe.class')},
            'assertions_per_execution': outcome['assertions'],
            'read_cases_per_phase': 21, 'timestamp_cases_per_phase': 160,
            'index_hint_cases_per_phase': len(outcome['before']['index_hints'])})
    if not (outcomes[0] == outcomes[1] == outcomes[2]):
        raise ValueError('cross-release methods disagree')
    report['cross_release_identical'] = True
    report['executions'] = 6
    report['controlled_expected_offset_failures'] = 3
    report['assertions'] = sum(x['assertions'] * 2 for x in outcomes)
    report['actual_read_cases'] = 21 * 2 * 6
    report['actual_timestamp_cases'] = 160 * 2 * 6
    (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('executions', 'assertions', 'actual_read_cases', 'actual_timestamp_cases', 'cross_release_identical')}))


if __name__ == '__main__':
    main()
