#!/usr/bin/env python3
"""Strictly compile the independent read controls with three pinned official SDKs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--attempt', type=int, required=True)
    parser.add_argument('--scratch', type=Path, default=Path('/workspace/work/broker-log-oracle/read-control-peer'))
    args = parser.parse_args()
    assert args.attempt > 0
    evidence = ROOT / 'read-control-build'
    evidence.mkdir(exist_ok=True)
    proof = evidence / f'validation-attempt-{args.attempt}.json'
    assert not proof.exists(), 'preserve all previous attempts'
    source = ROOT / 'ReadControlPeer.java'
    codec_source = ROOT / 'CodecReadPeer.java'
    native_source = ROOT / 'NativeReadPeer.java'
    expected = {row['version']: row['jar_sha256'] for row in json.loads((ROOT / 'upstream-pins.json').read_text())['releases']}
    results = []
    codec_results = []
    native_results = []
    for release in ('4.1.2', '4.2.1', '4.3.1'):
        work = args.scratch / release
        work.mkdir(parents=True, exist_ok=True)
        text = source.read_text()
        if release != '4.3.1':
            text = text.replace('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.')
        adapted = work / source.name
        adapted.write_text(text)
        jar = Path('/workspace/work/broker-wire/jars') / f'kafka-clients-{release}.jar'
        slf = Path('/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar')
        assert sha(jar) == expected[release]
        assert sha(slf) == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
        command = ['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
                   'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', f'{jar}:{slf}',
                   '-d', str(work), str(adapted)]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
        log = evidence / f'compile-{release}-attempt-{args.attempt}.log'
        assert not log.exists()
        log.write_text(result.stdout)
        item = {'release': release, 'command': command, 'exit_code': result.returncode,
                'adapted_source_sha256': sha(adapted), 'jar_sha256': sha(jar), 'slf4j_sha256': sha(slf),
                'log': str(log.relative_to(ROOT))}
        if result.returncode == 0:
            item['class_sha256'] = sha(work / 'ReadControlPeer.class')
        else:
            (evidence / f'source-failed-{release}-attempt-{args.attempt}.java').write_text(text)
        results.append(item)
        codec_text = codec_source.read_text()
        if release != '4.3.1':
            codec_text = codec_text.replace('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.')
        codec_adapted = work / codec_source.name
        codec_adapted.write_text(codec_text)
        codec_command = command[:-1] + [str(codec_adapted)]
        codec_result = subprocess.run(codec_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
        codec_log = evidence / f'codec-compile-{release}-attempt-{args.attempt}.log'
        assert not codec_log.exists()
        codec_log.write_text(codec_result.stdout)
        codec_item = {'release': release, 'command': codec_command, 'exit_code': codec_result.returncode,
                      'adapted_source_sha256': sha(codec_adapted), 'jar_sha256': sha(jar),
                      'log': str(codec_log.relative_to(ROOT))}
        if codec_result.returncode == 0:
            codec_item['class_sha256'] = sha(work / 'CodecReadPeer.class')
        else:
            (evidence / f'codec-source-failed-{release}-attempt-{args.attempt}.java').write_text(codec_text)
        codec_results.append(codec_item)
        native_adapted = work / native_source.name
        native_adapted.write_text(native_source.read_text())
        native_command = command[:-1] + [str(native_adapted)]
        native_result = subprocess.run(native_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
        native_log = evidence / f'native-read-compile-{release}-attempt-{args.attempt}.log'
        assert not native_log.exists()
        native_log.write_text(native_result.stdout)
        native_item = {'release': release, 'command': native_command, 'exit_code': native_result.returncode,
                       'adapted_source_sha256': sha(native_adapted), 'jar_sha256': sha(jar),
                       'log': str(native_log.relative_to(ROOT))}
        if native_result.returncode == 0:
            native_item['class_sha256'] = sha(work / 'NativeReadPeer.class')
        else:
            (evidence / f'native-read-source-failed-{release}-attempt-{args.attempt}.java').write_text(native_source.read_text())
        native_results.append(native_item)
    proof.write_text(json.dumps({'status': 'strict_compile_only_not_runtime', 'source_sha256': sha(source),
                                'codec_source_sha256': sha(codec_source),
                                'native_read_source_sha256': sha(native_source),
                                'namespace_adaptation': '4.1/4.2 replace common.record.internal with common.record only',
                                'java': subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True),
                                'results': results, 'codec_results': codec_results, 'native_read_results': native_results}, indent=2) + '\n')
    assert all(row['exit_code'] == 0 for row in results + codec_results + native_results)


if __name__ == '__main__':
    main()
