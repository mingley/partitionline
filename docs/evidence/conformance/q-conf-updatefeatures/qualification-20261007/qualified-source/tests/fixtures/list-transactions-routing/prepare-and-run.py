#!/usr/bin/env python3
"""Future authorized SDK generation; preparation itself does not execute this."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

PINS = {
    '4.1.2': ('c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c', '33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed'),
    '4.2.1': ('18d5ecd939c8d510fdd72d0abb1f7099659dcd58', '9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159'),
    '4.3.1': ('26b251a451ce941d3d7a55e6487bcb7f16b5ad48', 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'),
}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
HERE = Path(__file__).resolve().parent


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            h.update(chunk)
    return h.hexdigest()


def identity(path):
    info = path.stat()
    return {'path': str(path), 'bytes': info.st_size,
            'full_mode': info.st_mode & 0o7777, 'sha256': digest(path)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--jars', type=Path, default=Path('/workspace/work/broker-wire/jars'))
    parser.add_argument('--affinity', default='2,4')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    ledger = []
    report_path = args.output / 'validation.json'
    source = HERE / 'ListTransactionsOracle.java'
    baseline = [identity(source), identity(Path(__file__))]

    def save(passed, error=None):
        report_path.write_text(json.dumps({
            'schema_version': 1, 'passed': passed,
            'scope': 'Actual pinned Java serializers/AllBrokersStrategy/Handler/Result components only; no public Admin network or Rust acceptance.',
            'original_source': baseline, 'commands': ledger, 'error': error,
        }, indent=2) + '\n')
        report_path.chmod(0o600)

    def command(label, argv, pinned):
        before = [identity(Path(v['path'])) for v in baseline + pinned]
        if before != baseline + pinned:
            raise RuntimeError('source or pinned SDK changed before command')
        log = args.output / (label + '.log')
        code = None
        timeout = False
        output_refused = False
        pid = None
        output_bytes = 0
        process_group_empty = False
        with log.open('xb') as stream:
            process = subprocess.Popen(['taskset', '-c', args.affinity, 'java', '-Xmx128m'] + argv,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
            pid = process.pid
            assert process.stdout is not None
            os.set_blocking(process.stdout.fileno(),False)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout,selectors.EVENT_READ)
            deadline = time.monotonic()+45
            try:
                while selector.get_map():
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        timeout = True
                        break
                    for key,_ in selector.select(min(0.1,remaining)):
                        chunk = os.read(key.fileobj.fileno(),65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        room = 512*1024-output_bytes
                        stream.write(chunk[:room])
                        output_bytes += min(len(chunk),room)
                        if len(chunk) > room:
                            output_refused = True
                            break
                    if output_refused:
                        break
                if timeout or output_refused:
                    try:
                        os.killpg(pid,signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                try:
                    code = process.wait(timeout=max(0.01,deadline-time.monotonic()) if not timeout and not output_refused else 5)
                except subprocess.TimeoutExpired:
                    timeout = True
                    try:
                        os.killpg(pid,signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    code = process.wait(timeout=5)
                try:
                    os.killpg(pid,0)
                except ProcessLookupError:
                    process_group_empty = True
            finally:
                selector.close()
                process.stdout.close()
                if process.poll() is None:
                    try:
                        os.killpg(pid,signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    code = process.wait(timeout=5)
        log.chmod(0o600)
        after = [identity(Path(v['path'])) for v in baseline + pinned]
        row = {'label': label, 'argv': ['taskset', '-c', args.affinity, 'java', '-Xmx128m'] + argv,
               'exit_code': code, 'timeout': timeout, 'log': identity(log),
               'log_limit_refused': output_refused, 'owned_pid_and_process_group': pid,
               'joined_process_group_empty': process_group_empty,
               'source_and_SDK_before': before, 'source_and_SDK_after': after}
        ledger.append(row)
        save(False)
        if output_refused or timeout or code != 0 or not process_group_empty or before != after:
            raise RuntimeError('retained actual command failure: ' + label)

    try:
        expected = None
        cases = None
        for release, (commit, jar_sha) in PINS.items():
            jar = args.jars / ('kafka-clients-' + release + '.jar')
            slf = args.jars / 'slf4j-api-1.7.36.jar'
            if digest(jar) != jar_sha or digest(slf) != SLF4J:
                raise RuntimeError('exact SDK pin mismatch')
            pinned = [identity(jar), identity(slf)]
            classes = args.output / release / 'classes'
            classes.mkdir(parents=True)
            cp = os.pathsep.join([str(jar), str(slf)])
            command(release + '-compile', ['--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main',
                    '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(source)], pinned)
            retained_classes = sorted(classes.rglob('*.class'))
            if not 1 <= len(retained_classes) <= 16 or any(p.stat().st_size > 1024 * 1024 for p in retained_classes):
                raise RuntimeError('finite class retention limit')
            (args.output / release / 'classes.json').write_text(json.dumps({
                'release': release, 'source_commit': commit,
                'classes': [identity(p) for p in retained_classes],
                'retained_only_in_WORK': True,
            }, indent=2) + '\n')
            for replay in [1, 2]:
                vectors = args.output / release / ('replay-' + str(replay))
                command(release + '-replay-' + str(replay), ['-cp', str(classes) + os.pathsep + cp,
                        'org.apache.kafka.clients.admin.ListTransactionsOracle', 'generate', str(vectors), release], pinned)
                files = sorted(p for p in vectors.iterdir() if p.is_file())
                if len(files) != 33 or any(p.stat().st_size > 512 * 1024 for p in files):
                    raise RuntimeError('expected finite16-frame-pair corpus')
                bodies = {p.name: digest(p) for p in files if p.suffix == '.bin'}
                manifest = json.loads((vectors / 'goldens.json').read_text())
                if manifest['actual_vectors'] != 16:
                    raise RuntimeError('missing actual vectors')
                if expected is None:
                    expected = bodies
                    cases = manifest['cases']
                elif expected != bodies or cases != manifest['cases']:
                    raise RuntimeError('actual cross-release/replay wire mismatch')
                ledger[-1]['generated_files'] = [identity(p) for p in files]
                save(False)
        save(True)
        print(json.dumps({'passed': True, 'actual_commands': len(ledger),
                          'SDK_releases': 3, 'actual_replays': 6, 'vectors_per_replay': 16}))
        return 0
    except Exception as error:
        save(False, type(error).__name__ + ': ' + str(error))
        raise


if __name__ == '__main__':
    raise SystemExit(main())
