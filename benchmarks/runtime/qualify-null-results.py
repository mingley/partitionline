#!/usr/bin/env python3
"""Check corrected null result files, retained native files and rejection controls."""
import argparse
import copy
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import time
from jsonschema import Draft7Validator

REPO = Path(__file__).resolve().parents[2]
GUARD = {}
END = None


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def guard():
    if time.monotonic() >= END:
        raise TimeoutError('overall result qualification deadline')
    for path, digest in GUARD.items():
        if sha(path) != digest:
            raise ValueError('executed source or binary changed: ' + path)


def main():
    global END
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['compiler-source', 'binding', 'binaries', 'native-result', 'old-null-result', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    END = time.monotonic() + 300
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
        raise OSError('subreaper unavailable')
    def interrupted(signum, frame):
        raise InterruptedError('qualification owner interrupted')
    for signum in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(signum, interrupted)
    spec = importlib.util.spec_from_file_location('result_owner', REPO / 'scripts/run-benchmark-matrix.py')
    owner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owner)
    binding = json.loads(args.binding.read_text())
    for name, digest in binding['sources'].items():
        path = args.compiler_source / name
        if sha(path) != digest:
            raise ValueError('compiled source differs: ' + name)
        GUARD[str(path)] = digest
    for name, digest in binding['binaries'].items():
        path = args.binaries / name
        if sha(path) != digest:
            raise ValueError('compiled binary differs: ' + name)
        GUARD[str(path)] = digest
    for path in [Path(__file__).resolve(), args.binding, args.native_result, args.old_null_result,
                 REPO / 'scripts/benchmark-report.py', REPO / 'scripts/run-benchmark-matrix.py',
                 REPO / 'benchmarks/result-schema.json', REPO / 'benchmarks/runtime/tools/parent-bound-exec.py']:
        GUARD[str(path)] = sha(path)
    save(args.output / 'input-bindings.json', GUARD)
    schema = Draft7Validator(json.loads((REPO / 'benchmarks/result-schema.json').read_text()))
    def execute(command, env, label, timeout=15, expected=0):
        guard()
        wrapped = ['python3', '-B', str(REPO / 'benchmarks/runtime/tools/parent-bound-exec.py'), str(os.getpid())] + command
        try:
            result = owner.execute(wrapped, env, args.output, label, min(timeout, END - time.monotonic()))
        except ValueError:
            if expected == 0:
                raise
            result = args.output / (label + '.stdout')
        receipt = json.loads((args.output / (label + '.process.json')).read_text())
        if receipt.get('failure') or not receipt['parent_waited'] or receipt['exit_code'] != expected:
            raise ValueError('unexpected command or ownership outcome: ' + label)
        guard()
        return result
    env = owner.base_env()
    report = ['python3', '-B', str(REPO / 'scripts/benchmark-report.py')]
    results = []
    for cell in ['nb-send-seq', 'nb-fetch-bulk', 'nb-connect']:
        directory = args.output / cell
        job_env = env | {'NB_SERVE': str(args.binaries / 'nb-serve')}
        execute([str(args.binaries / 'runtime'), '--cell', cell, '--repetitions', '1', '--out', str(directory)],
                job_env, cell + '-run', 60)
        result = directory / (cell + '-rep0.result.json')
        data = json.loads(result.read_text())
        errors = [str(error) for error in schema.iter_errors(data)]
        save(args.output / (cell + '-schema.json'), dict(passed=not errors, errors=errors, sha256=sha(result)))
        if errors:
            raise ValueError('actual corrected result failed full schema: ' + cell)
        config = data['provenance']['config']
        if sha(config['path']) != config['sha256']:
            raise ValueError('configuration sidecar hash differs')
        if json.loads(Path(config['path']).read_text()) != config['effective_settings']:
            raise ValueError('configuration sidecar differs from result')
        ports = []
        for endpoint in data['provenance']['broker']['endpoints']:
            host, value = endpoint.rsplit(':', 1)
            with socket.socket() as stream:
                stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                stream.bind((host, int(value)))
            ports.append(dict(endpoint=endpoint, reusable=True))
        save(args.output / (cell + '-ports.json'), ports)
        execute(report + [str(result), '--json'], env, cell + '-report')
        results.append((cell, result, data))
    native = json.loads(args.native_result.read_text())
    if list(schema.iter_errors(native)):
        raise ValueError('retained original native result no longer validates')
    execute(report + [str(args.native_result), '--json'], env, 'retained-native-report')
    results.append(('retained-native', args.native_result, native))
    old = json.loads(args.old_null_result.read_text())
    old_errors = [str(error) for error in schema.iter_errors(old)]
    if not old_errors:
        raise ValueError('old incomplete null result was silently accepted')
    execute(report + [str(args.old_null_result), '--json'], env, 'retained-incomplete-null-report', expected=1)
    save(args.output / 'retained-incomplete-null-schema.json', dict(errors=old_errors, sha256=sha(args.old_null_result)))
    controls = []
    for label, original, data in results:
        changes = [('missing-config-path', ('provenance','config','path'), None),
                   ('wrong-broker-mode', ('provenance','broker','mode'), 'null' if label == 'retained-native' else 'kraft'),
                   ('false-phase', ('execution','phase'), 'banana' if label == 'retained-native' else 'steady_state'),
                   ('offered-under-accepted', ('outcomes','offered'), data['outcomes']['accepted'] - 1),
                   ('nonfinite-latency', ('measurements','latency','p99'), float('nan'))]
        for index, (name, keys, value) in enumerate(changes):
            candidate = copy.deepcopy(data)
            parent = candidate
            for key in keys[:-1]:
                parent = parent[key]
            if name == 'missing-config-path':
                del parent[keys[-1]]
            else:
                parent[keys[-1]] = value
            file = args.output / (label + '-control-' + name + '.json')
            save(file, candidate)
            schema_rejected = bool(list(schema.iter_errors(candidate)))
            if index < 3 and not schema_rejected:
                raise ValueError('schema did not reject actual changed result: ' + name)
            if index == 3 and schema_rejected:
                raise ValueError('semantic-only control unexpectedly violates schema')
            execute(report + [str(file), '--json'], env, label + '-control-' + name + '-report', expected=1)
            controls.append(dict(original=str(original), original_sha256=sha(original), candidate=str(file),
                candidate_sha256=sha(file), schema_rejected=schema_rejected, report_rejected=True))
    guard()
    save(args.output / 'summary.json', dict(compiler_source_commit=binding['source_commit'], actual_null_results=3,
        original_native_results_validated=1, original_incomplete_null_result_rejected=True,
        actual_changed_result_controls=len(controls), controls=controls, source_guards_passed=True,
        scope='Result-format qualification only. No performance comparison, scenario qualification or ranking.'))


if __name__ == '__main__':
    main()
