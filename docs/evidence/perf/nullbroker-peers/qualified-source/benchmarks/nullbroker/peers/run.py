#!/usr/bin/env python3
"""Run one bounded SDK Produce/seeded-Fetch compatibility check on Linux."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broker', type=Path, required=True)
    parser.add_argument('--command', type=Path, required=True,
                        help='JSON argument array; {bootstrap} is replaced with the address')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-binding', type=Path, required=True)
    args = parser.parse_args()
    binding = json.loads(args.source_binding.read_text())
    if binding['binary_sha256'] != sha(args.broker):
        raise ValueError('broker binary does not match its source binding')
    command = json.loads(args.command.read_text())
    if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
        raise ValueError('command must be a nonempty string argument array')
    args.output.mkdir(parents=True, exist_ok=False)
    out = args.output.resolve()
    wrapper = Path(__file__).resolve().parents[2] / 'runtime/tools/parent-bound-exec.py'
    if not wrapper.is_file():
        raise ValueError('parent-bound execution helper is required')
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        port = reserved.getsockname()[1]
    address = f'127.0.0.1:{port}'
    broker_command = [str(args.broker.resolve()), '--bind', address, '--partitions', '1',
                      '--seconds', '20', '--artifact', str(out / 'broker.json'),
                      '--trace-api-versions', 'true', '--fetch-seed', str(0x5eed0001),
                      '--fetch-records', '512', '--fetch-batch-records', '128',
                      '--fetch-payload-bytes', '100', '--fetch-headers', '0',
                      '--fetch-codec', 'none', '--fetch-abort-every', '0',
                      '--nodes', '1', '--dead-nodes', '0', '--fault-rate-per-million', '0']
    peer_command = [x.replace('{bootstrap}', address) for x in command]
    children = []
    receipts = []
    joined = set()
    error = None
    started = time.monotonic()

    def launch(argv, stdout, stderr):
        process = subprocess.Popen([sys.executable, str(wrapper), str(os.getpid()), *argv],
                                   stdout=stdout, stderr=stderr, start_new_session=True)
        children.append((process, argv))
        return process

    def join(process, argv, budget):
        result = process.wait(timeout=budget)
        joined.add(process.pid)
        receipts.append(dict(pid=process.pid, command=argv, exit=result, joined=True))
        return result

    try:
        with (out / 'broker.log').open('wb') as broker_log:
            broker = launch(broker_command, broker_log, subprocess.STDOUT)
            deadline = time.monotonic() + 3
            while True:
                try:
                    with socket.create_connection(('127.0.0.1', port), .1):
                        break
                except OSError:
                    if broker.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError('broker did not become ready')
                    time.sleep(.01)
            with (out / 'peer.json').open('wb') as stdout, (out / 'peer.log').open('wb') as stderr:
                peer = launch(peer_command, stdout, stderr)
                peer_exit = join(peer, peer_command, 15)
            broker_exit = join(broker, broker_command, 22)
        if peer_exit or broker_exit:
            raise ValueError(f'peer exit {peer_exit}; broker exit {broker_exit}')
        result = json.loads((out / 'peer.json').read_text())
        report = json.loads((out / 'broker.json').read_text())
        if (result['acknowledged'], result['validated_fetch'], result['validation_failures']) != (512, 512, 0):
            raise ValueError('peer counts or validation failures')
        if result['fetch_source'] != 'seeded-independent-of-produce':
            raise ValueError('fixture behavior must be explicit')
        if report['accepted_records'] != 512 or sum(report['validation_failures'].values()):
            raise ValueError('broker acceptance or validation failures')
        if report['end_offsets'] != {'nullbroker-peer/0': 512}:
            raise ValueError('Produce end offsets')
        versions = report['api_versions']
        if not all(any(x['api_key'] == key and x['requests'] > 0 for x in versions) for key in (0, 1, 3)):
            raise ValueError('missing observed Produce, Fetch or Metadata request')
        with socket.socket() as rebound:
            rebound.bind(('127.0.0.1', port))
        receipts.append(dict(port=port, rebound=True))
        (out / 'validated.json').write_text(json.dumps(dict(
            scope='functional-compatibility', peer=result, observed_versions=versions,
            broker_sha256=sha(args.broker), source_binding=binding,
            fetch_source='seeded-independent-of-produce', performance_claim=False), indent=2) + '\n')
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        for process, argv in children:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            if process.pid not in joined:
                join(process, argv, 3)
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                receipts.append(dict(process_group=process.pid, empty=True))
            else:
                os.killpg(process.pid, signal.SIGKILL)
                error = error or "process group retained descendants"
        (out / 'receipts.json').write_text(json.dumps(dict(
            elapsed_seconds=time.monotonic()-started, commands=receipts, error=error,
            budgets=dict(peer_seconds=15, broker_seconds=20, readiness_seconds=3)), indent=2) + '\n')
    if error:
        raise SystemExit(error)


if __name__ == '__main__':
    main()
