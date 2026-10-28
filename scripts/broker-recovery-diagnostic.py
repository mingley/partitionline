#!/usr/bin/env python3
"""Trace a retained recovery input with bounded logs; never qualify a campaign."""
import argparse
from collections import Counter, deque
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=3600)
    parser.add_argument('--strace', default='strace')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 86400 or not 0 < args.input.stat().st_size <= 65536:
        parser.error('duration must be 1..86400 seconds and input 1..65536 bytes')
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    (root / 'tmp').mkdir()
    (root / 'artifacts').mkdir()
    source = root / 'input.bin'
    source.write_bytes(args.input.read_bytes())
    read_fd, write_fd = os.pipe()
    command = [args.strace, '-f', '-qq', '-ttt', '-T', '-s', '64',
        '-e', 'trace=openat,write,close,unlink,fsync,fdatasync,ftruncate',
        '-o', f'/proc/self/fd/{write_fd}', str(args.binary.resolve()),
        '-runs=1000000000', '-timeout=2', '-rss_limit_mb=512', '-malloc_limit_mb=128',
        '-max_len=65536', '-print_final_stats=1', '-artifact_prefix=' + str(root / 'artifacts') + '/', str(source)]
    receipt = {'schema_version': 1, 'kind': 'fixed-input syscall diagnostic',
        'binary_sha256': digest(args.binary), 'input_sha256': digest(source),
        'strace_version': subprocess.check_output([args.strace, '--version'], text=True).splitlines()[0],
        'command': command, 'seconds_limit': args.seconds, 'campaign_qualified': False,
        'production_qualified': False, 'state': 'starting'}
    write(root / 'manifest.json', receipt)
    recent, slowest, counts = deque(maxlen=256), [], Counter()
    lock = threading.Lock()

    def consume():
        with os.fdopen(read_fd) as stream:
            for line in stream:
                # strace emits at most 64 bytes of user data per selected call.
                line = line[:4096].rstrip()
                match = re.search(r'\b([a-z_]+)\(.*<([0-9.]+)>$', line)
                with lock:
                    recent.append(line)
                    if match:
                        counts[match[1]] += 1
                        duration = float(match[2])
                        entry = (duration, line)
                        if len(slowest) < 256:
                            heapq.heappush(slowest, entry)
                        elif entry > slowest[0]:
                            heapq.heapreplace(slowest, entry)

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start = time.monotonic()

    def signal_group(pid, value):
        try:
            os.killpg(pid, value)
        except ProcessLookupError:
            pass

    with (root / 'fuzzer.log').open('x') as log:
        process = subprocess.Popen(command, env=dict(os.environ, TMPDIR=str(root / 'tmp')),
            stdout=log, stderr=subprocess.STDOUT, pass_fds=(write_fd,), start_new_session=True)
        os.close(write_fd)
        reader = threading.Thread(target=consume, daemon=True)
        reader.start()
        next_report = 0.0
        reason = 'worker_exit'
        while process.poll() is None:
            elapsed = time.monotonic() - start
            if stopping or elapsed >= args.seconds or (root / 'STOP').exists() or log.tell() > 4 * 1024 * 1024:
                reason = 'requested_stop' if stopping or (root / 'STOP').exists() else 'diagnostic_limit'
                signal_group(process.pid, signal.SIGTERM)
                break
            if elapsed >= next_report:
                with lock:
                    snapshot = {'elapsed_seconds': elapsed, 'syscalls': dict(counts),
                        'slowest_syscalls': sorted(slowest, reverse=True), 'recent_trace': list(recent)}
                write(root / 'status.json', snapshot)
                next_report = elapsed + 5
            time.sleep(0.1)
        try:
            code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            signal_group(process.pid, signal.SIGKILL)
            code = process.wait(timeout=5)
        reader.join(timeout=5)
    with lock:
        summary = {'state': 'failed' if code and reason == 'worker_exit' else 'stopped',
            'returncode': code, 'reason': reason, 'elapsed_seconds': time.monotonic() - start,
            'parent_waited': True, 'trace_reader_joined': not reader.is_alive(),
            'syscalls': dict(counts), 'slowest_syscalls': sorted(slowest, reverse=True),
            'recent_trace': list(recent), 'campaign_qualified': False, 'production_qualified': False}
    write(root / 'result.json', summary)
    print(json.dumps({key: summary[key] for key in ('state', 'reason', 'returncode', 'elapsed_seconds')}))
    if not summary['trace_reader_joined']:
        raise SystemExit('trace pipe did not close after joined worker')


if __name__ == '__main__':
    main()
