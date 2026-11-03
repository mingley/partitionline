"""Linux ownership of one serial CI command and its ordinary descendants.

Uses the unreaped-leader and scoped subreaper pattern from the retained x86
command-keeper controls. This is not a sandbox for arbitrary session changes.
"""
from contextlib import contextmanager
import ctypes
import os
from pathlib import Path
import signal
import subprocess
import time


def session_members(session):
    rows = []
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = path.read_text().rsplit(')', 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if int(fields[3]) == session:
            rows.append({'pid': int(path.parent.name), 'state': fields[0],
                'ppid': int(fields[1]), 'pgrp': int(fields[2]),
                'session': int(fields[3]), 'start': int(fields[19])})
    return rows


class OwnedCommand:
    def __init__(self, process, received):
        self.process = process
        self.signals = received
        self.cleanup_receipt = {'leader_pid': process.pid, 'leader_waited': False,
            'reaped_descendants': [], 'remaining_descendants': []}

    def finished(self):
        if self.signals:
            raise InterruptedError('controlled CI interruption: '+str(self.signals[0]))
        # Popen.poll()/wait() would release the PID that anchors this group.
        return os.waitid(os.P_PID, self.process.pid,
            os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    def close(self):
        process = self.process
        initial = session_members(process.pid)
        self.cleanup_receipt['observed_session_members'] = initial
        self.cleanup_receipt['descendants_present_at_close'] = [
            row for row in initial if row['pid'] != process.pid]
        # The leader has never been reaped, so its numeric group/session IDs
        # cannot be recycled while these signals are sent.
        for sig, grace in ((signal.SIGTERM, .25), (signal.SIGKILL, 3)):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                pass
            until = time.monotonic()+grace
            while True:
                remaining = []
                for row in session_members(process.pid):
                    if row['pid'] == process.pid:
                        continue
                    remaining.append(row)
                    # Orphans in this private session are adopted by this
                    # temporary subreaper. Reap only exact owned child PIDs.
                    if row['ppid'] == os.getpid():
                        if sig == signal.SIGKILL and row['state'] not in ('Z', 'X'):
                            try:
                                os.kill(row['pid'], signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                        try:
                            pid, status = os.waitpid(row['pid'], os.WNOHANG)
                        except ChildProcessError:
                            continue
                        if pid:
                            self.cleanup_receipt['reaped_descendants'].append({
                                'pid': pid, 'start': row['start'], 'wait_status': status})
                leader_done = os.waitid(os.P_PID, process.pid,
                    os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
                if leader_done and not remaining:
                    break
                if time.monotonic() >= until:
                    break
                time.sleep(.01)
            if leader_done and not remaining:
                break
        # Finish descendants before releasing the unreaped leader anchor.
        remaining = [row for row in session_members(process.pid) if row['pid'] != process.pid]
        self.cleanup_receipt['remaining_descendants'] = remaining
        if remaining:
            raise RuntimeError('owned command descendants did not join')
        code = process.wait(timeout=3)
        self.cleanup_receipt.update(leader_waited=True, leader_exit=code)


@contextmanager
def owned_command(argv, **popen_kwargs):
    """Yield process/finished()/cleanup_receipt; always close before returning.

    This helper is for one serial command in the main Python thread. It restores
    existing signal handlers and child-subreaper state after exact child cleanup.
    """
    library = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if library.prctl(37, ctypes.byref(previous), 0, 0, 0):
        raise OSError(ctypes.get_errno(), 'read child-subreaper state')
    if library.prctl(36, 1, 0, 0, 0):
        raise OSError(ctypes.get_errno(), 'enable child subreaper')
    received, handlers = [], {}
    owner, failure = None, None
    try:
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            handlers[sig] = signal.signal(sig, lambda number, frame: received.append(number))
        process = subprocess.Popen(argv, start_new_session=True, **popen_kwargs)
        owner = OwnedCommand(process, received)
        try:
            yield owner
        except BaseException as error:
            failure = error
            owner.cleanup_receipt['observer_exception'] = {
                'type': type(error).__name__, 'message': str(error)}
            raise
        finally:
            owner.close()
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if library.prctl(36, previous.value, 0, 0, 0):
            raise OSError(ctypes.get_errno(), 'restore child-subreaper state')
    if failure is None and received:
        raise InterruptedError('controlled CI interruption: '+str(received[0]))
