from pathlib import Path
p=Path('/workspace/work/client-sticky-performance-practical-review-02/benchmarks/sticky-partitioner/run-cell.py')
s=p.read_text()
start=s.index('def members(pgid):')
end=s.index('\ndef source_guard(',start)
s=s[:start]+'''class MembershipInspectionError(RuntimeError):
    """Unknown membership is not a verified empty owned process group."""


def members(pgid, owner_start_time=None):
    result = []
    try:
        directories = list(Path('/proc').iterdir())
    except OSError as error:
        raise MembershipInspectionError('proc enumeration errno=' + str(error.errno)) from error
    for directory in directories:
        if not directory.name.isdigit():
            continue
        try:
            raw = (directory / 'stat').read_text()
        except (FileNotFoundError, ProcessLookupError):
            continue  # Only an independently vanished PID can be absent.
        except OSError as error:
            raise MembershipInspectionError('stat unreadable pid=' + directory.name
                                            + ' errno=' + str(error.errno)) from error
        try:
            pid = int(directory.name)
            assert int(raw[:raw.index('(')].strip()) == pid
            fields = raw[raw.rfind(')') + 2:].split()
            assert len(fields) >= 20
            group, session, start_time = int(fields[2]), int(fields[3]), int(fields[19])
            if group != pgid:
                continue
            assert session == pgid, 'Owned start_new_session group must retain session identity'
            assert owner_start_time is None or start_time >= owner_start_time
            assert pid != pgid or owner_start_time is None or start_time == owner_start_time
            result.append({'pid': pid, 'group': group, 'session': session,
                           'state': fields[0], 'start_time_ticks': start_time})
        except (AssertionError, IndexError, ValueError) as error:
            raise MembershipInspectionError('stat malformed or ownership identity changed pid='
                                            + directory.name) from error
    return result


def stop(pgid, owner_start_time=None):
    current = members(pgid, owner_start_time)
    if current:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return current

''' +s[end:]
old='''        history = [{'event': 'started', 'owned_pgid': process.pid, 'members': members(process.pid)}]
        reasons = []
        if cancel_signals:
            reasons.append('host cancellation signals=' + str(cancel_signals))
            stop(process.pid)
        samples = []'''
new='''        history = []
        reasons = []
        owner_start_time = None
        def inspect_owned(event):
            try:
                current = members(process.pid, owner_start_time)
                snapshot = {'event': event, 'owned_pgid': process.pid,
                            'membership_verified': True, 'members': current}
            except MembershipInspectionError as error:
                reasons.append('membership unverified: ' + str(error))
                snapshot = {'event': event, 'owned_pgid': process.pid,
                            'membership_verified': False, 'members': None,
                            'inspection_error': str(error)}
            history.append(snapshot)
            return snapshot
        def stop_owned():
            try:
                observed = stop(process.pid, owner_start_time)
                history.append({'event': 'group stop after verified inspection',
                                'owned_pgid': process.pid, 'members': observed})
            except MembershipInspectionError as error:
                reasons.append('group signal refused: ' + str(error))
                history.append({'event': 'group stop refused; membership unknown',
                                'owned_pgid': process.pid, 'members': None,
                                'inspection_error': str(error)})
                # Popen still owns its unreaped direct child. Do not claim descendant closure.
                if process.poll() is None:
                    try:
                        process.kill()
                        history.append({'event': 'known direct child only killed', 'pid': process.pid})
                    except OSError as direct_error:
                        reasons.append('direct child kill errno=' + str(direct_error.errno))
        initial = inspect_owned('started')
        if initial['membership_verified']:
            leaders = [member for member in initial['members'] if member['pid'] == process.pid]
            if leaders:
                owner_start_time = leaders[0]['start_time_ticks']
            elif initial['members']:
                reasons.append('session leader identity unavailable before descendants inspection')
        if reasons or cancel_signals:
            if cancel_signals:
                reasons.append('host cancellation signals=' + str(cancel_signals))
            stop_owned()
        samples = []'''
assert old in s;s=s.replace(old,new)
# Every child-control stop must retain an explicit refusal rather than drop a monitor exception.
segment=s.index('        samples = []',s.index('        process = subprocess.Popen'))
prefix=s[:segment];suffix=s[segment:].replace('stop(process.pid)','stop_owned()')
s=prefix+suffix
old='''        current = members(process.pid)
        if current:
            stop_owned()
            closure = time.monotonic() + 5
            while current and time.monotonic() < closure:
                time.sleep(0.1)
                current = members(process.pid)
        for reader in readers:
            reader.join(timeout=5)
        history.append({'event': 'closed', 'owned_pgid': process.pid, 'members': current})'''
new='''        closure_snapshot = inspect_owned('closure inspection')
        if not closure_snapshot['membership_verified'] or closure_snapshot['members']:
            stop_owned()
            closure_deadline = time.monotonic() + 5
            while (not closure_snapshot['membership_verified'] or closure_snapshot['members']) and time.monotonic() < closure_deadline:
                time.sleep(0.1)
                closure_snapshot = inspect_owned('closure retry')
        for reader in readers:
            reader.join(timeout=5)
        closure_verified = closure_snapshot['membership_verified'] and not closure_snapshot['members']
        history.append({'event': 'closed', 'owned_pgid': process.pid,
                        'closure_verified': closure_verified,
                        'members': closure_snapshot['members']})'''
assert old in s;s=s.replace(old,new)
s=s.replace("'host_cancellation_signals': cancel_signals}", "'host_cancellation_signals': cancel_signals,\n              'closure_verified': closure_verified}")
s=s.replace('assert all(not reader.is_alive() for reader in readers) and not current','assert all(not reader.is_alive() for reader in readers) and closure_verified')
p.write_text(s)
print('WORK membership source corrected; no proc scan/process signal/runner executed')
