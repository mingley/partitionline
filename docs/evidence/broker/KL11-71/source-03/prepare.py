#!/usr/bin/env python3
"""Test-only refinement of immutable source02; no compiler/runtime."""
from pathlib import Path
import hashlib
import json
import os
import stat

HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'source-02'


def identity(path):
    s = path.lstat()
    assert stat.S_ISREG(s.st_mode)
    b = path.read_bytes()
    return {'path': str(path), 'bytes': len(b), 'mode': stat.S_IMODE(s.st_mode),
            'sha256': hashlib.sha256(b).hexdigest(),
            'git_blob_sha1': hashlib.sha1(f'blob {len(b)}\0'.encode() + b).hexdigest()}


def replace_once(text, before, after):
    assert text.count(before) == 1
    return text.replace(before, after, 1)


def main():
    os.umask(0o077)
    prior = identity(PRIOR / 'handoff.json')
    assert prior['sha256'] == 'b2fff7b8d0491a70f94a5df5dca9bd8242cde1613eef89df1d8e74d2f2dd3d97'
    packet = json.loads((PRIOR / 'handoff.json').read_text())
    guard = [identity(Path(row['candidate']['path'])) for row in packet['files']]
    assert guard == [row['candidate'] for row in packet['files']]
    path = 'partitionline-broker/tests/sasl_sessions.rs'
    old = Path(next(row['candidate']['path'] for row in packet['files'] if row['repo_path'] == path))
    test = old.read_text()
    test = replace_once(test, 'struct ReadyAfterExpiryProbe;',
                        '#[derive(Default)]\nstruct ReadyAfterExpiryProbe {\n    calls: AtomicUsize,\n}')
    tail_start = test.index('impl Handler for ReadyAfterExpiryProbe')
    prefix, tail = test[:tail_start], test[tail_start:]
    tail = replace_once(tail,
        '    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {',
        '    #[expect(\n        clippy::disallowed_methods,\n'
        '        reason = "deliberate synchronous work exercises timeout_at Ready completion after expiry"\n'
        '    )]\n'
        '    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {')
    tail = replace_once(tail, '        assert!(peer.identity().is_some());',
                        '        assert!(peer.identity().is_some());\n'
                        '        self.calls.fetch_add(1, Ordering::SeqCst);')
    tail = replace_once(tail, '    let store = store(&path).await?;\n    let mut server',
                        '    let store = store(&path).await?;\n'
                        '    let probe = Arc::new(ReadyAfterExpiryProbe::default());\n    let mut server')
    tail = replace_once(tail, '        Arc::new(ReadyAfterExpiryProbe),', '        probe.clone(),')
    tail = replace_once(tail,
        '    assert_eq!(report.accepted_connections, report.joined_connections);',
        '    assert_eq!(report.accepted_connections, report.joined_connections);\n'
        '    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);')
    target = HERE / 'candidate' / path
    target.parent.mkdir(parents=True, exist_ok=True)
    assert not target.exists()
    target.write_text(prefix + tail)
    os.chmod(target, 0o600)
    updated = identity(target)
    assert guard == [identity(Path(row['candidate']['path'])) for row in packet['files']]
    for row in packet['files']:
        if row['repo_path'] == path:
            row['candidate'] = updated
    packet.update({
        'supersedes_handoff_sha256': prior['sha256'],
        'previous_source02_before': guard,
        'previous_source02_after': guard,
        'test_only_followup': 'Narrow intentional blocking lint expectation and actual handler-entry assertion prevent vacuous no-reply success.',
        'test_followup_controls': [
            {'name': 'single_handler_body_counts_actual_entry', 'passed': tail.count('self.calls.fetch_add(1, Ordering::SeqCst)') == 1},
            {'name': 'regression_requires_exact_handler_dispatch', 'passed': 'assert_eq!(probe.calls.load(Ordering::SeqCst), 1)' in tail},
            {'name': 'blocking_lint_exception_is_local_and_reasoned', 'passed': tail.count('clippy::disallowed_methods') == 1},
            {'name': 'same_actual_100ms_300ms_transport_case', 'passed': 'Duration::from_millis(100)' in tail and 'Duration::from_millis(300)' in tail},
        ],
        'test_followup_recipe': identity(HERE / 'prepare.py'),
        'actual_cargo_commands': 0, 'actual_runtime_executions': 0, 'actual_rust_tests': 0,
        'actual_sdk_executions': 0,
    })
    assert all(row['passed'] for row in packet['test_followup_controls'])
    out = HERE / 'handoff.json'
    assert not out.exists()
    out.write_text(json.dumps(packet, indent=2, sort_keys=True) + '\n')
    os.chmod(out, 0o600)
    print(json.dumps({'handoff': identity(out), 'new_test': updated,
                      'unchanged_transport': next(row['candidate'] for row in packet['files']
                                                 if row['repo_path'] == 'partitionline-broker/src/transport.rs'),
                      'actual_rust_or_sdk_runs': 0}))


if __name__ == '__main__':
    main()
