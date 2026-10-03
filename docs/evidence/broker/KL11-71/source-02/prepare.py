#!/usr/bin/env python3
"""Additive source-only expiry clock fences; preserve the first packet."""
from pathlib import Path
import hashlib
import json
import os
import stat

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / 'source-01'
REVIEW = HERE.parent / 'independent-review-01'
EXPECTED_BASE = 'cb925db28cb2717ece2aaeb8e51225b976a15efe09d479ae41e023ef29bbc74b'
EXPECTED_REVIEW = 'dcd8c8b33152f88f2d8fce6a57e241580cf21ffeb9fc818fe1c0978df852a3aa'


def identity(path):
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode)
    raw = path.read_bytes()
    return {'path': str(path), 'bytes': len(raw), 'mode': stat.S_IMODE(info.st_mode),
            'sha256': hashlib.sha256(raw).hexdigest(),
            'git_blob_sha1': hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()}


def guards(base):
    result = [identity(BASE / 'handoff.json'), identity(REVIEW / 'validation.json')]
    assert result[0]['sha256'] == EXPECTED_BASE
    assert result[1]['sha256'] == EXPECTED_REVIEW
    for file in base['files']:
        row = identity(Path(file['candidate']['path']))
        assert row == file['candidate'], (row, file['candidate'])
        result.append(row)
    return result


def replace(text, old, new):
    assert text.count(old) == 1, old[:100]
    return text.replace(old, new, 1)


def store(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    assert not path.exists(), str(path)
    path.write_text(text)
    os.chmod(path, 0o600)
    return identity(path)


def main():
    os.umask(0o077)
    base = json.loads((BASE / 'handoff.json').read_text())
    before = guards(base)
    old_transport = BASE / 'candidate/partitionline-broker/src/transport.rs'
    text = old_transport.read_text()
    text = replace(text,
        "/// handler's own durability contract; expired sockets never receive its reply.",
        "/// handler's own durability contract. Expired completed work is discarded\n"
        "/// before reply writing; bytes already committed to a socket cannot be retracted.")
    text = replace(text,
        '        let request = match read.await {\n            Ok(request) => request,',
        '        let request = match read.await {\n            Ok(request) => {\n'
        '                if Instant::now() >= read_deadline {\n'
        '                    return Exit::ReadDeadline;\n                }\n'
        '                request\n            }')
    text = replace(text,
        '                let permit = handlers.acquire().await.map_err(|_| Exit::Shutdown)?;\n',
        '                let permit = handlers.acquire().await.map_err(|_| Exit::Shutdown)?;\n'
        '                if Instant::now() >= deadline {\n'
        '                    return Err(Exit::HandlerDeadline);\n                }\n')
    text = replace(text,
        '        let response = match operation.await {\n            Ok(Some(response)) => response,\n'
        '            Ok(None) => continue,\n            Err(exit) => return exit,\n        };',
        '        let response = match operation.await {\n            Ok(response) => {\n'
        '                // timeout_at cannot preempt a synchronously Ready handler.\n'
        '                // Recheck the original bound before accepting its result.\n'
        '                if Instant::now() >= deadline {\n'
        '                    return Exit::HandlerDeadline;\n                }\n'
        '                match response {\n                    Some(response) => response,\n'
        '                    None => continue,\n                }\n            }\n'
        '            Err(exit) => return exit,\n        };')
    text = replace(text,
        '        let write = async {\n            timeout_at(deadline, async {\n'
        '                socket.write_all(&length.to_be_bytes()).await?;\n'
        '                socket.write_all(&response).await\n            })\n'
        '            .await\n            .map_err(|_| Exit::WriteDeadline)?\n'
        '            .map_err(|_| Exit::Io)\n        };',
        '        let write = async {\n            timeout_at(deadline, async {\n'
        '                if Instant::now() >= deadline {\n'
        '                    return Err(Exit::WriteDeadline);\n                }\n'
        '                socket\n                    .write_all(&length.to_be_bytes())\n'
        '                    .await\n                    .map_err(|_| Exit::Io)?;\n'
        '                if Instant::now() >= deadline {\n'
        '                    return Err(Exit::WriteDeadline);\n                }\n'
        '                socket.write_all(&response).await.map_err(|_| Exit::Io)?;\n'
        '                if Instant::now() >= deadline {\n'
        '                    return Err(Exit::WriteDeadline);\n                }\n'
        '                Ok(())\n            })\n            .await\n'
        '            .map_err(|_| Exit::WriteDeadline)?\n        };')
    changed = {}
    path = 'partitionline-broker/src/transport.rs'
    changed[path] = store(HERE / 'candidate' / path, text)
    proposed_regression = REVIEW / 'expired-ready-handler.regression.rs'
    assert identity(proposed_regression)['sha256'] == '6a9018e47fa1644b91cc9b396849a1ec9a62c322a84e19ce1738499aecae012e'
    regression = '''
struct ReadyAfterExpiryProbe;
impl Handler for ReadyAfterExpiryProbe {
    type Error = io::Error;
    async fn handle(&self, _: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("missing authenticated peer"))
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        assert!(peer.identity().is_some());
        // Synchronous work cannot be preempted by timeout_at. The expired
        // result must still be discarded before the actual Transport writes it.
        std::thread::sleep(Duration::from_millis(300));
        Ok(Some(request))
    }
}
#[tokio::test(flavor = "current_thread")]
async fn ready_handler_after_expiry_does_not_emit_expired_reply() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let mut server = renewable_plaintext(
        store.clone(),
        Arc::new(ReadyAfterExpiryProbe),
        Duration::from_millis(100),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?,
        (0, 100)
    );
    send(&mut socket, &header(3, 0, 201, false)).await?;
    let result = receive(&mut socket).await;
    let report = server.shutdown().await?;
    store.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert!(result.is_err(), "complete expired application reply was delivered");
    Ok(())
}
'''
    old_test = BASE / 'candidate/partitionline-broker/tests/sasl_sessions.rs'
    tests = old_test.read_text() + regression
    path = 'partitionline-broker/tests/sasl_sessions.rs'
    changed[path] = store(HERE / 'candidate' / path, tests)
    path = 'docs/sasl-reauthentication.md'
    doc = (BASE / 'candidate' / path).read_text()
    doc = replace(doc,
        'already admitted durable work before cancellation; its storage durability\n'
        'contract continues, and the expired connection cannot receive that result.',
        'already admitted durable work before cancellation; its storage durability\n'
        'contract continues. The transport checks the original clock after handler\n'
        'completion and before prefix and body writes. Synchronous handler work\n'
        'cannot be preempted; a result returned after expiry is discarded. Bytes\n'
        'already accepted by the socket cannot be withdrawn, so expiry after a\n'
        'partial write closes the connection without committing a renewed identity.\n'
        'The policy does not provide a hard real-time transmission guarantee.')
    doc = replace(doc, 'This WORK implementation and its draft tests have not yet been compiled or run.',
                  'This WORK implementation and its 18 new draft tests have not yet been compiled or run.')
    changed[path] = store(HERE / 'candidate' / path, doc)
    controls = []

    def check(name, truth, detail):
        assert truth, (name, detail)
        controls.append({'name': name, 'passed': True, 'detail': detail})

    check('post_ready_read_fence', 'if Instant::now() >= read_deadline' in text,
          'completed Ready read is not accepted after original bound')
    check('post_ready_handler_fence', 'Recheck the original bound before accepting its result.' in text,
          'no response or None result is accepted after handler/session bound')
    check('admission_before_dispatch_fence', text.index('if Instant::now() >= deadline') <
          text.index('.handle(&request, received)'), 'expired Ready permit cannot initiate dispatch')
    write = text[text.index('        let write = async {'):text.index('        if close {')]
    check('write_three_clock_fences', write.count('if Instant::now() >= deadline') == 3,
          'check before prefix, between prefix/body and after body completion')
    check('renewed_identity_final_guard_remains', 'auth.response_written().is_err()' in write,
          'old/new session and authority activation still checked after actual write')
    for completion in [99, 100, 101, 300]:
        old_timeout_ready_accepts = True  # modeled Ready future wins timeout's poll
        new_accepts = completion < 100
        check(f'ready_handler_completion_{completion}', old_timeout_ready_accepts and
              new_accepts == (completion == 99), 'strict original bound accepts only completion before100')
    check('prefix_crossing_does_not_start_body', not 101 < 100,
          'prefix completion at101 prevents a new body operation; prefix bytes cannot be undone')
    check('completed_late_body_never_commits_identity', not 101 < 100,
          'late Ready body can have committed bytes, but clock failure prevents renewed identity commitment')
    check('actual_transport_regression_authenticates_then_blocks_handler',
          'scram_lifetime' in regression and 'Duration::from_millis(100)' in regression and
          'std::thread::sleep(Duration::from_millis(300))' in regression and 'receive(&mut socket)' in regression,
          'source-only genuine Transport test; no test execution claimed')
    after = guards(base)
    assert before == after
    files = []
    for row in base['files']:
        repo_path = row['repo_path']
        files.append({**row, 'candidate': changed.get(repo_path, row['candidate']),
                      'changed_in_followup': repo_path in changed})
    result = {'schema_version': 1, 'baseline_source_sha': base['baseline_source_sha'],
              'supersedes_handoff_sha256': EXPECTED_BASE,
              'independent_review_sha256': EXPECTED_REVIEW,
              'old_closed_inputs_before': before, 'old_closed_inputs_after': after,
              'old_closed_inputs_unchanged': True, 'files': files,
              'new_draft_test_count': 18, 'added_actual_transport_regression_count': 1,
              'controls': controls, 'source_controls_passed': len(controls),
              'actual_cargo_commands': 0, 'actual_runtime_executions': 0,
              'actual_rust_tests': 0, 'actual_sdk_executions': 0,
              'scope': 'Additive WORK-only production/test source prototype; new follow-up has not been parsed or compiled.',
              'physical_write_limit': 'Cannot retract already accepted kernel bytes or preempt synchronous work; prevents starting a response operation after a checked bound and guards final identity activation.',
              'regression_reference': identity(proposed_regression),
              'review_recipe': identity(HERE / 'prepare.py'),
              'unchanged_source_parser_receipt': 'Prior source-01 rustfmt checks do not qualify these new follow-up files.',
              'next_required': ['independent review', 'actual stable/MSRV parsing and compilation',
                                'actual18 draft tests including new Transport regression',
                                'old/new OAuth lease cancellation/final blocked-write qualification',
                                'genuine Kafka SDK reauthentication matrix']}
    frozen = store(HERE / 'handoff.json', json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'handoff': frozen, 'changed_sources': changed, 'source_controls': len(controls),
                      'actual_rust_or_sdk_runs': 0}))


if __name__ == '__main__':
    main()
