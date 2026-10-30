"""Apply only the recorded main-02ce test lint changes to an isolated snapshot."""
import difflib
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
rows = json.loads((ROOT/'diagnostics.json').read_text())
by_path = {}
for row in rows:
    by_path.setdefault(row['path'], []).append(row)


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(('expected one target', text.count(old), old))
    return text.replace(old, new, 1)


for path, diagnostics in by_path.items():
    source = (ROOT/'baseline'/path).read_text()
    lines = source.splitlines(keepends=True)
    seen = set()
    for row in diagnostics:
        line = row['line']-1
        if line in seen:
            continue
        seen.add(line)
        old = lines[line]
        new = old
        error = row['error']
        if 'casting' in error:
            if 'i32::from_be_bytes(size_buf).max(0) as usize' in new:
                new = new.replace('i32::from_be_bytes(size_buf).max(0) as usize',
                    'usize::try_from(i32::from_be_bytes(size_buf).max(0))\n'+
                    re.match(r'\s*', old)[0]+'    .expect("nonnegative test frame size fits usize")')
            elif 'same type' in error:
                new = new.replace('index as i32', 'index')
            elif 'index as usize' in new:
                new = new.replace('index as usize', 'usize::try_from(index).expect("nonnegative test index")')
            elif 'partitions as usize' in new:
                new = new.replace('partitions as usize', 'usize::try_from(partitions).expect("nonnegative test partition count")')
            elif 'partition as usize' in new:
                new = new.replace('partition as usize', 'usize::try_from(partition).expect("nonnegative test partition")')
            elif 'index as u64' in new:
                new = new.replace('(index as u64)', 'u64::try_from(index).expect("nonnegative test index")')
            elif 'index as u128' in new:
                new = new.replace('(index as u128)', 'u128::try_from(index).expect("nonnegative test index")')
            elif 'index as i32' in new:
                new = new.replace('index as i32', 'i32::try_from(index).expect("test index fits i32")')
            elif 'state as u8' in new:
                # The xorshift generator intentionally emits the low byte.
                new = new.replace('state as u8', 'state.to_le_bytes()[0]')
            else:
                raise ValueError(('unhandled cast', row))
        elif 'unnecessary use' in error:
            new = re.sub(r'([\w.]+)\.get\((.*?)\)\.is_none\(\)', r'!\1.contains_key(\2)', new)
            new = re.sub(r'([\w.]+)\.get\((.*?)\)\.is_some\(\)', r'\1.contains_key(\2)', new)
        elif 'non-binding `let`' in error:
            new = new.replace('let _ =', 'let _unused_result =')
        elif 'redundant pattern matching' in error:
            new = new.replace('matches!(saw, Err(_))', 'saw.is_err()')
        elif 'let-binding has unit value' in error:
            new = new.replace('let _encoded = ', '')
        else:
            continue
        if old == new:
            raise ValueError(('diagnostic did not change', row))
        lines[line] = new
    text = ''.join(lines)
    if path == 'src/admin.rs':
        text = replace_once(text, 'assert!(pending\n            .get(0)', 'assert!(pending\n            .first()')
    if path == 'src/consumer.rs':
        text = replace_once(text,
            'let mut session = BrokerFetchSession::default();\n        session.metadata = FetchMetadata::new(42, 3);\n        session.awaiting_response = true;',
            'let session = BrokerFetchSession {\n            metadata: FetchMetadata::new(42, 3),\n            awaiting_response: true,\n            ..BrokerFetchSession::default()\n        };')
        for name, value in [('session', 'true'), ('clean', 'false'), ('fresh', 'true')]:
            text = replace_once(text,
                f'let mut {name} = BrokerFetchSession::default();\n        {name}.id_names_dirty = {value};',
                f'let mut {name} = BrokerFetchSession {{\n            id_names_dirty: {value},\n            ..BrokerFetchSession::default()\n        }};')
        old = 'let mut session = BrokerFetchSession::default();\n        session.version = Some(12);\n        session.metadata = FetchMetadata::new(7, 1);\n        session.stamp = 1;'
        if text.count(old) != 2:
            raise ValueError('expected two prepared session initializers')
        text = text.replace(old,
            'let mut session = BrokerFetchSession {\n            version: Some(12),\n            metadata: FetchMetadata::new(7, 1),\n            stamp: 1,\n            ..BrokerFetchSession::default()\n        };')
        # Own the pinned future so the existing explicit drop actually cancels
        # it instead of dropping only a Pin<&mut Future> reference.
        text = replace_once(text,
            'let started = Instant::now();\n        let mut watching = std::pin::pin!(consumer.watch_muted_fetch_sockets(&[1]));',
            'let started = Instant::now();\n        let mut watching = Box::pin(consumer.watch_muted_fetch_sockets(&[1]));')
        text = replace_once(text,
            'let closing = send_fetch_session_close(\n            &mut conn,\n            &session,\n            Some(Instant::now() + Duration::from_millis(400)),\n            &wakeup_tx,\n            &cfg,\n            Instant::now() + Duration::from_secs(2),\n        );\n        tokio::pin!(closing);',
            'let mut closing = Box::pin(send_fetch_session_close(\n            &mut conn,\n            &session,\n            Some(Instant::now() + Duration::from_millis(400)),\n            &wakeup_tx,\n            &cfg,\n            Instant::now() + Duration::from_secs(2),\n        ));')
    if path == 'src/net.rs':
        anchor = '/// One ready chunk, then a pause, so a read loop that never yields can be timed.\n'
        helper = '''// These test doubles model a ready IO call that takes wall time inside one
// poll. An async timer would yield and stop testing the caller's poll budget.
#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "test IO deliberately spends wall time inside a ready synchronous poll"
)]
fn stall_ready_test_io(duration: Duration) {
    std::thread::sleep(duration);
}

'''
        text = replace_once(text, anchor, helper+anchor)
        if text.count('std::thread::sleep(') != 4:
            raise ValueError('expected three deliberately costly ready-IO calls')
        text = text.replace('std::thread::sleep(self.pause.saturating_mul(units))',
            'stall_ready_test_io(self.pause.saturating_mul(units))')
        text = text.replace('std::thread::sleep(Duration::from_millis(2).saturating_mul(units))',
            'stall_ready_test_io(Duration::from_millis(2).saturating_mul(units))')
        text = replace_once(text,
            '            loop {\n                tokio::select! {\n                    biased;\n                    _ = conn.peer_left(), if !conn.is_closed() => break,\n                    _ = tokio::time::sleep(Duration::from_millis(50)) => break,\n                }\n            }',
            '            tokio::select! {\n                biased;\n                _ = conn.peer_left(), if !conn.is_closed() => {},\n                _ = tokio::time::sleep(Duration::from_millis(50)) => {},\n            }')
    if path == 'src/producer.rs':
        text = replace_once(text,
            'let mut sample = ProduceGroupScratch::default();\n        sample.partition_order = vec![3, 1];',
            'let mut sample = ProduceGroupScratch {\n            partition_order: vec![3, 1],\n            ..ProduceGroupScratch::default()\n        };')
        text = replace_once(text, 'use std::sync::Mutex as StdMutex;', 'use parking_lot::Mutex as SlotMutex;')
        text = text.replace('StdMutex', 'SlotMutex')
        text = replace_once(text,
            'peer_slot.lock().unwrap_or_else(|e| e.into_inner()).clone()',
            'peer_slot.lock().clone()')
        text = replace_once(text,
            '*slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&shared));',
            '*slot.lock() = Some(Arc::clone(&shared));')
    (ROOT/'candidate'/path).write_text(text)

patch = ''.join(''.join(difflib.unified_diff(
    (ROOT/'baseline'/path).read_text().splitlines(keepends=True),
    (ROOT/'candidate'/path).read_text().splitlines(keepends=True),
    fromfile='a/'+path, tofile='b/'+path, n=3)) for path in sorted(by_path))
(ROOT/'test-lint-cleanup.patch').write_text(patch)
(ROOT/'preparation-receipt.json').write_text(json.dumps(dict(
    base='02ce806d7', executed_builds_or_checks=False, diagnostic_rows=len(rows),
    patch_sha256=hashlib.sha256(patch.encode()).hexdigest(),
    files={path:hashlib.sha256((ROOT/'candidate'/path).read_bytes()).hexdigest() for path in sorted(by_path)},
    note='Static candidate only. Parent must format and run exact all-target Clippy/test checks after correctness patches are integrated.'),indent=2)+'\n')
print('Prepared',len(rows),'diagnostics across',len(by_path),'files; no Rust tool invoked.')
