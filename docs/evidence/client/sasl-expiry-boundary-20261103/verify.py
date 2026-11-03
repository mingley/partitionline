#!/usr/bin/env python3
"""Check pinned source facts for the SASL expiry investigation; no Rust runs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import unittest

HERE = Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / 'manifest.json').read_text())
REPO = None
SOURCE = {}


def section(text, start, end):
    assert text.count(start) == 1, start
    after = text.split(start, 1)[1]
    assert end in after, end
    return after.split(end, 1)[0]


class SourceFacts(unittest.TestCase):
    def test_raw_produce_write_bypasses_expiry_guard(self):
        fire = section(SOURCE['src/producer.rs'], '    async fn fire(&mut self) -> Result<()> {', '    async fn wait_one(&mut self) -> Result<()> {')
        self.assertIn('.write_all_timeout(&self.write_buf, write_timeout)', fire)
        self.assertNotIn('is_session_expired', fire)
        self.assertLess(fire.index('assign_sequences('), fire.index('.write_all_timeout('))
        self.assertLess(fire.index('encode_produce_body_async('), fire.index('.write_all_timeout('))

    def test_expiry_check_before_encoding_and_async_lock(self):
        net = SOURCE['src/net.rs']
        send = section(net, '    pub async fn send_deadline(', '    pub async fn send(')
        self.assertLess(send.index('self.is_session_expired()'), send.index('self.write_request_deadline('))
        writer = section(net, '    pub async fn write_all_deadline(', '    pub async fn write_all_timeout(')
        self.assertIn('timeout_at(deadline.target(), stream.lock()).await', writer)
        self.assertIn('.run_io(write_all_pump(&mut *stream, &mut self.read_buf, bytes))', writer)
        self.assertNotIn('is_session_expired', writer)

    def test_first_poll_can_yield_before_application_write(self):
        pump = section(SOURCE['src/net.rs'], 'async fn write_all_pump<S>(', '\nasync fn ')
        yielded = pump.index('StalledScan::Yield => {')
        pending = pump.index('return Poll::Pending;', yielded)
        write = pump.index('.poll_write(cx, chunk)')
        self.assertLess(pending, write)
        self.assertIn('Poll::Pending => {', pump[write:])
        self.assertNotIn('session_expir', pump)

    def test_sasl_uses_shared_writer(self):
        net = SOURCE['src/net.rs']
        sasl = section(net, '    pub async fn roundtrip_sasl_deadline(', '    pub async fn roundtrip_sasl(')
        self.assertIn('self.write_request_deadline(', sasl)
        writer = section(net, '    async fn write_request_deadline(', '    pub async fn send_deadline(')
        self.assertIn('self.write_all_deadline(&payload, deadline)', writer)
        self.assertIn('.roundtrip_sasl(', SOURCE['src/protocol/sasl.rs'])

    def test_retirement_requeues_prior_inflight_while_open(self):
        producer = SOURCE['src/producer.rs']
        retire = section(producer, 'fn retire_worker_nudge(', '\nfn retire_closed_worker(')
        self.assertIn('worker.admission.close()', retire)
        drop = section(producer, 'impl Drop for Worker {', '\nimpl Worker {')
        self.assertIn('if !self.shared.closed.load(Ordering::SeqCst)', drop)
        self.assertIn('while let Some(inf) = self.in_flight.pop_front()', drop)
        self.assertIn('self.requeue_sync(inf.groups)', drop)
        wait = section(producer, '    async fn wait_one(&mut self) -> Result<()> {', '\n    fn requeue_sync(')
        self.assertIn('if e.is_retriable()', wait)
        self.assertIn('self.requeue(inf.groups).await', wait)

    def test_retry_sequences_can_be_rewound(self):
        producer = SOURCE['src/producer.rs']
        assign = section(producer, 'async fn assign_sequences(', '\nfn rewind_sequence(')
        self.assertIn('if sequences_already_assigned(pendings).await {', assign)
        self.assertIn('continue;', assign)
        rewind = section(producer, 'fn rewind_sequence(', '\nfn rollback_sequences(')
        self.assertIn('if *curr == base.saturating_add(count)', rewind)
        self.assertIn('*curr = base;', rewind)
        # A possibly accepted prior attempt has seq=0, len=1 and next=1.
        # The retry keeps the same producer ID, epoch and assigned sequence.
        # Rewinding it permits reuse of 0 for a different record.
        base, count, current = 0, 1, 1
        if current == base + count:
            current = base
        self.assertEqual(current, base)

    def test_local_pending_has_no_general_cancellation_owner(self):
        producer = SOURCE['src/producer.rs']
        fire = section(producer, '    async fn fire(&mut self) -> Result<()> {', '    async fn wait_one(&mut self) -> Result<()> {')
        grouped = fire.index('let (mut groups, shell) = group_pending(batch, &mut self.group_scratch).await;')
        write = fire.index('.write_all_timeout(&self.write_buf, write_timeout)')
        self.assertIn('.await', fire[grouped:write])
        self.assertNotIn('impl Drop for Pending {', producer)
        self.assertNotIn('impl Drop for ProduceGroups {', producer)
        enqueue = section(producer, '    async fn enqueue_one(', '    fn new_pending(')
        self.assertIn('reservation.active = false;', enqueue)
        fail = section(producer, 'fn fail_kept_pendings_sync(', '\nasync fn fail_kept_pendings(')
        self.assertIn('shared.release_buffer(', fail)


def main():
    global REPO
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    args = parser.parse_args()
    REPO = args.repo.resolve()
    for name, expected in MANIFEST['source_sha256'].items():
        out = subprocess.run(['git', 'show', MANIFEST['source_commit'] + ':' + name], cwd=REPO, check=True, capture_output=True).stdout
        actual = hashlib.sha256(out).hexdigest()
        if actual != expected:
            raise SystemExit('source hash mismatch: ' + name)
        SOURCE[name] = out.decode('utf-8')
    for name, expected in MANIFEST['package_sha256'].items():
        if hashlib.sha256((HERE / name).read_bytes()).hexdigest() != expected:
            raise SystemExit('package hash mismatch: ' + name)
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SourceFacts))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
