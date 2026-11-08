"""Executable design model; NOT connected to the producer or a Kafka broker.

Every method call represents one atomic owner transition, not a Tokio task,
socket, user callback, allocator, or broker operation. The model deliberately
refuses retry/identity progress after an unknown write. It explores ownership,
not Kafka idempotence or production readiness.
"""
from collections import deque
from dataclasses import dataclass
import itertools
import random
import unittest

MAX_SEQUENCE = 1 << 31
MAX_U64 = (1 << 64) - 1


def earlier(left, right):
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right, key=lambda item: item[0])


class Exhausted(Exception):
    pass


class CompletionIntervals:
    """Completed admission IDs, compressed around incomplete holes."""
    def __init__(self):
        self.prefix = 0
        self.intervals = []

    def finish(self, ticket):
        if ticket <= self.prefix or any(lo <= ticket <= hi for lo, hi in self.intervals):
            raise ValueError("completion repeated")
        all_intervals = sorted(self.intervals + [(ticket, ticket)])
        merged = []
        for lo, hi in all_intervals:
            if merged and lo <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
            else:
                merged.append((lo, hi))
        while merged and merged[0][0] == self.prefix + 1:
            self.prefix = merged.pop(0)[1]
        self.intervals = merged


@dataclass
class Record:
    ticket: int
    partition: str
    data: bytes
    deadline: int
    cohort: object = None
    explicit_timestamp: object = None
    timestamp: object = None
    batch: object = None
    outcome: object = None
    callback: str = "none"
    callback_lease: object = None


@dataclass
class Batch:
    key: int
    partition: str
    epoch: int
    base: int
    members: tuple
    clock: int
    phase: str = "ready"
    lease: object = None
    lease_revoked: bool = False
    attempt: object = None


@dataclass
class Flush:
    start: int
    cutoff: int
    error: object = None


class Owner:
    """Sequential model of an atomic, rooted admission and batch ledger.

    Limit 0 means no limit. The byte limit is logical payload, not total RAM.
    Time and broker outcomes are supplied by the test driver.
    """
    def __init__(self, slots=0, byte_limit=0, waiters=0, max_token=MAX_U64):
        self.slot_limit = slots or None
        self.byte_limit = byte_limit or None
        self.waiter_limit = waiters or None
        self.max_token = max_token
        self.next_reservation = 1
        self.next_ticket = 1
        self.next_batch = 1
        self.next_lease = 1
        self.next_attempt = 1
        self.next_waiter = 1
        self.reservations = {}
        self.records = {}
        self.queues = {}
        self.batches = {}
        self.active = {}
        self.next_sequence = {}
        self.epoch = 1
        self.cohorts = {}
        self.used_slots = 0
        self.used_bytes = 0
        self.closed = False
        self.completions = CompletionIntervals()
        self.reported = 0
        self.barriers = {}
        # Monotone flush cutoffs partition unreported errors; None is the tail.
        self.error_segments = [(None, None)]

    def _fresh(self, field_name):
        value = getattr(self, field_name)
        if value > self.max_token:
            raise Exhausted(field_name)
        setattr(self, field_name, value + 1)
        return value

    def reserve(self, size):
        if self.closed:
            return "closed"
        if size < 0 or size > MAX_U64:
            return "invalid"
        if self.byte_limit is not None and size > self.byte_limit:
            return "too_large"
        if ((self.slot_limit is not None and self.used_slots >= self.slot_limit)
                or (self.byte_limit is not None and self.used_bytes + size > self.byte_limit)
                or self.used_bytes + size > MAX_U64):
            return "full"
        token = self._fresh("next_reservation")
        self.reservations[token] = size
        self.used_slots += 1
        self.used_bytes += size
        return token

    def discard_reservation(self, token):
        size = self.reservations.pop(token, None)
        if size is None:
            return False
        self.used_slots -= 1
        self.used_bytes -= size
        return True

    def commit(self, token, partition, payload, deadline, cohort=None, timestamp=None):
        if self.closed or token not in self.reservations:
            return "closed"
        size = self.reservations[token]
        if len(payload) != size:
            raise ValueError("payload does not match reserved logical bytes")
        if cohort in self.cohorts and self.cohorts[cohort]["sealed"]:
            return "cohort_closed"
        # No resource is detached if ticket allocation fails.
        ticket = self._fresh("next_ticket")
        del self.reservations[token]
        rec = Record(ticket, partition, bytes(payload), deadline, cohort, timestamp)
        self.records[ticket] = rec
        self.queues.setdefault(partition, deque()).append(ticket)
        if cohort is not None:
            state = self.cohorts.setdefault(cohort, {"sealed": False, "cancelled": False, "members": []})
            state["members"].append(ticket)
        return ticket

    def seal_cohort(self, cohort):
        self.cohorts[cohort]["sealed"] = True

    def cancel_cohort(self, cohort):
        self.cohorts[cohort]["cancelled"] = True

    def seal_batch(self, partition, max_records, max_bytes, clock):
        if partition in self.active or max_records <= 0 or max_bytes <= 0:
            return None
        q = self.queues.get(partition, deque())
        if not q:
            return None
        first = self.records[q[0]]
        members = []
        if first.cohort is not None:
            cohort = self.cohorts[first.cohort]
            if not cohort["sealed"] or cohort["cancelled"]:
                return None
            expected = tuple(cohort["members"])
            if tuple(itertools.islice(q, 0, len(expected))) != expected:
                return None
            if len(expected) > max_records or sum(len(self.records[x].data) for x in expected) > max_bytes:
                return None
            members = list(expected)
        else:
            total = 0
            for ticket in q:
                rec = self.records[ticket]
                if rec.cohort is not None or len(members) == max_records:
                    break
                if members and total + len(rec.data) > max_bytes:
                    break
                members.append(ticket)
                total += len(rec.data)
        if not members or len(members) >= MAX_SEQUENCE:
            return None
        key = self._fresh("next_batch")
        base = self.next_sequence.get(partition, 0)
        for ticket in members:
            assert q.popleft() == ticket
            rec = self.records[ticket]
            rec.batch = key
            rec.timestamp = rec.explicit_timestamp if rec.explicit_timestamp is not None else clock
        batch = Batch(key, partition, self.epoch, base, tuple(members), clock)
        self.batches[key] = batch
        self.active[partition] = key
        self.next_sequence[partition] = (base + len(members)) % MAX_SEQUENCE
        return key

    def claim(self, key):
        batch = self.batches[key]
        if batch.phase != "ready" or batch.lease is not None:
            return None
        batch.lease = self._fresh("next_lease")
        batch.lease_revoked = False
        batch.phase = "claimed"
        return batch.lease

    def offer(self, key, lease, now):
        batch = self.batches[key]
        if batch.phase != "claimed" or batch.lease != lease or batch.lease_revoked:
            return None
        if any(self.records[t].deadline <= now or self._cohort_cancelled(self.records[t])
               for t in batch.members):
            return None
        batch.attempt = self._fresh("next_attempt")
        # Mark possible write *before* crossing any yielding socket boundary.
        batch.phase = "offered"
        return batch.attempt

    def revoke_worker(self, key, lease):
        batch = self.batches[key]
        if batch.lease != lease or batch.lease_revoked:
            return False
        batch.lease_revoked = True
        batch.phase = "unknown" if batch.phase in ("offered", "unknown") else "retiring"
        return True

    def worker_stopped(self, key, lease):
        batch = self.batches[key]
        if batch.lease != lease or not batch.lease_revoked:
            return False
        batch.lease = None
        batch.lease_revoked = False
        if batch.phase == "retiring":
            batch.phase = "ready"
        return True

    def ambiguous(self, key, lease, attempt):
        batch = self.batches[key]
        if (batch.phase != "offered" or batch.lease != lease
                or batch.attempt != attempt or batch.lease_revoked):
            return False
        batch.phase = "unknown"
        return True

    def _cohort_cancelled(self, rec):
        return rec.cohort is not None and self.cohorts[rec.cohort]["cancelled"]

    def acknowledge(self, key, lease, attempt):
        batch = self.batches[key]
        if (batch.phase != "offered" or batch.lease != lease or batch.lease_revoked
                or batch.attempt != attempt or batch.epoch != self.epoch):
            return False
        batch.phase = "done"
        self.active.pop(batch.partition)
        for ticket in batch.members:
            self._outcome(ticket, "ok")
        self._collect_batch(key)
        return True

    def _outcome(self, ticket, result):
        rec = self.records[ticket]
        if rec.outcome is not None:
            return False
        rec.outcome = result
        rec.callback = "queued"
        return True

    def claim_callback(self, ticket):
        rec = self.records[ticket]
        if rec.callback != "queued":
            return None
        rec.callback_lease = self._fresh("next_lease")
        rec.callback = "claimed"
        return rec.callback_lease

    def finish_callback(self, ticket, lease):
        rec = self.records[ticket]
        if rec.callback != "claimed" or rec.callback_lease != lease:
            return False
        rec.callback = "done"
        self.completions.finish(ticket)
        if rec.outcome != "ok":
            error = (ticket, rec.outcome)
            for barrier in self.barriers.values():
                if barrier.start <= ticket <= barrier.cutoff:
                    barrier.error = earlier(barrier.error, error)
            if ticket > self.reported:
                for index, (upper, first) in enumerate(self.error_segments):
                    if upper is None or ticket <= upper:
                        self.error_segments[index] = (upper, earlier(first, error))
                        break
        if rec.batch is None:
            self._release(ticket)
        else:
            self._collect_batch(rec.batch)
        return True

    def _release(self, ticket):
        rec = self.records.pop(ticket)
        assert rec.callback == "done"
        self.used_slots -= 1
        self.used_bytes -= len(rec.data)

    def _collect_batch(self, key):
        batch = self.batches[key]
        if batch.phase != "done":
            return
        for ticket in batch.members:
            rec = self.records.get(ticket)
            if rec is not None and rec.callback == "done":
                self._release(ticket)
        if all(ticket not in self.records for ticket in batch.members):
            del self.batches[key]

    def _dissolve_unwritten(self, key, targets):
        batch = self.batches[key]
        assert batch.phase == "ready" and batch.lease is None
        end = (batch.base + len(batch.members)) % MAX_SEQUENCE
        assert self.active[batch.partition] == key
        assert self.next_sequence[batch.partition] == end
        self.next_sequence[batch.partition] = batch.base
        del self.active[batch.partition]
        del self.batches[key]
        keep = []
        for ticket in batch.members:
            rec = self.records[ticket]
            rec.batch = None
            if ticket in targets:
                self._outcome(ticket, targets[ticket])
                if rec.callback == "done":
                    self._release(ticket)
            else:
                keep.append(ticket)
        q = self.queues.setdefault(batch.partition, deque())
        for ticket in reversed(keep):
            q.appendleft(ticket)

    def expire(self, now):
        # Any expired sticky member cancels its entire cohort explicitly.
        for rec in self.records.values():
            if rec.cohort is not None and rec.deadline <= now:
                self.cohorts[rec.cohort]["cancelled"] = True
        for partition, q in list(self.queues.items()):
            keep = deque()
            while q:
                ticket = q.popleft()
                rec = self.records[ticket]
                if rec.deadline <= now:
                    self._outcome(ticket, "timeout")
                elif self._cohort_cancelled(rec):
                    self._outcome(ticket, "cohort_failed")
                else:
                    keep.append(ticket)
            self.queues[partition] = keep

        for key, batch in list(self.batches.items()):
            if batch.phase == "done":
                continue
            affected = {}
            for ticket in batch.members:
                rec = self.records[ticket]
                if rec.deadline <= now:
                    affected[ticket] = "timeout"
                elif self._cohort_cancelled(rec):
                    affected[ticket] = "cohort_failed"
            if not affected:
                continue
            # A sticky cohort is atomic: failing it early is explicit.
            if self.records[batch.members[0]].cohort is not None:
                for ticket in batch.members:
                    affected.setdefault(ticket, "cohort_failed")
            if batch.phase == "ready" and batch.lease is None:
                self._dissolve_unwritten(key, affected)
                continue
            if batch.phase in ("claimed", "offered"):
                self.revoke_worker(key, batch.lease)
            for ticket, result in affected.items():
                self._outcome(ticket, result)

    def change_epoch(self, new_epoch):
        # External broker fencing/settlement is not modelled. No identity
        # replacement is permitted while any rooted batch is unresolved.
        if self.batches or new_epoch <= self.epoch:
            return False
        self.epoch = new_epoch
        self.next_sequence.clear()
        return True

    def begin_flush(self, internal=False):
        if not internal and self.waiter_limit is not None and len(self.barriers) >= self.waiter_limit:
            return "full"
        waiter = self._fresh("next_waiter")
        cutoff = self.next_ticket - 1
        error = None
        for _, first in self.error_segments:
            if first is not None and self.reported < first[0] <= cutoff:
                error = earlier(error, first)
        self.barriers[waiter] = Flush(self.reported + 1, cutoff, error)
        if cutoff > self.reported and all(upper != cutoff for upper, _ in self.error_segments):
            assert self.error_segments[-1][0] is None
            _, first = self.error_segments[-1]
            self.error_segments[-1] = (cutoff, first)
            self.error_segments.append((None, None))
        return waiter

    def _compact_errors(self):
        boundaries = {b.cutoff for b in self.barriers.values() if b.cutoff > self.reported}
        out = []
        first = None
        for upper, item in self.error_segments:
            if upper is not None and upper <= self.reported:
                continue
            if item is not None:
                assert item[0] > self.reported
            first = earlier(first, item)
            if upper is None or upper in boundaries:
                out.append((upper, first))
                first = None
        assert out and out[-1][0] is None
        self.error_segments = out

    def cancel_flush(self, waiter):
        if self.barriers.pop(waiter, None) is None:
            return False
        self._compact_errors()
        return True

    def poll_flush(self, waiter):
        barrier = self.barriers[waiter]
        if self.completions.prefix < barrier.cutoff:
            return "pending"
        result = barrier.error if barrier.error is not None else "ok"
        self.reported = max(self.reported, barrier.cutoff)
        del self.barriers[waiter]
        self._compact_errors()
        return result

    def close(self):
        if self.closed:
            return "closed"
        self.closed = True
        for token in list(self.reservations):
            self.discard_reservation(token)
        # The real implementation needs a reserved close-control budget.
        return self.begin_flush(internal=True)


def admit(owner, partition, data=b"", deadline=100, cohort=None, timestamp=None):
    token = owner.reserve(len(data))
    if not isinstance(token, int):
        return token
    return owner.commit(token, partition, data, deadline, cohort, timestamp)


def finish(owner, ticket):
    lease = owner.claim_callback(ticket)
    assert lease is not None
    assert owner.finish_callback(ticket, lease)
class OwnerControls(unittest.TestCase):
    def test_limits_and_close_revoke_pre_admission(self):
        owner = Owner(slots=1, byte_limit=2, waiters=1)
        reservation = owner.reserve(0)
        self.assertEqual(owner.reserve(0), "full")
        flush = owner.begin_flush()
        self.assertEqual(owner.begin_flush(), "full")
        close = owner.close()  # Reserved control slot is separate.
        self.assertEqual(owner.commit(reservation, "p", b"", 10), "closed")
        self.assertFalse(owner.discard_reservation(reservation))
        self.assertEqual((owner.used_slots, owner.used_bytes), (0, 0))
        self.assertEqual(owner.poll_flush(close), "ok")
        self.assertEqual(owner.poll_flush(flush), "ok")
        unlimited = Owner(slots=0, byte_limit=0)
        for _ in range(20):
            self.assertIsInstance(admit(unlimited, "p", b"xxx"), int)
        self.assertEqual((unlimited.used_slots, unlimited.used_bytes), (20, 60))

    def test_rooted_claim_waits_for_old_worker_to_stop(self):
        owner = Owner(slots=4, byte_limit=8)
        one = admit(owner, "p", b"a", timestamp=None)
        two = admit(owner, "p", b"b", timestamp=17)
        batch = owner.seal_batch("p", 2, 8, 123)
        old = owner.claim(batch)
        self.assertTrue(owner.revoke_worker(batch, old))
        self.assertIsNone(owner.claim(batch))
        self.assertTrue(owner.worker_stopped(batch, old))
        new = owner.claim(batch)
        self.assertNotEqual(old, new)
        self.assertIsNone(owner.offer(batch, old, 1))
        self.assertEqual(owner.batches[batch].members, (one, two))
        self.assertEqual((owner.records[one].timestamp, owner.records[two].timestamp), (123, 17))
        attempt = owner.offer(batch, new, 1)
        self.assertTrue(owner.acknowledge(batch, new, attempt))
        finish(owner, one)
        finish(owner, two)
        self.assertEqual((owner.used_slots, owner.used_bytes), (0, 0))

    def test_unknown_handoff_remains_a_whole_batch_during_flush_and_close(self):
        owner = Owner(slots=8194, byte_limit=8194)
        members = tuple(admit(owner, "p", b"x") for _ in range(8193))
        batch = owner.seal_batch("p", 8193, 8193, 123)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 1)
        self.assertTrue(owner.ambiguous(batch, lease, attempt))
        self.assertTrue(owner.revoke_worker(batch, lease))
        self.assertTrue(owner.worker_stopped(batch, lease))
        following = admit(owner, "p", b"y")
        flush = owner.begin_flush()
        self.assertIsNone(owner.claim(batch))
        self.assertIsNone(owner.seal_batch("p", 8193, 8193, 456))
        self.assertFalse(owner.change_epoch(2))
        self.assertEqual(owner.batches[batch].phase, "unknown")
        self.assertEqual(owner.batches[batch].members, members)
        self.assertEqual(bytes(owner.records[t].data[0] for t in members), b"x" * 8193)
        self.assertEqual(owner.queues["p"][0], following)
        self.assertEqual(owner.poll_flush(flush), "pending")
        close = owner.close()
        self.assertEqual(owner.poll_flush(close), "pending")
        self.assertEqual((owner.used_slots, owner.used_bytes), (8194, 8194))

    def test_equal_numeric_bases_do_not_make_batches_equal(self):
        owner = Owner(slots=2)
        one = admit(owner, "one", b"a")
        two = admit(owner, "two", b"b")
        a = owner.seal_batch("one", 1, 5, 1)
        b = owner.seal_batch("two", 1, 5, 1)
        self.assertNotEqual(a, b)
        self.assertEqual((owner.batches[a].base, owner.batches[b].base), (0, 0))
        la = owner.claim(a)
        lb = owner.claim(b)
        self.assertIsNone(owner.offer(a, lb, 1))
        self.assertEqual((owner.batches[a].members, owner.batches[b].members), ((one,), (two,)))
        self.assertIsNotNone(owner.offer(a, la, 1))

    def test_unknown_batch_retains_individual_deadlines_and_fence(self):
        owner = Owner(slots=3, byte_limit=3)
        one = admit(owner, "p", b"a", deadline=10)
        two = admit(owner, "p", b"b", deadline=20)
        batch = owner.seal_batch("p", 2, 2, 1)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 1)
        self.assertTrue(owner.ambiguous(batch, lease, attempt))
        self.assertTrue(owner.revoke_worker(batch, lease))
        self.assertTrue(owner.worker_stopped(batch, lease))
        flush = owner.begin_flush()
        owner.expire(10)
        self.assertEqual(owner.records[one].outcome, "timeout")
        self.assertIsNone(owner.records[two].outcome)
        finish(owner, one)
        self.assertEqual(owner.used_slots, 2)
        self.assertEqual(owner.poll_flush(flush), "pending")
        owner.expire(20)
        finish(owner, two)
        self.assertEqual(owner.poll_flush(flush), (one, "timeout"))
        self.assertEqual(owner.used_slots, 2)
        self.assertEqual(owner.batches[batch].members, (one, two))
        self.assertIsNone(owner.claim(batch))
        self.assertFalse(owner.change_epoch(2))

    def test_proven_unwritten_expiry_rolls_back_before_repacking(self):
        owner = Owner(slots=2, byte_limit=2)
        one = admit(owner, "p", b"a", deadline=10)
        two = admit(owner, "p", b"b", deadline=20)
        owner.next_sequence["p"] = MAX_SEQUENCE - 1
        batch = owner.seal_batch("p", 2, 2, 1)
        self.assertEqual(owner.next_sequence["p"], 1)
        owner.expire(10)
        self.assertNotIn(batch, owner.batches)
        self.assertEqual(owner.next_sequence["p"], MAX_SEQUENCE - 1)
        finish(owner, one)
        next_batch = owner.seal_batch("p", 2, 2, 2)
        self.assertEqual(owner.batches[next_batch].members, (two,))
        self.assertEqual(owner.batches[next_batch].base, MAX_SEQUENCE - 1)
        self.assertEqual(owner.records[two].timestamp, 2)

    def test_expiring_sticky_member_cannot_split_an_unknown_batch(self):
        owner = Owner(slots=2)
        one = admit(owner, "p", b"a", deadline=10, cohort=7)
        two = admit(owner, "p", b"b", deadline=20, cohort=7)
        self.assertIsNone(owner.seal_batch("p", 2, 2, 1))
        owner.seal_cohort(7)
        self.assertIsNone(owner.seal_batch("p", 1, 2, 1))
        batch = owner.seal_batch("p", 2, 2, 1)
        lease = owner.claim(batch)
        owner.offer(batch, lease, 1)
        owner.expire(10)
        self.assertEqual(owner.records[one].outcome, "timeout")
        self.assertEqual(owner.records[two].outcome, "cohort_failed")
        self.assertEqual(owner.batches[batch].phase, "unknown")
        finish(owner, one)
        finish(owner, two)
        self.assertEqual(owner.batches[batch].members, (one, two))
        self.assertEqual(owner.used_slots, 2)

    def test_callback_claim_and_flush_wait_for_terminal_publication(self):
        owner = Owner(slots=1)
        ticket = admit(owner, "p", deadline=1)
        flush = owner.begin_flush()
        owner.expire(1)
        callback = owner.claim_callback(ticket)
        self.assertIsNone(owner.claim_callback(ticket))
        self.assertFalse(owner.finish_callback(ticket, callback + 1))
        self.assertEqual(owner.poll_flush(flush), "pending")
        self.assertTrue(owner.finish_callback(ticket, callback))
        self.assertEqual(owner.poll_flush(flush), (ticket, "timeout"))
        self.assertEqual(owner.used_slots, 0)

    def test_overlapping_flushes_and_cancel_do_not_consume_each_others_errors(self):
        owner = Owner(slots=2, waiters=4)
        first = admit(owner, "a", deadline=1)
        early = owner.begin_flush()
        second = admit(owner, "b", deadline=1)
        late = owner.begin_flush()
        canceled = owner.begin_flush()
        owner.expire(1)
        finish(owner, second)
        finish(owner, first)
        self.assertTrue(owner.cancel_flush(canceled))
        self.assertEqual(owner.poll_flush(early), (first, "timeout"))
        newer = owner.begin_flush()
        self.assertEqual(owner.poll_flush(newer), (second, "timeout"))
        self.assertEqual(owner.poll_flush(late), (first, "timeout"))
        self.assertEqual(owner.poll_flush(owner.begin_flush()), "ok")
        self.assertEqual(len(owner.error_segments), 1)

    def test_completed_intervals_compact_behind_one_stalled_record(self):
        owner = Owner(slots=2)
        first = admit(owner, "slow", deadline=1_000_000)
        for _ in range(5000):
            item = admit(owner, "fast", deadline=1)
            owner.expire(1)
            finish(owner, item)
            self.assertLessEqual(len(owner.completions.intervals), 1)
            self.assertEqual(owner.used_slots, 1)
        self.assertEqual(owner.completions.prefix, 0)
        owner.expire(1_000_000)
        finish(owner, first)
        self.assertEqual(owner.completions.prefix, 5001)
        self.assertEqual(owner.completions.intervals, [])
        self.assertEqual(owner.used_slots, 0)

    def test_all_reserve_commit_close_orders_preserve_credit(self):
        for order in itertools.permutations(("reserve", "commit", "close")):
            if order.index("commit") < order.index("reserve"):
                continue
            owner = Owner(slots=1, byte_limit=1)
            reservation = None
            accepted = None
            for action in order:
                if action == "reserve":
                    reservation = owner.reserve(1)
                elif action == "commit":
                    accepted = owner.commit(reservation, "p", b"x", 10)
                else:
                    owner.close()
            self.assertEqual(owner.used_slots, len(owner.records) + len(owner.reservations))
            self.assertEqual(owner.used_bytes,
                             sum(len(r.data) for r in owner.records.values())
                             + sum(owner.reservations.values()))
            if isinstance(accepted, int):
                self.assertIn(accepted, owner.records)
            else:
                self.assertEqual(accepted, "closed")

    def test_flush_summary_matches_unbounded_oracle(self):
        for seed in range(30):
            rng = random.Random(seed)
            owner = Owner(slots=6, waiters=3)
            completed = {}
            pending_barriers = {}
            oracle_reported = 0
            for _ in range(200):
                choices = ["admit", "begin"]
                eligible = [ticket for ticket, rec in owner.records.items()
                            if rec.outcome is None and rec.batch is None]
                if eligible:
                    choices.append("complete")
                if pending_barriers:
                    choices.extend(["poll", "cancel"])
                action = rng.choice(choices)
                if action == "admit":
                    admit(owner, "p", b"x")
                elif action == "begin":
                    start = oracle_reported + 1
                    cutoff = owner.next_ticket - 1
                    waiter = owner.begin_flush()
                    if isinstance(waiter, int):
                        pending_barriers[waiter] = (start, cutoff)
                elif action == "complete":
                    ticket = rng.choice(eligible)
                    owner.queues["p"].remove(ticket)
                    outcome = rng.choice(("ok", "timeout"))
                    owner._outcome(ticket, outcome)
                    finish(owner, ticket)
                    completed[ticket] = outcome
                elif action == "cancel":
                    waiter = rng.choice(list(pending_barriers))
                    self.assertTrue(owner.cancel_flush(waiter))
                    del pending_barriers[waiter]
                else:
                    waiter = rng.choice(list(pending_barriers))
                    start, cutoff = pending_barriers[waiter]
                    result = owner.poll_flush(waiter)
                    if any(ticket not in completed for ticket in range(1, cutoff + 1)):
                        self.assertEqual(result, "pending", (seed, start, cutoff))
                    else:
                        errors = [(ticket, completed[ticket])
                                  for ticket in range(start, cutoff + 1)
                                  if completed[ticket] != "ok"]
                        expected = min(errors) if errors else "ok"
                        self.assertEqual(result, expected, (seed, start, cutoff))
                        oracle_reported = max(oracle_reported, cutoff)
                        del pending_barriers[waiter]
                self.assertEqual(owner.reported, oracle_reported)
                self.assertEqual(owner.used_slots, len(owner.records) + len(owner.reservations))
                self.assertLessEqual(len(owner.error_segments), len(owner.barriers) + 1)
                self.assertLessEqual(len(owner.completions.intervals), len(owner.records) + 1)

    def test_exhausted_ticket_does_not_detach_reservation(self):
        owner = Owner(slots=3, max_token=1)
        one = owner.reserve(0)
        self.assertEqual(owner.commit(one, "p", b"", 10), 1)
        # Independent reservation counter also fails closed at exhaustion.
        with self.assertRaises(Exhausted):
            owner.reserve(0)
        self.assertEqual(owner.used_slots, 1)
        self.assertIn(1, owner.records)


if __name__ == "__main__":
    unittest.main(verbosity=2)
