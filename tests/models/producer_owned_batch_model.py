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
    def __init__(self, slots=0, byte_limit=0, waiters=0, partitions=0, max_token=MAX_U64):
        self.slot_limit = slots or None
        self.byte_limit = byte_limit or None
        self.waiter_limit = waiters or None
        self.partition_limit = partitions or None
        self.partitions = set()
        self.max_token = max_token
        self.next_reservation = 1
        self.next_ticket = 1
        self.next_batch = 1
        self.next_lease = 1
        self.next_attempt = 1
        self.next_waiter = 1
        self.next_cohort = 1
        self.reservations = {}
        self.records = {}
        self.queues = {}
        self.callback_order = {}
        self.batches = {}
        self.active = {}
        self.next_sequence = {}
        self.epoch = 1
        self.cohorts = {}
        self.used_slots = 0
        self.used_bytes = 0
        self.closed = False
        self.close_result = None
        self.now = 0
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
        # Normalize before mutating reservation or accepted-record state.
        payload = bytes(payload)
        if len(payload) != size:
            raise ValueError("payload does not match reserved logical bytes")
        if partition not in self.partitions and self.partition_limit is not None and len(self.partitions) >= self.partition_limit:
            self.discard_reservation(token)
            return "partition_full"
        state = self.cohorts.get(cohort) if cohort is not None else None
        if cohort is not None:
            if state is None or state["sealed"] or state["cancelled"]:
                self.discard_reservation(token)
                return "cohort_closed"
            if state["partition"] is not None and state["partition"] != partition:
                self.discard_reservation(token)
                return "cohort_partition"
            queue = self.queues.get(partition)
            if state["members"] and (not queue or queue[-1] != state["members"][-1]):
                self.discard_reservation(token)
                return "cohort_conflict"
        try:
            ticket = self._fresh("next_ticket")
        except Exhausted:
            self.discard_reservation(token)
            raise
        # A different subsequent admission seals the open tail cohort.
        queue = self.queues.get(partition)
        if queue:
            prior = self.records[queue[-1]].cohort
            if prior is not None and prior != cohort:
                self.cohorts[prior]["sealed"] = True
        del self.reservations[token]
        self.partitions.add(partition)
        rec = Record(ticket, partition, payload, deadline, cohort, timestamp)
        self.records[ticket] = rec
        self.queues.setdefault(partition, deque()).append(ticket)
        self.callback_order.setdefault(partition, deque()).append(ticket)
        if state is not None:
            state["partition"] = partition
            state["members"].append(ticket)
            state["live"] += 1
        return ticket

    def new_cohort(self):
        if self.closed:
            return "closed"
        if self.slot_limit is not None and len(self.cohorts) >= self.slot_limit:
            return "full"
        cohort = self._fresh("next_cohort")
        self.cohorts[cohort] = {
            "sealed": False, "cancelled": False, "members": [],
            "live": 0, "partition": None,
        }
        return cohort

    def seal_cohort(self, cohort):
        state = self.cohorts.get(cohort)
        if state is None:
            return False
        state["sealed"] = True
        if state["live"] == 0:
            del self.cohorts[cohort]
        return True

    def cancel_cohort(self, cohort):
        state = self.cohorts.get(cohort)
        if state is None:
            return False
        state["cancelled"] = True
        if state["live"] == 0:
            del self.cohorts[cohort]
        return True

    def seal_batch(self, partition, max_records, max_bytes, clock,
                   hard_records=None, hard_bytes=None):
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
            size = sum(len(self.records[x].data) for x in expected)
            if ((hard_records is not None and len(expected) > hard_records)
                    or (hard_bytes is not None and size > hard_bytes)):
                for ticket in expected:
                    assert q.popleft() == ticket
                    self._outcome(ticket, "cohort_too_large")
                if not q:
                    self.queues.pop(partition, None)
                return "too_large"
            if len(expected) > max_records or size > max_bytes:
                return None
            members = list(expected)
        else:
            if hard_bytes is not None and len(first.data) > hard_bytes:
                ticket = q.popleft()
                self._outcome(ticket, "too_large")
                if not q:
                    self.queues.pop(partition, None)
                return "too_large"
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
            if rec.timestamp is None:
                rec.timestamp = rec.explicit_timestamp if rec.explicit_timestamp is not None else clock
        if not q:
            self.queues.pop(partition, None)
        batch = Batch(key, partition, self.epoch, base, tuple(members), clock)
        self.batches[key] = batch
        self.active[partition] = key
        self.next_sequence[partition] = (base + len(members)) % MAX_SEQUENCE
        return key

    def claim(self, key):
        batch = self.batches.get(key)
        if batch is None or batch.phase != "ready" or batch.lease is not None:
            return None
        batch.lease = self._fresh("next_lease")
        batch.lease_revoked = False
        batch.phase = "claimed"
        return batch.lease

    def offer(self, key, lease, now):
        self.expire(now)
        batch = self.batches.get(key)
        if batch is None or batch.phase != "claimed" or batch.lease != lease or batch.lease_revoked:
            return None
        if any(self.records[t].deadline <= now or self._cohort_cancelled(self.records[t])
               for t in batch.members):
            return None
        batch.attempt = self._fresh("next_attempt")
        # Mark possible write *before* crossing any yielding socket boundary.
        batch.phase = "offered"
        return batch.attempt

    def revoke_worker(self, key, lease):
        batch = self.batches.get(key)
        if (batch is None or lease is None or batch.lease != lease or batch.lease_revoked
                or batch.phase not in ("claimed", "offered", "unknown")):
            return False
        batch.lease_revoked = True
        batch.phase = "unknown" if batch.phase in ("offered", "unknown") else "retiring"
        return True

    def worker_stopped(self, key, lease):
        batch = self.batches.get(key)
        if (batch is None or lease is None or batch.lease != lease or not batch.lease_revoked
                or batch.phase not in ("retiring", "unknown")):
            return False
        batch.lease = None
        batch.lease_revoked = False
        if batch.phase == "retiring":
            batch.phase = "ready"
        return True

    def ambiguous(self, key, lease, attempt):
        batch = self.batches.get(key)
        if (batch is None or batch.phase != "offered" or batch.lease != lease
                or batch.attempt != attempt or batch.lease_revoked):
            return False
        batch.phase = "unknown"
        return True

    def _cohort_cancelled(self, rec):
        return rec.cohort is not None and self.cohorts[rec.cohort]["cancelled"]

    def acknowledge(self, key, lease, attempt, now):
        # The model chooses timeout at equal times; actual runtime ordering and
        # timer fairness are not represented by this synthetic clock.
        self.expire(now)
        batch = self.batches.get(key)
        if (batch is None or batch.phase != "offered" or batch.lease != lease or batch.lease_revoked
                or batch.attempt != attempt or batch.epoch != self.epoch
                or self.active.get(batch.partition) != key):
            return False
        batch.phase = "done"
        batch.lease = None
        del self.active[batch.partition]
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

    def fail_queued(self, ticket, result):
        rec = self.records.get(ticket)
        if rec is None or rec.batch is not None or rec.outcome is not None:
            return False
        q = self.queues.get(rec.partition)
        if q is None or ticket not in q:
            return False
        q.remove(ticket)
        if not q:
            del self.queues[rec.partition]
        return self._outcome(ticket, result)

    def claim_callback(self, ticket):
        rec = self.records.get(ticket)
        if (rec is None or rec.callback != "queued"
                or not self.callback_order.get(rec.partition)
                or self.callback_order[rec.partition][0] != ticket):
            return None
        # A record has one callback claim, keyed by its nonreused ticket.
        # Callback-worker crash/replay semantics are deliberately unmodelled.
        rec.callback_lease = ticket
        rec.callback = "claimed"
        return rec.callback_lease

    def finish_callback(self, ticket, lease):
        rec = self.records.get(ticket)
        if rec is None or rec.callback != "claimed" or rec.callback_lease != lease:
            return False
        rec.callback = "done"
        order = self.callback_order[rec.partition]
        assert order.popleft() == ticket
        if not order:
            del self.callback_order[rec.partition]
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
        if rec.cohort is not None:
            state = self.cohorts[rec.cohort]
            state["live"] -= 1
            if state["live"] == 0 and (state["sealed"] or state["cancelled"]):
                del self.cohorts[rec.cohort]

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
        if now < self.now:
            raise ValueError("model time cannot move backwards")
        self.now = now
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
            if keep:
                self.queues[partition] = keep
            else:
                self.queues.pop(partition, None)

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

    def change_epoch(self, new_epoch, broker_authorized=False):
        # This boolean stands for an external proof, which the model does not
        # implement. It is not evidence that any broker operation is sufficient.
        if (not broker_authorized or self.batches or self.records or self.reservations
                or new_epoch <= self.epoch):
            return False
        self.epoch = new_epoch
        self.next_sequence.clear()
        self.partitions.clear()
        self.queues.clear()
        return True

    def begin_flush(self, internal=False):
        if not internal and self.waiter_limit is not None and len(self.barriers) >= self.waiter_limit:
            return "full"
        # Close has one reserved, non-reused control identity even if public
        # waiter IDs are exhausted.
        waiter = "close" if internal else self._fresh("next_waiter")
        if waiter in self.barriers:
            raise ValueError("control waiter already registered")
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
        if waiter == "close":
            # Cancel an observer, not the durable close cutoff.
            return waiter in self.barriers or self.close_result is not None
        if self.barriers.pop(waiter, None) is None:
            return False
        self._compact_errors()
        return True

    def poll_flush(self, waiter):
        if waiter == "close" and self.close_result is not None:
            return self.close_result
        barrier = self.barriers.get(waiter)
        if barrier is None:
            return "unknown"
        if self.completions.prefix < barrier.cutoff:
            return "pending"
        if waiter == "close" and any(
            batch.phase != "done" and any(ticket <= barrier.cutoff for ticket in batch.members)
            for batch in self.batches.values()
        ):
            return "pending"
        result = barrier.error if barrier.error is not None else "ok"
        self.reported = max(self.reported, barrier.cutoff)
        if waiter == "close":
            self.close_result = result
        del self.barriers[waiter]
        self._compact_errors()
        return result

    def close_deadline(self):
        if not self.closed:
            raise ValueError("close has not begun")
        if self.close_result is None:
            # Timeout returns to the observer; the reserved close barrier,
            # rooted batch and uncertainty fence remain in the owner.
            self.close_result = "timeout"
        return self.close_result

    def close(self):
        if self.closed:
            return "close"
        self.closed = True
        for token in list(self.reservations):
            self.discard_reservation(token)
        for cohort, state in list(self.cohorts.items()):
            state["sealed"] = True
            if state["live"] == 0:
                del self.cohorts[cohort]
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
        self.assertTrue(owner.acknowledge(batch, new, attempt, 1))
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

    def test_completed_batch_stale_lease_cannot_clear_new_batch(self):
        owner = Owner(slots=3)
        first = admit(owner, "p", b"a")
        old_batch = owner.seal_batch("p", 1, 5, 1)
        old_lease = owner.claim(old_batch)
        old_attempt = owner.offer(old_batch, old_lease, 1)
        self.assertTrue(owner.acknowledge(old_batch, old_lease, old_attempt, 1))
        # The completed batch is still rooted while its callback is pending.
        second = admit(owner, "p", b"b")
        new_batch = owner.seal_batch("p", 1, 5, 2)
        new_lease = owner.claim(new_batch)
        self.assertIsNotNone(owner.offer(new_batch, new_lease, 2))
        self.assertFalse(owner.revoke_worker(old_batch, old_lease))
        self.assertFalse(owner.revoke_worker(old_batch, None))
        self.assertFalse(owner.worker_stopped(old_batch, old_lease))
        self.assertIsNone(owner.claim(old_batch))
        self.assertEqual(owner.active["p"], new_batch)
        third = admit(owner, "p", b"c")
        self.assertIsNone(owner.seal_batch("p", 1, 5, 3))
        self.assertEqual(owner.queues["p"][0], third)
        self.assertEqual(owner.batches[new_batch].members, (second,))
        finish(owner, first)

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
        self.assertEqual(owner.records[two].timestamp, 1)

    def test_expiring_sticky_member_cannot_split_an_unknown_batch(self):
        owner = Owner(slots=2)
        cohort = owner.new_cohort()
        one = admit(owner, "p", b"a", deadline=10, cohort=cohort)
        two = admit(owner, "p", b"b", deadline=20, cohort=cohort)
        self.assertIsNone(owner.seal_batch("p", 2, 2, 1))
        owner.seal_cohort(cohort)
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
                    admit(owner, f"p{owner.next_ticket}", b"x")
                elif action == "begin":
                    start = oracle_reported + 1
                    cutoff = owner.next_ticket - 1
                    waiter = owner.begin_flush()
                    if isinstance(waiter, int):
                        pending_barriers[waiter] = (start, cutoff)
                elif action == "complete":
                    ticket = rng.choice(eligible)
                    outcome = rng.choice(("ok", "timeout"))
                    self.assertTrue(owner.fail_queued(ticket, outcome))
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

    def test_close_control_survives_public_waiter_id_exhaustion(self):
        owner = Owner(slots=1, max_token=1)
        self.assertEqual(owner.poll_flush(owner.begin_flush()), "ok")
        with self.assertRaises(Exhausted):
            owner.begin_flush()
        reservation = owner.reserve(0)
        close = owner.close()
        self.assertEqual(owner.commit(reservation, "p", b"", 10), "closed")
        self.assertEqual(owner.used_slots, 0)
        self.assertEqual(owner.poll_flush(close), "ok")

    def test_ticket_exhaustion_releases_uncommitted_credit(self):
        owner = Owner(slots=1, max_token=1)
        reservation = owner.reserve(0)
        owner.next_ticket = 2
        with self.assertRaises(Exhausted):
            owner.commit(reservation, "p", b"", 10)
        self.assertEqual((owner.used_slots, owner.used_bytes), (0, 0))
        self.assertFalse(owner.discard_reservation(reservation))

    def test_sticky_interleaving_and_cross_partition_rejected_before_admission(self):
        owner = Owner(slots=3, byte_limit=3)
        cohort = owner.new_cohort()
        first = admit(owner, "p", b"a", cohort=cohort)
        second = admit(owner, "p", b"b")
        self.assertEqual(admit(owner, "p", b"c", cohort=cohort), "cohort_closed")
        self.assertEqual(owner.used_slots, 2)
        batch = owner.seal_batch("p", 1, 1, 0)
        self.assertEqual(owner.batches[batch].members, (first,))
        self.assertEqual(owner.queues["p"][0], second)

        other = Owner(slots=2)
        cohort = other.new_cohort()
        self.assertIsInstance(admit(other, "p", b"a", cohort=cohort), int)
        self.assertEqual(admit(other, "q", b"b", cohort=cohort), "cohort_partition")
        other.cancel_cohort(cohort)
        self.assertEqual(admit(other, "p", b"c", cohort=cohort), "cohort_closed")
        self.assertEqual(other.used_slots, 1)

    def test_cohort_metadata_is_reclaimed_but_sequence_state_is_bounded_separately(self):
        owner = Owner(slots=1, byte_limit=1, partitions=2)
        for index in range(200):
            cohort = owner.new_cohort()
            ticket = admit(owner, "p", b"x", cohort=cohort)
            owner.seal_cohort(cohort)
            batch = owner.seal_batch("p", 1, 1, 0)
            self.assertEqual(owner.batches[batch].base, index)
            lease = owner.claim(batch)
            attempt = owner.offer(batch, lease, 0)
            self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
            finish(owner, ticket)
            self.assertEqual(len(owner.cohorts), 0)
            self.assertNotIn("p", owner.queues)
        ticket = admit(owner, "q", b"x")
        batch = owner.seal_batch("q", 1, 1, 0)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
        finish(owner, ticket)
        self.assertEqual(admit(owner, "r", b"x"), "partition_full")
        self.assertEqual(owner.used_slots, 0)
        ticket = admit(owner, "p", b"x")
        batch = owner.seal_batch("p", 1, 1, 0)
        self.assertEqual(owner.batches[batch].base, 200)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
        finish(owner, ticket)
        self.assertFalse(owner.change_epoch(2))
        self.assertTrue(owner.change_epoch(2, broker_authorized=True))
        self.assertIsInstance(admit(owner, "r", b"x"), int)

    def test_sticky_hard_limit_fails_all_without_assigning_sequence(self):
        owner = Owner(slots=2)
        cohort = owner.new_cohort()
        one = admit(owner, "p", b"a", cohort=cohort)
        two = admit(owner, "p", b"b", cohort=cohort)
        owner.seal_cohort(cohort)
        self.assertEqual(owner.seal_batch("p", 1, 2, 1, hard_records=1), "too_large")
        self.assertEqual(owner.records[one].outcome, "cohort_too_large")
        self.assertEqual(owner.records[two].outcome, "cohort_too_large")
        self.assertNotIn("p", owner.next_sequence)
        finish(owner, one)
        finish(owner, two)
        self.assertEqual(owner.used_slots, 0)
        soft = Owner(slots=1)
        single = admit(soft, "p", b"abc")
        batch = soft.seal_batch("p", 1, 1, 0)
        self.assertEqual(soft.batches[batch].members, (single,))

    def test_ack_at_record_deadline_is_not_reported_as_success(self):
        owner = Owner(slots=2)
        first = admit(owner, "p", b"a", deadline=5)
        second = admit(owner, "p", b"b", deadline=10)
        batch = owner.seal_batch("p", 2, 2, 0)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertFalse(owner.acknowledge(batch, lease, attempt, 5))
        self.assertEqual(owner.records[first].outcome, "timeout")
        self.assertIsNone(owner.records[second].outcome)
        self.assertEqual(owner.batches[batch].phase, "unknown")
        self.assertFalse(owner.acknowledge(batch, lease, attempt, 6))
        self.assertRaises(ValueError, owner.expire, 5)

    def test_callback_claim_does_not_consume_worker_lease_counter(self):
        owner = Owner(slots=1, max_token=1)
        ticket = admit(owner, "p", b"")
        batch = owner.seal_batch("p", 1, 1, 0)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
        finish(owner, ticket)
        self.assertEqual(owner.used_slots, 0)

    def test_canceled_close_observer_does_not_erase_durable_cutoff(self):
        owner = Owner(slots=1)
        ticket = admit(owner, "p", deadline=10)
        close = owner.close()
        self.assertTrue(owner.cancel_flush(close))
        self.assertEqual(owner.close(), close)
        self.assertEqual(owner.poll_flush(close), "pending")
        owner.expire(10)
        finish(owner, ticket)
        self.assertEqual(owner.poll_flush(close), (ticket, "timeout"))
        self.assertEqual(owner.poll_flush(owner.close()), (ticket, "timeout"))

    def test_stale_events_after_cleanup_are_rejected(self):
        owner = Owner(slots=1)
        ticket = admit(owner, "p", b"")
        batch = owner.seal_batch("p", 1, 1, 0)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
        callback = owner.claim_callback(ticket)
        self.assertTrue(owner.finish_callback(ticket, callback))
        self.assertFalse(owner.finish_callback(ticket, callback))
        self.assertIsNone(owner.claim_callback(ticket))
        self.assertIsNone(owner.claim(batch))
        self.assertFalse(owner.revoke_worker(batch, lease))
        self.assertFalse(owner.worker_stopped(batch, lease))
        self.assertFalse(owner.ambiguous(batch, lease, attempt))
        self.assertFalse(owner.acknowledge(batch, lease, attempt, 0))

    def test_invalid_precommit_payload_keeps_credit_revocable(self):
        owner = Owner(slots=1, byte_limit=1)
        reservation = owner.reserve(1)
        with self.assertRaises(ValueError):
            owner.commit(reservation, "p", [-1], 10)
        self.assertEqual(owner.used_slots, 1)
        self.assertTrue(owner.discard_reservation(reservation))
        self.assertEqual(owner.used_slots, 0)

    def test_callbacks_remain_ordered_within_a_partition(self):
        owner = Owner(slots=2)
        first = admit(owner, "p", b"a")
        old = owner.seal_batch("p", 1, 1, 0)
        old_lease = owner.claim(old)
        old_attempt = owner.offer(old, old_lease, 0)
        self.assertTrue(owner.acknowledge(old, old_lease, old_attempt, 0))
        second = admit(owner, "p", b"b")
        new = owner.seal_batch("p", 1, 1, 0)
        new_lease = owner.claim(new)
        new_attempt = owner.offer(new, new_lease, 0)
        self.assertTrue(owner.acknowledge(new, new_lease, new_attempt, 0))
        flush = owner.begin_flush()
        self.assertIsNone(owner.claim_callback(second))
        self.assertEqual(owner.poll_flush(flush), "pending")
        finish(owner, first)
        finish(owner, second)
        self.assertEqual(owner.poll_flush(flush), "ok")

    def test_close_seals_clone_admission_and_reports_a_non_timeout_error(self):
        owner = Owner(slots=2)
        ticket = admit(owner, "p", b"x")
        close = owner.close()
        self.assertEqual(admit(owner, "p", b"y"), "closed")
        self.assertTrue(owner.fail_queued(ticket, "encoding_error"))
        finish(owner, ticket)
        self.assertEqual(owner.poll_flush(close), (ticket, "encoding_error"))
        self.assertEqual(owner.poll_flush(owner.close()), (ticket, "encoding_error"))

    def test_close_seals_open_sticky_cohort_and_drains_it(self):
        owner = Owner(slots=1)
        cohort = owner.new_cohort()
        ticket = admit(owner, "p", b"x", cohort=cohort)
        close = owner.close()
        batch = owner.seal_batch("p", 1, 1, 0)
        self.assertIsInstance(batch, int)
        lease = owner.claim(batch)
        attempt = owner.offer(batch, lease, 0)
        self.assertTrue(owner.acknowledge(batch, lease, attempt, 0))
        finish(owner, ticket)
        self.assertEqual(owner.poll_flush(close), "ok")
        self.assertEqual(owner.used_slots, 0)

    def test_close_cannot_report_success_after_unknown_error_was_consumed(self):
        owner = Owner(slots=1)
        ticket = admit(owner, "p", b"x", deadline=1)
        batch = owner.seal_batch("p", 1, 1, 0)
        lease = owner.claim(batch)
        owner.offer(batch, lease, 0)
        flush = owner.begin_flush()
        owner.expire(1)
        finish(owner, ticket)
        self.assertEqual(owner.poll_flush(flush), (ticket, "timeout"))
        close = owner.close()
        self.assertEqual(owner.poll_flush(close), "pending")
        self.assertEqual(owner.close_deadline(), "timeout")
        self.assertEqual(owner.poll_flush(close), "timeout")
        self.assertEqual(owner.batches[batch].phase, "unknown")
        self.assertEqual(owner.used_slots, 1)

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
