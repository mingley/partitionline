# Producer completion-domain model

This is an isolated executable model. It does not change the producer and is
not a production implementation.

The model serializes admission, a flush cutoff, completion and worker lease
arbitration with one mutex. Admitted records receive monotonically increasing
sequence numbers. The flush cutoff and deadline are captured at first poll,
matching the current async method boundary. A flush waits until a contiguous
completed prefix reaches its cutoff, so canceled and overlapping flushes do not
lose older pending records.
The model retains a bounded slot for every active sequence, coalesces completed
out-of-order intervals, rejects stale lease incarnations, and stops admission
on sequence or incarnation exhaustion. A separate bounded publisher-operation
set survives record completion or retry, and prevents a late publisher from
mistaking an accepted record for a rejected one. A bounded first-error slot is
claimed only by the flush future that returns it; later worker errors are coalesced.
Deadline expiry and a recorded delivery timeout are distinct model variants.

The tests manually poll flush futures. They explore a concurrent claim versus
recovery race, publisher return after completion, retired workers, ordered
error publication, finalization steps, overlapping and canceled flushes,
bounded capacity, exhaustion, and a reference-set comparison for random
out-of-order completions. The x86 Actions job also reports timings for
the standalone model. Those timings do not measure the producer or establish a
performance change.

This model leaves essential work open:

- A test explicitly marks resource release, receipt publication, and callback
  completion. It does not own or execute those operations.
- There is no real receiver, channel, waker registration, wakeup, timeout
  driver, retry actor, sticky partitioner, or broker. The model future must be
  manually repolled. The publisher set models bookkeeping, not whether an OS
  thread or channel operation will eventually return.
- It does not implement an acyclic supervisor, bounded deferred callback
  executor, transaction generation fence, producer close, or runtime shutdown.
- Pre-admission ownership and byte reservations are not modeled. The model
  begins at a committed logical admission.
- Recovery from an in-flight lease assumes the old owner has stopped; it does
  not prove this or resolve an ambiguous Kafka write or sequence.
- Worker IDs are limited to 64 unique values and are never reused. A real
  implementation needs a bounded, ABA-safe worker lifecycle.
- The first-error policy intentionally coalesces later errors and is not scoped
  to individual flush cutoffs. Finalizing a modeled failed record installs its
  modeled error in the same mutex transition as completion, but arbitrary
  independent worker errors may still be published at other times. This is a
  placeholder, not a decision to change the producer's documented error behavior.
- The model's fixed static error strings avoid user-defined clone and destructor
  behavior. Its mutex and manual polling do not demonstrate fairness or
  nonblocking cleanup. The deadline starts before acquiring the domain mutex,
  but a contended blocking lock can itself delay the poll past that deadline.

The existing producer can still return from flush while a retiring worker owns
an accepted record. This model is a tool for testing part of a possible design,
not evidence that this or any other production gap is fixed.
