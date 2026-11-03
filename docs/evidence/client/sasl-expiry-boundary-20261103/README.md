# Producer SASL expiry at the write boundary

This is a source review of the `main` snapshot at commit
`e45c369f915cab4d4f70b20668973957458e1dd2`. It does not demonstrate that a
broker accepted a request after its SASL session expired. No Rust patch,
native test, network request, benchmark or publication was made.

The separately staged v16/v17 client uses different producer and network
sources. Its [live-socket design](https://github.com/mingley/partitionline/blob/e45c369f915cab4d4f70b20668973957458e1dd2/docs/evidence/client/live-socket-reuse-20261103/DESIGN.md)
already records the same raw-Produce inspection gap for those candidate bytes;
the source evidence below concerns the distinct main-branch implementation.

## Source findings

`Worker::fire` assigns sequences, encodes a Produce frame and calls
`BrokerConn::write_all_timeout` directly. That path does not check the socket's
SASL expiry. `BrokerConn::send_deadline` does check expiry, but it is a separate
path and checks before encoding and before the asynchronous stream lock.
Consequently, adding a check only at the start of `fire`, or just before waiting
for the stream lock, would leave an interval in which the session can expire.

The write pump may also return `Pending` while scanning buffered input before
its first application `poll_write`, or after a write attempt has returned
`Pending`. A guard immediately before entering the pump does not cover that
interval. A future fix needs an application-write-specific check at each
first-write attempt, while no bytes of this frame have been accepted. Checking
and issuing that poll must have no suspension between them. Actual elapsed
time can still advance between a clock read and the operating-system call;
the design must state that linearization point instead of promising a physical
instant it cannot observe.

`write_all_deadline` is shared by other request paths, including SASL. A global
expiry refusal in that method could block authentication or reauthentication;
the application-frame guard must distinguish those operations. Once a partial
frame has been accepted locally, its peer outcome is not known. Expiry or
cancellation then requires the existing connection to be quarantined and the
operation resolved under an explicit ambiguous-outcome policy, rather than
writing a new frame or silently replaying it.

## Why a small retirement patch is not yet justified

* The worker can already have earlier fully written requests awaiting replies.
  Its own `in_flight` queue tracks these, whereas `BrokerConn::in_flight` does
  not track these raw writes. `retire_worker_nudge` closes admission; when an
  active producer's worker is dropped, `Worker::drop` requeues that queue.
  `wait_one` also requeues outstanding batches on retriable read failures.
  Closing the session merely because a later frame was rejected could thus
  replay an earlier request whose broker outcome is unknown.
* A definitely unwritten *attempt* is not necessarily a never-written batch.
  `assign_sequences` preserves sequences on a retried batch, but
  `rollback_sequences` rewinds a matching map tail without knowing whether
  that sequence was used on an earlier connection. For example, if a broker
  accepted sequence 0, its reply was lost, and the retry retains the same
  producer ID, epoch and sequence, refusing that retry before its first write
  and rewinding the shared next sequence from 1 to 0 can assign sequence 0 to
  a later record. A changed epoch generation clears the old assignment. This
  source-derived counterexample has not been run against a broker here.
* In this main snapshot, accepted records are held in local `batch` and `groups`
  values across cancellable awaits in `fire`. They have no per-record Drop
  cleanup for the shared byte reservation. `Worker::drop` only sees records
  still in its own queues. Cancelling the worker while those local values are
  held can drop their senders and leave their accounting unreleased. This is
  an inherited source-derived risk, not a new expiry-patch regression. The
  staged v16/v17 candidate has a different `Preparing` owner and must be
  assessed independently.

Making an expired-before-write outcome distinguishable from an I/O error is
necessary but insufficient. A safe change also needs durable ownership for
the current batch, a policy for earlier unresolved requests, and sequence
recovery that distinguishes a fresh unsent batch from a retry. Reauthenticate
only after the connection is wire-quiescent; a close flag alone is not proof
that the local transport has been destroyed. These requirements make an
unreviewed two-line guard or a generic retriable error unsuitable as a fix.

## Controls needed for an implementation

Use a controllable clock and actual Worker/transport phase barriers. Tokio's
paused clock alone does not control the `std::time::Instant` used here.

1. Check an already-expired session, expiry while waiting for the stream lock,
   and expiry after a prewrite `Pending` poll. Confirm that the peer sees no
   bytes from the refused Produce frame. A valid session, no SASL session and
   zero/unlimited lifetime must retain their existing behavior; authentication
   frames must not be blocked by an application-write guard.
2. Hold one and then several earlier replies while a later frame reaches the
   expiry boundary. Resolve the earlier replies in order, then separately drop
   the peer with unknown outcomes. Assert no earlier request is silently
   replayed, no later request overtakes it, and the rejected frame is either
   still owned or explicitly failed. Observe actual socket identities and
   local retirement, not just a connection flag.
3. Exercise both fresh and previously attempted idempotent batches. For a
   retry with an already assigned sequence, arrange a previously accepted
   request with a lost response; a prewrite refusal must not make a later
   record reuse its sequence. Include producer epoch changes and a
   transactional batch, with no sequence gap or silent deduplication claim.
4. Gate partial writes, cancellation while waiting for the lock, cancellation
   during encoding and rejection cleanup, and cancellation while draining
   older responses. Every accepted record must retain exactly one owner or a
   reported terminal/ambiguous outcome; verify callbacks and buffer accounting
   once, with no replay after an ambiguous partial write.
5. Cover `acks=0`, delivery deadlines, reconnect/authentication failure and
   both normal and all-feature builds. An unsent request must never report
   success. Preserve the existing mock and peer tests and add a real SASL
   broker or faithful protocol-peer history before claiming the gap closed.

Run `python3 -B verify.py --repo /path/to/partitionline` to check the exact
source pins and the scoped source evidence using local Git objects. The check
is static; it does not execute the Rust implementation or establish runtime
behavior. `manifest.json` records the inputs and SHA-256 hashes.
