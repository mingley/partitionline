# KL11-02: bounded framed transport

The transport owns a Tokio TCP listener and connection task set. It validates a
signed big-endian four-byte length before reserving the request payload, then
handles and optionally writes one response at a time on each connection. A
`Some(payload)` result writes one frame; `None` writes no bytes and keeps the
connection usable. This supports a future no-reply API without implementing
Kafka Produce or interpreting `acks` here.

The implementation has independent positive connection, handler, request,
response and deadline limits. Maximum supported configuration is 65,536 owned
connection tasks, no more handler futures than connections, 64 MiB per request
or response payload, and a positive deadline no longer than 24 hours. Empty
payloads are valid for this generic framing layer. Defaults are 64 connections,
32 handler futures, 8 MiB request/response payloads and 10s/30s/10s deadlines.

One absolute read deadline covers prefix plus body. One absolute handler
deadline covers both semaphore acquisition and the application future. One
absolute write deadline covers prefix plus body. No progress resets a deadline.
Handler errors and invalid frames close only that connection. Reaping completed
workers before accepting sockets prevents completed tasks from unnecessarily
occupying admission slots.

Shutdown stops admission, drops the listener, signals every worker, drops its
socket and application future, and drains the owned `JoinSet`. Cancelling the
shutdown await retains its runner handle for retry. Successful repeated calls
return the same final joined report. Dropping the transport requests cleanup;
explicit shutdown is how a caller observes that all tasks joined.

## Retained validation

`stable-results.json` and `1.85.0-results.json` record exact SHA256 hashes of the
transport source/test and manifest/lock/export context. The tested Git base is
`4b3d11fae284fc4b314d9f2f225d68900aa717ec`; the new transport was an uncommitted,
frozen candidate during these checks. The integration coordinator will pin the
source commit separately rather than describing that base as the source commit.

Both runs verified their recorded files remained unchanged during validation.
Stable is rustc 1.99.0; MSRV is rustc 1.85.0. Each ran four successful checks:
focused format, 16 transport integration tests, strict library plus transport
test Clippy, and strict rustdoc. Thus the final candidate has 32 successful test
executions in two suites, with eight retained check invocations and zero final
failures. The logs contain no host identifiers or workspace paths.

The behavioral cases are:

| Cases | Observable assertions |
| --- | --- |
| Configuration | Zero/out-of-range connection, handler, byte and duration limits rejected before I/O; valid defaults exposed. |
| Fragmentation and boundaries | Bytewise prefix/body fragmentation, empty payload, below-limit and exact-limit round trips. |
| Pipelining | Delayed first handler preserves wire and handler order; a no-reply request emits no empty frame and the next two requests receive replies. |
| Length attacks | Prefix-only `-1`, `i32::MIN`, cap+1 and `i32::MAX` close before a handler call; a healthy peer still works. Source ordering verifies no length-derived reservation precedes the cap check. |
| Truncation | Incomplete prefix and incomplete body close without handler invocation. |
| Read deadline | Idle read expires; 60ms progress gaps under a 100ms cap still expire after aggregate prefix/body time exceeds the cap. |
| Handler isolation | Application error and over-budget response close only their connection; healthy peer still replies. |
| Handler deadline | A pending future expires and its guard drops; semaphore wait consumes the same absolute deadline instead of starting a fresh timeout. |
| Admission | Full connection cap drops an excess socket without invoking its handler. |
| Work and shutdown | Four admitted requests with two handler permits never exceed two active futures; shutdown joins four workers and drops both active guards and both queued requests. |
| Cancellation | A manually polled and cancelled shutdown await can be retried; joined counters, dropped guard and closed socket are checked. |
| Drop | Dropping the transport requests listener/socket/handler cleanup; reconnect fails. Joined completion is observed through the explicit-shutdown cases. |
| Write deadline | An unread 64 MiB response blocks under loopback backpressure, expires its absolute 50ms write deadline, and its worker joins. |

To reproduce, use installed toolchains and an appropriate external CPU affinity
(the retained runs used CPUs 0-2,4), then run from the repository:

```sh
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 python3 docs/evidence/broker/KL11-02/run-validation.py stable
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 python3 docs/evidence/broker/KL11-02/run-validation.py 1.85.0
```

The runner inherits `CARGO_HOME`, `RUSTUP_HOME`, `PATH` and `CARGO_TARGET_DIR`,
adds `RUSTDOCFLAGS=-D warnings`, and records actual commands and their exits.

## Failures and scope limits

The first draft compile failed because the integration test crate lacked crate
documentation under `missing_docs=deny`. Adding its crate documentation resolved
the failure. The draft then passed 15 tests on stable and strict Clippy. Review
required an optional reply result for future `acks=0` support; the final
candidate changed that interface and added the sixteenth test. These earlier
draft checks are not counted as final candidate validation. The original
compiler failure excerpt is retained in `draft-failure.log`.

The tests are local TCP framing tests with an independently operated client
socket in the test process. They establish no Kafka API/schema/version behavior
or external Kafka interoperability. The no-reply test checks only the transport
primitive. Later Kafka handler cards own pinned upstream semantics and peers.

Handlers must cooperate with the async executor and have nonblocking
destructors. Rust/Tokio cannot preempt a synchronously blocking poll or destructor,
and the transport does not own tasks a handler independently spawns. Joined
shutdown of a hanging handler means a pending asynchronous future in these
tests, not arbitrary blocking application code. Dropping the transport requires
the runtime to continue running for cleanup; explicit shutdown observes it.

Request reservations and wire response sizes are bounded; handlers must bound
their own allocations. The connection cap also bounds buffered queued requests,
but there is no independent total-byte budget, OS socket-buffer cap, process RSS
claim, quota fairness guarantee, or load/performance qualification. The write
deadline test depends on the retained environment's socket backpressure; it is
not a cross-platform memory or timing claim. Runtime/process termination and
listener accept failures have no fault-injection qualification in this card.

No authentication, TLS, Kafka request headers, storage, readiness, executable
broker, or production qualification is implemented or inferred here. The
unpublished independent crate uses its existing dependency graph; this task adds
no dependency or published client default.
