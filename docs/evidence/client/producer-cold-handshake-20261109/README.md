# Producer first-send socket trace

9 November 2026

The current producer keeps the bootstrap metadata connection and each Produce
worker connection as separate physical sessions. A new worker negotiates its
own API versions and, when configured, authenticates its own session. This
trace measures a narrow cold-send cost; it is not a Kafka benchmark or a
client ranking.

## Socket controls

[Run 42672573216](https://github.com/mingley/partitionline/actions/runs/42672573216)
used the public `Producer` API and the repository loopback mock on x86_64
Linux, stable rustc 1.99.1 and LLVM 23.1.1. The producer used one bootstrap,
one partition, one worker connection, `acks=1`, idempotence disabled, zero
linger, and a 30-second idle limit. The mock recorded bounded peer-side socket
events; it did not inspect local file descriptors.

In both default and all-feature profiles, a gate held the first new
ApiVersions response after `Producer::new`. The peer saw Metadata on
bootstrap socket 1 and a new ApiVersions request on worker socket 2. While
that response was held, the send remained pending and no Produce request
arrived. After release, two sends succeeded at offsets 0 and 1; both Produce
requests used socket 2, and the second send required no new ApiVersions
request. A separate SASL PLAIN case observed SASL handshake and authenticate
requests on each of the two sockets. These are peer-assigned socket IDs in a
mock run, not proof of local descriptor destruction.

A version control changed the mock's advertised maximum Produce version after
the bootstrap session had opened. The bootstrap had advertised v12; the
new worker session advertised v3, and its Produce request used v3. This is
a controlled peer model, not an observed broker upgrade. It shows why
copying old negotiated ranges onto a new physical session is unsafe.

## Injected-delay diagnostic

The same run built an optimized test binary and ran 12 alternating pairs
on one available CPU, with no injected delay or a 50 ms mock delay on the
new worker's ApiVersions response. Each trial used a fresh mock and
producer, checked two successful offsets and two stored records, and
verified that both Produce requests used the one newly negotiated worker
socket. First-send elapsed time includes metadata discovery, connection
setup, mock scheduling, negotiation and delivery; warm-send elapsed time
is a second send on that worker.

| Setting | First-send median | Warm-send median |
| --- | ---: | ---: |
| No injected handshake delay | 0.639 ms | 42.059 µs |
| 50 ms injected handshake delay | 50.641 ms | 42.225 µs |

The median paired increase in first-send time was 50.011 ms, with a
49.549–50.189 ms observed range. The [raw samples](samples.csv) retain
all 24 trials. These are controlled loopback measurements with an artificial
delay and a tracing mock, not a network RTT, throughput or tail-latency
comparison. The workflow did not retain a hash of the release test binary.

## Source and limits

The production source was main commit
[`a7b6edc`](https://github.com/mingley/partitionline/tree/a7b6edc1645e3a4789cf699a3b9e7656c55a3d10):
`src/producer.rs` Git blob `61b084c065291f077aa6112fdba6dc15b33848bd`
and SHA-256
`6641b7a107b2bb2b74976d1d72cdf4a9a6c09bc02f98dfd33947c4c977485345`.
The mock `tests/common/mod.rs` Git blob was
`4afd104e0b5e10227167d0d38328fe03dda07a0a` and SHA-256
`45921a161ac3d1e310558d2ec37a9f5c7b4f8b5e7ed1813b34f6ccf468c27559`.
The test fixture Git blob was `a54a90c833fb25bcb580bbaf5622ab6963d622e2`
and SHA-256
`827f2ad2b599bc0e081838c2acb9aacb5ffca9933a06fa711d869ef0cfef6a3a`.
The `Cargo.lock` blob was `96e364d34121fe90183f87f4f49fd92b883bc447`.
The [fixture](producer_socket_trace.rs) can be copied into `tests/` of
that source to run `cargo test --locked --test producer_socket_trace
cold_send_`, and the same command with `--all-features`. The ignored
`paired_injected_handshake_delay` test is the release diagnostic.

An earlier [run](https://github.com/mingley/partitionline/actions/runs/42671862406)
failed with no jobs returned by the GitHub job summary. Its cause is unknown;
it produced no test results. The successful run used a separate serial-job
qualification commit
[`57577b2`](https://github.com/mingley/partitionline/commit/57577b2c37dcdd00caabe84ce5f9c3d9c403fddf).

## Speculative handoff assessment

One possible direction is to open a bounded provisional connection to a
bootstrap address while first-topic Metadata is in flight, then transfer that
same negotiated session only when the returned leader and route remain valid.
This is source analysis, not a measured candidate or an implementation.
A speculative connection is externally observable: even if record results
remain unchanged, it can add connection attempts, authentication and broker
resource use before Metadata succeeds. A default-on design cannot claim all
observable behavior is unchanged; an opt-in policy or documented change is
needed.

The pinned [readiness path](https://github.com/mingley/partitionline/blob/a7b6edc1645e3a4789cf699a3b9e7656c55a3d10/src/producer.rs#L2540-L2578)
awaits topic metadata before it can identify a node and nudge a worker slot.
The [connect loop](https://github.com/mingley/partitionline/blob/a7b6edc1645e3a4789cf699a3b9e7656c55a3d10/src/producer.rs#L6500-L6610)
accepts known node/slot pairs and limits its in-flight dials. It cannot own
an address-only provisional session without an explicit new state and budget.
`spawn_slot_worker` starts a worker before the handle is published, while
dropping the final `WorkerTask` aborts it without by itself proving physical
transport destruction. A speculative path should retain a raw session in a
supervisor-owned state before any cancellable await, then transfer ownership
only after reserving the actual worker slot. It needs a finite count and byte
budget that covers pending connects, successful provisional sessions, worker
transfer and cleanup, in addition to the existing per-node connection limits.

Metadata failure, cancellation of all waiting sends, close, idle expiry and
deadline expiry must have defined cleanup and admission behavior. A probe must
not mask a metadata error or silently extend the caller's original
`max_block` budget. Existing [metadata publication](https://github.com/mingley/partitionline/blob/a7b6edc1645e3a4789cf699a3b9e7656c55a3d10/src/producer.rs#L5939-L5952)
clones the cluster, awaits image application, then replaces the shared
cluster; Produce responses can also update endpoint data. An address-only
comparison, or a generation incremented outside the same publication and
slot-ownership boundary, does not prove safety for observed A→B→A changes.
A coherent design needs a non-repeating route identity tied to every relevant
publication, an atomic recheck before transfer, and explicit behavior for
already published workers. The saved address string alone is not a proof of
remote broker identity.

The existing raw Produce write path has a separately reviewed
[SASL-expiry gap](../sasl-expiry-boundary-20261103/README.md). A provisional
session can age while Metadata is pending, so checking only at dial completion
is insufficient. The handoff must preserve its actual TLS/authentication
session and per-socket versions, check expiry at the application write boundary,
and preserve ownership of partial writes, outstanding replies, retries and
ambiguous outcomes. Reauthentication cannot interleave with an application
frame. These requirements are not tested by the socket trace above.

A candidate needs event-gated valid-record controls for successful overlap,
changed and A→B→A endpoints, failed or canceled Metadata, connection and byte
admission, idle and authentication expiry, TLS/SASL, in-flight or partial
writes, retry ordering, shutdown and actual cleanup. It also needs a matched
measurement on the real candidate and an independent Kafka history before
claiming a benefit. No such candidate, valid-record overlap trace or speedup
has been established here.
