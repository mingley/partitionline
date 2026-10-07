# Full Rust Kafka client and broker goal

The user's 2026-10-02 goal covers a fully featured Rust Kafka client **and**
broker, production qualification, and reproducible client/server performance
leadership. Work continues in successive small, owned tasks, with direct pushes
to `main`. The earlier client-only scope does not exclude broker/control-plane
APIs from this expanded goal. Existing client tasks and evidence remain intact.

`tasks.json` remains the only status database. KL01–KL10 cover the client;
KL11 adds the broker. Finish independent ready cards while respecting active
hot-file claims. Split any large subsystem contract into dependent children
before implementation. A measured rejected optimization is a completed study,
but does not implement its rejected behavior or prove performance leadership.

## Crate and protocol boundary

`partitionline-broker` is an unpublished crate built with latest stable Rust.
It is excluded from the root workspace and client package. Its dependency graph
is separate; backend and authentication changes follow the dependency policy.

The initial server feature matrix is
`tests/conformance/broker/features.json`. Its API keys 0–92 form a classification
envelope, not verified version ranges. KL11-04 must pin the exact Apache source
and complete/adjust that envelope for target releases 4.1.2, 4.2.1 and 4.3.1.
Every current data/control/coordinator/security/admin API needs a disposition and
versioned independent evidence. Retired/legacy APIs require an explicit upstream
reason; they cannot be silently counted as implemented. Advertise only actual,
tested handlers. Protocol unsupported/error responses must follow the pinned
wire contract, never fabricated success.

## First independent implementation contracts

KL11-02 creates `src/transport.rs` and transport integration tests. Use Tokio
framed I/O with positive configured limits for connections, request/response
bytes and absolute read/handler/write deadlines. Bound active tasks; cap length
before allocation; preserve per-connection response order; join/drop sockets and
tasks during cancellation/shutdown. Provide an asynchronous handler interface
that can later await storage without blocking the executor. Module exports and
shared manifest edits are coordinated by the main agent.

KL11-03 creates `src/journal.rs` and journal integration tests. Use an append-only
checksummed entry format with checked monotonic logical offsets and an explicit
record count. A durable append synchronizes data before reporting success.
Caller-supplied paths are configuration, not unchecked topic names. Bound entry
size, total file bytes, recovery/index memory and fetch output. A demonstrably
incomplete final entry can be truncated on recovery with a reported outcome;
interior CRC/length/offset corruption fails closed. Failed/partial writes must
not silently advance the offset or allow subsequent appends into damaged state.
Recovery scans must not allocate from unchecked lengths or assume a partial read
is EOF. This journal foundation is not yet Kafka record-batch validation,
replication, transaction durability, or a complete broker.

These modules own their public errors/configuration/interfaces. They do not
modify client hot files or impersonate the benchmark null broker. Later cards
add real header/API negotiation, persistent topic metadata, validated Produce,
Fetch and ListOffsets before cross-peer single-node interop.

## Full server behavior

The queue covers segment/index recovery, retention and compaction; durable
metadata quorum election/replication/snapshots and broker fencing; replica
catch-up, ISR/high watermarks and failover; idempotent producers and transactional
markers/LSO/offset atomicity; classic/cooperative/static/modern consumer groups,
share and streams coordinator protocols; TLS/mTLS, SASL, validating OIDC,
optional reviewed GSSAPI, ACLs and delegation tokens; quotas, telemetry, full
administration and tiered logs.

Each implementation needs failing behavioral/fault cases and pinned independent
wire/semantic evidence where relevant. Persistence tests include restart, torn
writes and corrupt bytes. Distributed histories include partitions, stale
epochs, quorum loss and lost acknowledgments. Acks, fsync, replication visibility
and ambiguous outcomes are explicit; no durability guarantee is inferred from
matching record totals.

## Production and leadership completion

Production needs complete independent API/version/profile matrices; at least
1,000 seeded faults per supported profile; bounded overload and shutdown; current
mixed-version upgrade/security histories; sustained fuzz; 24-hour resource and
seven-day operational continuity evidence; verified packaging/docs, backup,
restore and rollback. Running or launched campaigns remain unfinished until
their actual duration and acceptance criteria pass. Preserve failed attempts
and minimized regressions. Existing Suite HOLD and release/signoff rules remain.

Client and server comparisons use pinned relevant competitors on equal hardware,
payloads, durability, security and offered load. Report verified useful work,
CPU/RSS/disk and tail latency; retain every losing cell and confidence interval.
Require repeated paired measurements on x86_64/arm64 and independent reproduction.
Profile leadership needs superiority in every required named cell. A finite
benchmark set establishes a scoped result, not a mathematical universal claim
over every possible machine/workload/client/server in the world.

KL11-56 is the final fail-closed audit and depends on the entire existing client
queue plus the server completion chain. It cannot close with missing required
features, unresolved correctness failures, unexecuted operational evidence or
unsupported performance claims. Publishing/deploying, paid infrastructure,
external messages and live traffic require their own authorization; implementing
and testing the goal does not silently perform those operations.

