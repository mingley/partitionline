# Worker socket write controls

This source-only v3 patch adds two proposed controls for the v16/v17 fixed client.
It does not change production code, implement socket reuse or claim a speedup.
The controls have not been compiled or run. V2 bounds gate installation, peer response writing and outcome-drain waits by five seconds. V3 binds the three `read_exact` results and uses a checked gate slice; prior versions and all original assertions remain intact. Stable rustfmt parses and checks
all three changed files; exact source pins and patch hashes are in `pins.json`.

The fixture admits one real `Pending` through the existing test Worker channel,
performs a real ApiVersions exchange on that Worker's plaintext loopback TCP
connection, and sends Produce v3 on that same connection. It uses neither
`Producer::send` nor `Worker::run`; SASL, TLS, coordinator roles and real Kafka
are outside its scope. The remaining fixture records are explicitly failed
before the one-record wire control begins.

The first control wraps that real TCP stream in a test-only gate. The gate
counts bytes actually accepted by `TcpStream::poll_write`, handles short writes,
reports its prefix only after eight bytes were accepted, then returns `Pending`
until released. The gate is scoped to these single-writer test fixtures; concurrent release during a write poll is not qualified. The peer reads the eight-byte Kafka length/API/version prefix;
the declared frame is longer. A further poll of the actual `Worker::fire`
remains pending and the peer has no extra byte available. Dropping that future
marks the connection closed; the check then observes the one record in
`Preparing`, `write_started`, an empty Worker in-flight queue, and a zero
`BrokerConn::in_flight` counter. Dropping the closed Worker reports the record
as ambiguous Timeout and releases its buffered bytes. The counter and Worker
state are inspected **after cancellation**, not concurrently during the
mutable borrow; source inspection shows the cancellation guard only marks the
connection closed. This is not an owner-lease or descriptor-retirement test.

The second control lets the peer read a complete Produce frame but holds its
response. After the actual `Worker::fire` returns, the Worker owns one pending
correlation while `BrokerConn::in_flight` is zero and its buffered response is
empty. The peer then replies with the same correlation; `Worker::wait_one`
consumes it and completes the record. This demonstrates the proposed witness
only if the test is compiled and passes; no such runtime result exists yet.

Before using these controls to justify socket reuse, add a real owner-decision
witness for the same physical socket and generation, partial-write and
in-flight cancellation/close with retained ownership, local descriptor and
watcher retirement, authentication expiry/reauthentication, TLS, session and
routing changes, and no silent ambiguous replay. Preserve the original tests,
assertions and timeouts. The v17 benchmark is additionally blocked by its
baseline case 23: its broad expected-failure matcher can accept error 14 before
the intended cancellation barrier. The full baseline witness must be repaired
before any v17 benchmark execution or interpretation.
