# Coordinator socket retirement

When reconnecting directly to a known idle or failed group coordinator, the
client retires the old socket before opening a replacement. The replacement
negotiates API versions and authenticates on the same operation deadline. A
canceled or failed handshake leaves the coordinator slot closed so a later call
can retry. The slot keeps the previous coordinator versions and offset ranges
as retry metadata. It clears SASL session times; frame buffers and correlation
IDs belong to the retired socket and are not carried to a new connection.
The replacement takes its client ID from the current configuration.
Coordinator rediscovery through a live bootstrap uses separate paths and was
not covered by these checks.

Each broker connection allocates one shared atomic retirement flag. If the
stream mutex is busy, its existing idle watcher waits for the mutex and closes
the stream; retirement does not spawn another task. A holder that never releases
the mutex can keep that watcher pending until runtime shutdown.

Timeouts and watcher progress require a running Tokio runtime. Dropping a
pending retirement future makes a synchronous cleanup attempt after its waiter
releases the mutex. A caller that retains a pending future after runtime
shutdown can retain a reserved mutex permit until it drops that future.

The stream is private. Production I/O borrows the connection, and the idle
watcher is its only independent stream owner. Test-only code can retain another
stream reference: if the watcher has already exited and such a holder retains
its reference, this design does not guarantee closure after the holder releases.

Closing a local socket does not determine whether the broker processed a
request. A blocked TLS shutdown may be cut short to release the stream. The
qualification tests use local TCP and TLS peers, including SASL PLAIN; live
broker failure recovery, other authentication mechanisms, connection churn, and
end-to-end performance require separate tests.

## Qualification

[Run 42447373118](https://github.com/mingley/partitionline/actions/runs/42447373118)
tested commit `682c11706bdf9b8964b4f316449658b71622a0f8`, based on main
`7f59ef470340db6027958f439df3716a8dde536c`, on a shared x86-64 GitHub
runner with Rust 1.99.1. Its source and test blobs match the clean candidate
`eeb339dcf677f5089f88e4c71d1a665e9e988a29`.

- All 33 selected library tests passed with default features and again with
  all features. Both local TLS peer-close tests, one with SASL PLAIN, also
  passed under each feature set.
- All three original-baseline coordinator peer-retirement witnesses failed
  with their expected messages. Separate target directories and executable
  hashes guarded against reusing candidate binaries.
- Three mutation controls failed as expected: reverting terminal TLS EOF shutdown,
  aborting the retirement watcher on owner drop, and suppressing both watcher
  and synchronous shutdown after a write timeout. These controls exercised the
  corresponding peer-close assertions.
- Strict all-target, all-feature Clippy, formatting and both TLS shutdown
  deadline tests passed. The candidate network suite passed 67/67 tests.
  The unchanged baseline passed 54/55, with the 8-byte peer-drain timeout
  failure described in [the fixture record](../write-timeout-peer-drain-20261108/README.md).
  The overall job failed because that baseline failure was retained.

Eight alternating local framed-loopback trials measured 10,000 sequential
round trips after 1,000 warmups. The median was 282.714 ms for baseline and
281.576 ms for candidate; individual paired changes ranged from -0.59% to
+0.77%. This shared-host mock test does not establish a performance gain,
Kafka throughput, connection-churn cost, or a ranking against other clients.
