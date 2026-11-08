# Coordinator socket retirement

When reconnecting directly to a known idle or failed group coordinator, the
client retires the old socket before opening a replacement. The replacement
negotiates API versions and authenticates on the same operation deadline. A
canceled or failed handshake leaves the coordinator slot closed so a later call
can retry. Coordinator rediscovery through a live bootstrap uses separate
paths.

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
