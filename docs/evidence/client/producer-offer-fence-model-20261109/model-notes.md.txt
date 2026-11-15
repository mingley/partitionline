# Producer offer and identity fence model

This is an isolated state-machine experiment. It is not linked to the producer,
broker connection, admission queue, callbacks, or transaction implementation.
Passing its tests does not make the producer safe or production ready.

The model starts with an already initialized identity. It atomically reserves all
partitions in a frame, roots immutable sequenced record-batch bytes, and retains
the same identity and bytes after an ambiguous offer. Only one batch per partition may be active. A drain prevents
new frame reservations but permits a previously reserved frame to finish.
An identity-changing RPC can start only after every retained frame has a
definitive result and its receipt has been recorded and retired. Dropping an
RPC guard leaves the identity outcome unknown and the gate closed.

The model requires the offer to be registered before the first possible socket
poll. A dropped offer remains ambiguous and retains its session/correlation
reservation. Before retry, the caller must attest that all writers and response
dispatchers for the exact session and attempt are retired and joined. Joining
does not prove that the broker did not append. The retry retains the same batch
contents, producer identity, and sequences; it does not authorize a new
identity or sequence. The retry requires a newer session number. The caller
must provide honest, never-reused session generations; this model does not
allocate or authenticate them. The success path assumes an
externally validated, complete Kafka response for every requested partition.
Partial outcomes and protocol-specific error dispositions are not implemented.

A fatal fence blocks later offers but an already offered socket task may still
write until it is stopped and joined. The model's quiescence check waits for explicit retirement attestations for
dropped frame and identity RPC guards. These are caller assertions, not proof
that real transport tasks stopped. Publishing a successful identity RPC likewise
requires the caller to attest that its transport actors stopped and joined. At a fatal
shutdown deadline, an offered or retried frame reports a broker outcome of
unknown and remains rooted. A frame that was never offered reports unsent only
within this model. There is no bounded time to resolve an ambiguous frame:
drain and identity transition can remain blocked forever. Process exit loses
the in-memory journal, so restart safety and durable receipts remain unsolved.

The one-active-batch rule is a concurrency restriction. In an idealized model
with otherwise identical batch sizes and network RTT, its throughput ceiling
is one batch per partition per RTT, versus up to N batches per RTT with a
pipeline of N. For N = 5 that is up to an 80% reduction in the idealized
ceiling. This is not a measurement or a performance improvement claim.

Limits on frame count, parts per frame, and payload bytes constrain logical
journal contents, not process RSS or peak admission memory. Allocation, map
metadata, caller-owned data, callbacks, and pre-reservation construction are
outside those limits. The mutex is held only for state operations; there is no
lock held through encoding, I/O, callbacks, or an identity RPC.

Before this can inform a production fix, the real implementation needs:

- Root ownership of already-admitted records before batch formation, immutable
  batch membership, sequences, payload and topic incarnation.
- A cancellation-safe root journal and authoritative joined transport tokens,
  wired into the actual write and response paths; every identity-changing RPC
  must use the same gate before its first possible network write.
- An explicit policy for Kafka versions without topic IDs, acks=0, partial
  multi-partition responses, protocol errors, transactions, terminal delivery
  uncertainty, shutdown, restart, and receipt/callback accounting.
- Crash, cancellation, partial-write, concurrency, real-broker, compatibility,
  and performance qualification. This model does not supply that evidence.
