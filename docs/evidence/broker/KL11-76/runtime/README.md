# Private metadata peer runtime

The fixed-peer runtime passed finite three/five-node TCP fault and observer
snapshot histories on latest stable Rust. Independent byte and journal replay
checked commit majorities, cancellation, restart prefixes and joined shutdown.
Whole broker checks passed344 default and484 all-feature tests, strict Clippy,
formatting and rustdoc. See [qualification](qualification/README.md) for the
sources, captures, process receipts, limits and negative controls.

The runtime owns one storage thread and bounded network tasks. Successful
shutdown joins workers, restores admission permits and closes the listener.
Numeric routes avoid DNS work. Directory keys and replayed membership determine
voting authority; a configured route alone does not authorize a leader.

This framing requires a trusted private network. Native Kafka replication,
peer authentication, production readiness and performance comparisons remain
open. Logical ownership counters exclude allocator overhead, RSS, OS buffers
and permits acquired before their RAII counter is constructed.

The qualification snapshot retains earlier source preparation, Rust1.85 syntax
checks, failed attempts and the Fetch shutdown test admission-race correction.
Those historical records are preserved; current checks use latest stable only.
