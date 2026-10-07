# Private peer capture checks

These Python programs decode captured peer frames and replay raw metadata,
election and snapshot journals. They import no Rust implementation.

`check-runtime-capture.py` checks the original Cargo invocation, frozen source
files, permission modes, capture files and retained test executable. It then
checks four declared topologies: three and five voters, plus three and five
configured peers with one initial observer. Configured routes do not grant
voting authority. Observer promotion comes from replayed membership records.

The replay checks exact requests, cancellations, consumed acknowledgements,
majority commits, snapshot chunks, durable restart prefixes and joined shutdown.
Proxy receipts distinguish packet reads, partition drops, delayed forwarding and
successful writes. Each process keeps its own monotonic clock epoch.

`check-causal-controls.py` checks altered packets and receipts in memory; the
original capture stays unchanged. `check-owner-controls.py` checks genuine
abandoned votes, late acknowledgements, stale timeouts and owner joins. Direct
owner controls use their captured one- or sixteen-slot channels and are reported
separately from public TCP runtime limits.

`peer_bytes.py`, `check-peer-bytes.py` and `decoder-controls.json` preserve the
original hand-built framing checks. Run that script in a separate copy because
it writes its receipt. `run-development-checks.py` is a retained historical
runner; current qualification uses latest stable Rust and fresh execution paths.

These are finite private-peer histories. They establish no native Kafka
replication compatibility, authentication, production readiness or performance
ranking. Ownership counters exclude allocator overhead, RSS, OS buffers and
permits acquired before their RAII counter is constructed.
