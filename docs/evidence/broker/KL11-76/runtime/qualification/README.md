# Private metadata runtime qualification

Latest-stable Rust1.99.0 passed344 default and484 all-feature broker tests.
Formatting, strict all-target Clippy and rustdoc checks passed for both builds.
Fresh captures exercised three/five voters and three/five configured peers with
one initial observer. Each build also ran eight ownership controls.

Independent Python replay checked majority commits, exact request cancellation,
multichunk snapshot installation, observer promotion and four durable restarts.
Each build retained16 joined TCP owners/supervisors and four parent-waited
controlled crashes. Packet reads, forwarding writes and consumed acknowledgements
are checked separately. The replay rejected59 forged history/receipt variants
and six ownership variants per build; the original framing checks reject26
malformed packets. No cross-process global clock is inferred.

The runtime uses fixed numeric routes on a trusted private network. Configured
routes do not grant membership authority. This qualification covers finite
private-peer behavior. Native Kafka replication compatibility, authentication,
production readiness, soak results and performance ranking remain separate work.
Resource counters cover constructed RAII owners; allocator overhead, RSS, OS
buffers and acquired-but-unpublished permits are excluded.

summary.json records the published Rust and independent checker sources.
execution/ retains command logs, captures, executables and earlier failures.
sources/ contains verified complete source archives. oracle/ and oracle-raw/
retain the independent parsers and controls. FILES.json and SHA256SUMS describe
this snapshot. Do not regenerate it.
