# Client candidate v10

V10 extends the isolated v9 candidate with a test-only Produce response gate.
The retained-backing test can hold its first reply until cancellation is
observed, and setting the mock delay to zero disables its reply timer. A new
integration control checks gate release and cleanup. The prior 167 required
identities, 22 baseline cases, test targets and budgets are preserved.

Exact full-source reconstruction, forward/reverse patches, Rustfmt and an
independent static fixture review passed. Cargo compilation, Clippy and Rust
runtime tests have not run for this combination. The earlier retained-backing
timeout's full cause is unproven; Mock Produce handshake failures remain.
Live Kafka compatibility, producer ordering and replay, production readiness
and performance qualification remain open.
The original v9 assets and import manifest are retained in this directory.

A second new integration control measures handshake overlap. The original
450 ms end-to-end test is unchanged and its v7 failure remains open.
