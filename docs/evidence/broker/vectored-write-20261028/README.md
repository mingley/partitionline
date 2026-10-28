# Vectored transport write candidate

This candidate offers the Kafka response length prefix and body together to a
vectored writer, up to the existing 8 KiB per-poll limit. It handles partial
writes, including a writer that accepts only the first slice. A timed-out frame
can be truncated; the connection is then closed. The candidate was tested in an
isolated worktree at base commit `d3e1726a174ea09aa99a2412625e1ec5285f344f`;
[candidate.patch](capture/candidate.patch) has SHA-256
`54ac30e10a3de224a797e317bda598c261dd7d7c402658665bfe57b005d245da`.
The patch was uncommitted at test time.

On x86_64 with stable Rust 1.99.1, the clean-target run passed 11 scoped unit
tests, 16 transport integration tests with default features, 16 transport
integration tests with all features, 15 TLS integration tests with all features,
and strict all-features Clippy on the selected lib and integration targets. The
unit tests cover the 8 KiB limit, prefix and body partial writes, an empty body,
a first-slice-only mock writer, and timeout followed by peer EOF. The TLS suite
covers real encrypted connections and frame round trips; it does not force each
possible TLS partial-write boundary. The complete broker test suite and the
integrated committed tree were not qualified by this run. No performance
comparison was performed, so these results do not establish a speedup.

The [clean run](clean-target/) contains the commands, logs, environment, and
source and binary archives. The pre-run source archive, including root
`clippy.toml` and the broker's test fixtures, matches the 5,922 pre-run file
hashes; the pre- and post-run source indexes also match. The four test executables
were copied and hashed after execution and the archived files match that
post-run index. These checks do not attest to the bytes at execution time; the
snapshot does not include dependency source. Root Cargo and toolchain files in
[source-root-post-run](source-root-post-run/) were copied after the run. The
[validation receipt](clean-target/archive-validation.json) records the source and
binary checks.

The earlier [shared-target run](discarded-shared-target/) is retained for audit:
its unit log reported seven tests and omitted the candidate's four new tests
because Cargo reused a stale binary. It must not be used to qualify the
candidate. The initial toolchain inspection failed because the ambient `PATH`
did not include Rust; the checks used the configured stable toolchain. An earlier
formatting attempt failed on a Rust 2024 let-chain in the 2021-edition crate;
its observation is recorded without a raw log, and the candidate was rewritten
before testing.

Verify stored and reconstructed artifacts from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/vectored-write-20261028
```
