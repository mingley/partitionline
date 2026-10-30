# Malformed record decoding compatibility

The candidate restores Kafka Java's first-error ordering for ordinary malformed
magic-v2 batches. A small declared record-count mismatch now reaches the earlier
record error or premature EOF instead of failing a wire-size precheck. Header
counts use Java's remaining-byte comparison, so a negative key length is reported
before a later missing field. The seven-byte record minimum is an initial reserve
hint; it does not decide the parse result.

The aggregate declared-count and header-metadata limits remain in force. An
extreme declared count can therefore return a metadata-limit error before Java's
EOF error. Growth stays fallible and peak-accounted, including replacement-vector
capacity. This does not establish an allocator or total process-memory bound.

On x86_64 using stable Rust 1.99.1 (LLVM 23.1.1), twelve selected unit tests passed.
They include both original malformed-error cases, two new ordering/reservation
controls, a complete 10,000-record synchronous/cooperative decode, aggregate
record/header limits, scratch capacity, allocation rollback, shared payload bytes
and compressed cooperative decoding. The new malformed-Java integration target
passed two tests under default and all features. In each feature mode the existing
codec matrix passed two tests and ignored two native-broker cases, while the
sparse fixture target passed one test. The ignored tests were not run. Strict
all-feature Clippy passed for the library and these three integration targets.
No full-client, broker, performance, production or release qualification follows.

A fresh Kafka Java 4.3.1 oracle run reproduced all five fixture files byte for byte
and confirmed the EOF, negative-header-key and excessive-header-count ordering.
Its source, jar, Java launcher and JDK modules hashes matched before and after.
The huge-count fixture intentionally demonstrates the Rust metadata-limit boundary
rather than identical error precedence. The fixture manifest and oracle receipt
record the hashes; Java dependencies are not bundled here.

The source was a sparse dirty worktree based on commit `63aa045be`, not a clean
integrated commit. The final patch and source snapshots are included. The
8,022 visible input paths selected by `rg --files --hidden` (excluding docs,
`.git`, target output and ignored files) had matching pre/post hashes. Dependencies,
compiler binary bytes, operating-system state and all environment variables were
not pinned. Source timestamps were refreshed without changing bytes; Cargo JSON
marked every selected executable as rebuilt from the candidate path. Saved
executable hashes and local paths appear in the artifact records, but executable
bytes are not included in this archive. The saved copies remain in scratch.

The initial rustfmt check failed on the previously unformatted draft. Its output
is retained; formatting was applied in the isolated source before the recorded
build, and the final formatting check passed. Each command used a 300-second
bound and a 0.25-second disk monitor with a 2 GiB stop floor. The lowest sampled
free space was 2,516,037,632 bytes. Shorter dips could be missed. No recorded
cgroup OOM kill increased. Commands, logs, disk samples and input hashes are under
`runs/`; original text is preserved as deterministic gzip.

Verify stored and reconstructed bytes after installing this directory in the
repository:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/codec-malformed-20261030
```
