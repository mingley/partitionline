# Kafka Java record codec checks, 2026-10-28

On x86_64, the all-features Rust codec tests read Java-generated record batches.
Kafka Java 4.3.1 then read Rust-generated batches for all five supported
compression modes: none, gzip, Snappy, LZ4, and zstd.

- `codec_matrix`: two tests passed. Java read five ordinary and five signed
  offset/timestamp boundary batches, including a raw Snappy native cross-check.
- `codec_sparse`: one test passed. Java read five sparse and five wrapped-sparse
  batches and checked the last-offset delta and record fields.
- Two native broker matrix tests were ignored: they require an owned broker
  topic, independent SDK peers, and completed stored-segment artifacts. They
  were not run and are not covered by this result.

The test log and Java output are in this directory. The Java messages about
SLF4J's missing binding indicate use of its no-op logger. The Java programs
exited successfully. This is codec interoperability evidence, not a native
broker, full protocol, production readiness, or performance qualification.

## Commands and environment

The Rust run used stable `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, LLVM 23.1.1,
`x86_64-unknown-linux-gnu`, and the following command from the repository root:

```sh
RUSTUP_HOME=/workspace/work/rustup \
CARGO_HOME=/workspace/work/cargo \
PATH=/workspace/work/cargo/bin:$PATH \
CARGO_TARGET_DIR=/workspace/work/qualification-20261027/broker-check-target \
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
PL_SPARSE_CODEC_OUTPUT=/workspace/work/goal-qualification-20261028/codec-all-features-01/rust-sparse \
PL_CODEC_MATRIX_OUTPUT=/workspace/work/goal-qualification-20261028/codec-all-features-01/rust-matrix \
cargo +stable test --locked --all-features --test codec_sparse --test codec_matrix -- --test-threads=1
```

OpenJDK 21.0.12.1 ran the reverse checks:

```sh
java --class-path '/workspace/work/goal-qualification-20261028/narrow-profile/java/kafka-clients-4.3.1.jar:/workspace/work/goal-qualification-20261028/codec-peer-jars/*' \
  tests/conformance/java/SparseFixture.java verify \
  /workspace/work/goal-qualification-20261028/codec-all-features-01/rust-sparse

python3 tests/conformance/run-codec-matrix.py verify \
  --jar /workspace/work/goal-qualification-20261028/narrow-profile/java/kafka-clients-4.3.1.jar \
  --libs /workspace/work/goal-qualification-20261028/codec-peer-jars \
  --rust-output /workspace/work/goal-qualification-20261028/codec-all-features-01/rust-matrix \
  --output /workspace/work/goal-qualification-20261028/codec-all-features-01/java-matrix-verify
```

`commands.json` records the Java matrix verifier's invocation and exit status.
`summary.json` records its result and fixture manifest hash. The programs,
generated batches, and saved Rust test binaries were retained under the work
directory above; `artifacts.sha256` identifies the outputs and binaries. The
executables and Java dependency jars are not stored in this archive.

## Source identity and limits

The Rust test was built from a working tree with uncommitted changes. The
before/after checks in `source-and-peer-before.sha256` and
`source-and-peer-after-check.log` pin `src/protocol/records.rs`, the two Rust
test files, both Java verifier sources, Kafka's client jar, and its codec and
logging dependencies. The copied test executable hashes are in
`artifacts.sha256`. These checks did not cover the entire source tree or all
fixture files before the run, so a complete source-tree identity is not
established. The receipt must not be treated as proof for a different commit
or build.
