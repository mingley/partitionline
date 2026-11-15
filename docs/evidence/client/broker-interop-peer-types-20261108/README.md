# Broker interoperability peer type compatibility

The broker interoperability peer failed to compile after the standalone
client lockfiles were refreshed. In the [locked check][before], its
`src/main.rs:249` compared an `Arc<str>` producer receipt with a
`String`, and `src/main.rs:385` compared `Option<RecordBytes>` with
`Option<Bytes>`. Both produced E0308; dependency resolution itself had
succeeded.

The [applied change][candidate] compares the receipt topic as borrowed strings
and compares nullable keys as `Option<&[u8]>`. The latter preserves the
difference between a null key and an empty key without allocating. It also
uses `as_chunks::<2>()` for the peer's fixed-width UUID byte pairs.

The candidate is based on main
`9f82eaa94252fbce489a5404ddfb93717ebfd756`. The complete changed source,
including its test, is Git blob
`47517b63c7a61ae75ebeedfeed2f9c8a07ebacab` (SHA-256
`375de10a2afe026e353af0267eff7355bcb9f4625190479c8c1cd3284650c643`).
The unchanged peer lock is Git blob
`5ba93e161c03ba1ccf25b189de917ad4b9ca5811` (SHA-256
`afff78be5519050d456c04252b21855f77cc56e34b6a9968c90028e782f53d9e`).

The [final gate][final] passed on stable Rust 1.99.1: peer rustfmt,
`cargo check --locked --all-targets --all-features`,
`cargo test --locked --all-features`, and strict all-target/all-feature
Clippy. The focused test checks short and long key storage across 12
record indices, including matching bytes and mismatches in keys, values,
timestamps and headers. It explicitly distinguishes null from empty keys
and values.

Three independently restored mutations each made that named test fail
with Cargo exit 101: collapsing a null key to empty bytes, ignoring the
key, and ignoring the value. The final source and lock hashes matched
again after restoration.

The [first fixed-source draft][draft] passed the locked check and the test
but failed strict Clippy on the pre-existing `chunks_exact(2)` call.
The candidate's `as_chunks::<2>()` change addresses that lint; the
original draft failure remains separate from the final gate.

These checks qualify the peer's compilation and local comparison cases.
They did not run a live broker interoperability history, and do not
establish production readiness or a Kafka performance ranking.

[before]: https://github.com/mingley/partitionline/actions/runs/42493530806
[draft]: https://github.com/mingley/partitionline/actions/runs/42494014306
[final]: https://github.com/mingley/partitionline/actions/runs/42494210766
[candidate]: https://github.com/mingley/partitionline/commit/fb7d359ff4e2f5d9ba93ac0560973a1e4a2a410b
