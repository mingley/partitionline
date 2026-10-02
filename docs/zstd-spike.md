# Pure-Rust zstd backend decision (KL05-02)

**Decision, 2026-10-02:** recommend **`zstd-rs = "=0.1.0"`**, from
[Avarok-Cybersecurity/zstd-rs](https://github.com/Avarok-Cybersecurity/zstd-rs),
for an **optional, non-default `zstd` feature**, with bounded decoding and an
explicit encoder level. It supplies both directions, builds on Rust 1.85,
forbids unsafe code, and has no runtime or build dependencies. A bounded
independent frame evaluation passed in both directions against libzstd 1.5.7.
Its very short maintenance history is a material adoption risk.

This is a backend decision, **not shipped zstd support**. `Cargo.toml`,
`Cargo.lock`, defaults and `deny.toml` are unchanged. Compression ID 4 remains
unsupported in partitionline. The complete-codec profile stays incomplete
until KL05-03, KL05-04 and KL05-05 supply the implementation and qualification.

The recommendation requests no native, unsafe or default-feature policy
exception. [The plan](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/plan/README.md) requires maintainer approval before
policy changes; [gateway boundaries](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/plan/gateway-adoption.md#6-boundaries)
also require it before making a backend default or adding a backend that uses
unsafe internally. Neither action is proposed here, and no maintainer approval
is invented or recorded by this spike. Any future change to those boundaries
needs a separate explicit decision.

## Kafka wire expectation

Kafka record-batch attributes use compression ID 4 for standard zstd frames.
There is no Kafka-specific zstd envelope comparable to snappy-java framing.
The record-batch length and CRC still apply around the compressed records.
Standard frame interoperability is necessary evidence, but it does not prove
Kafka record-batch framing, broker produce/fetch behavior or qualification.

## Pinned candidates: decode and encode separately

| Candidate | Decode | Encode | License / Rust 1.85 | Decision |
|---|---|---|---|---|
| `zstd-rs 0.1.0` | One-shot `Decompressor`; caller output cap, checksum validation, concatenated/skippable frames | Fallible `Compressor`; levels -7..22; explicit window/checksum/content-size configuration | `MIT OR Apache-2.0`; declared 1.85 and builds on 1.85.0 | Recommended optional backend; test levels 1, 3 and 19 passed independent decoding |
| `ruzstd 0.8.1` | `StreamingDecoder` / `FrameDecoder`; unsafe ring-buffer internals; adapter must enforce limits and compare checksums | Working `Fastest` and `Uncompressed`; `Default`, `Better`, `Best` call `unimplemented!`; `Read`/`Write` errors are unwrapped | `MIT`; no declared MSRV; measured build on 1.85.0 succeeds | Fallback research candidate; not preferred for either direction |
| `ruzstd 0.8.2` | Similar capability; unsafe internals | Working `Fastest`; other levels still incomplete | `MIT`; no declared MSRV; **1.85.0 build fails**, eight `is_multiple_of` errors | Reject at current MSRV; no silent source patch |
| `ruzstd 0.9.0` | Pure-Rust decoder, unsafe internals | Working compressor with limited supported levels | `MIT`; declares **1.87** | Reject at current MSRV |
| `zstd 0.14.0` → `zstd-safe 8.0.0` → `zstd-sys 2.1.0+zstd.1.5.7` | Reference C codec | Reference C codec | `BSD-3-Clause`; each declares 1.64 | Native chain violates the existing `zstd-sys` ban; external reference tooling only |

The earlier blanket assertion that no usable pure-Rust encoder exists is
outdated. `ruzstd 0.8.1` already emits interoperable `Fastest` frames; its
unsupported levels and infallible/panic-prone interface must not be exposed as
working level choices. The newly published `zstd-rs` is a separate crate and
repository from both `ruzstd` and the native-binding crate `zstd`.

Exact archive SHA-256 checksums, VCS revisions, release dates, licenses,
manifests and dependency metadata are retained in
[candidates.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/candidates.json) and
[peer-and-transitive.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/peer-and-transitive.json).
The proposed backend pin is archive checksum
`41514ccc30389f95bb9e6ab6d5634f17121c7b4fead6722a5fceec1c7891d78f`,
VCS revision `bac4c37d86e5307537c145823a106f7ed8f386d7`.

## Dependency, license and maintenance review

Proposed implementation graph, **not applied by this decision**:

```toml
[features]
zstd = ["dep:zstd-rs"]

[dependencies]
zstd-rs = { version = "=0.1.0", optional = true }
```

The existing default-feature list remains as it is. With `zstd` disabled,
the new package is absent from the active runtime graph. With it enabled,
`zstd-rs 0.1.0` is the sole added package: no transitive runtime dependency,
build script, native library, SIMD intrinsic or unsafe code is required.
[`src/lib.rs`](https://github.com/Avarok-Cybersecurity/zstd-rs/blob/bac4c37d86e5307537c145823a106f7ed8f386d7/zstd-rs/src/lib.rs)
declares `#![no_std]` and `#![forbid(unsafe_code)]`. Its MIT/Apache licenses are
allowed by the current deny policy. Upstream golden test vectors have the
reference project's BSD/GPL dual-license provenance and an included
BSD-3-Clause notice; this spike vendors none of those vectors or source files.

The `ruzstd` comparison enabled only `std` and `hash`: its active dependency
is `twox-hash 2.1.4` (MIT, Rust 1.81), with only `xxhash64`. Optional `rand`,
`serde`, dictionary-builder and rustc-internal features are absent. Both
codec projects use native `zstd` as a **dev** dependency for upstream interop;
dependency dev tooling is absent from this spike's active graph. The independent
peer here is the installed CLI, never a partitionline runtime dependency.
The measured tree and build results are in
[build-graph.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/build-graph.json).

`ruzstd` has a public repository history from 2019, recent releases through
July 2026, upstream fuzz regression tests and Miri checks for its unsafe ring
buffer. `zstd-rs 0.1.0` was published on 2026-09-28 and its public repository
was created that day. Its pinned CI includes unit/independent C/golden tests,
no-std and wasm builds, and three 60-second fuzz-smoke targets. Pinned
[CI run 36489341448](https://github.com/Avarok-Cybersecurity/zstd-rs/actions/runs/36489341448)
and [release run 36490837566](https://github.com/Avarok-Cybersecurity/zstd-rs/actions/runs/36490837566)
completed successfully. Those results establish a release gate, not a long-term
maintenance or security-response commitment. Sanitized upstream metadata and
the exact workflow contents are retained in
[upstream.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/upstream.json).

Neither a security audit, an upstream response SLA nor sustained downstream
adoption was established. Keep the initial feature optional and the version
pinned; review future upgrades separately. Implementation must run the normal
cargo-audit/cargo-deny and MSRV gates before changing the core dependency graph.
No supply-chain pass for an unapplied graph is claimed here.

## Independent bounded frame evaluation

The retained [standalone harness](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/harness/Cargo.toml)
uses unmodified, checksum-pinned registry sources and an independent
**Zstandard CLI / libzstd 1.5.7** peer. It does not call partitionline codec
code. [evaluate.py](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/evaluate.py) deterministically
generates six payloads: empty, one byte, runs, synthetic JSON records,
incompressible bytes, and mixed data; maximum **393,216 bytes**. Three
reference profiles use levels 1/3/19, checksums present/absent, known/unknown
content size, and requested windows 17/19. Every process has a 30-second cap.

Results were identical on **Rust 1.85.0** and **stable 1.99.0**:

| Evidence per toolchain | `zstd-rs 0.1.0` | `ruzstd 0.8.1` with spike adapter |
|---|---:|---:|
| Independently generated single-frame decodes | 18/18 | 18/18 |
| Encoded frames decoded by independent CLI | 18/18 (levels 1, 3, 19) | 6/6 (`Fastest`) |
| Output cap one byte below expected size | 15/15 rejected | 15/15 rejected |
| Bad magic, flipped checksum, truncated checksum | 3/3 rejected | 3/3 rejected |
| Advertised 2 TiB window on an empty frame | Rejected (`WindowTooLarge`) | **Accepted** |
| Concatenation and skippable-prefix cases | 2/2 decoded | Explicitly unsupported by this single-frame adapter |

Each toolchain evaluation ran **144 subprocess commands**. The reference
corpus has 18 frames; the two candidates contribute 36 decode checks and 24
encoder/peer checks per toolchain. Raw results, hashes and individual commands
are in [results-msrv.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/results-msrv.json),
[results-stable.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/results-stable.json) and
[commands.json](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/commands.json). Six compact reference
fixtures total 265 bytes; the remaining frames can be regenerated. No Kafka,
Java/librdkafka, broker, throughput, RSS or architecture qualification was run.

The `ruzstd` adapter's cap-plus-one read and explicit checksum comparison are
visible in the harness. The passes do **not** establish those guarantees for
an unwrapped `StreamingDecoder`. Its 100 MiB window check is in
[`FrameDecoderState::reset`](https://github.com/KillingSpark/zstd-rs/blob/c51eda1c8fd60a4acf28d4fbfb12f0657bd25bc9/src/decoding/frame_decoder.rs#L110),
while initial state construction omits that check. Streaming reads also retain
history internally; bounding the returned vector alone is insufficient.

## Required implementation limits

- **Decode:** start from the KL02-03 decoded-byte reservation and hard ceiling.
  Pass the remaining logical output allowance into `zstd-rs`; reject excess
  advertised content size and unacceptable window sizes before decoding. Use
  standard frames with no dictionary. Define concatenated/skippable-frame and
  trailing-byte handling against independent Kafka peers; do not assume the
  library's multi-frame behavior settles the Kafka adapter policy.
- **Output-cap semantics:** `Decompressor::decompress` computes
  `out.len() + max_output`; its argument limits **bytes appended**, despite a
  documentation sentence describing a total cap. Use an empty destination or
  subtract existing output from the reserved total. The library may retain
  partial output on an error. Roll back the batch and release reservations on
  every error before delivering records.
- **Allocation:** the cap is on logical decoded bytes, not process memory.
  The implementation pre-sizes output with 32 spare bytes and decodes literal
  blocks into separate scratch (up to 128 KiB plus spare bytes), with entropy
  tables and vector-capacity rounding in addition. Include these costs in the
  resource contract; do not advertise a strict RSS cap from this API alone.
  Its internal advertised-window ceiling is 2 GiB; partitionline must choose
  and enforce its own smaller accepted-window policy.
- **Encode:** expose only approved typed levels; initial recommendation is
  explicit level 3 with a standard frame, content size and checksum enabled,
  no dictionary. Validate accepted input and output sizes, reserve compressed
  output separately, and account for level/window-dependent matcher scratch.
  `Compressor::compress` has no caller-supplied compressed-output budget.
  The spike's 19-bit window is an evaluation setting, not a changed client
  default or proof that all larger Kafka batches should use that window.
- **Qualification:** KL05-03/04 must cover independent Kafka/Java/librdkafka
  frames, CRC/record lengths, corruption, window/content-size forms, reservation
  rollback, feature-disabled `Unsupported` and fuzzing. KL05-05 must qualify
  both directions. KL09-36 owns measured throughput/ratio/allocation cells.

There is a concrete policy-compliant encoder candidate; encoding is not
blocked by the absence of a pure-Rust implementation. Both directions remain
**missing in partitionline** until their implementation cards land. Native
support, unsafe backend adoption or default enablement would require the
explicit approvals described above; this decision grants none of them.
