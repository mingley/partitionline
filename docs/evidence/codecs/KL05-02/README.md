# KL05-02 zstd decision evidence

Decision source: partitionline `fd1ee1e54e9ca3142c54a9c0628cd141cbd4a189`.
Survey date: 2026-10-02. See [the decision](../../../zstd-spike.md).

The standalone harness compares unmodified `ruzstd =0.8.1` and `zstd-rs =0.1.0`
with an external Zstandard CLI 1.5.7. Its normal/build dependency tree contains
only those crates and `twox-hash 2.1.4`; no native codec is linked into it.
It does not modify or exercise partitionline's codec implementation.

`results-msrv.json` and `results-stable.json` retain observed Rust 1.85.0 and
1.99.0 outcomes. The results are byte-for-byte equivalent as parsed JSON.
`commands.json` retains one complete 144-command evaluation log; its commands
and outcomes also matched the second toolchain. `$SPIKE` and `$CORPUS` are
sanitized local path placeholders, not shell invocations to execute directly.
Six representative independently generated frames are retained in `frames/`
(265 bytes total); `evaluate.py` regenerates the complete corpus, payloads and
hashes using seed 8878. Large random/raw payloads and compiled artifacts stay
in scratch storage. Upstream metadata retains no author emails or host identity.

Run from the repository root with the recorded Rust toolchains and CLI:

```sh
zstd --version
export CARGO_TARGET_DIR="$PWD/../work/zstd/reproduce-target"
cargo +1.85.0 build --locked --manifest-path docs/evidence/codecs/KL05-02/harness/Cargo.toml
python3 docs/evidence/codecs/KL05-02/evaluate.py \
  --binary "$CARGO_TARGET_DIR/debug/partitionline-zstd-decision-spike" \
  --out ../work/zstd/reproduce-msrv
cargo +stable build --locked --manifest-path docs/evidence/codecs/KL05-02/harness/Cargo.toml
python3 docs/evidence/codecs/KL05-02/evaluate.py \
  --binary "$CARGO_TARGET_DIR/debug/partitionline-zstd-decision-spike" \
  --out ../work/zstd/reproduce-stable
cargo +stable tree --locked --manifest-path docs/evidence/codecs/KL05-02/harness/Cargo.toml -e normal,build
```

The evaluator reports every result, including the accepted excessive initial
window in `ruzstd` and its single-frame adapter's unsupported multi-frame cases;
successful script exit alone is not an assertion that all candidates are safe.
The selected `zstd-rs` passed all of its recorded decode, encode/peer, rejection
and multi-frame cases. Its caller cap limits logical appended output, not RSS.

`build-graph.json` records the separate failed MSRV probe for unmodified
`ruzstd 0.8.2` (eight E0658 errors). It also shows the optional non-default
dependency proposal; that proposal is **not applied** to the core manifest.
`candidates.json`, `peer-and-transitive.json` and `upstream.json` identify
archive checksums, source revisions, release metadata, license/MSRV fields,
runtime versus development dependencies, upstream workflow contents and the
successful pinned release CI runs. No cargo-audit/cargo-deny pass, upstream
security audit, throughput/RSS measurement, live Kafka qualification or
maintainer policy approval is claimed.
