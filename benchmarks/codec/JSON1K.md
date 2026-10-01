# JSON payload codec cells (KL10-03)

Seed `0xC0DEC` builds valid, deterministic JSON values of exactly 1024 ASCII
bytes: gateway-style metadata, seeded IDs and metrics, and a word-stream message.
Every record has a seeded 16-byte key, a fixed-base timestamp and no headers.
The `16k` and `256k` labels mean **value bytes**, not total Kafka wire bytes:
16 or 256 records. The census records the exact serialized record-section size
and SHA-256 so input changes cannot silently redefine a cell.

Each gzip, lz4 and snappy size has full `micro-compress` / `micro-decompress`
cells and `raw-compress` / `raw-decompress` comparison cells. Raw compression
feeds the **same uncompressed Kafka record-section bytes** to the backend in
one call. Gzip uses the same default compression level; LZ4 uses the same
independent 64 KiB blocks, content size and checksum settings. Raw Snappy omits
only the 20-byte Xerial prefix; the census checks that difference explicitly.
Full cells include record serialization or parsing, CRC and client framing.
Both paths include fresh output allocation and destruction. The comparison
also includes differences in buffer sizing and decode expansion checks; it is
not a subtraction that isolates only one cost. The client currently already
feeds gzip/LZ4 with one `write_all` call.

Raw decoding handles only these trusted, fixed fixtures and excludes the public
client's bounded expansion enforcement. Fixture construction and source input
cloning are outside timed/instruction/allocation collection. Ratios use
compressed **record section** bytes divided by the uncompressed section bytes;
batch headers are excluded. Lower is smaller.

```bash
cargo test --locked --manifest-path benchmarks/codec/Cargo.toml
cargo run --locked --manifest-path benchmarks/codec/Cargo.toml --bin json1k-census
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench codec -- json1k
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench json1k_iai -- --allow-aslr --output-format=json
```

The complete requested filter command is also supported on a host with Valgrind
and `iai-callgrind-runner` installed. In containers that forbid disabling ASLR,
set `IAI_CALLGRIND_ALLOW_ASLR=yes` before running
`cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml -- json1k`.

`json1k-alloc-baseline.json` records the 24 initial allocation/ratio cells; the
serial `json1k` test checks them in the allocation CI job. The separate
`json1k_iai` bench retains new instruction baselines without changing the
historical `iai` regression group. Its source-pinned measurements, raw Criterion
samples and instruction output are in [KL10-03 evidence](../../docs/evidence/perf/KL10-03/).
Wall times are local unsigned measurements, not a fastest-client claim.
Historical allocation and instruction baseline files remain unchanged. Raising
an allocation budget requires the existing separate baseline-change review.
