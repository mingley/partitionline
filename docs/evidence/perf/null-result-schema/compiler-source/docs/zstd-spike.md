# zstd codec

Enable `zstd` for compression-ID-4 encoding and decoding. The feature adds the pinned
`zstd-rs = "=0.1.0"` backend and is disabled by default. Encoding supports explicitly validated levels 1 through 19, with level 3 as
the default.
All current checks use the latest stable Rust.

```sh
cargo test --locked --features zstd --test zstd_decode --test zstd_encode --test fuzz_decode_smoke
cargo test --locked --no-default-features --test zstd_decode
```

## Decode limits

The decoder checks every frame header before backend allocation. It accepts
ordinary frames, concatenated frames and skippable sections. It rejects
truncation, invalid checksums, dictionary-dependent frames and trailing bytes.
The existing outer Kafka length, CRC and record-count checks still apply.

Decoded record sections are limited to the smaller of the caller's byte budget
and 64 MiB. Declared windows are limited to 64 MiB, independently of the output
budget, so normal Kafka streaming frames can use a window larger than a small
record section. A batch may contain at most 1,024 frames, including skippable
sections. Each frame's declared content size must fit the remaining output
budget. Frames without a content size use the backend's incremental output
limit. An error discards partial output.

These limits describe decoded length and declared windows. Backend scratch,
allocator capacity rounding and the record objects add memory; this is not a
process RSS limit. Dictionaries are outside the supported profile.

## Encoding

`ZstdLevel::new(level)` validates the level before configuration. Set it with
`ProducerConfig::zstd_level` or `CompressScratch::set_zstd_level`; select the codec
with `Compression::Zstd`. The type is available in `protocol::records` when the
feature is enabled. Every frame uses standard magic, a content size, checksum,
no dictionary and a window of at most 8 MiB. The encoder context is reused for
successive batches and reset when the level changes or oversized scratch is
released.

A record section may contain at most 64 MiB before compression. The writer checks
record lengths before copying payloads. Compressed output is limited to 64 MiB
plus 64 KiB. Producer batches also respect `max_request_size` after compression,
including the Kafka batch header; compression expansion can therefore fail with
`Error::RecordTooLarge`. Encode failures preserve the caller's buffer prefix and
release producer reservations. Produce versions below v7 reject zstd before
application bytes are sent.

These are logical byte limits. Encoder workspace and allocator capacity add
memory beyond them. The retained context follows the configured level/window;
this feature does not impose a process RSS limit.

## Independent checks

The fixture suite reads Apache Kafka 4.3.1/Java record batches, libzstd 1.5.7
frame variants and batches retained from an actual librdkafka 2.15.0 producer.
Java independently checks the varied frames and the C producer's 128 records.
Rust checks records, headers, nulls, timestamps, exact output-budget boundaries,
corruption and bounded mutations of inner frames with repaired outer CRCs.

Fixtures and peer pins are in [the manifest](../tests/fixtures/zstd-decode/manifest.json).
Decoder evidence in the repository under `docs/evidence/codecs/zstd-decode/`
records commands, source hashes and native process closure. The encoder checks all 19 levels and record/block boundaries with independent
Java decoding of actual Rust frames. Fresh native Kafka readback uses both Java
and librdkafka for 128 Rust records at levels 1, 3 and 19. Encoder evidence is in
`docs/evidence/codecs/zstd-encode/`.

The codec matrix covers none, gzip, Snappy, LZ4 and zstd with Rust, Java and C
clients on Kafka 4.1.2, 4.2.1 and 4.3.1, plus the historical 3.9.1 and 4.1.0
lines. Each client writes nine records; the other two check offsets, timestamps,
keys, values, headers and nulls. Topics either retain the producer's codec or
force gzip. All 150 cells and 300 cross-client readbacks passed, with stored
batch checks and broker shutdown receipts in
`docs/evidence/codecs/codec-matrix/`.

The records include UTF-8, empty values, compressible and random data, and sizes
around the 128 KiB block boundary. An independently generated raw Snappy fixture
complements the live SDKs' framed Snappy output. Java also checks signed-field
wrapping in actual Rust batches. See [the fixture manifest](../tests/fixtures/codec-matrix/manifest.json).

These are single-partition, single-broker plaintext correctness checks.
Compression ratio, CPU, latency and fault campaigns have separate tasks.

To rerun the current-release profile with prepared, pinned peers:

```sh
python3 tests/conformance/run-codec-matrix.py native \
  --homes /work/codec-brokers --jar /work/kafka-clients-4.3.1.jar \
  --libs /work/java-codec-libs --rdkafka-root /work/librdkafka \
  --output /work/codec-results
```

The driver requires a fresh output directory, verified Apache archives and
extracted homes, and the pinned native SDK build. It bounds each command and
joins SDK and broker processes. `PL_CODEC_MATRIX=1` selects the same profile
through `scripts/ci-broker-smoke.sh`; its input variables are checked there.

## Backend selection

The backend has no runtime or build dependencies and uses the existing
MIT/Apache license policy. Its archive SHA-256 is
`41514ccc30389f95bb9e6ab6d5634f17121c7b4fead6722a5fceec1c7891d78f`;
upstream revision is `bac4c37d86e5307537c145823a106f7ed8f386d7`.
The earlier comparison and independent backend experiment remain in
[KL05-02 evidence](https://github.com/mingley/partitionline/blob/b58b01955544eceba8bd97b0b5615fc63bc1dc99/docs/evidence/codecs/KL05-02/candidates.json). Their historical toolchain
results do not set the current supported Rust version. The backend's short
maintenance history still limits production confidence.
