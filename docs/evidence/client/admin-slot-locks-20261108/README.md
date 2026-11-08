# Standalone client-dependent lockfiles

Main commit
[`a0ad79d`](https://github.com/mingley/partitionline/commit/a0ad79df8a5d9872eacd1cd322b59d8d979f04e3)
added `hashbrown 0.17.1` to the client without updating its standalone
consumers. The [main CI run](https://github.com/mingley/partitionline/actions/runs/42492157950)
failed the instruction and allocation gates before measurement because
`benchmarks/codec/Cargo.lock` could not be updated under `--locked`.

This change updates the codec, runtime, sticky-partitioner, client fuzz,
broker interoperability and librdkafka interoperability lockfiles. Each adds
only `hashbrown 0.17.1` with the checksum from the root lock and the
`partitionline` dependency edge; existing resolved package entries are
unchanged. The isolated [candidate](https://github.com/mingley/partitionline/commit/c4c2e8d19af7059a73118f0a8d32e1048634ae91)
was based on main `adde42ac3e087bd891043e7ce5668d7e1badfd4a`.

The [first qualification](https://github.com/mingley/partitionline/actions/runs/42493434314)
checked the exact lock graph delta and passed stable `cargo check --locked`
for codec, runtime, sticky-partitioner, librdkafka interoperability and the
selected client record-batch fuzz target. The existing codec allocation gate
passed all three census tests and its six failure-preservation unit tests.
The instruction-count gate completed but reported `BASELINE_MISSING` for
`x86_64-1.99.1`; it printed ten instruction counts and did not compare
them to a baseline. The two runs printed the same values:

| Codec case | Instructions |
| --- | ---: |
| `iai_compress_gzip/text` | 11,146,149 |
| `iai_compress_lz4/text` | 2,182,009 |
| `iai_compress_snappy/text` | 1,468,674 |
| `iai_crc/f03` | 148,476 |
| `iai_decode/f03` | 466,620 |
| `iai_decompress_gzip/text` | 1,298,623 |
| `iai_decompress_lz4/text` | 1,044,834 |
| `iai_decompress_snappy/text` | 913,835 |
| `iai_encode/f03` | 706,204 |
| `iai_request/v9` | 185,021 |

The broker interoperability peer resolved its lock and compiled the client,
but its binary failed with two E0308 errors: a topic comparison expected
`Arc<str>` rather than `String`, and a nullable key comparison expected
`Option<RecordBytes>` rather than `Option<Bytes>`. That was a failed
locked build, not a lockfile error.

The [rebased qualification](https://github.com/mingley/partitionline/actions/runs/42493530806)
used the corrected roadmap links and the same six lock blobs. Its exact
dependency-delta check, the same five standalone checks, allocation gate,
and packed-crate adopter passed. The adopter checked 30 packaged documents,
228 local links, 108 pinned repository targets, and four stable feature
cells with 24 snippet tests each. The peer failed with the same two type
errors. The instruction-count gate again reported `BASELINE_MISSING`, so
there was no measured regression comparison. A separate [peer type
adaptation](../broker-interop-peer-types-20261108/README.md) addresses the
compile errors with focused checks. No Kafka comparison or production
qualification is implied by these checks.
