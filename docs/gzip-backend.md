# gzip backend

The default gzip backend is `flate2` with zlib-rs. `default-features = false`
selects miniz_oxide. The maintainer approved that dependency and the default
on 2026-10-01 (KL10-05). The record is
`docs/plan/evidence/KL10-05.json`.

This is one exception to the boundary in
[performance leadership, section 9](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/performance-leadership.md#9-boundaries-unchanged-unless-the-maintainer-decides-otherwise).
It covers this dependency. The crate still forbids `unsafe` in its own code.

| Build | gzip backend | `unsafe` in the gzip path |
|---|---|---|
| default (`zlib-rs` feature) | zlib-rs 0.6 | yes, inside zlib-rs (SIMD, raw buffers) |
| `default-features = false` | miniz_oxide 0.9 | only crc32fast's CRC-32 kernel; miniz_oxide forbids `unsafe` |

The miniz_oxide build still has a small `unsafe` path. flate2 computes the
gzip CRC-32 with `crc32fast`, about 15 lines of SIMD intrinsics. It also
enables miniz_oxide's `simd` feature. That Adler-32 checksum is used for
zlib-wrapped data, not for gzip. The fallback narrows `unsafe` to the CRC
kernel. It does not remove it.

Both builds turn on flate2 `runtime_detection`. zlib-rs and `crc32fast` then
pick PCLMULQDQ, AVX2, or ARM CRC at runtime. Before KL10-06 that switch was
off, so generic x86_64 and aarch64 Linux binaries computed the gzip CRC-32
in software.

flate2 selects one backend for the whole build. If another crate enables a
C zlib feature (`zlib`, `zlib-ng`, or another), flate2 uses that C library
for partitionline as well.

## Wire compatibility

Both backends write standard gzip at level 6, which is Java's default, and
both read any standard gzip. Compressed bytes differ by about ±0.4% on the
json1k cells, so compare the decoded records rather than the compressed
batches.

KL10-05 checked both directions on 1, 16, and 256 json1k records. The JDK
`java.util.zip` codec and zlib 1.2.12 with librdkafka's `deflateInit2`
settings decode both backends. Both backends decode Java and zlib output at
levels 1, 6, and 9, and they decode each other. The decoded-byte limits
(KL02-03) are unchanged.

## Measurements

KL10-03 json1k cells plus the historical gzip cells. Throughput change
against the miniz_oxide parent: medians of five interleaved repetitions
(raw data: `docs/evidence/perf/KL10-05/` in the repository).

| Cell | arm64 (Apple M4 Pro, macOS) | x86_64 (Xeon Platinum 8573C, hosted Linux) |
|---|---:|---:|
| micro-compress gzip json1k 16k / 256k | +29% / +68% | +56% / +60% |
| micro-decompress gzip json1k 16k / 256k | +15% / +25% | +22% / +53% |
| raw-decompress gzip json1k 16k / 256k | +2% / +47% | +15% / +99% |
| compress gzip text / random | +75% / +6% | +63% / +40% |
| decompress gzip text / random | +7% / **−11.5%** | +41% / +10% |

Instructions (callgrind) fall by 21–41% (compress) and 23–49% (decompress) on
aarch64 Linux, and by 24–50% and 25–40% on x86_64. Non-gzip cells are
unchanged. `runtime_detection` alone adds 5–12% to miniz_oxide decompression
on x86_64 and cuts its decompression instructions by 8–14% on aarch64 Linux;
on Apple aarch64, CRC instructions are already enabled at compile time.

The one loss is incompressible input on arm64: decoding the `random` cell
(stored blocks) is 11.5% slower. It is 10% faster on x86_64.

## Memory

zlib-rs makes fewer allocations but uses more bytes per stream: each json1k
gzip compress makes 5 fewer allocations and about 60 KB more (bytes +4% to
+17% by cell); decompress makes the same number of allocations and about 4 KB
more. KL10-15 re-baselined these cells with maintainer approval. Baselines are
recorded on x86_64: zlib-rs keeps a 64-byte SIMD CRC accumulator in each stream
there, so other targets allocate 64 bytes less per stream.

## Dependency audit (zlib-rs 0.6.8)

- License `Zlib` (allowed by `deny.toml`); tested with the current stable toolchain.
- Maintained by the Trifecta Tech Foundation, started by ISRG's Prossimo
  project; regular releases (0.6.6 on 2026-07-09, 0.6.7 on 2026-08-03, 0.6.8 on
  2026-09-15).
- About 470 source lines contain `unsafe`; 13 files use `core::arch` SIMD
  (Adler-32, CRC-32, `compare256`, the inflate writer, CPU detection).
- Upstream fuzz targets cover compress, gzip compress, inflate (chunked and
  random input), infback, uncompress, checksums and end-to-end round trips.
  This crate's fuzz targets decode gzip batches through the default build.
- flate2 builds it with its Rust allocator (`rust-allocator`), so allocations
  go through the global allocator and the allocation gates see them.

## Changing the backend

Moving the default back to miniz_oxide, or to any other backend, needs a new
decision card with measurements on both architectures and maintainer
approval. Nothing in partitionline's source depends on which backend is
active.
