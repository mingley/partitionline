# gzip backend

**Decision (KL10-05, 2026-10-01):** gzip uses `flate2` with the zlib-rs
backend by default. `default-features = false` selects miniz_oxide instead.
The maintainer approved both the dependency and the default change (evidence:
`docs/plan/evidence/KL10-05.json` in the repository). This is a scoped exception
to the "no `unsafe`, no SIMD intrinsics" boundary in
[performance-leadership.md](https://github.com/mingley/partitionline/blob/65d3e89a4775f6bfd913ed1467647b93a11b9329/docs/plan/performance-leadership.md#9-boundaries-unchanged-unless-the-maintainer-decides-otherwise)
section 9. It covers
this one dependency; partitionline's own `unsafe_code = "forbid"` is unchanged.

| Build | gzip backend | `unsafe` in the gzip path |
|---|---|---|
| default (`zlib-rs` feature) | zlib-rs 0.6 | yes, inside zlib-rs (SIMD, raw buffers) |
| `default-features = false` | miniz_oxide 0.9 | no (`forbid(unsafe_code)`) |

Both builds enable flate2's `runtime_detection`, so zlib-rs and the CRC-32 used
with miniz_oxide (`crc32fast`) pick hardware paths (PCLMULQDQ, AVX2, ARM CRC)
at runtime. Before KL10-06 the build disabled it, so generic x86_64 and
aarch64 Linux binaries computed gzip CRC-32 in software.

flate2 picks one backend per build. If another crate in your graph enables a
C zlib flate2 feature (`zlib`, `zlib-ng`, ...), flate2 uses that C library for
partitionline too.

## Wire compatibility

Both backends write standard gzip at level 6 (Java's default) and read any
standard gzip. The compressed bytes differ between backends (about ±0.4% in
size on the json1k cells), so do not compare compressed batches byte for byte
across builds. KL10-05 checked both directions on 1, 16 and 256 json1k records:
the JDK's `java.util.zip` (the Kafka Java codec) and zlib 1.2.12 with
librdkafka's `deflateInit2` settings decode both backends' output, and both
backends decode Java and zlib output at levels 1, 6 and 9 and each other's
output. The decoded-byte limits (KL02-03) are unchanged.

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

- License `Zlib` (allowed by `deny.toml`); MSRV 1.75, below this crate's 1.85.
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
