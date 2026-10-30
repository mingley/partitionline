# Buffer reservation checks, 2026-10-30

These checks used an uncommitted working tree based on `a97a2eb58`, stable
Rust 1.99.1, and the shared x86_64 workspace (four CPUs, 16 GiB memory limit).
The receipts retain before/after input hashes and patches. They qualify the
cases below, not a throughput ranking or production readiness.

## Changes and allocation counts

`reserve` takes additional capacity relative to a buffer's length. The patch
corrects 219 calls that subtracted capacity instead. An independent static
review checked their guards, size calculations, split buffers and zero-sized
vector elements.

The encoder census covers ApiVersions v0/v4, AddPartitionsToTxn v0/v3 and
EndTxn v0/v5. Each runs with two initial capacities and four occupied-prefix
lengths, for 48 cells. Fixture construction, hashes and printing are outside
the measured closures.

| Result | Before | After |
| --- | ---: | ---: |
| Cells with two growth allocations | 36 | 0 |
| Cells with one growth allocation | 12 | 48 |
| Total growth allocations | 84 | 48 |

Every warmed encode allocated zero times. Prefixes and encoded body hashes
matched. Counts come from the existing `CountingAlloc`; its realloc fallback
allocates and copies, whereas the system allocator may grow in place. These
counts do not measure actual copies, elapsed time or throughput.

Record decoding also restores the existing 1,000-record budget: two
allocations and 104,024 requested bytes instead of six and 203,864. Fresh
record vectors use a power-of-two initial hint bounded by 128 KiB of requested
metadata and available wire bytes. Reused vectors retain their previous growth
schedule. Shared metadata limits and fallible peak accounting remain in force;
larger initial storage can change how record and header allocation peaks
overlap near an explicit limit.

Six JSON codec allocation budgets were lowered after the census confirmed
lower counts and byte totals. No budget was raised, and payload facts and
hashes were unchanged. The intermediate failed gates are retained.

## Validation

- All three serial allocation-gate tests passed, including the new encoder
  reuse census; its six script-control tests passed.
- Default-feature protocol unit tests reported **1,385 passed and two failed**.
  The failures were `grouping_many_owned_topics_does_not_hold_the_caller`
  (88.09 ms) and `a_share_fetch_with_several_batches_sizes_the_batch_list_once`
  (capacity 8 versus 5). Both appear in the earlier clean-default failure
  index. This is not a passing full protocol suite.
- The malformed Java, codec matrix and sparse-batch integration targets
  reported five passed and two ignored. The ignored matrix cases require
  native peers. Three producer/share buffer-retention controls passed.
- Strict all-feature Clippy passed for the library and those three integration
  targets. Strict Clippy for the new census binary/test, formatting and shell
  syntax passed. The full all-feature runtime matrix was not run here.
- Forty local documentation links were checked; released 0.1.0 notes were
  preserved byte for byte when the unreleased notes were condensed.

The first unit build stopped at its 2 GiB disk guard. Only that build's
intermediate objects were removed, with hashes recorded. A build-only retry
used a 1.5 GiB guard and completed with 2,103,005,184 bytes minimum free space.
Runtime checks kept their 2 GiB guard. One census lint runner was lost during
a session restart; its exit status is unknown. A separate recorded retry
passed. No OOM event was recorded.

## Evidence

`runs/` contains receipts, logs, source-input manifests, patches and cleanup
records; `source/` contains the runner and new census sources. Exact test and
census executable hashes are retained. Verified gzip copies remain in local
scratch, outside this archive; that scratch is not guaranteed to persist.
The three integration executables were freshly built before the interrupted
build; the retry reused them with identical source manifests. The library test
executable was fresh on the retry. External tools and dependency bytes are not
exhaustively pinned, and before/after captures cannot detect transient changes.

Verify stored and reconstructed files from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/perf/buffer-reservations-20261030
```
