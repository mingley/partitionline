# KL10-10 consumer peak RSS on lb-gateway-fetch-gzip

Analysis only. No production code change. Suite HOLD remains.
**No performance claim.**

## Question

gateway-adoption.md section 2 (2026-09-30, `290adda5`) reported
partitionline peak RSS **1,426 MB** against librdkafka **193 MB** on a
gzip consume. Is that the current default, and does it exceed the
documented budget?

## Effective consumer configuration (KL10-14 cell)

| Knob | Value |
|---|---|
| `fetch.min.bytes` | 1 |
| `fetch.max.wait.ms` | 500 |
| `fetch.max.bytes` | 52,428,800 (50 MiB) |
| `max.partition.fetch.bytes` | 1,048,576 (1 MiB) |
| `buffer_memory` | 33,554,432 (32 MiB, crate default) |
| `isolation.level` | read_uncommitted |
| `max_poll_records` | none |
| assignment | manual, 8 partitions, explicit start offsets |
| payload | json1k, 1024 B, gzip, COUNT=25000 |
| security | PLAINTEXT |

`docs/resource-contract.md` already states process RSS is **not** bounded
by `buffer_memory`. The decoded-payload gauge is. A first batch may exceed
the soft limit (KIP-74). Decode has a 64 MiB hard ceiling.

## Attribution

| Bucket | Bound at these knobs | Role in the dated 1.4 GB peak |
|---|---|---|
| Fetched wire bytes | 8 × 1 MiB per round, request cap 50 MiB | Dated consume used ~8 MiB/partition (KL10-16 re-probe) |
| Decompressed expansion | gzip ratio ~7; 25k × 1 KiB ≈ 25 MiB if all held decoded | Decode-then-discard inflated every over-budget batch |
| Pending records | `buffer_memory` 32 MiB | After KL10-16/17, further batches stay framed |
| Allocator / runtime | not in the payload gauge | Tokio, metadata, sockets, object headers (~20–25 MB residual at 52 MB peak) |

KL10-16's real-broker probe at the large partition fetch: 1,334 MB → 308 MB
after decode-on-apply. KL10-14 at matched Java/librdkafka fetch sizes:
median **52 MB** for both clients (samples in
`docs/evidence/perf/KL10-14/summary.json`). KL10-09 parent (same library,
raw consume, no hash verify): median **43 MB**.

## Decision

RSS at the cell settings is **inside** the documented model. No P0 bounds
card. No default change. The 1.4 GB figure is explained by a large
per-partition fetch plus decode-then-discard on `290adda5`; it is not
reproduced at the KL10-14 knobs. Knobs that reduce RSS:
`max_partition_fetch_bytes`, `fetch.max.bytes`, `buffer_memory`,
`max_poll_records`.
