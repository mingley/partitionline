# Schema Registry JSON object indexing

9 November 2026

The Registry JSON reader checks each object key against earlier keys in a
vector. This preserves order and rejects duplicates, but a large object
requires a quadratic number of comparisons. A candidate keeps the vector
and its small-object scan, then adds a temporary randomized hash-to-slot
index. It stores no second copy of the key strings. Full-hash collisions
still compare each original key; worst-case collisions can remain
quadratic. The index grows with keys actually parsed and is not an RSS cap.

The first candidate indexed after 16 keys. It remains unapplied because the
32-key case regressed.

## Measurement

[Run 42666459316](https://github.com/mingley/partitionline/actions/runs/42666459316)
used Rust 1.99.1 on a hosted x86_64 runner. It ran 12 alternating pairs
pinned to one available CPU. The timed code parsed a prepared, successful
Registry JSON body in-process; it did not include HTTP, Kafka or an
external registry. Each body contained a schema field and distinct,
equal-length extra keys. Parse and result checks were performed before
timing. Percentages below are the median of paired candidate/baseline
changes; ranges are the smallest and largest paired changes. The
[raw paired samples](threshold16-samples.csv) retain every timed value.

| Keys | Baseline median | Candidate median | Paired median | Range |
| --- | ---: | ---: | ---: | ---: |
| 5 | 622 ns | 610 ns | −1.86% | −3.70% to +13.49% |
| 16 | 2.281 µs | 2.293 µs | +0.91% | −5.12% to +3.42% |
| 32 | 5.596 µs | 6.686 µs | +19.08% | +16.23% to +21.20% |
| 4,097 | 33.90 ms | 1.203 ms | −96.46% | −96.70% to −96.36% |
| 8,193 | 143.06 ms | 2.456 ms | −98.28% | −98.35% to −98.19% |

Each sample timed 100,000, 20,000, 5,000, three and one parse respectively.
The baseline and candidate used separate release target directories. Their
qualification source Git blobs were `87e493b85e37879aa973a18656c1e03a995bd938`
and `d6f5a6f636df46d346c7b7b673aec7174fd4f4f6`; executable SHA-256 values
were `18f52e34ccdef6b4eea5270e38146487dcffd16707a5c17b848a1c1636e5c812`
and `0acaf1acc8f51c811ca26b3400779d9d500ec96fd240fa9284d09a8a1e5b0e02`.
The fully tested formatted candidate source blob was
`7ae0f56c1fafd6f667aeba12c802d64957b8315b`.

The candidate passed full default and all-feature schema profiles, strict
Clippy and rustdoc, plus forced full-hash collision, order, duplicate and
public 4,096-field response controls. No live registry was used. A
[separate run](https://github.com/mingley/partitionline/actions/runs/42666977450)
also passed nested and malformed-value precedence, a configured body
boundary, no-default-feature tests and strict checks; its candidate source
blob `8c44c6c1c4841a976a3151d8f57d34e5d85b5105` adds these controls.

For 16,384 distinct names the temporary index in that run held 16,384 hash
buckets with capacity 28,672, zero observed full-hash collision buckets,
and a 32-byte key-and-slot tuple. Hash table metadata, collision vectors,
allocator overhead, names, parse results and input are additional.

That run also read an exactly 64 MiB HTTP body with 8,193 keys and rejected
a 64 MiB + 1 body in both baseline and candidate. One debug-process sample
reported peak RSS of 269,664 KiB baseline and 269,576 KiB candidate for
the accepted body; these totals include the loopback server and body copies.
They do not establish an allocation bound or a statistically meaningful
comparison. Parsing is synchronous and these measurements do not establish
deadline or cancellation guarantees.

An attempted cancellation arrived after that run completed and received
HTTP 409. A separate [few-key maximum-body check](https://github.com/mingley/partitionline/actions/runs/42667230493)
also passed. Both arms accepted 64 MiB with nine keys and rejected 64 MiB
plus one byte. One accepted-body debug-process sample reported 269,436 KiB
and 269,608 KiB peak RSS and elapsed 2.24 and 2.21 seconds respectively.
This includes the loopback server and body copies; it is not an allocation
bound or a repeated comparison. Baseline and candidate used separate target
directories. Their source blobs were `ab27516335dfa4a026835654c0da63bca174b9a4`
and `0bfcdaea7d729b206b8741d7892ec6154182080b`; executable SHA-256 values
were `3ef3f396b46077805efccad788c1498de872d1d818dbd56c0c36ade379a741c3`
and `0a83e2293702d3cadcc0fbf046d352a3d87aefa1b85ad0f0f31f1adb3cce6510`.

## Larger indexing thresholds

[Run 42667554614](https://github.com/mingley/partitionline/actions/runs/42667554614)
compared thresholds 64 and 128 against the same baseline. All three
implementations used separate release target directories; execution order
rotated through six permutations over 12 pairs on one pinned x86_64 CPU.
The [raw samples](threshold-comparison-samples.csv) contain all 432 timings.
These are prepared, successful in-process JSON parses, not HTTP or Kafka
throughput. Percentages are paired median changes; ranges are the minimum
and maximum paired changes.

| Keys | 64-key threshold | Range | 128-key threshold | Range |
| --- | ---: | ---: | ---: | ---: |
| 5 | −4.12% | −9.76% to +15.51% | −2.54% | −4.49% to +6.04% |
| 32 | −0.33% | −6.18% to +17.66% | +4.34% | −4.28% to +7.97% |
| 64 | −2.28% | −9.69% to +7.41% | +2.23% | −8.14% to +8.14% |
| 65 | +36.43% | +31.99% to +39.35% | +2.16% | −1.90% to +11.58% |
| 128 | −17.44% | −22.02% to −15.46% | −2.49% | −5.94% to +2.26% |
| 129 | −22.79% | −27.65% to −22.09% | +23.27% | +18.30% to +27.45% |
| 160 | −37.48% | −41.54% to −36.51% | −5.27% | −9.61% to −2.15% |
| 256 | −58.29% | −61.15% to −57.54% | −43.51% | −46.71% to −38.93% |
| 257 | −58.08% | −61.23% to −57.82% | −42.58% | −46.75% to −41.94% |
| 512 | −77.84% | −80.65% to −77.56% | −73.13% | −76.68% to −72.84% |
| 4,097 | −95.60% | −96.00% to −95.55% | −95.47% | −95.86% to −95.37% |
| 8,193 | −97.75% | −97.96% to −97.73% | −97.86% | −98.06% to −97.84% |

Both thresholds substantially reduce large-object time but introduce a
measured slowdown immediately after index construction. Neither was
adopted or independently run through the complete schema control suite.

| Implementation | Benchmark source Git blob | Executable SHA-256 |
| --- | --- | --- |
| Baseline | `23fe4965d21fd260f54bd6e94595a0ea994d870c` | `37b8b2754576efc0a187abb5fca008afb6be141a5a5c06af7d7b180c3074d69b` |
| Threshold 64 | `1f8cf71a2434cd99dc890df2f29ae0ab1079529c` | `4e206eb7d5e23939a244b2259c5d9f2c6b741eeeaad9ec698f8831ca3420702b` |
| Threshold 128 | `0585b41beb9c9b8fb9d7df797eb849bb151f6d1b` | `465ad27828ac76c8ecac12971f705907224a2addfa4de408cbe929836c69e828` |
