# Compact Schema Registry JSON index

9 November 2026

This experiment uses a randomized hash-to-slot table for large Registry
JSON objects. It keeps names and output order in the original vector. The
primary table holds one `(u64, usize)` pair per distinct hash; a second
table and vectors are allocated only for full-hash collisions. Colliding
names are compared for equality against the original strings. This avoids
copying keys, but worst-case collisions can still require quadratic work.
The index is temporary and grows with parsed object keys; it is not an RSS
bound.

This candidate is unapplied. Its transition from the linear scan to the
index is still slower in the tested 65- and 129-key cases.

## Controls and measurement

[Run 42668996397](https://github.com/mingley/partitionline/actions/runs/42668996397)
passed focused controls at both thresholds for forced full-hash collisions,
duplicate names, object order, nested scopes, malformed-value error
precedence, a configured body limit and a public 4,096-field loopback
response. Each threshold's observed primary tuple size was 16 bytes; for
16,384 distinct names the map reported 16,384 entries and capacity 28,672.
Table metadata, collision storage, allocator overhead, names, parse results
and input are additional. This was not a heap allocation or RSS measurement.
The exact control source blobs were
`c125e287adae212676906797bfc4931770b4ad05` and
`e58bf3467137abcee67f8def84216de25a26bf4d`. Neither candidate has run
the complete schema profiles or strict checks.

The performance portion used Rust stable on a hosted x86_64 runner, with
separate release target directories. It timed 12 pairs of three
implementations pinned to one available CPU, rotating their execution
order through six permutations. Each sample parses a prepared successful
Registry JSON body in-process. This does not measure HTTP, Kafka or a live
registry. The [raw samples](compact-samples.csv) retain all 432 timings.
Percentages are paired median changes versus baseline; ranges are minimum
and maximum paired changes.

| Keys | Compact, threshold 64 | Range | Compact, threshold 128 | Range |
| --- | ---: | ---: | ---: | ---: |
| 5 | −4.06% | −5.73% to +3.81% | −2.10% | −6.17% to +0.77% |
| 32 | +0.83% | −3.24% to +3.43% | +2.04% | −9.64% to +4.82% |
| 64 | +1.60% | −4.93% to +5.33% | +1.56% | −1.88% to +6.49% |
| 65 | +31.56% | +26.09% to +37.04% | +0.44% | −3.73% to +1.23% |
| 128 | −27.45% | −31.05% to −22.43% | +0.74% | −0.19% to +5.07% |
| 129 | −30.89% | −33.77% to −29.17% | +20.76% | +19.64% to +21.82% |
| 160 | −44.03% | −44.27% to −41.41% | −9.33% | −9.69% to −9.00% |
| 256 | −66.55% | −67.37% to −65.85% | −50.51% | −51.81% to −49.01% |
| 257 | −66.16% | −67.24% to −64.58% | −51.23% | −53.01% to −48.91% |
| 512 | −82.54% | −83.11% to −82.31% | −78.99% | −79.63% to −76.50% |
| 4,097 | −97.48% | −97.65% to −97.44% | −97.52% | −97.72% to −97.47% |
| 8,193 | −98.38% | −98.42% to −98.35% | −98.43% | −98.48% to −98.35% |

| Implementation | Benchmark source Git blob | Executable SHA-256 |
| --- | --- | --- |
| Baseline | `23fe4965d21fd260f54bd6e94595a0ea994d870c` | `37b8b2754576efc0a187abb5fca008afb6be141a5a5c06af7d7b180c3074d69b` |
| Compact 64 | `f2be36fc1beeff761abc1082a0b897467b49dc1e` | `462352068e16ed77eef9d663ff439fee251948ef40edea8f0a6a8840706d5d90` |
| Compact 128 | `edd030e19b49e06db6baaf3d1d84c2bb28677bff` | `a249755fa72b6d7b2d12fa30ea36988b674ab042d3dd56b94a1deedf7c7b0ce7` |

The earlier [index experiments](../json-duplicate-index-20261109/README.md)
used separate runners; their timings should not be directly compared to
these samples. Parsing remains synchronous. None of these results
establishes a deadline, cancellation latency or process-memory bound.
