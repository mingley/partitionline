# DescribeTopicPartitions response reservations

The isolated [candidate][candidate] is not applied. The synchronous and
cooperative DescribeTopicPartitions response decoders reserve topic and
partition vectors from the peer's declared counts, and the nested replica,
ISR, eligible-leader and offline-replica integer arrays do the same. The
common array-length parser first requires the count to be no larger than the
remaining wire bytes. A padded malformed frame can still request substantially
more memory than its size before the first structured element is validated.

The candidate uses the existing `response_vec` helper for these vectors.
Its initial requested capacity is at most 16 KiB of elements, or one element
if larger. Valid results can grow beyond that amount. This is not a bound on
allocator capacity, the final response, cumulative decoded allocations or
process RSS.

[Run 42469062654][checks] passed both response-reservation integration tests
and all 472 existing protocol Admin library tests in each default and
all-feature profile, plus strict all-target/all-feature Clippy. The new
control checks a 400-partition response including 5,000 replica IDs, cursor,
nullable fields, synchronous and cooperative decoders, fragmented input, 256
truncated prefixes, and three padded malformed inputs. It does not prove
complete wire compatibility or process-wide allocation limits.

The checked formatted source SHA-256 was
`b4105f3af788f8e583df7e80a0249c79057cb19fb346d4bd1a34435259829298`
for `src/protocol/admin.rs` and
`2a1e450da9991104a537c3461a31e3d6f645d585b58e0123833a0d3fefe74282`
for `tests/admin_response_reservation.rs`. The workflow-only commit did not
change these bytes.

The first [allocation comparison attempt][first] failed to compile its
standalone probe because it compared `Result<_, Error>` values directly
without `Error: PartialEq`. No allocation or timing measurements ran.

The [corrected comparison][comparison] used distinct baseline and candidate
release builds with the same generated dependency lock on a shared x86 AMD
EPYC 9V74 runner, Rust 1.99.1 and LLVM 23.1.1. The standalone probe's system
allocator tracked individual allocation requests, not RSS or allocator
metadata. For padded 100,000-count inputs, synchronous and cooperative
decoders preserved the same errors and remaining bytes:

| Malformed response | Baseline largest request | Candidate largest request |
| --- | ---: | ---: |
| Invalid first topic | 7,200,000 bytes | 16,344 bytes |
| Invalid first partition | 13,600,000 bytes | 16,320 bytes |
| Truncated replica array | 400,000 bytes | 131,072 bytes |

The replica input contained 100,000 remaining bytes, so the decoder read
25,000 four-byte values before reporting truncation. The vector had grown past
its initial request. A small differential corpus compared both decoders'
outputs, errors and remaining bytes for every prefix of a valid response and
four byte replacements at each of its first 100 positions. This does not
cover every malformed frame.

Eight alternating pairs of valid synchronous decodes ran on one pinned
logical CPU. Ratios compare candidate elapsed time to baseline:

| Response | Median | Range | Slower pairs |
| --- | ---: | ---: | ---: |
| Small | 0.9959 | 0.9584–1.6543 | 4/8 |
| 10,000 topics | 1.1888 | 1.0522–1.4905 | 8/8 |
| 10,000 partitions | 1.1383 | 0.7505–1.1856 | 6/8 |
| 20,000 replica IDs | 1.0044 | 0.7926–1.0526 | 5/8 |

[All raw samples](paired.csv) are retained. These short in-memory samples
are not Kafka throughput. The consistent regression in the large-topic
shape and the median regression in the partition shape are material
tradeoffs; the candidate remains unapplied.

The probe's generated common lock SHA-256 was
`4755e186ccaa40de195dc10a0f7ea5ae33e17bcf333c7498a5af10d243ddb02b`.
Its baseline and candidate binary hashes were
`6453d7523923f8d9b53bf7631797b27488e9665b69496ea3f9f84fbbf4746c1e` and
`3d78288191327204b72b83219ac3adc28b2d89ccfa37198da469b7e4fab9a8d9`.
The probe source SHA-256 was
`e582eb38fa54cfd1a573a2d2c0557c6c000bd029fe508a2b60c6a8a512ca9ba2`.

The client still has unrelated failed CI, producer correctness, broker and
performance qualification gates. This focused change does not qualify a
release.

[candidate]: https://github.com/mingley/partitionline/commit/c1454cf03104abe0ffb100f5e62a1bffebf7e0aa
[checks]: https://github.com/mingley/partitionline/actions/runs/42469062654
[first]: https://github.com/mingley/partitionline/actions/runs/42469726659
[comparison]: https://github.com/mingley/partitionline/actions/runs/42469981321
