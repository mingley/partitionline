# DescribeTopicPartitions row conversion: baseline

The aggregate Admin call converts every named topic row in a response before it
starts its separately cooperative page merge. The per-topic converter yields
for large partition lists, but it does not yield for a sequence of empty or
error rows. This can make one poll long. A row checkpoint is under evaluation;
no change has been applied.

## Representative poll and destruction probe

[Run 42480362800](https://github.com/mingley/partitionline/actions/runs/42480362800)
passed 42 cases: three separate process samples for each of seven row counts
and two row kinds. [Raw samples](representative-baseline.csv) contain the poll,
drop and fixture measurements in nanoseconds and bytes.

The fixture mirrors the production conversion loop and then calls the actual
page merge. Each first poll also increments a test-only `Cell` once per row.
At 8,192 or more rows the existing merge yielded after merging 8,192, but the
fixture had already converted the entire page. The first-poll numbers include
both that conversion and the first 8,192 merge operations.

| Rows | Empty topics: first poll | Topic errors: first poll |
| ---: | ---: | ---: |
| 1 | 0.029–0.047 ms | 0.038–0.043 ms |
| 32 | 0.056–0.066 ms | 0.045–0.056 ms |
| 8,191 | 3.197–3.267 ms | 3.174–3.242 ms |
| 8,192 | 3.195–3.484 ms | 3.174–3.225 ms |
| 8,193 | 3.203–3.253 ms | 3.182–3.222 ms |
| 100,000 | 13.104–21.305 ms | 13.298–15.781 ms |
| 500,000 | 50.668–57.808 ms | 49.523–50.178 ms |

All 500,000-row cases yielded only after converting all 500,000 rows and
merging 8,192. After that pending poll, the probe separately timed destroying
the pending fixture future (7.896–8.302 ms), the retained output and index
(5.098–5.354 ms), and the retained input response (8.247–8.429 ms). These are
sequential measurements of this fixture's pieces, not a public cancellation
latency or an upper bound. Cleanup on cancellation can remain synchronous even
if the conversion loop gains a cooperative yield.

Each 500,000-row fixture encoded a 20,000,009-byte response body, below the
100 MiB frame cap. The conversion page reserved 36,000,000 bytes for the
`TopicDescription` vector, excluding heap strings and other allocations.
The probe does not measure peak heap use or count allocator calls. Names are
14 bytes; the fixture uses unique names but repeats the success topic ID. The
response is constructed and encoded in the test; it is not decoded from a
broker. The 5 ms column in the raw data compares a wall clock after the poll;
it does not register or measure a Tokio timer.

The test uses empty successful rows or `TOPIC_AUTHORIZATION_FAILED` rows,
with no partitions. It does not establish how often a real broker produces such
large pages, nor measure a Kafka deployment. Large partitions, replica arrays,
allocation and destruction can themselves hold a poll or cancellation; a
row-count checkpoint would not bound their cost.

## Reproduction and pins

- Production source: [`src/admin.rs`](https://github.com/mingley/partitionline/blob/426041c9f5526dfb6a788344a0e03b3edfefffb0/src/admin.rs),
  Git blob `4d4212057984e47201b2bb1fe383ac2ae5a55562`,
  SHA-256 `25fac8f3acbb31f08417d374cff8156cef34d66ca08eb002b08bd6d9e32eb98f`.
- [Test-only fixture](https://github.com/mingley/partitionline/blob/8501a8ba810f8abaa64cecaf3cbea700e670a18e/src/admin/pagination_tests.rs):
  Git blob `9261538d5a161df20432a6de8755a128ddd2a075`,
  SHA-256 `479d255e0a356c3eedabdb166efe331b014733a9fc9b8906a67604d0deb819d1`.
  Workflow commit: `05a6117217178e11325149ad576cd4642dd96394`.
- x86-64 GitHub-hosted Ubuntu 24.04, AMD EPYC 7763, stable Rust 1.99.1
  (`58cf8f9c3`), LLVM 23.1.1. The job reported `cpu.max unavailable`;
  no CPU quota is inferred.
- `cargo test --locked --release --lib dtp_row_conversion_poll_witness --no-run`,
  default features, release thin LTO and one codegen unit.
  The executable SHA-256 was
  `cafca127436940c4a09fd24fefe1dde1237276a876401357fb35ec5dcc662033`;
  the binary itself was not retained. Each case ran that executable once with
  `--exact admin::pagination_tests::dtp_row_conversion_poll_witness --test-threads=1 --nocapture`.
- The [run artifact](https://github.com/mingley/partitionline/actions/runs/42480362800)
  retained build, environment and raw output with 30-day retention.

A separate public Admin call against a mock peer is running. It will be
reported separately because response reading and decoding also affect its
poll timings.
