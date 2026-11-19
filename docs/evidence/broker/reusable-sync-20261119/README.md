# Reusable partition sync helpers

Flat-journal Produce reuses sync workers across waves. The storage actor syncs
the first partition. It starts one helper per additional partition as needed,
up to `--sync-concurrency` minus one. Each helper has one job slot and one
completion slot. The actor waits for every running sync before finishing the
wave. Successful jobs return their staged file leases before commit. A
missing completion fails the partition. Shutdown joins the helpers before
closing the partition logs.

Each helper requests a 256 KiB stack. The default allows three helpers and
768 KiB of requested stack space per actor. The maximum allows 63 helpers and
15.75 MiB. These are requested sizes. The operating system controls stack
mappings and residency.

## Paired Produce results

The test used one hosted Ubuntu 24.04 x86-64 runner and stable Rust 1.100.0.
The runner’s temporary filesystem reported `ext2/ext3`.
The baseline was `713c8a3d861a843bc1e6e229eecf53ce5632b3bf` and the candidate
was `cf026391b7839a7e84eaeab7f9111c7533842aa7`. [Benchmark job](https://github.com/mingley/partitionline/actions/runs/44611321354/job/135868364090)

| Workload | Baseline records/s | Candidate records/s | Median paired change (range) | Median p99 (µs) |
| --- | ---: | ---: | ---: | ---: |
| 1 client, 1 partition | 5,796 | 6,207 | +0.05% (−7.99% to +30.27%) | 290 → 300 |
| 1 client, 4 partitions | 14,001 | 18,125 | +29.67% (+22.37% to +36.37%) | 456 → 403 |
| 1 client, 8 partitions | 18,464 | 24,188 | +31.49% (+27.55% to +32.18%) | 804 → 684 |
| 4 clients, 4 shared partitions | 30,636 | 38,484 | +25.79% (+23.87% to +32.09%) | 699 → 570 |

Each workload ran eight alternating baseline and candidate pairs. Both
binaries used default release features, two Tokio worker threads,
`--sync-concurrency 4`, local plaintext TCP and Produce v3 with `acks=1`.
Requests carried one uncompressed 128-byte value per partition. Each client
sent 24 warm-up and 240 timed requests per arm. The probe checked every
acknowledgment, then stopped and restarted the broker and verified 122,496
record batches with Fetch v4. [Raw measurements](pairs.jsonl)

Throughput improved in every multi-partition pair. The single-partition result
was inconclusive. These short RF1 runs cover the four workloads above.
Comparisons with other brokers and sustained clustered workloads remain open.

## Checks

Rust 1.100.0 passed formatting and strict Clippy. Default and all features
each passed 22 focused unit tests, nine Produce integration tests and one
operator test. One existing unit test remained ignored.
[Qualification](https://github.com/mingley/partitionline/actions/runs/44611321354)

The first main inventory run rejected the stale Produce source checksum.
The updated inventory binds both Produce source files and rejects changes
in either file without a matching checksum.
[Initial failure](https://github.com/mingley/partitionline/actions/runs/44612976923/job/135874289507)
[Inventory check](https://github.com/mingley/partitionline/actions/runs/44614194954)

## Reproduce

Build the two commits with stable Rust using `cargo build --locked --release
--manifest-path partitionline-broker/Cargo.toml --bin operator`. Use separate
checkouts and target directories. Run both binaries on the same host.

```sh
python3 docs/evidence/broker/reusable-sync-20261119/produce-pairs.py \
  /path/to/baseline/operator /path/to/candidate/operator --scratch /path/to/scratch
```

Release binary SHA-256 hashes

- Baseline `5b8ebb8e567977b786f9df8b9bd27a172d9c56b67e72b989750554406ca8788c`
- Candidate `ca93c1be632f64aa9de5cae37403fe8c172195888cabfb309c83b78b585fd08f`
