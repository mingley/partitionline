# Matched journal release measurements

[Run 42451087923](https://github.com/mingley/partitionline/actions/runs/42451087923) compared baseline `ba8d72f51c23b4bfbd1057e61bbd483da831aa91` with the landed ownership repair `4e965c2de6810f687b1c3656f1d379036b4a0e60` on one x86_64 Linux runner. These measurements describe this host and workload; they do not establish general broker throughput, competitive performance, or power-loss durability.

| Workload | Baseline median | Repair median | Median paired repair/base | Paired range |
| --- | ---: | ---: | ---: | ---: |
| Open, recover one record, close | 24.83 µs/op | 26.28 µs/op | 1.040 | 0.970–1.092 |
| Append one record and sync | 174.09 µs/op | 183.51 µs/op | 1.067 | 0.893–1.615 |
| Stage one record, sync and commit | 180.18 µs/op | 189.75 µs/op | 1.056 | 0.639–1.726 |

A paired ratio above 1 means the repair took longer in that pair. Each cell uses 12 measured samples per arm; the median of paired ratios is calculated separately from the two per-arm medians. The full [78 raw sample lines](samples.log) include one warm-up per arm and workload. All copies matched the same seed and all final reopen checks validated every offset, record count and payload byte. The write ranges are wide. For example, append's median paired ratio was 1.203 when baseline ran first and 1.024 when repair ran first, six pairs in each order. This order sensitivity and the shared virtual host limit what can be inferred from the differences.

## Workload and host

Each sample copied one identical one-record seed into the same temporary pathname. The open workload performed 2,048 opens with recovery and close, including the journal's normal file and parent-directory synchronization. The append and staged workloads each performed 512 serial appends of a fixed 256-byte payload, with one `sync_data` per record. The staged benchmark computes the payload CRC before timing and calls stage, sync and commit together; it does not measure grouped Produce or concurrent syncs. Each timed sample was followed outside the timer by a reopen and full record verification. For every trial the two arms were adjacent, alternating baseline/repair and repair/baseline; the workload order was reversed on alternate trials.

Both examples used `cargo +stable build --release --locked`, the same broker manifest and lockfile, and an identical temporary public wrapper around the existing crate-private staged path. The wrapper and example were injected into both temporary checkouts; the production source itself was not changed. Source, wrapper and executable pins are below. Harness and workflow are isolated in commit [3535b0c](https://github.com/mingley/partitionline/commit/3535b0c5feae69ff93909316880580388bebb110); the [run artifact](https://github.com/mingley/partitionline/actions/runs/42451087923/artifacts/13605567801) retains build/setup logs, hashes and raw output.

The GitHub-hosted VM reported four vCPUs on an AMD EPYC 9V74, Microsoft hypervisor, Linux `7.0.0-1017-azure`, rustc 1.99.1 and cargo 1.99.1. All timed processes were pinned to vCPU 0; the host was not dedicated or otherwise isolated. The temporary directory was on ext4 `/dev/nvme0n1p1` mounted with `journal_async_commit,nobarrier,data=writeback`. This is a particular virtual storage configuration and its mount settings do not establish durable power-loss behavior.

| Item | SHA-256 |
| --- | --- |
| Baseline `journal.rs` before wrapper | `87269cb14a1d623cd352ba6ac1dfd0997aec3e99ae17a1c88131bd660d97315a` |
| Repair `journal.rs` before wrapper | `130adb705e0e3150508c7381cb1c572d293881bd6a9f3416a2df1fe21e42b549` |
| Identical wrapper | `dbf37a2e9b1a883b47acf416961c42c508ea98a461a913b7ad298fbbdd617784` |
| Identical benchmark example | `4e7ec8fccf715be268a3fe6e3ecc5643cbb687dba46a513ca73abe692754623b` |
| Baseline executable | `1324765f9e3f80b996a81e086dd7e1b37e69e8d2092000ff705f2b185189275e` |
| Repair executable | `9e757ddd59f61f0f9ab34448933ee591940535e5aca149a6a9e1b417bd4992a2` |
| Identical seed | `ffdd8983d06bf91bf6967933720b2b61cf8e20c447dba7e732e6a324daf7826e` |

The first setup [run 42450828575](https://github.com/mingley/partitionline/actions/runs/42450828575) failed workflow validation and started no job. After moving runner-path initialization to a step, [run 42450875535](https://github.com/mingley/partitionline/actions/runs/42450875535) verified both source pins but failed to build because the temporary example lacked required crate documentation; its [setup/build artifact](https://github.com/mingley/partitionline/actions/runs/42450875535/artifacts/13605815852) is retained. The successful run added that documentation and used fresh checkouts. Neither failed attempt produced timing samples.
