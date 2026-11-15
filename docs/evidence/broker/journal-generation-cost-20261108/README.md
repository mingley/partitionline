# Journal generation-lease experiment

This candidate is **not proposed for main**. On the measured host it reduced the staged-ticket structure from 24 to 16 bytes, but increased each lease from 32 to 40 bytes and did not show a convincing timing improvement.

The candidate binds each replacement inode lease to the original destination-path lease before publication, verifies that association at adoption, and lets a staged ticket retain just the inode-generation lease. Old generations remain distinct. The formatted source commit is [540ba22](https://github.com/mingley/partitionline/commit/540ba2269cfea002ad584d825c0e0b3adebc34bd), based on `c471f5eb0d7b92959e23e5880e35109060bff5d2`. The source commits differ only in `partitionline-broker/src/journal.rs`.

## Matched release measurement

[Run 42452353075](https://github.com/mingley/partitionline/actions/runs/42452353075) built both arms with `cargo +stable build --release --locked` and identical temporary wrappers. Twenty measured samples per arm and workload alternated baseline/candidate order; one warm-up per arm and workload is included in the [168 raw sample lines](samples.log). Every sample copied the same seeded journal and reopened it afterward to check all record offsets, counts and payload bytes.

| Workload | Current median | Candidate median | Median paired candidate/current | Paired range |
| --- | ---: | ---: | ---: | ---: |
| Open, recover and close | 21.883 µs/op | 21.863 µs/op | 1.003 | 0.707–1.485 |
| Append and sync | 204.735 µs/op | 202.835 µs/op | 0.992 | 0.823–1.198 |
| Stage, sync and commit | 203.176 µs/op | 201.825 µs/op | 0.999 | 0.245–1.206 |
| Eight-entry group: stage and ticket drop | 4.510 µs/entry | 4.526 µs/entry | 0.999 | 0.928–1.039 |

The last row times the accumulated staging and ticket-drop portions of 256 groups of eight entries. It holds all eight tickets through one sync and their commits, but excludes vector allocation, sync and commit from the timer. It is a focused staging proxy, not acknowledgment latency. The broad paired ranges and short shared-host run limit inference. In particular, staged trial 16 took 384.98 ms in the current arm and 94.51 ms in the candidate arm; it remains in the data.

Both arms used an AMD EPYC 9V45 GitHub-hosted VM, four vCPUs, Linux `7.0.0-1017-azure`, rustc/cargo 1.99.1 and the same ext4 `/dev/nvme0n1p1` mounted `journal_async_commit,nobarrier,data=writeback`. Timed processes were pinned to vCPU 0, but the host was not isolated. These measurements do not establish sustained broker performance or power-loss durability. The [run artifact](https://github.com/mingley/partitionline/actions/runs/42452353075/artifacts/13606193256) retains setup, build and raw output.

| Item | SHA-256 |
| --- | --- |
| Current journal source before wrappers | `130adb705e0e3150508c7381cb1c572d293881bd6a9f3416a2df1fe21e42b549` |
| Candidate journal source before wrappers | `6deea84fd4251c56a7d1d03eb6566c50609b5a4eb030267821173968348a3015` |
| Shared wrappers | `4291dc910b319a7a643278cc968754697a42c1efe9420e8372d8f6d8ada60263` |
| Shared lease-size helper | `8a2e2d7e4f8dc0f24eff3b41e1331884c3bce6d261ed20f0c4eb3ad05cdf40c0` |
| Shared benchmark example | `3dad0b93f0ee4a7b98ff02b88e24c034ded2817f7c9491b609fa8c3927c0fc73` |
| Current executable | `566262a51bb14ba917db34aff30fa156ad63acb85d5396bb2ed1287affb21b44` |
| Candidate executable | `59e73d6735f15a9b2d416240b140da4388a1c874b6b2ba1f761dce1f8c67d17a` |
| Seed | `ffdd8983d06bf91bf6967933720b2b61cf8e20c447dba7e732e6a324daf7826e` |

On this x86_64 build the ticket structure shrank 24→16 bytes and each lease structure grew 32→40 bytes. A live Unix Journal has both a path and an inode lease; the exact heap charge depends on allocator rounding. The candidate saves one Arc clone and one Arc drop per staged ticket, but adds a path reference inside each identified inode lease. Descriptor counts did not change in the focused controls.

## Correctness scope and retained failures

The expanded unformatted source `c222fd2c1181a24ab4fc39fead726892992d1d89` differs from the formatted candidate by rustfmt and a later documentation-only base. Its Linux [run 42451902629](https://github.com/mingley/partitionline/actions/runs/42451902629) passed targeted pre-publication and rejected-association controls, checkpoint/epoch and three-generation tickets, alias and path exclusion, inode pins, descriptor counts, EMFILE cleanup, strict all-feature Clippy, and instrumented default/all-feature library suites (471/515 passed; three isolated diagnostics ignored in each and passed separately). It did not run the full all-target suites or inventory. Its Windows job failed checkout on long historical paths.

The sparse Windows [run 42452235907](https://github.com/mingley/partitionline/actions/runs/42452235907) passed one path-only ticket/adoption lifetime test on x86_64 Windows MSVC and compiled the all-feature library. Earlier Windows attempts [42452117917](https://github.com/mingley/partitionline/actions/runs/42452117917) and [42452220509](https://github.com/mingley/partitionline/actions/runs/42452220509) failed before compilation due to an incomplete sparse tree and a workflow parent-check error, respectively.

The earlier candidate's [run 42451768079](https://github.com/mingley/partitionline/actions/runs/42451768079) passed its targeted controls but failed three Raft TCP library tests with `Node(NotLeader)`; its all-feature suite did not run. Separately, main's [run 42451588960](https://github.com/mingley/partitionline/actions/runs/42451588960) recorded two all-feature Raft failures. These failures remain part of the evidence; no causal link to this experiment is established. Full exact-source all-target and inventory gates remain unrun, and this measurement does not justify merging the optimization.
