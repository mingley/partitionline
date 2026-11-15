# Broker CI at c471f5e

[Run 42452041729](https://github.com/mingley/partitionline/actions/runs/42452041729)
tested main commit [`c471f5e`](https://github.com/mingley/partitionline/commit/c471f5eb0d7b92959e23e5880e35109060bff5d2)
on 8 November 2026. The broker source was unchanged from the earlier
[3fc8059 run](../main-ci-3fc-20261108/README.md), which recorded two
different Raft runtime failures.

The default all-target suite passed 754 tests with three ignored. The
all-feature run passed 639 tests with three ignored, then stopped at one
failed integration test. Later targets and the broker job's Clippy and
Rustdoc steps did not run. The ignored descriptor diagnostics passed separately
in both profiles during the
[combined-source qualification](https://github.com/mingley/partitionline/actions/runs/42450883159).
Inventory and fuzz supervision passed as separate jobs.

`three_process_metadata_quorum_fails_over_and_replays` failed when its
`before-failover` delete received error 56. The test treated the result as
ambiguous or unexpected and did not retry it. The retained log does not
establish whether that delete committed or identify the cause. This failure,
the previous Raft failures, and their effect on production readiness remain
open.
