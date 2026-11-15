# Broker CI at 3fc8059

[Run 42451588960](https://github.com/mingley/partitionline/actions/runs/42451588960)
tested main commit [`3fc8059`](https://github.com/mingley/partitionline/commit/3fc805902f40421deb99f79786071e3e90e71079)
on 8 November 2026. Its broker source matched the earlier
[`efb3190` broker run](https://github.com/mingley/partitionline/actions/runs/42451247065);
the intervening commits changed documentation.

The default all-target suite passed 754 tests, with three descriptor
diagnostics ignored in the suite. The all-feature library then failed with
509 passed, two failed and three ignored, so its later targets and the broker
job's subsequent Clippy and Rustdoc steps did not run. The three descriptor
diagnostics passed separately in both profiles during the
[combined-source qualification](https://github.com/mingley/partitionline/actions/runs/42450883159).
Inventory and fuzz supervision passed as separate jobs.

The two failing Raft runtime tests were:

- `actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join`:
  the checkpoint image base was 4 where the test expected 3.
- `actual_three_node_tcp_minority_partition_late_ack_image_restart_and_removal`:
  a proposal to node 2 returned `Node(NotLeader)`; the logged diagnostics
  reported node 1 as leader at that point.

These results do not establish whether either failure is due to test
coordination, timing, or runtime behavior. Earlier runs passed these cases and
other Raft failures have also been recorded. The failures remain open; no
production-readiness or performance conclusion follows from a passing retry.
