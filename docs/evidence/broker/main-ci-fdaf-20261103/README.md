# Broker CI failure at fdafca794

The stable broker job in [run 41578299671](https://github.com/mingley/partitionline/actions/runs/41578299671)
failed during default-feature tests. Its `raft_runtime` binary reported six
passes and two failures:

- `actual_five_node_tcp_minority_partition_late_ack_image_restart_and_removal`
- `actual_three_node_tcp_minority_partition_late_ack_image_restart_and_removal`

Both reported `ParseIntError { kind: InvalidDigit }`. The all-feature tests
and later broker lint and documentation steps did not run in that job. The
separate API inventory and short fuzz-supervision jobs passed.

The same class of Raft observer failure was retained in an earlier native
round; the passing broker run at `e7660f6e5` did not resolve it. The cause is
not established. No multi-node durability or release qualification follows
from either result.

The exact [failed job log](job-125726869343.log.gz) is retained. A reviewed
test-only [diagnostic patch](raft-diagnostics.patch.gz) adds bounded response
and phase context to the shared Raft histories without changing their retry
or deadline settings. Its [manifest](diagnostic-manifest.json) and
[independent review](diagnostic-review.json) record the source checks. The
patch and its added controls still need a native run. The excerpts are bounded
but not redacted; these histories use synthetic test inputs.

At checkout `c03416372`, the [next broker run](https://github.com/mingley/partitionline/actions/runs/41579081441)
passed default-feature tests, then reported five passes and three failures in
the all-feature `raft_runtime` binary. The two minority-partition tests again
reported bare `ParseIntError`. The five-peer observer test separately reported
`Node(NotLeader)` during its first prefix proposal. That response does not
establish the cause of the two parse failures. Later broker lint and docs
steps did not run. The [complete job log](job-125729388284.log.gz) is retained.
