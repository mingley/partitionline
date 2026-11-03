# Main client CI at 2645e96ce

In the [main CI run](https://github.com/mingley/partitionline/actions/runs/41593203567)
for commit `2645e96ce693fcdaa9d8c56f6f0c910934fd19a2`, the live Kafka
[4.1.2](https://github.com/mingley/partitionline/actions/runs/41593203567/job/125774457229),
[4.2.1](https://github.com/mingley/partitionline/actions/runs/41593203567/job/125774457154)
and [4.3.1](https://github.com/mingley/partitionline/actions/runs/41593203567/job/125774457011)
jobs aborted with a stack overflow in `live_compatibility_required` during
classic group join. Each exited with status 101. The logs establish the phase,
not the cause of the overflow.

The [Windows default library job](https://github.com/mingley/partitionline/actions/runs/41593203567/job/125774456918)
reported 2,181 passed and 15 failed tests. Its failures include socket
retirement and scheduling controls; they need separate diagnosis. These job
results do not describe any jobs that had not completed when recorded.

The [v16 candidate results](../candidate-v16-job-20261103/REPORT.md) record
the separate x86 client qualification. Neither set of results qualifies the
complete client, a release or a performance ranking.
