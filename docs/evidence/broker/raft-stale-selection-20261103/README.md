# Stale Raft leader selection

The retained CI history shows an observer proposal and a later leader-removal
request receiving `NotLeader` after the test selected a leader from a state
snapshot. Those are separate failures; the snapshots do not establish their
full causal ordering.

The [new control](control.patch.gz) exercises one bounded case with a virtual
clock: a previously selected genesis leader loses its quorum lease, rejects
a proposal, and retains its WAL, positions and nonempty committed barrier.
The [design](DESIGN.md) and [independent review](independent-review.json)
explain the scope. Exact source reconstruction and formatting passed; the
control still needs a native run.

This case does not prove every `NotLeader` response precedes an append. The
current proposal and voter-change paths can also return that error after
other work. No automatic retry was added, and neither CI failure's cause is
claimed resolved.
