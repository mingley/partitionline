# Stale Raft leader selection

The retained CI history shows an observer proposal and a later leader-removal
request receiving `NotLeader` after the test selected a leader from a state
snapshot. Those are separate failures; the snapshots do not establish their
full causal ordering.

The [new control](control.patch.gz) exercises one bounded case with a virtual
clock: a previously selected genesis leader loses its quorum lease, rejects
a proposal, and retains its WAL, positions and nonempty committed barrier.
The [design](DESIGN.md) and [independent review](independent-review.json)
explain the scope. At `32ec4ab80`, the hosted focused control passed, as did
727 default and 875 all-feature broker tests. Strict Clippy then failed on two
file reads in this new synchronous test. The [job log](job-125736974518.log.gz)
and [result manifest](clippy-failure-manifest.json) retain that run.
The [correction](clippy-correction.patch.gz) uses a bounded synchronous read
and preserves the complete-byte comparison; its
[review](clippy-correction-review.json) passed. The focused control, 727
default and 875 all-feature tests, and strict Clippy passed in the hosted
[rerun](https://github.com/mingley/partitionline/actions/runs/41582894338)
at `b1cfbfd96`.

This case does not prove every `NotLeader` response precedes an append. The
current proposal and voter-change paths can also return that error after
other work. No automatic retry was added, and neither CI failure's cause is
claimed resolved.
