# Raft vote route control, 2026-11-08

This control establishes that a full one-slot outbound route can discard a
candidate's newest vote request while retaining an older one. It demonstrates
one lost campaign in a forced two-voter schedule. It does not establish
unbounded starvation or explain the earlier 15-second leader-wait failure
documented in [Raft liveness](../raft-liveness-20261108/README.md).

## Source and environment

The production implementation was pinned to commit
[`f092d69d43ebb300c756cca006f4964c9820e491`](https://github.com/mingley/partitionline/commit/f092d69d43ebb300c756cca006f4964c9820e491).
The isolated, test-only fixture and workflow were run from commit
[`9c5d07a1be382404bbf49e91ffc2df268222fde2`](https://github.com/mingley/partitionline/commit/9c5d07a1be382404bbf49e91ffc2df268222fde2);
its `partitionline-broker/src/raft/runtime.rs` Git blob was
`027a0bb82a587b0b159b146ab39a0993372f0eaf`. It did not change production
behavior. The fixture used a GitHub-hosted x86_64 Linux runner, Rust 1.99.1
and Tokio 1.53.3. The runner was shared; these tests did not measure throughput
or establish any performance ranking.

[Run 42460992863](https://github.com/mingley/partitionline/actions/runs/42460992863)
ran the focused test once with each of the broker's default and all-feature
profiles:

```sh
cargo test --locked --manifest-path partitionline-broker/Cargo.toml --lib \
  queued_old_vote_loses_a_new_campaign_and_later_recovers -- --nocapture
cargo test --locked --manifest-path partitionline-broker/Cargo.toml --lib \
  --all-features queued_old_vote_loses_a_new_campaign_and_later_recovers -- --nocapture
```

Both passed, one test in each profile. The fixture used two fresh durable
`LocalOwner` instances and a real outbound worker, TCP connection, framing,
and one-slot route. The test peer acted as a controlled wire endpoint and
asked its genuine owner to process votes; it was not a full autonomous
two-node cluster. The test explicitly joined its one manually spawned worker,
checked worker and socket peak counts against one, and checked their current
counts returned to zero.

## Forced schedule and counterfactual

In both profiles, the test held the response to the first request after it
appeared on the wire. The source then started two newer campaigns: the second
request occupied the one-slot route, and dispatch canceled the third request
when the route was full. After release, the genuine grants for the first two
requests reached the source on the real worker. The owner had moved to the
third term, so each stale acknowledgment was rejected and the worker closed
its connection. The second, older request was observed on the wire. A genuine
grant for the canceled third request was then delivered directly to the source
and rejected for its absent correlation. After the stale queue drained, a
fourth campaign's genuine wire grant elected the source.

The emitted terms were identical in the two profile runs:

```text
queue-control cancel_newest=true first=2 second=3 dropped=4 recovered=5
```

A separate fresh fixture used the same first two observed wire requests and
genuine peer grants, but did not dispatch or cancel the third request. The
test delivered its genuine peer grant directly to the owner, where it elected
the candidate:

```text
queue-control cancel_newest=false first=2 second=3 elected=4
```

The counterfactual intentionally bypassed the outbound route for that last
grant. It shows that the third correlation was eligible under that fixture's
controlled history; it does not measure autonomous cluster recovery.

## Negative control

In the default profile, the workflow changed only the full-route error branch
from `control.cancel_job(error.into_inner()).await` to dropping the returned
job without canceling its owner correlation. The focused test then failed at
its intended check:

```text
Error: "queue-full-cancels-current-correlation: abandoned correlation was accepted"
test raft::runtime::ownership_tests::queued_old_vote_loses_a_new_campaign_and_later_recovers ... FAILED
```

The workflow required that exact failure message; compilation or setup failure
would not have satisfied the negative control.

## Setup failures and limits

[Run 42460580627](https://github.com/mingley/partitionline/actions/runs/42460580627)
stopped before compilation because the workflow used `-p partitionline-broker`
against the root workspace instead of the broker manifest.
[Run 42460690226](https://github.com/mingley/partitionline/actions/runs/42460690226)
reached the first forced schedule and printed terms 2, 3, 4, and 5, but failed
because the pre-existing direct-owner test helper assumed zero network worker
and socket peaks. The corrected helper separately accounts for the manually
spawned, joined worker.
[Run 42460845088](https://github.com/mingley/partitionline/actions/runs/42460845088)
failed compilation because an existing helper call was not updated for that
new bound. None of these failed runs passed the full controls.

The single-slot observation is a real bounded behavior, but its practical
frequency and effect on election availability remain unmeasured. The held RPC
is bounded by the configured RPC deadline; successful draining in this fixture
does not prove every real deployment will recover. No conclusion about the
cause of the earlier stalled election or the separate unexplained small
observer image follows from this control.
