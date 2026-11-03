# Main broker CI at 2645e96ce

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/41593203697)
passed on commit `2645e96ce693fcdaa9d8c56f6f0c910934fd19a2` using stable
Rust 1.99.1 on Linux x86_64. Its
[broker job](https://github.com/mingley/partitionline/actions/runs/41593203697/job/125774457274)
passed formatting, the stale-leader control, default and all-feature behavioral
tests, strict all-feature Clippy and Rustdoc. The default profile reported
729 passed, zero failed and zero ignored tests; all features reported 877,
zero and zero. The separately run stale-leader control passed. Both doctest
commands succeeded with zero doctests discovered.

The workflow's API inventory and negative-control job and instrumented fuzz
supervision job also passed. These are the completed GitHub job statuses; the
fuzz supervisor checks do not establish a long-running fault campaign. No OOM
counter observation is claimed here.

The [isolated capacity qualification](../runtime-capacity-native-e2a0-20261103/README.md)
records the baseline and fixed controls for the same runtime patch. The main
broker CI result does not resolve the earlier intermittent Raft failures or
qualify the complete client, production deployment, release or performance
claims.
