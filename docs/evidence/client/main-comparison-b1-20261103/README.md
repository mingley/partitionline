# Main client comparison

The original client source and tests at `b1b048809` ran on x86_64 in
[run 41576616151](https://github.com/mingley/partitionline/actions/runs/41576616151),
job `125721482298`, at checkout `e7660f6e5`. Both feature profiles used one
test thread and an 8 MiB stack, matching the v10 candidate's selected runs.

Each profile had 2,185 library passes and 11 failures, 27 Mock Produce passes
and 18 failures, and seven retained-backing passes, one failure and one
ignored test. The original redial and retained-backing tests also failed as
separate baseline cases that had been expected to pass. Formatting and strict
Clippy passed.

All 15 Mock Produce identities that failed in v10 also failed here. Main had
three additional Mock Produce failures. The retained-backing timeout also
occurred on main. V10 changes some fixtures and adds test identities, so these
results do not establish identical causes or isolate a performance effect.
Both results remain failures.

The [analysis](analysis.json), [manifest](manifest.json) and object archive
retain all 154 report members. All 31 before/after source inventory pairs
matched, and the runner reported no remaining owned child processes. OOM
counters were unavailable. The separate full executable and source artifact
was not downloaded. These selected binaries do not replace the full
all-target, live-broker or performance qualification.
