# Capability case selection

The capability workflow at `77021f7dc` failed because its expected counts were
stale. It also included two ignored external server tests in the total it
expected to execute. Those tests require independent peers and do not establish
SDK compatibility in this lane.

The gate now requires exact case names from a reviewed inventory:

| Suite | Required Rust cases | External server tests reported separately |
| --- | --- | --- |
| WriteTxnMarkers v2 | 13 | 1 |
| Share offsets v1 | 13 | 1 |
| Partitioner | 14 | 0 |
| Sticky partitioner | 29 | 0 |
| Streams protocol | 24 | 0 |

Discovery must match all 95 names, including the two servers. Execution must
report each of the 93 selected cases once, with no failures or ignored cases
and a successful process exit. The four required public API controls and the
234-file Streams export check remain required.

All 11 Python controls passed. They reject missing, extra, renamed and
duplicate cases, changed selectors, reclassified external tests, inconsistent
execution summaries, ignored required tests and compiler failures. Separate
process controls retain a failed report for optimized Python and helper import
failure before any proof command runs.

The new selection also matches discovery from the retained all-feature CI
artifact. This replay executes no Rust tests. The original workflow remains
failed; both repaired lanes still need to run on the new commit.

`receipt.json` pins the checked scripts and inventory, commands, logs and
original artifact. The original validation report is retained in the
[CI snapshot](../../releases/main-ci-77021-20261030/README.md).
