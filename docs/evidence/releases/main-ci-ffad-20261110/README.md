# Main CI snapshot: ffad9ef

This records workflow results for main commit
[`ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9`](https://github.com/mingley/partitionline/commit/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9).
It is not a release qualification for a later commit.

The [client CI run](https://github.com/mingley/partitionline/actions/runs/42872521845)
finished with 18 successful jobs, nine failures and one skipped job. The
default Linux library reported 2,312 passed and 16 failed; the all-feature
library reported 2,305 passed and 23 failed. Windows reported 2,318 passed
and 10 failed, and macOS arm64 reported 2,310 passed and 18 failed. Later
steps in those jobs did not all run. Failure identities and causes were not
inspected for this record.

Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs and the 3.9.1 and 4.1.0
smoke jobs failed. Format, Clippy, audit, docs, package, authentication and
integrity smoke jobs passed. The finite resource-soak CI job passed, but is
not a 24-hour fault campaign and does not explain the earlier OS-denied
process-descriptor sample. The instruction-count gate reported
`BASELINE_MISSING` for `x86_64-1.99.1`; it did not compare a baseline.
The live conformance job was skipped.

The [broker run](https://github.com/mingley/partitionline/actions/runs/42872521818)
passed both locked behavioral feature profiles, formatting, strict Clippy,
rustdoc, doctests, inventory and fuzz supervision. This pass does not explain
the preceding broker failure. Both jobs in the
[capabilities run](https://github.com/mingley/partitionline/actions/runs/42872521944)
passed; their uploaded archives were not independently inspected.

The client failures, missing performance baseline, incomplete interoperability,
and longer reliability campaigns remain open. Exact-commit CI and the release
guide's package and publication gates are required for any later version.
