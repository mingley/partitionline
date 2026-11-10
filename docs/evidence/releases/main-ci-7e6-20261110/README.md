# Main CI snapshot: 7e6dff9

This records workflow results for main commit
[`7e6dff932db4128596ad45811f03c8c9d4afb4b7`](https://github.com/mingley/partitionline/commit/7e6dff932db4128596ad45811f03c8c9d4afb4b7).
It is not release qualification for a later commit.

The [client CI run](https://github.com/mingley/partitionline/actions/runs/42883365631)
finished with 18 successful jobs, nine failures and one skipped job. The
default Linux library reported 2,313 passed and 15 failed; the all-feature
library reported 2,314 passed and 14 failed. Windows reported 2,308 passed
and 20 failed; macOS arm64 reported 2,313 passed and 15 failed. Later steps
in those jobs did not all run. Failure identities and causes were not
inspected for this record.

Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs and the 3.9.1 and 4.1.0
smoke jobs failed. Schema, docs, package, format, Clippy, audit,
authentication and integrity jobs passed. The finite resource-soak job
passed; it is not a 24-hour fault campaign and does not explain the earlier
OS-denied process-descriptor sample. The instruction-count gate reported
`BASELINE_MISSING` for `x86_64-1.99.1`; it did not compare a baseline.
Live conformance was skipped.

The [broker run](https://github.com/mingley/partitionline/actions/runs/42883365584)
passed its default library with 496 tests passed and four ignored, but a
later default-feature test target reported one pass and one failure. The
default job stopped; all-feature behavioral tests and the later strict checks
were skipped. The failure's identity and cause were not inspected. Inventory
and fuzz supervision passed separately. Both jobs in the
[capabilities run](https://github.com/mingley/partitionline/actions/runs/42883365598)
passed; their archives were not independently inspected.

The client and broker failures, missing performance baseline, incomplete
interoperability, and longer reliability campaigns remain open. Exact-commit
CI and the release guide's package and publication gates are required for
any later version.
