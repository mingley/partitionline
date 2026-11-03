# Release tooling investigation

The Release-plz run at `350b5e534` passed the semver scan after archived
manifests were excluded. It identified breaking API changes and selected
`0.2.0`. The PR-only workflow then failed to create its branch with GitHub
HTTP 403, `Resource not accessible by integration`; it did not publish.
See [run 41572635916](https://github.com/mingley/partitionline/actions/runs/41572635916).

The current `0.1.0` rehearsal cannot be reused unchanged for a `0.2.0`
candidate. Its self-test requires version `0.1.0`; an offline registry probe
also classified an API 404 plus index 503 as absent. The
[baseline negatives](baseline-negative.json) retain those observations.

An isolated v3 patch prepares a version-aware rehearsal and a fail-closed
registry probe. The patch is retained as `v3.patch.gz`, but is not applied to
`main`. Its [receipt](v3-receipt.json) pins the source and records the
pending full rehearsal validation.

After the receipt was written, an independent offline review ran 33 probe
controls against v3; all passed. The [results](v3-probe-results.json) and
review script are retained. The review used substitute HTTP responses and did
not run a live registry request or publish a crate.

The author ran v3 tests with the existing `0.1.0` manifest. A subsequent
[independent cross-version check](v3-cross-version-receipt.json) failed on a
`0.2.0` manifest: a negative test changes a version to `0.2.0`, which is a
no-op when that is already the candidate. The
[failure log](v3-cross-version-stderr.log) is retained. V3 must not be
adopted until that control is fixed and the full suite passes for both
versions. Complete patch review and exact-commit CI also remain open.

V4 fixes that test and is applied to the release scripts. The offline author
run passed 64 release tests and 25 recovery scenarios with the manifest set
to `0.1.0` and again to `0.2.0`. The 33 independent probe controls passed.
An [independent review](v4-independent-review.json) also checked both manifest
versions and the exact patch. Original test logs, source inventories and
receipts are retained in `v4-validation.tar.gz`; the patch is
`v4.patch.gz`. Hosted CI for the adopted source remains open.

The release workflow's earlier post-upload confirmation checked the API only.
The applied confirmation change requires a matching, unyanked version from
both the API and Cargo's sparse index, including after a skipped upload.
Offline tests passed, and the retained
[v2 failures](confirmation-v2-failures.json) and
[v3 replay](confirmation-v3-malformed-replay.json) cover malformed numeric
constants. The [receipt](confirmation-v3-receipt.json) and validation archive
pin the reviewed source. Hosted CI and an actual publication remain unrun.
The index check validates identity and yank state; it does not by itself prove
that Cargo can parse or download the crate. The release guide separately
requires an external consumer check.

The local owner-cut installability path can still accept index-only evidence
when the API is unavailable. Align that path with the documented dual-source
confirmation rule before release.
