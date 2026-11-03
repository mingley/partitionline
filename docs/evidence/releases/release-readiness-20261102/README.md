# Release tooling investigation

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
not run a live registry request or publish a crate. Full v3 rehearsal tests,
review of the complete patch, and exact-commit CI remain open.

The release workflow's post-upload confirmation currently checks the API
without confirming that Cargo's sparse index serves the version. That issue is
outside the candidate patch and must also be fixed before release.
