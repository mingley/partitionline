# Release CI attempt binding

The release checker now uses GitHub REST run and job data instead of asking
`gh run list` for the unsupported `attempt` field. It checks the repository,
full commit, workflow, run attempt, 27 required main-push jobs and an artifact
named for that run and attempt. Older artifacts without the suffix do not
qualify. The checker also safely cleans up temporary paths containing spaces
or shell metacharacters.

The [source manifest](manifest.json), [validation](validation.json) and
[independent review](independent-review.json) identify the exact patch and
its checks. The complete offline release suite passed 119 tests with network
socket calls denied and Cargo and registry commands guarded. Separate mocked
job and cleanup controls also passed. [Stored file hashes](stored-files.json)
identify the compressed patch, test logs and review reproductions.

A [follow-up](../ci-colon-tmpdir-20261103/README.md) fixes literal colons in
temporary paths and passes the expanded 122-test offline release suite.

One [read-only GitHub response](../local-release-gates-20261103/github-rest-shape.json)
confirmed the relevant field shapes and job names for a single run. Live
pagination, a current-attempt artifact and an end-to-end release check have
not yet been verified. The API reads are separate and non-atomic, so a new run
or rerun can start after the final check. A fresh complete CI run may be needed
to create qualifying artifacts. No registry publication was performed.
