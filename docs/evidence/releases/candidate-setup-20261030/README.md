# Candidate CI setup

The first workflow at `408a2765b` failed GitHub validation before creating
jobs ([run](https://github.com/mingley/partitionline/actions/runs/41133016807)).
The runner context was used at job environment scope. That was corrected
before the next attempt.

At `886b4d82e`, both jobs passed their Python guard and process-cleanup
controls ([run](https://github.com/mingley/partitionline/actions/runs/41133208826)).
The client then stopped before Rust compilation because its input manifest
included generated, untracked Python caches. Version 4 removes exactly five
cache entries from each phase; the Rust patches and all 122 required controls
are unchanged. Its 18 Python controls pass.

The broker completed six commands, then stopped because Git normalized a
patched source file's permissions. Its driver now verifies the complete byte
inventory before restoring the pinned source-file modes, then verifies bytes
and modes together. Restoration is limited to the eight patched source paths;
runtime journal files and fixtures are unaffected. All 13 broker guard controls
and six shared process controls pass. The Rust capsule and patch are unchanged.

Both artifact uploads also failed because their glob paths contained `..`.
The workflow now uses the normalized runner temporary path and uploads a
separate small reports artifact alongside the complete evidence. The original
job logs are retained here; the failed uploads mean their detailed native
receipts were not recovered. No candidate runtime qualification is credited
to these attempts.

The ordinary broker workflow at `886b4d82e` passes all three jobs: functional
tests and lint/docs, the pinned API inventory with all 55 negative controls,
and instrumented fuzz supervision. The retained inventory log reports 55
passing controls. This does not qualify the isolated broker fixes or close the
earlier intermittent Raft failures.

`files.json` records hashes for the complete job logs and job API snapshots.
