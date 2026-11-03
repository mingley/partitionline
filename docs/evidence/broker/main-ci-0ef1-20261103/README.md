# Main broker CI at 0ef1cf85

[Workflow run 41603566088](https://github.com/mingley/partitionline/actions/runs/41603566088) completed successfully for commit `0ef1cf8581605cedff1117c99bcbbccc6a9cb68e`. All three jobs passed.

The [broker job](https://github.com/mingley/partitionline/actions/runs/41603566088/job/125807810258) reported:

| Check | Directly reported result |
| --- | --- |
| Default-feature all-target tests | 734 passed across 36 result sections, including 447 library tests; 0 failed or ignored |
| All-feature all-target tests | 882 passed across 36 result sections, including 491 library tests; 0 failed or ignored |
| Stale-leader fencing | 1 passed; 32 filtered out |
| Default and all-feature doctests | Both commands passed with 0 doctests executed |
| Format, strict all-feature Clippy and Rustdoc | Passed |

Each profile total sums its own reported target result sections. The profiles run separately, so the totals should not be added as unique tests.

The [instrumented fuzz-supervision job](https://github.com/mingley/partitionline/actions/runs/41603566088/job/125807810560) passed 12 Python supervision tests, including real short workers and interruption/resume checks. This is not a qualified long fuzz campaign. The [Apache inventory job](https://github.com/mingley/partitionline/actions/runs/41603566088/job/125807810755) reported a passing inventory verdict and 55 passing controls; the inventory is a static source and fixture check.

`source-pins.json` records SHA-256 and Git blob IDs for the workflow and relevant source files at the exact commit. The controller source and registry match the reviewed fixed candidate hashes. `analysis.json` records the count derivation from the three retained decoded logs. Those logs are connector-decoded text re-encoded as UTF-8, not original compressed GitHub logs. GitHub artifact identities, reported sizes and hashes are in `connector-metadata.json`; artifact ZIP bytes were not downloaded or independently verified. `SHA256SUMS` covers the package.

This CI run does not establish complete production readiness, full Kafka compatibility, or a global performance ranking.
