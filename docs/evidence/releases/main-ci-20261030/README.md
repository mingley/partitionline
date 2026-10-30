# Main CI snapshot

This records GitHub Actions for commit
`02ce806d7ea4970897d51604e71e6a1fa069ca4c`, retrieved on 30 October 2026.
The API responses and selected complete job logs are retained as exact gzip
copies, with hashes in `files.json`. This is a snapshot of that commit, not
the current branch status.

The [broker test job](https://github.com/mingley/partitionline/actions/runs/41107327067/job/124240488981)
passed default and all-feature all-target tests: respectively 701 and 849
reported passes across 36 result blocks each, with zero failures or ignored
cases. Broker formatting, all-target Clippy, rustdoc and both doctest commands
also passed; the latter each found zero doctests. Opt-in probes do not establish
external peer coverage merely by returning successfully.

The broker workflow still failed its source-pinned API inventory and fuzz
build jobs. The inventory reported a stale metadata source checksum. The fuzz
job retained an internal build log; its outer log alone does not establish
the underlying cause.

The [client workflow](https://github.com/mingley/partitionline/actions/runs/41107326987)
was still running at retrieval and had failed test, strict Clippy,
package-consumer, security and live-broker jobs. Its allocation, instruction,
resource-soak, schema and release-rehearsal jobs passed. Neither an empty
commit-status list nor individual passing jobs establishes green CI.

The retained security audit reported seven vulnerability entries and one Tokio
unsoundness entry. The dependency-floor update has separate resolution
evidence; a new audit and runtime matrix are still required. The capability
workflow also failed. No release qualification follows from this snapshot.
