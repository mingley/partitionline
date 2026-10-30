# Native candidate results

These are failed qualification rounds. Their passing subsets do not establish
release readiness or performance rank.

| Candidate and checkout | Default full run | All-feature full run | Other failures |
| --- | --- | --- | --- |
| Broker v1 at `dcbbfa84e` | 719 passed, one failed | 868 passed | Strict Clippy: ten blocking filesystem calls in three new tests |
| Broker v1 at `3ba5b544f` | 718 passed, two failed | 868 passed | The same Clippy errors |
| Client v4 at `3ba5b544f` | No runtime tests | Not reached | Eight unused decoder return values; one unused helper warning |
| Client v5 at `209d0b068` | No runtime tests | Not reached | One unqualified type name in a nested test module |

Both broker rounds observed all seven expected baseline failures and passed
all twelve fixed controls in each feature profile. Formatting, docs, doctests
and all 55 inventory controls also pass. The first default failure is a Fetch
deadline case returning EOF. The second round instead fails joined Fetch
shutdown with `QueueFull` and a five-node Raft observer with `ParseIntError`.
The all-feature passes do not resolve those observations.

All recorded source and runner checks stay stable. The client build receipts
report no resource stop and joined child processes. OOM counters are unavailable;
zero OOM events are not claimed. Native builds run on existing x86 GitHub
runners, with the local workspace's compilation guard unchanged.

The next broker revision converts the new filesystem tests to asynchronous
access and adds queue synchronization, phase diagnostics and repeated Fetch
checks. The next client revision fixes the nested test type name. Their
results must be checked separately.

`files.json` pins four complete report ZIPs and `summary.json`. Each ZIP retains
its original command logs and receipts. Executable archives and source capsules
remain in the full GitHub artifacts listed in the summary; they are not included
in these small reports. Those external artifacts have finite retention.

Sources: [first broker round](https://github.com/mingley/partitionline/actions/runs/41134197148),
[second broker and client v4](https://github.com/mingley/partitionline/actions/runs/41134619281),
[client v5](https://github.com/mingley/partitionline/actions/runs/41137428983).
