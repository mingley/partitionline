# Write-timeout peer drain

The network suite failed `write_timeout_shuts_the_broker_down` on both the
baseline and the coordinator-retirement candidate. The test sends an 8 MiB
payload to a peer that does not read until the 200 ms write timeout returns,
then requires the peer to finish reading within 300 ms.

A [same-host diagnostic](https://github.com/mingley/partitionline/actions/runs/42445641892)
kept those deadlines and recorded when the peer started reading and reached
EOF. With an 8-byte read buffer, the baseline saw EOF after 331.5 ms and the
candidate after 309.4 ms. Each read 4,270,080 bytes in 533,760 reads, so both
original assertions failed. The peer started reading within 59 µs of the gate.

With an 8 KiB peer buffer, the same diagnostic read the same number of bytes in
545 reads. EOF arrived after 1.13 ms on the baseline and 0.97 ms on the
candidate, and the full network suite passed in both cases. The diagnostic
added counters and used one shared x86-64 runner; its timings are observations,
not a general guarantee about socket shutdown.

The fixture now uses an 8 KiB peer buffer while preserving the write timeout,
the peer deadline, and the EOF-or-error check. This removes the 8-byte drain
cost from a test of whether the peer can finish reading.

## Earlier network-suite results

The following same-host runs used separate target directories for the
unchanged baseline and each candidate. Counts are passed/failed tests; each
failure was `net::tests::write_timeout_shuts_the_broker_down`.

| Run | Baseline | Candidate |
| --- | --- | --- |
| [42443132136](https://github.com/mingley/partitionline/actions/runs/42443132136) | 54/1 | 64/1 |
| [42444052378](https://github.com/mingley/partitionline/actions/runs/42444052378) | 54/1 | 66/1 |
| [42444268953](https://github.com/mingley/partitionline/actions/runs/42444268953) | 54/1 | 66/1 |
| [42445398458](https://github.com/mingley/partitionline/actions/runs/42445398458) | 54/1 | 66/1 |
| [42446027877](https://github.com/mingley/partitionline/actions/runs/42446027877) | 54/1 | 66/1 |
| [42446311046](https://github.com/mingley/partitionline/actions/runs/42446311046) | 54/1 | 67/0 |
| [42446563353](https://github.com/mingley/partitionline/actions/runs/42446563353) | 54/1 | 55/0 |
| [42447373118](https://github.com/mingley/partitionline/actions/runs/42447373118) | 54/1 | 67/0 |

Run 42443132136 recorded the failing commands but its diagnostic job did not
propagate their exit status. The later comparison jobs retained the baseline
failure as a failed job. The last three candidates included the larger peer
buffer; 42446563353 tested the fixture separately from the coordinator change.
Their candidate and baseline executable hashes differed.

Earlier candidate-only network runs also failed this test:
[42440392101](https://github.com/mingley/partitionline/actions/runs/42440392101)
had 59 passed, one failed and two ignored; [42442348308](https://github.com/mingley/partitionline/actions/runs/42442348308)
had 63 passed and one failed. An earlier baseline witness in
[42439981758](https://github.com/mingley/partitionline/actions/runs/42439981758)
reused the candidate target directory and cannot establish the baseline result.
Later witness runs used separate target directories and compared binary hashes.

In [42446563353](https://github.com/mingley/partitionline/actions/runs/42446563353),
the 55-test standalone candidate suite passed. A separate mutated build that
aborted the watcher and omitted the write-timeout shutdown failed at the
unchanged 300 ms peer-blocked assertion, confirming the fixture still detects
that suppressed-closure case.

Earlier versions had qualification failures that were fixed before the final
run: [42438826572](https://github.com/mingley/partitionline/actions/runs/42438826572)
failed Clippy on unused functions, [42439843113](https://github.com/mingley/partitionline/actions/runs/42439843113)
failed to compile test code, and [42441804474](https://github.com/mingley/partitionline/actions/runs/42441804474)
and [42444638800](https://github.com/mingley/partitionline/actions/runs/42444638800)
failed lint or formatting checks.
