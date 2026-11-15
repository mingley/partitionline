# Partial CI at c4819e8

This records CI for main commit
[`c4819e83e6f0f5a8a4e1f9dd0f0bbdcad81b862c`](https://github.com/mingley/partitionline/commit/c4819e83e6f0f5a8a4e1f9dd0f0bbdcad81b862c).

[Broker run 42477681322](https://github.com/mingley/partitionline/actions/runs/42477681322)
passed 781 default and 929 all-feature tests, with four ignored per profile.
Broker formatting, strict Clippy, Rustdoc and doctests passed, as did separate
inventory and fuzz supervision jobs. These passes do not explain the earlier
[disconnect](../../broker/disconnect-eof-20261108/README.md) or
[Raft proposal](../main-ci-7fb-partial-20261108/README.md) failures.

[Client run 42477681232](https://github.com/mingley/partitionline/actions/runs/42477681232)
had 18 successful jobs, eight failures, one canceled and one skipped.
Linux default had 2,238 passed and 16 failed; Linux all features had 2,244
passed and 10 failed. Windows had 2,241 passed and 13 failed; macOS arm64 had
2,245 passed and nine failed. Later targets did not run after the library
failures. Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs aborted with stack
overflows. Kafka 4.1.0 smoke reported JoinGroup error 16 and timeouts; the
Kafka 3.9.1 smoke was canceled when main advanced and has no result.

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42477681250)
passed; their archives were not independently inspected. This partial snapshot
does not establish production readiness or a performance ranking.
