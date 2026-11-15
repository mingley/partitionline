# Main CI partial: 0bed184

9 November 2026

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42674081802)
on main commit `0bed1841c22d09decad6408bb2b44f60f933449a` concluded
failure. At the time of this record, the available GitHub job list contained
only the broker behavioral job and the pinned API inventory job; both
reported success. The instrumented fuzz-supervision job was not listed, and
the reason for the workflow failure is unknown. These results do not establish
that fuzz supervision passed.

The reported broker behavioral job passed 814 default and 962 all-feature
tests, with four ignored per profile. It also passed formatting, strict
all-feature Clippy and rustdoc, and doctests. The inventory job passed.

The [capability workflow](https://github.com/mingley/partitionline/actions/runs/42674081759)
also concluded failure. Its available job list contained a successful default
profile, but did not list the all-feature profile. The cause is unknown;
this does not establish an all-feature result. Its archive was not
independently inspected.

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42674082081)
concluded failure. Its available job list showed 13 successes and nine
failures; six jobs present in the preceding full run were not listed. The
Linux default library had 2,313 passes and 15 failures, all features had
2,312 passes and 16 failures, Windows had 2,310 passes and 18 failures, and
macOS had 2,315 passes and 13 failures. The three current Kafka compatibility
jobs and both historical smoke jobs failed. No result is inferred for the
unlisted jobs.

A separate [package documentation check](https://github.com/mingley/partitionline/actions/runs/42674111830)
passed on the same main source plus a temporary workflow: 30 documents,
229 local links and 67 pinned repository targets, with license and exclusion
inventory. It checked 58 documentation links and compiled no examples.
The earlier [combined-head run](../../ci/combined-a7b6-20261109/README.md)
compiled examples across four feature cells.

The complete [a7b6 CI snapshot](../main-ci-a7b6-20261109/README.md) is the
latest fully recorded client and broker snapshot. This partial broker record
does not qualify production readiness or explain any earlier failure.
