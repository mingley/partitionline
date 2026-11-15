# Main CI at 36424a5

This records CI for main commit
[`36424a580913e3d77571beed65b91d6751d7b108`](https://github.com/mingley/partitionline/commit/36424a580913e3d77571beed65b91d6751d7b108).
The client run was canceled when main advanced. The broker and selected
capability jobs passed.

[Client run 42464551954](https://github.com/mingley/partitionline/actions/runs/42464551954)
finished with 16 successful jobs, three failures, eight cancellations and
one skipped job. Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs failed
with stack overflows. The Linux default and all-feature, Windows, macOS,
both Kafka smoke, package and integrity-smoke jobs were canceled; they have
no result for this commit. Formatting, strict Clippy, audit, docs,
release rehearsal, authentication, resource-soak and the other completed jobs
passed. Live conformance was skipped.

[Broker run 42464551921](https://github.com/mingley/partitionline/actions/runs/42464551921)
passed 766 default and 914 all-feature all-target tests, with three ignored
descriptor diagnostics per profile. Strict Clippy, Rustdoc, inventory and fuzz
supervision passed. Both doctest commands reported zero tests. The ignored
descriptor diagnostics had passed separately on the same broker production
source in an earlier run, not in this broker job.

[Capability run 42464551924](https://github.com/mingley/partitionline/actions/runs/42464551924)
passed both selected profiles. Its artifacts were not independently inspected.
The passing broker run does not explain the two Raft failures in the
[preceding complete snapshot](../main-ci-02c-20261108/README.md), and the
canceled client jobs are not qualification passes.
