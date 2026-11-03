# Client candidate v7 results

The isolated v7 candidate was tested on x86_64 at checkout
`c025be19b7d19dfd17d85eea66b43de93288dffb`. It is not the client source on
`main`. The candidate did not pass.

Both the default and all-feature runs produced the same counts:

| Test binary | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Library | 2,253 | 1 | 0 |
| Mock Produce | 31 | 15 | 0 |
| Producer retained backing | 7 | 1 | 1 |
| Buffer ownership | 12 | 0 | 0 |
| Group coordinator startup | 13 | 0 | 0 |
| Group stack | 1 | 0 | 0 |
| InitProducerId v6 | 12 | 0 | 1 |
| Produce cancellation | 23 | 0 | 0 |
| Producer initialization | 8 | 0 | 0 |
| Sticky partitioner | 29 | 0 | 0 |

The library failure is a test setup panic from starting a runtime inside a
runtime. The Mock Produce failures include connection handshake expectations;
the retained-backing test timed out. Their causes and fixes require further
qualification. Strict Clippy found two test-only `drop_non_drop` errors.
Format passed. Two of 22 baseline controls passed when failure was predicted.
Those controls remain useful, but they do not reproduce the predicted
negative behavior.

The run used Rust 1.99.1, LLVM 23.1.1, one build job and one test thread.
The ten selected binaries used an 8 MiB stack, except for an explicit 2 MiB
group mock. This was not the complete all-target suite, a live-broker test,
a release qualification or a performance benchmark.

The compact [manifest](manifest.json) and `report-objects.tar.gz` preserve
all 610 members of the downloaded reports, deduplicated by SHA-256. Each member
can be reconstructed using its manifest entry. All 133 before/after source
inventory pairs matched. The command receipts report no resource stop and no
remaining owned child processes; OOM counters were unavailable.

The original report ZIP was 15,096,391 bytes with SHA-256
`ab64b3a35cc2c38b487e6b3f992076627406e7f9045bee0c997bd651dd6f3d59`.
The separate full artifact includes executable and source archives and has
finite GitHub retention; it was not downloaded for this report.

Source: [candidate run](https://github.com/mingley/partitionline/actions/runs/41141176310).
