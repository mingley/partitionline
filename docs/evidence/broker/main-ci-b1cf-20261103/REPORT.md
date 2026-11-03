# Broker CI at b1cfbfd968

GitHub push run 41582894338, attempt 1, checked out main commit `b1cfbfd968c9d53a8416d1dfd93410a4fb5fd630` and concluded success. The `broker (stable)` job 125741552184 used stable Rust 1.99.1 on x86_64. Its complete decoded log is retained in `job-125741552184.log.gz`; `run-metadata.json` and `github-metadata.json` record the connector's run, job and step results.

The locked broker commands use the independent `partitionline-broker/Cargo.lock` (SHA-256 `42eaddf993ab4dfbe99e5af9d7c1a7cd858776a214eedbd6112de6a146933a2c`). The root workspace excludes `partitionline-broker`; its root `Cargo.lock` is retained in the manifest only as additional context. The prior receipt omitted the broker lockfile and is preserved as historical evidence.

The exact stale-leader proposal fencing test passed 1/1, with 32 other tests filtered. The default all-targets run passed 727 tests across 36 binaries; the all-feature all-targets run passed 875 across 36 binaries. Each had 0 failed or ignored. These totals count separate invocations and include overlapping tests; they must not be added as independent tests. `test-counts.json` retains each result line and target announcement. One target announcement is interleaved before the prior result, so the receipt does not pair individual result lines to binary names.

The earlier intermittent three-node fault history, five-node fault history and five-peer observer history each passed both as library tests and as integration tests in both all-target runs. A single green run does not explain or erase the earlier failures. Their complete logs and hashes remain in `manifest.json`.

All-feature, all-target Clippy with `-D warnings`, broker format, the OpenSSL prerequisite and all-feature Rustdoc with `RUSTDOCFLAGS=-D warnings` succeeded. Default and all-feature doctest commands succeeded but each reported 0 doctests. GitHub metadata also reports success for the independent Apache API inventory job and instrumented fuzz supervision job; this receipt did not inspect those jobs' logs. The connector's job listing is latest-attempt, first-page only; the run metadata explicitly identifies attempt 1.

This is one hosted CI run for the broker, not a production-readiness or performance qualification. No local Cargo, native test, benchmark, or publication was performed.
