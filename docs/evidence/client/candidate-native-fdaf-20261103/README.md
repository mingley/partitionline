# Client v12 native attempt

Run 41578299697 tested the isolated v12 candidate at checkout fdafca794. The
baseline phase ran 23 cases: 16 expected failures and five expected passes
matched, while two cases were observations. The new third-request case exited
before the report demonstrated that its barrier was reached.

Formatting passed. Strict Clippy and the default test build both failed with
the same E0433: the `rediscover_stop_tests` module uses `PendingWork` without
importing it. The driver stopped at the default build; no fixed runtime tests,
all-features build, or final summary were produced.

The manifest and content-addressed archive preserve all 336 compact report
members. All 75 source before/after guard pairs match their phase catalogs,
and their command receipt log hashes match. OOM counters were unavailable;
the larger executable/source artifact remains external.
