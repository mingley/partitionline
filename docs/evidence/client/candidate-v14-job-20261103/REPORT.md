# Client v14 hosted qualification failure

GitHub push run 41581442964, attempt 1, checked out `32ec4ab8040721339181918dafaa8edda0a8cce8`. Job 125741909203 ran client v14 with stable Rust 1.99.1 on x86_64 and failed. The exact v14 specification and fixed-input hash map are retained alongside the complete decoded job log. The spec identifies the isolated fixed client's base as `33744862cd9e3263de502d57525619bd41704fec`; it is distinct from the workflow checkout SHA.

The baseline build passed. All 21 baseline cases with declared pass/fail expectations matched their reported outcome; the two observation-only cases 07 and 13 both passed. These baseline cases include intentional negative controls and are not fixed-candidate results. Format, strict Clippy, the default build and the all-feature build all exited 0.

Each fixed profile ran the 10 specified targets and reported 2,431 passed, 17 failed, 2 ignored. The profiles overlap and must not be summed as independent tests. In each profile:

* `mock_produce` reported 32 passed and 15 failed; the exact identities are in `observed-results.json`. The sets match across the two profiles.
* The `partitionline` library target reported 2,292 passed and one failed: `producer::tests::cancelled_pending_deadline_scan_retains_the_detached_prefix_until_worker_cleanup`. Target process durations were 301.547 s default and 302.053 s all-features. These are aggregate target durations, not measured durations for the individual failing test.
* `producer_retained_backing` reported eight passed, one failed and one ignored. The same `cancelled_channel_admission_releases_only_the_unenqueued_reservation` identity failed in both profiles.
* `init_producer_id_v6` reported 12 passed and one ignored.

The job summaries do not print individual ignored identities. The exact fixed source files, independently checked against `fixed-inputs.json` and retained under `source-pins/`, each declare one ignored native test: `native_retained_backing_peer` and `native_ordinary_transaction_history`, respectively. These are source-bound identifications, not a retained ignored-list output from the hosted artifact.

The complete job log records the names and exit summaries but not the actual panic messages or failure-phase diagnostics. The GitHub reports artifact 13207777434 is reported as 15,508,957 bytes with SHA-256 `4ae1e7bf190fa904831b5a757eb4de1f263db7eb5721163f0f5650f6cb8d2ffa`; the connector returned a File Service reference while the available downloader accepts Sediment uploads only. Its bytes, hash and member contents were not independently verified. Do not infer the cause of the failures or the missing diagnostics.

The run's separate `main-comparison-b1` job is reported failed in GitHub metadata; its log was not reviewed for this receipt. No local Cargo, compiler, native tests, benchmark, or publication was run.
