# Hosted producer flush controls, run 41674604362

[The workflow run](https://github.com/mingley/partitionline/actions/runs/41674604362) completed successfully on attempt 1. Its green conclusion means that eight expected assertion failures were reproduced; it does not mean the defects are fixed. The trigger commit was `86f285c706645e68146737a243707953c524b89d`. The tested client source was pinned separately to `45cbff24de76490594970fc962cd04e7be9fafb1` with the test-only patch in the retained artifact.

Both `cargo test --locked --lib --no-run` builds passed, once with default features and once with all features. Each of the following controls then ran alone in both profiles, selected with `--exact`, and failed at its specified assertion with exit code 101:

- An accepted record expired in the pending queue, but flush returned `Ok(())` instead of reporting the delivery timeout.
- Dropping a waiting flush consumed the only failure report, so the following flush did not report it.
- A flush returned successfully while an unpolled retired worker still owned an accepted record.
- A retry guard failed an accepted record, but flush returned `Ok(())` instead of the timeout.

These are observations from local socket and directly constructed worker/guard fixtures. They establish neither real-broker behavior nor correctness of any repair, later commit, or release. They do not cover throughput or the full test suite. The pinned base's documentation said `try_send` delivery was observed on `flush`; a later documentation change does not repair the tested behavior.

[Job 126051745072](https://github.com/mingley/partitionline/actions/runs/41674604362/job/126051745072) ran on Linux x86_64 with stable Rust 1.99.1 and LLVM 23.1.1. All ten retained compile/test cleanup receipts report a waited leader and no remaining descendants. Cleanup receipts for metadata and patch-application commands were not retained, so the archive does not independently establish their cleanup. The lowest of 87 disk samples was 91,159,412,736 bytes free; polling cannot rule out a lower value between samples.

The retained [GitHub artifact](https://github.com/mingley/partitionline/actions/runs/41674604362/artifacts/13249627527) has SHA-256 `e51a183af3728e19d39a54e3c3e669b2d684939afb176daf9e65854d52eeb32a`, matching the connector's artifact metadata and `artifact.zip`. The patch bytes match the workflow's trigger commit; independently applying them to the pinned base produced the recorded postimage SHA-256 `226ca81821663a3d67488dc6086607bde0722ae5bf72a163e8524457fb8ffa50`. `source-pins.json` contains exact Git blob and SHA-256 pins for the workflow, test runner, patch, helper, report and source.

`analysis.json` contains the eight exact test names, expected assertion texts and individual log hashes. `run.json`, `jobs.json` and `artifacts.json` are GitHub connector snapshots; `artifact.zip` is unmodified and includes the original logs, patch and result. Run `sha256sum -c SHA256SUMS` for package integrity, and `python3 analyze.py > /tmp/flush-review-analysis.json` to reproduce the offline analysis. `SHA256SUMS` is an integrity list, not an independent signature. This review did not rerun the Rust tests.
