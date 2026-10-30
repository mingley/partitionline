# Test Clippy cleanup candidate

`test-lint-cleanup.patch` targets the 100 library-test diagnostics and six TLS
integration-test diagnostics retained from main `02ce806d7` in
`/workspace/work/ci-main-02ce-20261030/job-124240489310.log`. The sparse
`baseline/` and `candidate/` directories contain only the 12 affected files.
`diagnostics.json` retains every diagnostic; source and patch hashes are in
`source-receipt.json` and `preparation-receipt.json`. `patches/` contains a small
patch per file for integration after the correctness candidates.

No repository file was edited. No Cargo, rustc or rustfmt command ran. The full
patch passes `git apply --check` against both the exact baseline snapshot and
the current repository as inspected during preparation. This is a static
candidate; formatting, Clippy and behavioral checks remain required.

Most changes replace unchecked test casts with checked conversions and useful
assertion messages, consume intentionally ignored results, use direct map
membership checks, or initialize test fixtures in one expression. The
xorshift data generator still emits precisely its low byte. The short shared
test-slot mutex uses `parking_lot`, which is already a dependency.

Two test futures now use owned `Box::pin` values. Their existing explicit drops
therefore cancel the futures, instead of dropping just a pinned reference.
Run `queued_byte_does_not_finish_the_fetch_quota_watch` and
`queued_byte_does_not_skip_the_fetch_close_quota` against the integrated source.

The scripted ready-IO fixtures intentionally spend time inside one synchronous
poll, which tests whether callers enforce their poll budget. Their three sleeps
now call one small `cfg(test)` helper that retains `std::thread::sleep` with a
narrow, explained `#[expect(clippy::disallowed_methods)]`. This explicit fixture
exception follows root review. It does not replace the wait with a different
blocking primitive or weaken the timing assertion. Stable Rust checks that the
expectation is fulfilled when that helper is compiled.

Root should apply the correctness patches first, then these minimal hunks,
format the changed files with stable Rust, and run the exact all-target Clippy
check from CI plus the affected tests. Passing a static patch check does not
establish test equivalence or production readiness.
