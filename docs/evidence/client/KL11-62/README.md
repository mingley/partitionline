# Integrated client lint correction

Strict all-target, all-feature Clippy passes both current Rust and Rust 1.85 on
the exact pushed `7ee1a405fed66a6bc2f600ffd5485eb7fe5a01bb` source. Existing
metrics tests pass 10 cases per toolchain. Credential-redaction tests pass 28
with defaults and 29 with tracing per toolchain. The same frozen matrix passes
1,801 default and 1,802 all-feature client tests per toolchain, with four existing
ignored tests in each cell; these ignored cases remain qualification limits.

Changes preserve behavior: throttle comparison uses an explicit ordering match,
feature-conditional credential test variables are named or guarded correctly,
the history helper writes hexadecimal without temporary per-byte strings, and a
redundant test-only lint expectation is removed. The producer Drop lifetime and
protocol-oracle hexadecimal corrections are coordinated under KL05-11 ownership.

`results.json` references the exact final commands and verified log hashes from
KL05-11; these checks are reused rather than rerun after a documentation-only
closure. `development` retains the earlier failed checks. The original scratch
`lint-qa` directory contains mixed contexts because a runner overwrote its
command report and stable lint log: credential/metrics logs are from `6f04`,
the stable lint failure is from `0f382`, and the first MSRV history-helper failure
is retained. This mixed developmental directory is not final-source evidence.
`lint-qa-3b1d0af` records the corrected helper passing stable and the remaining
MSRV oracle helper failures subsequently corrected under KL05-11.

These are warning and behavior-regression checks, with no performance or full
production qualification claim.
