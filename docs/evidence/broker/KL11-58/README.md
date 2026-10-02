# KL11-58 independent broker CI

The workflow is frozen at
`39bd19a2f824a2c25b847eecfd3b3484aa5ca338`. It runs for main pushes,
pull requests and manual dispatch. Every Cargo command addresses the excluded
broker crate through its explicit manifest. Stable and Rust 1.85.0 execute
locked default/all-feature behavioral tests, format checking, strict Clippy,
strict Rustdoc and both default/all-feature doctest builds. The separate API job
verifies the retained Apache source pins and runs the inventory's negative
controls. Its verdict is uploaded on failure as well as success.

The complete local matrix passes at the frozen source: 42 default-feature and
57 all-feature test executions on each of stable 1.99.0 and Rust 1.85.0. Both
strict doctest builds pass with zero doctests currently present. Format,
denied-warning Clippy and Rustdoc pass on both toolchains.

The exact archived workflow is retained as `broker.yml`.
`workflow-contract.json` and `workflow-yaml-check.log` record local YAML and
structural validation. `source-hashes.json` binds the broker sources, lockfile,
TLS fixtures, verifier, negative tests and retained API inventory to the exact
source commit. The local snapshot was created with `git archive`; uncommitted
protocol changes from another worker were excluded.

The workflow worker independently ran both inventory shell commands on that
snapshot. `api-inventory/` retains their stdout/stderr, including 24 successful
mutation tests. `api-matrix-verification.json` records all three pinned Apache
releases with 93 complete API keys each. It makes no implementation claim.

`hosted-run.json` records an independently observed GitHub result, obtained
through the GitHub connector on 2026-10-02. Run
[37057864310](https://github.com/mingley/partitionline/actions/runs/37057864310)
completed successfully for the exact source SHA. The two Rust jobs and the API
inventory job, including every shell step, succeeded. Direct `gh` access was
forbidden; the connector GET succeeded. Local checks are not used to infer this
hosted conclusion. The retained observation includes its timestamp, run ID,
source SHA, job IDs and individual step statuses.

`preliminary/` preserves the workflow worker's first local execution and its
coordinator-requested interruption. All Rust 1.85.0 steps, the inventory checks,
and stable default tests completed before interruption during stable
all-feature compilation. The coordinator stopped duplicate builds to conserve
disk and reused the TLS worker's finalized exact-source matrix with explicit
attribution. An interrupted command has no successful verdict and is not
counted as completed validation.

`rust-matrix/` contains byte-for-byte copies of the finalized source, toolchain,
command and result records and full logs executed by `/root/zstd_decision` for
KL11-35. `rust-matrix-attribution.json` documents the reuse and environment
differences: explicit `cargo +TOOLCHAIN` selects the same compiler as the
workflow's `RUSTUP_TOOLCHAIN`; the worker used one build job to conserve shared
host resources. Source hashes match the commit, the archive remained unchanged,
and a package clean was recorded before each lane. `command-coverage.json`
maps all 18 workflow shell executions to these local results. The workflow
worker's independent OpenSSL prerequisite and API commands supply the remaining
four executions. `validate-evidence.py` verifies command equivalence, source
hashes, local verdicts and the separate hosted observation.

These checks validate the continuous gate and the implemented crate behavior
at one source revision. They do not qualify a production broker or establish
Kafka API support beyond the separate implementation evidence. Doctest builds
currently execute zero doctests; that fact is retained explicitly. The local
host is shared, and local Python/runtime versions are recorded separately from
the hosted setup action's requested version.
