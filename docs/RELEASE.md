# Release policy

partitionline stays on **0.x** until the API stability bar in
[`api-stability.md`](api-stability.md) is met. Semver for 0.x:

| Change | Version bump |
|---|---|
| Breaking change to a **Stable** public item | `0.MINOR` (treat minor as major while on 0.x) |
| Additive API, bugfix, docs, perf | `0.MINOR` or `0.PATCH` — prefer patch for fixes-only |
| **Evolving** / protocol-helper churn | patch allowed; document in CHANGELOG |

MSRV is declared in `Cargo.toml` (`rust-version`). Raising MSRV is a minor
bump on 0.x and must be called out in the CHANGELOG.

## Cadence

Cut a crates.io release when there is a user-facing batch (fix, feature, or
docs that change how operators depend on the crate), not on every protocol
helper map commit. Mock-only internal refactors can wait.

## Owner publish checklist (WP-0.5)

Requires a crates.io token owned by a crate owner. Agents without the token
stop after a successful `cargo package` dry-run.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo package
# review the .crate contents
cargo publish
```

After publish:

1. Tag `vX.Y.Z` matching `Cargo.toml`.
2. Move CHANGELOG `[Unreleased]` into `[X.Y.Z]`.
3. Point README install at crates.io (not git) if not already.

## Honesty

Do not claim Suite HOLD / signed bench wins in release notes without the
process in [`STATUS.md`](STATUS.md) and [`benchmark.md`](benchmark.md).
