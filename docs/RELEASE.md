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
stop after a successful `cargo package` dry-run. Owner one-shot checklist:
`bash scripts/owner-unblock.sh`.

### Preferred: GitHub Actions tag publish

One-time setup:

1. Create a crates.io API token (publish-update for `partitionline`).
2. Add repository secret `CARGO_REGISTRY_TOKEN` (Settings → Secrets → Actions).
3. Ensure CHANGELOG has a dated `0.1.0` (or next) section and README is ready
   to show the crates.io dependency line after the run.

Then (on `main`, after civilization is merged):

```bash
# Final version only — must match Cargo.toml exactly (e.g. 0.1.0 → v0.1.0).
# Do not use v0.1.0-rc.1 here: RC tags are git install pins and do not
# trigger a publish job (tag glob is final vX.Y.Z; job also refuses '-'/'+').
git tag v0.1.0
git push origin v0.1.0
```

Workflow: `.github/workflows/release.yml` (fmt / clippy / test / package / publish).
The tag filter is GitHub's workflow glob (`+` = one-or-more of the preceding
character), not regex. The Release-notes step must keep shell strings indented
under `run: |` (a flush-left multiline string is invalid YAML and used to create
empty-job `release` failures on branch pushes). A job-level `if` also skips
non-tag evaluations.

### Manual publish

From a **clean `main`** checkout with `CARGO_REGISTRY_TOKEN` exported:

```bash
bash scripts/ci-publish-ready.sh
bash scripts/owner-publish.sh
bash scripts/day1-after-publish.sh    # verifies crates.io + flips README
# or: bash scripts/check-installable.sh  # Installable bar probe only
git tag "v$(sed -n 's/^version = \"\(.*\)\"/\1/p' Cargo.toml | head -1)"
git push origin "v$(sed -n 's/^version = \"\(.*\)\"/\1/p' Cargo.toml | head -1)"
```

Or run the individual `cargo fmt` / `clippy` / `test` / `package` / `publish`
steps yourself. Prefer the tag → Actions path when runners are healthy.

After publish:

1. Tag `vX.Y.Z` matching `Cargo.toml` (if not already tagged by Actions).
2. Move CHANGELOG `[Unreleased]` into `[X.Y.Z]` if anything remains.
3. Run `bash scripts/day1-after-publish.sh` (crates.io confirm + README flip)
   and commit the README if needed.

## Honesty

Do not claim Suite HOLD / signed bench wins in release notes without the
process in [`STATUS.md`](STATUS.md) and [`benchmark.md`](benchmark.md).
