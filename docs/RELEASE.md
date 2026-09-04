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

One-time setup (first crates.io cut):

1. Create a crates.io API token at https://crates.io/settings/tokens — for the
   **first** cut of a new crate select **`publish-new`** (and usually also
   **`publish-update`** for later versions). `publish-update` alone cannot
   create `partitionline` on crates.io. After 0.1.0 exists, prefer Trusted
   Publishing and keep only a short-lived or narrowly scoped token as backup.
2. Add repository secret `CARGO_REGISTRY_TOKEN` (Settings → Secrets → Actions).
   Probe without printing: `bash scripts/check-registry-token.sh` (exit 2 = missing,
   0 = crates.io accepted the token for publish-new auth via a structured
   empty-tarball PUT that cannot create a crate, 1 = rejected).
   `bash scripts/check-registry-token.sh --self-test` proves fake tokens fail.
   Required for the **first** publish — Trusted Publishing can only be
   configured after the crate exists on crates.io.
3. Ensure CHANGELOG has a dated `0.1.0` (or next) section and README is ready
   to show the crates.io dependency line after the run.

Before merging/tagging, run `bash scripts/check-merge-ready.sh` (add `FULL=1` for tip Verifiable proxy). Does not require a token.

Fastest first cut when `CARGO_REGISTRY_TOKEN` is already in the environment
(Cloud Agent or owner shell) and Actions runners are starved:

```bash
bash scripts/check-installable-preflight.sh   # expect READY_EXCEPT_TOKEN before cut
bash scripts/owner-finish-installable.sh
# DRY_RUN=1 to rehearse; PUBLISH_LOCAL=0 to tag → release.yml instead
# ALLOW_RED_MAIN=1 overrides a red main CI refuse (not recommended)
# REQUIRE_MAIN_CI defaults to 1 on real cuts (0 on DRY_RUN); set 0 to override
```

That fast-forwards `main` to the civilization tip, publishes locally, runs
day1, and proves Installable. Before cut it probes main CI via
`scripts/check-main-ci.sh` so a known-red or still-running Verifiable tip
cannot silently ship (real cuts require terminal green unless overridden).

If the token is only in **GitHub Actions** secrets (not Cloud Agent):

1. Merge/FF civilization tip → `main` first — GitHub only lists
   `workflow_dispatch` workflows from the default branch, so
   `first-publish.yml` is not runnable until it exists on `main`.
2. Cancel stuck queued runs (`bash scripts/owner-cancel-stuck-runs.sh`).
3. Actions → **First publish** → `confirm=publish` (optional `ref`, default
   `main`), or `bash scripts/owner-dispatch-first-publish.sh`.

Prefer `bash scripts/owner-finish-installable.sh` when the token is already
in-env — it FF-merges, publishes locally, and does not wait on runners.

To FF tip → `main` without cutting: `CONFIRM=1 bash scripts/owner-sync-main.sh`.
That refuses while main HEAD CI is still running (protects in-flight Verifiable)
unless `ALLOW_BUSY_MAIN=1`. While crates.io `0.1.0` is still absent, it also
refuses docs/scripts-only tip→main unless `ALLOW_DOCS_THRASH=1` — leave tip
ahead and let `owner-finish-installable` FF once at cut time.

One-shot on clean `main` (tag → Actions): `bash scripts/owner-cut-release.sh`
(pushes `vX.Y.Z`, waits for crates.io, runs day1, then
`audit-civilization-bars`). `PUBLISH_LOCAL=1` uses `owner-publish` instead of
Actions; `DRY_RUN=1` prints actions only (allowed on the civilization tip for
rehearsal — still no tag/push); `REQUIRE_ACTIONS_SECRET=1` refuses to cut if
Actions lacks `CARGO_REGISTRY_TOKEN` (when `gh secret list` is permitted).

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

Auth: the job requests `id-token: write` and tries
[`rust-lang/crates-io-auth-action`](https://github.com/rust-lang/crates-io-auth-action)
(OIDC Trusted Publishing) first, then falls back to `CARGO_REGISTRY_TOKEN`.
See [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).

### After 0.1.0: prefer OIDC (no long-lived Actions token)

Probe anytime: `bash scripts/check-trusted-publishing-ready.sh`
Owner one-shot after first cut: `bash scripts/owner-enable-trusted-publishing.sh`
(`REQUIRE_INSTALLABLE=1` after crates.io cut).

1. crates.io → `partitionline` → Settings → Trusted Publishing → Add.
2. Platform: GitHub. Owner: `mingley`. Repository: `partitionline`.
   Workflow filename: `release.yml`.
3. Re-tag / next release: OIDC should mint a ~30-minute token; the secret
   becomes optional. Remove `CARGO_REGISTRY_TOKEN` from Actions secrets once
   a trusted-publishing publish has succeeded.

### Manual publish

From a **clean `main`** checkout with `CARGO_REGISTRY_TOKEN` exported:

```bash
bash scripts/ci-publish-ready.sh
bash scripts/owner-publish.sh
bash scripts/day1-after-publish.sh    # crates.io confirm + adopter consumer check + README flip
# or: bash scripts/check-installable.sh  # Installable bar probe only
#     bash scripts/verify-crates-io-consumer.sh  # adopter cargo-depend compile proof
git tag "v$(sed -n 's/^version = \"\(.*\)\"/\1/p' Cargo.toml | head -1)"
git push origin "v$(sed -n 's/^version = \"\(.*\)\"/\1/p' Cargo.toml | head -1)"
```

Or run the individual `cargo fmt` / `clippy` / `test` / `package` / `publish`
steps yourself. Prefer the tag → Actions path when runners are healthy.

After publish:

1. Tag `vX.Y.Z` matching `Cargo.toml` (if not already tagged by Actions).
2. Move CHANGELOG `[Unreleased]` into `[X.Y.Z]` if anything remains.
3. Run `bash scripts/day1-after-publish.sh` (crates.io confirm + adopter consumer compile + README flip)
   and commit the README if needed.

## Honesty

Do not claim Suite HOLD / signed bench wins in release notes without the
process in [`STATUS.md`](STATUS.md) and [`benchmark.md`](benchmark.md).
