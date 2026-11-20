# Releases

Version `0.2.1` is prepared in `Cargo.toml`, `Cargo.lock`, and
[`CHANGELOG.md`](../CHANGELOG.md#021---2026-11-20) for cutting `v0.2.1` once
exact-SHA `main` CI passes.

Version 0.2.0 is published. The signed `v0.2.0` tag points to
`87eb1b824c97943207d3de36c6683402fe52d316`. CI passed on that commit and
the release workflow confirmed publication on 13 November 2026.
See the [release](https://github.com/mingley/partitionline/releases/tag/v0.2.0),
[CI run](https://github.com/mingley/partitionline/actions/runs/43431753659)
and [publish run](https://github.com/mingley/partitionline/actions/runs/43437873262).
An [external consumer check](https://github.com/mingley/partitionline/actions/runs/44410755330/job/135177924973)
compiled against the published 0.2.0 crate and verified its registry source
on Rust 1.100.0.

## Crates and versions

| Crate | Current publication state |
| --- | --- |
| `partitionline` | [0.2.0 on crates.io](https://crates.io/crates/partitionline/0.2.0); `0.2.1` prepared in manifest and changelog |
| `partitionline-broker` | Experimental, `publish = false` |
| `partitionline-schema` | Companion under development, `publish = false` |

Qualify each crate before publishing it. The client release workflow publishes
only the root crate; changing a companion's `publish` setting does not add it
to that workflow.

Use latest stable Rust for builds and validation. `rust-version` in each
manifest records the minimum compiler accepted by Cargo; it is currently `1.99`.
Tested broker and platform combinations are listed in [support](support.md).

While the API is 0.x, breaking changes to stable public items and increases to
`rust-version` require a minor release. Additions and fixes can use a patch
release. Changes to [protocol helpers](api-stability.md) may use a patch if
the changelog describes them. Release notes for `0.2.1` and upgrade notes for
`0.2.0` are in the [changelog](../CHANGELOG.md#021---2026-11-20).

## Prepare the candidate

1. Fix open release blockers and run default and all-feature tests, formatting,
   strict all-target Clippy, rustdoc, and package-consumer checks on the
   candidate. Retain failed and skipped cases. A focused passing test does not
   replace a failed full suite.
2. Ensure `Cargo.toml` and `Cargo.lock` match the target `X.Y.Z` version and
   that [`CHANGELOG.md`](../CHANGELOG.md) has a dated `## [X.Y.Z]` section
   documenting API migrations, feature defaults, and the compiler requirement.
   Verify installation snippets (`0.2` for the `0.2.x` series) and packaged
   documentation links (`scripts/check-package-docs.py` and
   `scripts/check-doc-examples.py`). Keep production and performance claims
   within recorded evidence.
3. Commit and push the candidate to `main`. Require successful `ci` for that
   exact commit, then validate the packaged crate with
   `scripts/ci-crate-consumer.sh` and `scripts/ci-publish-ready.sh`.
4. Rehearse interruption recovery without publication:

   ```sh
   bash scripts/rehearse-partial-release.sh --self-test
   bash scripts/rehearse-partial-release.sh --run target/release-rehearsal
   ```

The rehearsal uses local substitutes. Its success does not establish a real
registry upload, authentication, index confirmation or release-note creation.

## Publish

Use the canonical publisher from a clean `main` checkout:

```sh
DRY_RUN=1 bash scripts/owner-cut-release.sh
bash scripts/owner-cut-release.sh
```

A real cut requires exact-commit green CI, package-consumer evidence, a final
`X.Y.Z` version, and registry authentication. `DRY_RUN=1` rehearses actions;
it does not publish. The script uses local publication when a registry token
is available, or pushes the matching final tag for
[release.yml](https://github.com/mingley/partitionline/blob/9f35f0a9d171855b93c8f9cd5f629495ba1e70a8/.github/workflows/release.yml). Set `PUBLISH_LOCAL=0` to use
the workflow explicitly. The workflow supports crates.io Trusted Publishing
and a `CARGO_REGISTRY_TOKEN` fallback.

For manual dispatch, enter the existing tag in `vX.Y.Z` form. The workflow
checks that the tag points to the checked-out commit and matches the package
version. The GitHub release uses that tag and links to the registry, API docs
and changelog. The [rehearsal](https://github.com/mingley/partitionline/actions/runs/44411669796)
passed 31 offline scenarios and checked out the existing 0.2.0 tag by its
full ref. The later [release test run](https://github.com/mingley/partitionline/actions/runs/44414059916)
passed the full 122-test suite and the rehearsal after updating the test
count and workflow mutation for the new tag selection step. The
[intermediate CI run](https://github.com/mingley/partitionline/actions/runs/44412321206)
retains those two failures.

The CI gate checks every required main-push job and the conformance artifact for
the same run and attempt. A rerun of only failed jobs may omit previously
successful jobs or the current-attempt artifact. When that evidence is
incomplete, start a new CI workflow dispatch on main so the full workflow gets
a new run ID. Artifacts from older runs or attempts do not qualify.
GitHub run and artifact checks are separate API reads; a new run or rerun can
start while a release is being checked.

The workflow serializes publication with the `release-publish-lock`
concurrency group and does not cancel an upload in progress. Already-published
versions are skipped on re-entry. The obsolete `first-publish.yml` workflow is
retired. Do not bypass failed checks with `ALLOW_RED_MAIN=1` or a non-main
publication override.

## Confirm the release

Confirm that crates.io's API and sparse index expose the new version, compile
an external consumer against it, and verify the matching Git tag and GitHub
release notes. Update installation and migration examples to the version
actually published. `scripts/day1-after-publish.sh` performs these follow-up
checks and documentation updates; inspect its result before reporting success.

Keep retries idempotent: resume index confirmation or release-note creation
for an existing version without uploading a different archive under that
version. The [archived guide](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/archive/release-0.1.0.md) records the older
first-publication procedure and its evidence links.
