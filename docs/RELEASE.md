# Releases

`partitionline` 0.1.0 is already published on crates.io. Never recut that
version. Current source contains API changes and unresolved correctness
failures; it is not ready for the next release. The [status](STATUS.md) and
[next-work handoff](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/next-work.md) link to the recorded results.

## Crates and versions

| Crate | Current publication state |
| --- | --- |
| `partitionline` | 0.1.0 published; the next release needs a new version |
| `partitionline-broker` | Experimental, `publish = false` |
| `partitionline-schema` | Companion under development, `publish = false` |

Qualify each crate before publishing it. The client release workflow publishes
only the root crate; changing a companion's `publish` setting does not add it
to that workflow.

Use latest stable Rust for builds and validation. `rust-version` in each
manifest records the minimum compiler accepted by Cargo; it is currently 1.99.
Tested broker and platform combinations are listed in [support](support.md).

While the API is 0.x, a breaking change to a stable public item needs a minor
version bump. Additive changes and fixes can use a patch version. Changes to
[protocol helpers](api-stability.md) may use a patch if the changelog describes
them. Raising `rust-version` also needs a minor bump. The existing
`ConsumerConfig` field addition and compiler requirement mean the next client
release must be at least 0.2.0; do not relabel them as a 0.1.x patch.

## Prepare the candidate

1. Fix the open correctness and release blockers. Run default and all-feature
   tests, formatting, strict all-target Clippy, rustdoc and package-consumer
   checks on the candidate. Retain failed and skipped cases. A focused passing
   test does not replace a failed full suite.
2. Update `Cargo.toml` and `Cargo.lock`, and move the relevant unreleased notes
   into a dated changelog section. Document API migrations and the compiler
   requirement. Keep production and performance claims within the evidence.
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
[release.yml](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/.github/workflows/release.yml). Set `PUBLISH_LOCAL=0` to use
the workflow explicitly. The workflow supports crates.io Trusted Publishing
and a `CARGO_REGISTRY_TOKEN` fallback.

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
