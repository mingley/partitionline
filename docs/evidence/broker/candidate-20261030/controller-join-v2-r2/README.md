# Broker controller-join candidate: inventory pin repair

This isolated candidate evaluates the reviewed controller shutdown change against the pinned `6fa8cdb3588d884580d01331e44df657c02f7670` broker source archive with the reviewed runtime-capacity patch applied. The earlier hosted r1 run reported two inventory gate failures. Its compact report is unavailable here. On a locally reconstructed r1 source tree, the inventory failed because the registry still pinned the pre-fix controller source hash after the join patch changed that source.

`join-registry.patch` updates only `controller_source_sha256` in `tests/conformance/broker/implemented-api-versions.json`: from the archived controller source hash to the exact reviewed join-fixed source hash. It does not change the protocol implementation, the published registry in the checkout, the source inventory checker, or its controls. The original capacity and join patches and their independent reviews remain byte-identical. The candidate verifies the old registry binding, applies the join and registry patches as separate stages, verifies the complete source inventory and allowed modes after each stage, and checks the final binding before running the existing gates.

The local inventory failed before this update with `controller_source: profile source checksum mismatch` and passed after it. The unchanged 55-control Python suite is **pending** at this source freeze; the prior pre-fix local run was interrupted by its diagnostic monitor and is not a complete 55-control result. A separate validation receipt will record the completed repaired run. No r2 Cargo build, native test, real broker test or hosted qualification has run. The hosted r1 report cannot be inspected, so the local reproduction does not prove the exact hosted stderr.

The verified workflow-loader replay ran all 12 candidate static checks in an isolated Git checkout with conflicting `GIT_*` variables; they passed. The existing baseline witnesses, exact negative cases, timeouts, resource and disk checks, feature profiles, all-target minimum, Fetch repeats, formatting, strict Clippy, docs, doctests and inventory controls remain required. The new static control verifies the registry patch roundtrip and that the JSON changes in exactly one field.

## Import after review and local validation

1. Require the target checkout's `.github/workflows/candidates.yml` to have SHA-256 `09fc1b57f3e2dc21562fecb35354d0e14c5b2ae63e1567c904e6d82fd9671b00`. Verify every entry in `SHA256SUMS` from this directory.
2. Copy this directory to `docs/evidence/broker/candidate-20261030/controller-join-v2-r2` without changing the existing r1 capsule. Check and apply its `candidates-workflow.patch` from the checkout root. The patched workflow has SHA-256 `97101330984500aa0b0fe6ae0d45fa287def807378c1c92e685d47430974a641` and replaces the r1 matrix entry with r2. It preserves the v17 client entry and non-cancelling concurrency.
3. Review the separate completed local validation and independent review before dispatching hosted r2. This source-only candidate is not a production patch adoption or a qualification result.

The synthetic join controls do not independently establish real controller wake, WAL or transport behavior. This selected candidate does not establish general production readiness, completeness, or performance leadership.
