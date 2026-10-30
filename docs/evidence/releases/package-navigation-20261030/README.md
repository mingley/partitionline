# Packaged documentation

The package-consumer CI job at `02ce806d7` stopped at a README link to the
excluded broker directory. Other documentation linked to excluded plans,
scripts and evidence. Those targets now use immutable repository URLs;
packaged pages keep local links. Every replacement target and Markdown anchor
was checked against that source revision.

The checker also missed wrapped link labels and omitted the packaged
changelog. New controls exposed both omissions before the checker fixes.
The failed baselines are retained here.

Validation on the integrated dirty source:

- All 20 documentation tests passed, including two real rustdoc compilation
  controls and the package exclusion/navigation controls.
- `cargo +stable package --locked --allow-dirty --no-verify` succeeded.
- The actual extracted crate passed navigation, license and exclusion checks:
  30 documents, 225 local links and 51 distinct pinned repository targets.
- The selected documentation, checker, test, manifest and lockfile inputs
  remained unchanged during those commands.

The package check used stable Rust 1.99.1, one Cargo job and a continuously
sampled 1.5 GiB disk floor. Commands, exits, source hashes, package hash and
sampled minimum space are in [actual-summary.json](actual-summary.json) and
the individual receipts. The package was not compiled, and its external
consumer and publication checks did not run. It is a development archive with
version 0.1.0, which is already published and must not be uploaded again.

The CI archive listing now consumes the whole tar stream while displaying
its first 80 lines, avoiding the previous broken-pipe diagnostic.
