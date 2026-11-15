# Packaged documentation link repair

On main commit `8570b01da6693acbcaa66120c861c5fd689628c4`, the
[client CI run](https://github.com/mingley/partitionline/actions/runs/42878680393)
failed the docs and package jobs because updated repository links used
`blob/main`, while packaged documentation requires immutable targets. Strict
rustdoc and the docs unit tests had passed before the packaged-docs check
stopped those jobs.

The repair pins the updated destinations to commit
`8570b01da6693acbcaa66120c861c5fd689628c4` and explains that development
links point to a snapshot. All seven updated targets were checked at that
commit.

The [targeted run](https://github.com/mingley/partitionline/actions/runs/42881362005)
on `3b5f13c255bfb71085a1c6e618c2a3e3e18195d4` used Rust 1.99.1 and passed
20 docs unit tests. It ran a clean `cargo package --locked --no-verify`,
extracted the crate, and checked 30 documents, 230 local links and 103 pinned
repository targets with `scripts/check-package-docs.py`. These checks do not
replace the full crate-consumer or release gate.
