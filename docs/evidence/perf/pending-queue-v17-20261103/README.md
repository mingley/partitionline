# Pending queue benchmark for v17

This [prototype](prototype.tar.gz) remains inactive until the complete v17
qualification succeeds in the same job. The Rust harness and workflow are
uncompiled and unrun. No benchmark result or speed claim is available.

The prototype retains the reviewed v16 benchmark's 19 cases, warmup, paired
run order, sample identity checks and timing. The fixed arm keeps the `v16`
label because the v17 fixed source catalog has identical bytes. The witness
also requires the exact 22 format, Clippy and full-target gates in both feature
profiles, alongside the existing baseline and control checks. It records
same-job workflow ordering; it is not an independent signature.

The [manifest](manifest.json), [bundle receipt](bundle-receipt.json) and
[independent review](independent-review.json) bind the inputs and source-only
checks. The review independently reran nine static Python controls. The
[finalizer](finalizer/finalize.py) and templates are included for inspection;
regenerated archives can differ because static test logs include elapsed time.
Run `sha256sum -c SHA256SUMS` in this directory to check the retained files.
