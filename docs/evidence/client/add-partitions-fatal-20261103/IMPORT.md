# AddPartitions mixed-fatal candidate

This source-only candidate is not adopted or qualified. It has not been
compiled or run, and no benchmark or production-readiness claim is made.
It does not check missing, duplicated, null or unrequested response identities.
Cancellation during structural decode, or before the reduction scan reaches a
later fatal partition, can still prevent recording a fence. See README.md for
all source findings, test scope and other limitations.

`evidence.tar.gz` contains the exact patch, source manifest, verifier and
pinned sparse preimage/postimage files. `verify-import.py` checks the archive
against the files in this directory and the manifest, without compiling or
executing Rust. The contained source verifier additionally requires the retained
`/workspace/partitionline` catalogs and source, `/workspace/work`, and `git`.
The source manifest's review status describes the earlier archive freeze; the
independent source-only review is included separately.
