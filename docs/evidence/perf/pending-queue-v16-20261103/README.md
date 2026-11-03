# Pending queue benchmark for v16

This inactive [prototype](prototype.tar.gz) rebinds the paired queue benchmark
to the v16 source and its full same-job correctness gate. Its 19 cases,
warmups, balanced run order and record-identity checks retain the previous
benchmark's settings. The [manifest](manifest.json), [bundle receipt](bundle-receipt.json)
and [independent review](independent-review.json) record the exact inputs.

The source and static controls passed, but the Rust harness and workflow have
not been compiled or run. V16's first hosted correctness run failed; the
benchmark remains disabled until its full correctness gate passes in the same
job. Synthetic completions do not prove
Kafka delivery; no speedup or performance ranking has been measured.
