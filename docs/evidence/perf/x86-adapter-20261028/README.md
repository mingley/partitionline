# x86 benchmark adapter checks

The franz-go peer previously reported a per-batch record cap and one total
connection per broker even though the SDK does not enforce those settings. The
adapter now rejects explicit requests for them and omits them from its effective
configuration. The matrix refuses a peer with missing settings before creating
a topic. The peer also classifies successful `acks=0` promises as unknown
rather than acknowledged and can establish TLS without conflicting SDK dialer
options.

The Go checks used Go 1.26.0 from the image digest in `provenance.json`. The
initial attempt to fetch compression modules failed when the module proxy
redirected to a forbidden archive host. No signed redirect URL is retained.
The retry used `test.mod` to replace two dependencies with the source snapshots
already in this repository. Their file counts and tree hashes are recorded in
`provenance.json`; this run did not independently compare those copies with
upstream module zip checksums. The first retry failed because a transient edit
removed a required import; its log is retained. Later logs show passing checks,
including the final TLS and acknowledgment-accounting tests. The receipt
records final source hashes and distinguishes the earlier intermediate runs.

The retained Python log records a separate repeat of the matrix test suite. Its
exit status is in `matrix-python.exit`; the earlier run was seen in tool output
and was not retained as a raw file.

These are adapter and harness tests. They are not matched client performance
measurements and do not qualify a fastest-client or production-readiness claim.

Command: `python3 -B -m unittest discover -s benchmarks/tests -p test_matrix.py -v` (27 passed, exit 0).
