# Orchestrator tier checks

KL09-68 is in progress. The orchestrator now refuses a null-broker cell in a
Kafka manifest, client-ceiling result labels in Kafka cells, and client-ceiling
artifacts returned by a Kafka peer. Null-broker orchestration remains disabled
until its fixture lifecycle and adapters are qualified.

A real process test reproduced the previous error: two explicitly synthetic
Kafka-shaped results were accepted in a cell declaring a null-broker target and
client-ceiling result kind. The copied process outputs and receipts are under
`study/before-mixing-processes-01/`. They are test fixtures, with no broker or
performance evidence.

Twenty-four process tests pass, including retained failures, mismatched settings,
crash, timeout and resume under the franz-go peer ID. These use fake adapters.
A separately compiled, pinned franz-go 1.22.0 driver actually ran `emit-config`
and `scenarios` on Go 1.26.0. Its config omits queue capacities, delivery/flush/run/
consume timeouts, payload/key modes, connection count and Nagle configuration.
The real output and explicit refusal are retained; missing settings are not
filled from another client or guessed. No native Go comparison is qualified.

The remaining work is to supply truthful, applied driver settings and owned
null-broker lifecycle handling, then rehearse the tier with genuine result
artifacts. Source and executable hashes, build logs and failed preparation
outputs are retained. Verify this archive with `python3 -B verify-evidence.py`
and `sha256sum -c SHA256SUMS` from this directory.
