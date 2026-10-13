# Orchestrator tier checks

KL09-68 registers the null-broker tier in the orchestrator. A Kafka manifest
still refuses a null-broker cell id, a client-ceiling result kind, and a schema
2 or null-mode artifact. A null-broker manifest has no Kafka cluster id. It
runs the same owned create, peer, and delete lifecycle, accepts only schema 2
exploratory null results, and records `result_kind=client-ceiling` with
`performance_claims_valid=false`. The process rehearsal uses synthetic
adapters, including a peer id of franz-go. It is not an nb-serve measurement
and it is not a comparable franz-go campaign.

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

The real franz-go 1.22.0 driver still omits queue capacities, delivery, flush,
run and consume timeouts, payload and key modes, connection count, and Nagle
configuration. Those omissions stay an explicit refusal. Missing settings are
not filled from another client. The historical study archive remains. Verify
it with `python3 -B verify-evidence.py` and `sha256sum -c SHA256SUMS` from this
directory. This card does not lift Suite HOLD or support a fastest-client claim.
