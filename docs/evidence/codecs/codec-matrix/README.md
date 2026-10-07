# Codec interoperability

KL05-05 passes a finite Java/C/Rust correctness profile across five Apache Kafka
releases. The current-release matrix has90 cells; the historical matrix has60.
Each codec/client/topic-policy cell writes nine records and the other two SDKs
verify every field and HW9. Retained topic segments verify actual recompression.

`native/` retains current Kafka4.1.2/4.2.1/4.3.1 commands, segment bytes, hashes
and closure. `historical/` retains the separate3.9.1/4.1.0 candidate.
`executed-source/` binds the current native executable; `source/` also snapshots
the codec candidate and final documentation/registry. Fixture outputs and three
actual Java negative controls are retained separately.

The first interrupted attempt is retained without a completed-matrix claim.
The former signed-arithmetic panic, compile/lint failures and filesystem test
setup failure remain in validation logs. The complete matrix passed after those
corrections. Subsequent KL02-11 backing probes deliberately expose another open
implementation gap; they were added after the full codec-candidate suites.

This profile uses RF1, plaintext and one partition. It supplies no compression
ratio, CPU, latency, fault-campaign or world-ranking result. See summary.json
for precise scope, counts, provenance and limitations.
