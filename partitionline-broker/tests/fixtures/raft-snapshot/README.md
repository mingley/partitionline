`apache-opaque-state.tsv` is generated from actual official Apache snapshot
writer/reader observations for Kafka 4.1.2, 4.2.1 and 4.3.1. The three releases and
two replays per release produce identical observations. Its SHA256 is
84da0988ffa8dde61751e3091df474fbbc33ad3523bb273c01aecefa7f2bf331.

The generator, original source/distribution pins, adapted Java sources, runtime
output and retained upstream LICENSE/NOTICE are in
`docs/evidence/broker/KL11-15/oracle/apache/`. The foundation source-stage run is
`development/attempt-03/validation.json`: 120 component assertions and three
controlled wrong-offset failures. Earlier setup failures are retained separately.

The Rust case checks exact opaque record order and the N/exclusive-offset versus
N-1/inclusive-offset and term/epoch mapping after custom image transfer/reopening.
It does not compare serialization formats or qualify Node installation/replay.
