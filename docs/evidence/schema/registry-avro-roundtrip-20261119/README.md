# Avro round trip with a live registry and Kafka

Stable Rust 1.100.0 built and ran the fixture at
`9ae9536c98f11be3c5f58a1ae519a6d4e5dd15ca` on Linux x86_64. It used Redpanda 26.2.2 with image digest
`a9dffb84f388a7a2d4ab1aaa541cd0b9a484f0f5c9b72cb54c551953352769ec`.
Both feature lanes passed the plaintext registry cache checks and trusted
HTTPS lookups. The all-feature lane ran the Kafka round trip.
[Live run](https://github.com/mingley/partitionline/actions/runs/44635876530)

The writer and reader independently resolve the event schema and its
named `Place` reference over HTTPS. The writer sends key `event-42` and
an Avro frame with schema ID 6. Produce with `acks=all` acknowledges
partition 0, offset 0. The reader checks the fetched key and frame byte
for byte, then decodes the value with the independently resolved schema.
The nested city is `Zürich`. Cleanup verifies removal of the owned
container, volumes and generated certificate files.
[Round-trip job](https://github.com/mingley/partitionline/actions/runs/44635876530/job/135954114769)

The fixture has a separate manifest and lock. Its lock SHA-256 is
`507c9fd9b5adcc69449cc8cdb9bd0d399171a7b89b94570d95ae7e3431a16b92`.
Rustfmt, strict Clippy, the isolated lock check, 20 no-default tests and
153 all-feature tests and doctests passed.
[Offline checks](https://github.com/mingley/partitionline/actions/runs/44635876449)

The first TLS startup failed due to a port collision. The initial
combined test timed out at 30 seconds after topic creation. The next
fixture run added stage receipts and explicit five-second client request
budgets and completed the round trip. The final source also applies
the formatter's line wrap.
[Port collision](https://github.com/mingley/partitionline/actions/runs/44633560008) ·
[Initial combined run](https://github.com/mingley/partitionline/actions/runs/44634323113) ·
[Diagnostic run](https://github.com/mingley/partitionline/actions/runs/44635129962)

The registry uses a generated CA and the Kafka listener uses plaintext.
Live credentials, Kafka TLS and other schema formats have their own
qualification work.
