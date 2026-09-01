# Status

As of 2026-09-01: no GitHub issues. Tracking for missing work is this
file and [gaps.md](gaps.md).

## Benches

| Item | State |
|---|---|
| Produce vs librdkafka 2.15.0 C | Lab A, signed in-tree (HW equals records sent) |
| Fetch vs C 2.15.0 | Lab A historical table only. This-VM 2026-08-28 is vs rust-rdkafka 0.39.0 and **unsigned** |
| Produce-ack latency vs C 2.15.0 | not run. This-VM 2026-08-28 vs rust-rdkafka 0.39.0 is **unsigned** and not a win (p50 62 µs vs 58 µs) |

Numbers and reproduce steps: [benchmark.md](benchmark.md).

## e2e

`tests/e2e.rs` is produce + classic JoinGroup + fetch against the
**in-tree mock broker**. CI does not start Kafka. A live broker on
`127.0.0.1:9092` can trip `admin_against_kafka_if_present`
(`AllocateProducerIds` advertised as unsupported on KRaft).

## Closed APIs

ElectLeaders (43), DescribeLogDirs v5, DescribeQuorum (55), raft voters
(80–82). Full list: [gaps.md](gaps.md).
