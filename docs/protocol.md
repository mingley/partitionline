# Protocol

Versions this crate speaks. Rustdoc on `partitionline::protocol` has the
field-level notes (Java `getErrorResponse`, throttle placement, tagged
fields).

Kafka 4.0 dropped some old versions (Produce v0–v2, Fetch v0–v3, …). This
crate still encodes a few of those where tests need them.

## Produce and fetch

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3–12 |
| Fetch | 1 | 4–17 |
| ListOffsets | 2 | 1–10 |
| Metadata | 3 | 1–13 |
| OffsetForLeaderEpoch | 23 | 0–4 |
| InitProducerId | 22 | 0–5 |
| AddPartitionsToTxn | 24 | 0–3 |
| AddOffsetsToTxn | 25 | 0–4 |
| EndTxn | 26 | 0–5 |
| WriteTxnMarkers | 27 | 0–1 |
| TxnOffsetCommit | 28 | 0–5 |

## Groups

| API | Key | Versions |
|---|---|---|
| OffsetCommit | 8 | 2–9 |
| OffsetFetch | 9 | 1–9 |
| FindCoordinator | 10 | 1–6 |
| JoinGroup | 11 | 2–9 |
| Heartbeat | 12 | 0–4 |
| LeaveGroup | 13 | 0–5 |
| SyncGroup | 14 | 0–5 |
| DescribeGroups | 15 | 0–6 |
| ListGroups | 16 | 0–5 |
| DeleteGroups | 42 | 0–2 |
| OffsetDelete | 47 | 0 |
| ConsumerGroupHeartbeat | 68 | 0–1 |
| ConsumerGroupDescribe | 69 | 0–1 |
| ShareGroupHeartbeat | 76 | 0–1 |
| ShareGroupDescribe | 77 | 0–1 |
| ShareFetch | 78 | 0–1 |
| ShareAcknowledge | 79 | 0–1 |
| DescribeShareGroupOffsets | 90 | 0 |
| AlterShareGroupOffsets | 91 | 0 |
| DeleteShareGroupOffsets | 92 | 0 |

## Admin and cluster

| API | Key | Versions |
|---|---|---|
| ApiVersions | 18 | 0–4 |
| CreateTopics | 19 | 0–7 |
| DeleteTopics | 20 | 0–6 |
| DeleteRecords | 21 | 0–2 |
| DescribeAcls / CreateAcls / DeleteAcls | 29–31 | 0–3 |
| DescribeConfigs | 32 | 0–4 |
| AlterConfigs | 33 | 0–2 |
| AlterReplicaLogDirs | 34 | 1–2 |
| DescribeLogDirs | 35 | 1–4 |
| CreatePartitions | 37 | 0–3 |
| Create / Renew / Expire / DescribeDelegationToken | 38–41 | 1–3 / 1–2 / 1–2 / 1–3 |
| IncrementalAlterConfigs | 44 | 0–1 |
| Alter / ListPartitionReassignments | 45–46 | 0 |
| Describe / AlterClientQuotas | 48–49 | 0–1 |
| Describe / AlterUserScramCredentials | 50–51 | 0 |
| UpdateFeatures | 57 | 0–2 |
| DescribeCluster | 60 | 0–2 |
| DescribeProducers | 61 | 0 |
| UnregisterBroker | 64 | 0 |
| Describe / ListTransactions | 65–66 | 0 / 0–1 |
| AllocateProducerIds | 67 | 0 |
| GetTelemetrySubscriptions / PushTelemetry | 71–72 | 0 |
| AssignReplicasToDirs | 73 | 0 |
| ListConfigResources | 74 | 0–1 |
| DescribeTopicPartitions | 75 | 0 |

## SASL

| API | Key | Versions |
|---|---|---|
| SaslHandshake | 17 | 0–1 |
| SaslAuthenticate | 36 | 0–2 |

## Wire gotchas

These bite when changing encode/decode:

- Request `ClientId` is always a classic nullable string, even on flexible
  headers.
- ApiVersions **response** header is never flexible. Parsing it as
  flexible eats the error code.
- Produce throttle time comes **after** the topic array. Metadata throttle
  time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the
  end.
- Record lengths are zigzag varints. Compact protocol lengths are
  unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`.
  Zero is a real id.
- `acks=0` means the broker sends no Produce response. Do not read one.
- Fetch v11 `RackId` is a non-nullable STRING. Kafka 3.9.1 rejects null;
  this crate writes an empty string when no rack is set.

APIs this crate does not speak: [gaps.md](gaps.md).
