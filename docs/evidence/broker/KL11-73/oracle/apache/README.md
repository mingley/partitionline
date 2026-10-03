This lane executes the official Apache Kafka 4.1.2, 4.2.1 and 4.3.1 `LeaderState`, `FollowerState` and local metadata-log components. It supplies component evidence for KL11-73. It does not run partitionline replication, an Apache network quorum, or a Kafka wire exchange.

`pins.json` fixes peeled release commits, original source archives and official distribution SHA256/SHA512 hashes. The runner recomputes all hashes, checks the original Apache checksum files and embedded client commit properties, extracts only named official dependency jars, and retains a deterministic licensed source subset with per-file hashes. `basis.json` names the upstream behavioral tests and the explicit setup/API adaptations.

Run from a checkout containing this directory, using fresh mutable work and output directories:

```sh
python3 docs/evidence/broker/KL11-73/oracle/apache/prepare-and-run.py \
  --work /workspace/work/raft-quorum-oracle/fresh-run \
  --output /workspace/work/raft-quorum-oracle/fresh-results \
  --source-archives /workspace/work/broker-api \
  --distributions /workspace/work/broker-wire/releases
```

The JDK compiler module strict-compiles the probe with `-Xlint:all -Werror`. Each release executes 55 assertions twice against fresh local log directories, with identical same-release and cross-release semantic histories. Three deliberate wrong-barrier expectations must produce the named failed assertion and nonzero process exit. The latest development run records 330 positive assertions and three detected negative executions. Exact generated adapter sources, class/jar/source hashes, commands, stdout and stderr are retained.

The assertions cover current-epoch commit barriers, strict majority medians for 1/3/5 voters, duplicate/observer/regressing positions, follower watermark monotonicity, rejection of truncation below the high-watermark, permitted suffix truncation at it, epoch divergence, and exact record offsets/values/timestamps after synced reopen. Scala `KafkaMetadataLog` in 4.1/4.2 and renamed Java `KafkaRaftLog` in 4.3 require declared factory, read-signature and record-package adaptations.

An intentionally invalid component-input history shows bare `LeaderState` can advance beyond the local durable end when its caller supplies two over-end replica positions. This demonstrates its caller-validation boundary; it is not an executed `KafkaRaftClient` network history. KL11-73 must separately validate outstanding request/member/term/sequence/prefix correspondence. The reopened Apache local log resets this component high-watermark to 0; durable replicated commit needs the independent WAL/causal-history lane.

`development-results.json` classifies every retained setup failure and successful development run. Final qualification requires replay from the actual pushed probe source and a separate immutable-source receipt; development success alone does not qualify partitionline replication.
