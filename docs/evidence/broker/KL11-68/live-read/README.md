# Actual ordinary persisted read qualification

All four accepted runs pass against source
`58810f2ed90ac9643d59a60098a29f5a2f87a8d0`:

| Toolchain | Initial copied-journal run | Same-journal restart |
| --- | --- | --- |
| stable1.99.0 | `stable/attempt-4` | `stable/attempt-5-restart` |
| Rust1.85.0 | `1-85-0/attempt-1` | `1-85-0/attempt-2-restart` |

The source archive independently matches all 17,370 Git blobs. Both Rust
executables were copied from the sibling's exact full-source default build;
copied build/compiler/test and before/after source provenance is under
`prebuilt/`. The sibling separately qualified stable/MSRV default/all-feature
broker tests and strict gates. This runtime evidence claims the actual default
executables and their stored uncompressed histories.

`validation.json` verifies 66 actual Java/native executions, 79,590 assertions,
1,464 Fetch/ListOffsets golden outcomes (1,320 complete response comparisons and
144 clean structural EOFs), and 11,328 exact record-hash comparisons. The native
producer acknowledged 48 records across both lanes. All 22 allocated topic
UUIDs per lane and all catalog/partition journal hashes survive process restart.

Actual manual Apache consumers read their prior c34 acknowledged/noack histories.
All three SDKs read normalized rich codec histories through Fetch4–6 in both
ordinary isolation modes. Actual librdkafka2.15.0 produces/consumes magic2 and
reads each JavaSDK's histories; all three actual Java consumers also read the
native-produced records. Native all-topics metadata calls use the original
`rd_kafka_metadata(client,1,NULL)` path. There is no synthetic Fetch advertisement
or substitution of a named metadata query.

`api-versions.json` retains 60 actual API18 v0–4 request/response payloads:
stable/MSRV × three SDKs × seed/restart × five versions. It records exact header
versions and correlations and is independently decoded by the shared gate.
`native-receipts.json` converts actual native delivery/consume receipts to the
same canonical SHA256 record format and cross-checks them against actual Java
consumer histories.

Every accepted run retains commands, peer/jar/class/native hashes, stdout/debug
logs, client histories, and a complete bounded journal snapshot. Their JSON
contains only actual exit statuses; all four servers and all 66 peers exit0.
Original c34 Produce journals/receipts remain unchanged; each read lane began
with a byte-for-byte copy of them. The initial 1,074 oracle cases remain intact.

Development attempts1–3 and their source/configuration classifications are
retained. The first control expected direct-Partition epoch-1 whereas actual
Produce assigns epoch0 outside the protected CRC. The official Apache mutable
batch setter explicitly adapts the expected live metadata. The native probe's
Java-spelled wait setting and fetch/message-size constraint were corrected
before its first consumer request. No broker source change fixed these failures.

Ordinary local single-node durability is the qualification scope. No replication,
transactions, idempotent/control records, retention, group coordination or
incremental Fetch sessions are claimed. Read-committed LSO equals HW only because
this profile rejects transactional/control writes. Local acks-1 represents the
declared single-node ISR and fsync.

Run `python3 docs/evidence/broker/KL11-68/seal-read.py` to repeat the report,
correlation, receipt hash, cross-language, UUID and restart-byte consistency
checks. Runtime replay needs fresh numbered attempt directories, the pinned
official jars/native library, and a new source archive created with
`archive-read-source.py`; the exact commands are retained in each accepted run.
