# Protocol inventory correction proposal

Read-only review of main `f0ca05da9c04942b211e2f56ddc615b9860cdea2`; reviewed files match exact Git objects (`reviewed-source.json`). No repository writes, Cargo, JVM or network commands were used. The local comparison script initially attempted to print a nonexistent `exit_code` result key; that reporting error was corrected to inspect `summary.exit_code`. No protocol outcome was inferred from that failed print.

## Verified source

Each official cached full source archive matches the existing broker api-matrix archive pin. All 186 request/response schema objects plus ApiKeys.java per release match that matrix's SHA256s (561 objects total). There are exactly 93 reserved API keys, 0–92, per release. Reserved removed APIs 4–7 have `validVersions: none` and must not become advertised operations. `verified-inventory-objects.json` records the verification; `official-audit.json` records the stale table differences. Eight relevant actual client jar classes per release were checked against the pinned client jar SHA256 and separately hashed without executing classes (`class-provenance.json`).

| Release | Actual source commit | API27 | API88/89 | API90 | API91/92 |
| --- | --- | --- | --- | --- | --- |
| 4.1.2 | c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c | 1 | 0, unstable | 0 | 0 |
| 4.2.1 | 18d5ecd939c8d510fdd72d0abb1f7099659dcd58 | 1–2 | 0, stable | 0–1 | 0 |
| 4.3.1 | 26b251a451ce941d3d7a55e6487bcb7f16b5ad48 | 1–2 | 0, stable | 0–1 | 0 |

Paths in these pinned archives:

- `clients/src/main/resources/common/message/{WriteTxnMarkers,StreamsGroupHeartbeat,StreamsGroupDescribe,DescribeShareGroupOffsets,AlterShareGroupOffsets,DeleteShareGroupOffsets}{Request,Response}.json`.
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`.
- `clients/src/main/java/org/apache/kafka/clients/admin/{Admin,KafkaAdminClient}.java`, `internals/{AbortTransactionHandler,DescribeStreamsGroupsHandler}.java`.

API27v2 adds signed INT8 TransactionVersion after CoordinatorEpoch, default 0, ignorable before v2 (KIP-1228). Response shape stays unchanged. API90v1 adds signed INT64 Lag after LeaderEpoch, default -1 and ignorable before v1 (KIP-1226); negative values indicate absent lag. API88/89 are broker listener, clusterAction=false. The three pinned Streams schemas have the same reachable wire layout; 4.1.2 carries latestVersionUnstable=true, one unused common structure later disappears, and the heartbeat response Status constructor default annotation changes. These metadata differences should not be misreported as different v0 wire layouts.

## Concrete owned correction scope

1. `scripts/check-protocol-coverage.py`: make the current-three-release inventory derive from the already pinned `tests/conformance/broker/api-matrix.json` release inventories, checking its 93-key set and retaining removed/reserved and unstable metadata. Preserve legacy 3.9.1/4.1.0 historical rows only as separately sourced historical contracts. `corrected-inventory-proposal.json` contains every exact current version range, including null for removed APIs, plus the unchanged old historical table for comparison. Do not merely append five constants to the old table.
2. Correct CLIENT_SPOKEN_VERSIONS[27] to 0–2 and [90] to 0–1, backed by the existing typed codec and real Admin negotiation; [91]/[92] remain v0. Do not put 88/89 in the implemented list until codecs **and a callable client operation** have been qualified. Classify them as actual missing runtime APIs and retain them in the required full-Kafka denominator.
3. Stop treating clusterAction as an exclusion from all public client operations. Official Admin.java exposes abortTransaction and AbortTransactionHandler explicitly uses PartitionLeaderStrategy and WriteTxnMarkers. API27 therefore has both internal transaction-marker use and a public force-abort operation. Remove its wholesale excluded classification. API57 UpdateFeatures similarly has a genuine public Admin operation; its existing blanket exclusion also needs correction. Internal APIs such as AllocateProducerIds may retain internal protocol classification even when a Rust low-level wrapper exists; the wrapper alone does not establish a supported public Apache client operation.
4. `tests/conformance/cases.json`: replace `api-broker-internal-027-write-txn-markers`'s blanket client-inapplicable reason with separate public force-abort and internal-use applicability. Add required public Streams heartbeat/describe cases and explicit current-version cells. New applicability is not a new live qualification: bind each qualified cell to its actual existing/future evidence and source, keep not_run where no result exists, and retain old archived results at their original source pins.
5. `tests/conformance/features.json`: change public abort notes to v0–2/default TransactionVersion0 and share-offset notes to v0–1 with the typed lag API and unchanged legacy projection. Add missing protocol/client features for Streams88/89 beside the separate out-of-scope Java Streams framework runtime. Framework exclusion must not erase these public broker APIs. Existing GroupType parses `Streams` as Unknown; an explicit Streams type/public wrapper remains a separate compatibility change.
6. The checker currently enumerates only its hardcoded inventory and labels membership in CLIENT_SPOKEN_VERSIONS as a runtime operation without checking the feature entrypoint or its actual negotiation. Its current result is PASS, 88 inventory entries, 66 implemented operations, zero unclassified drift despite omitted five current keys. Cross-check every spoken claim with a declared codec range and actual runtime binding; compare current runtime coverage by operation, not constants. Validate every pinned inventory key even when its internal use is excluded, so internal version drift is not silently skipped. Keep a deterministic classification report distinct from a full-functionality completion gate: classified missing work cannot satisfy a full-Kafka goal.

## Real current functionality gaps

The following current source caps were read directly, not guessed from the stale table. They are implementable follow-up work; none is a new implementation claim.

| API | Official missing version(s) | Current source | New behavior to implement |
| --- | --- | --- | --- |
| 88/89 | v0 in all three releases | No core constants, codec module or runtime wrapper | Streams topology/member/task heartbeat and typed Streams group describe; not consumer/share group aliases |
| 8/9 | v10 in all three | Admin/consumer/group caps through9 | Topic UUID offset commit/fetch mapping (KIP-848 evolution), preserve topic identity and actual response mapping |
| 22 | v6 in all three | Producer/Admin cap5 | Enable2Pc and KeepPreparedTxn request semantics, two-phase transaction lifecycle rather than merely writing default bits |
| 45 | v1 in all three | Admin reassign_version cap0 | AllowReplicationFactorChange with explicit caller option |
| 66 | v2 in all three | Admin list_transactions_version cap1 | Nullable TransactionalIdPattern and corresponding public option |
| 80 | v1 in4.2.1/4.3.1 | Public add_raft_voter cap0 | AckWhenCommitted option and its actual completion semantics |
| 24 | v4/v5 | Existing known cap3 | Batched coordinator transaction enrollment; already classified, still missing functionality |

API27v2 and API90v1 are already implemented in current source and must not be reopened as missing codecs. FindCoordinator6 and ListOffsets11 are also implemented; their official ranges need correction (FindCoordinator6 exists in all three, ListOffsets11 already in4.2.1). JoinGroup validVersions is0–9, not2–9: broker negotiation can still reject0/1 separately via its advertised minimum. ShareGroupHeartbeat/Describe official validVersions is1 in the current three releases, despite legacy client v0 helpers. Correct schema ranges without claiming the legacy client helpers are wrong.

For the full **broker** objective, the broker registry already correctly records API27 and88–92 handlers as missing/not_run; client codecs do not fill those server gaps. Existing read/write/log, transport, SASL, and fixed-voter foundations are bounded subsets. Full group/Streams coordinators and assignment/reconciliation, transactional coordinator + markers/control history/LSO, share state/delivery/ack coordination and offset management, and required KRaft/controller/replication/admin behaviors remain real server work. Removed reserved4–7 are inventory entries, not demanded live handlers. Core client-only internal exclusions cannot be copied to the broker objective.

The feature matrix also contains stale non-protocol notes for completed Avro/JSON companion work and group scheduling; these need their owners' source/evidence reconciliation, not a blanket present rewrite in this inventory patch. Core zstd, Kerberos, and OAuth refresh need current source-specific review before being called full-functionality blockers; no inference was made from their stale feature rows alone.

## Minimal verification for a correction

No full Rust rebuild is needed for a Python/JSON-only correction. Parse/validate the registry schema, rerun coverage checker/self-tests, and add meaningful fail-closed Python cases: removing any of93 keys fails; API27v2/API90v1 drift fails; public abort cannot be classified wholly out-of-scope; const-only Streams cannot become runtime implemented; unstable4.1.2 Streams is distinct from stable4.2.1/4.3.1; removed4–7 cannot be advertised. Do not rewrite frozen historical result artifacts to the new source or convert not_run into passed.
