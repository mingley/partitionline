# Static client-surface reconciliation for KL01-14

The audit reads immutable partitionline source `d3dfffb2737ec90c14f8c48b8106de92c917df12` and the original, SHA-verified Apache 4.1.2/4.2.1/4.3.1 source archives. It changes only this WORK directory. It executes no SDK, JVM, broker, Cargo, product runtime or live OIDC cohort. Full protocol qualification remains **false**.

The five named latest-version families are genuine gaps: OffsetCommit/OffsetFetch10 use topic UUIDs in both directions; InitProducerId6 adds two flags and a second identity pair; AlterPartitionReassignments1 adds AllowReplicationFactorChange; ListTransactions2 adds TransactionalIdPattern; AddRaftVoter1 adds AckWhenCommitted. AddRaftVoter1 is absent from4.1.2, and its Java public options expose no false acknowledgement knob.

ListTransactions has a separate existing routing bug: Rust uses FindCoordinator with an empty transaction ID and returns one coordinator's listing. All three Java implementations discover all brokers, expose per-broker results, concatenate listings without deduplication, and make complete results fail on a broker error. The bounded routing card precedes the pattern card.

All three genuine broker coordinators explicitly return UNSUPPORTED_VERSION for KeepPreparedTxn. Java public Producer has no prepareTransaction method; flag setters are absent from the actual TransactionManager request construction, and4.1 has a prepared-resume TODO. The proposed policy card requires concrete caller behavior, negotiation and actual unsupported outcomes; neither source internals nor serializers qualify successful prepared resume.

API24 versions0–3 are the genuine client factory's form. Versions4–5 are the broker factory's batched verification form, mapped to existing KL11-24/25. No new public Producer4/5 gap is proposed. API27,55,57 retain public applicability despite cluster-action metadata. APIs67/73 are broker traffic with nonstandard Rust raw Admin helpers, not genuine public Java Admin equivalents.

Current-valid Metadata0, FindCoordinator0 and JoinGroup0/1 remain explicit gaps below Rust's declared minimum versions. No existing bounded legacy compatibility card was found. Two additional bounded proposals implement representable discovery/classic group behavior and preserve typed refusals for intent that an old body cannot carry. A narrower declared minimum cannot close the full-protocol denominator.

Nine proposal objects in `proposed-bounded-cards.json` include exact write sets, dependencies, finite resource/deadline/state rules, independent peer requirements and old-negotiation cases. They have **no assigned task IDs** and are not existing taskbook claims. Root coordinates all shared Admin/source ownership and taskbook integration.

`all-93-reconciliation.json` maps every key0–92 to actual taskbook ownership or explicit removed/internal applicability. Four removed keys4–7 do not get invented implementation cards. Existing client Streams cards31/32/33 and server KL11-32 remain explicit; no KL11-88/89 card exists at this audit pin. The mapping includes broker74/75/76 with their exact audited statuses and no transferred completion claim.

`latest-75-status-reference.json` separately verifies that root's later7b108f84 taskbook changes only KL11-75 to done; the other306 task objects are identical. It transfers no source/runtime qualification to the earlier audit. The ordinary compaction proof does not automatically close the broader KL11-11 or full broker scope.

Artifact map:

- `input-identities.json`:18 immutable Git input identities and3 original archive pins.
- `all-93-schema-identities.json`:558 actual request/response schema identities, including the ListClientMetricsResources / ListConfigResources schema-name alias.
- `selected-schema-fields.json`:42 named request/response field trees from those original sources.
- `upstream-source-evidence.json`:source hashes, original paths/modes and bounded line snippets for all three actual SDK/server implementations.
- `rust-source-evidence.json`:pinned current codec/runtime snippets.
- `all-93-reconciliation.json`:93 applicability/task/version rows; declarations are distinguished from execution.
- `proposed-bounded-cards.json`:nine finite, unassigned proposals.
- `validation.json`:static relationship/field/source rechecks; full_protocol_complete=false.
- `packet-manifest.json` and `SHA256SUMS`:exact public artifact byte/mode/hash seal, under4MiB.

Static preparation initially guessed Metadata helper filenames before locating the actual `src/protocol/api.rs`, used the ApiKeys public74 name instead of the genuine ListConfigResources schema filename, and matched uncast factory constants before correcting to the literal `(short)` source. These were audit-tool/source-navigation failures, not product runtime results. The final field/identity checks preserve the genuine aliases/casts and execute no product behavior.
