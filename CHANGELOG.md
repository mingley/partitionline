# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

- `ShareAcknowledgementMode` implements Java `share.acknowledgement.mode`.
  Implicit accepts the previous poll on the next `poll` / `leave` / `close`.
  The client default stays **explicit** (Java defaults to implicit).
  Implicit ACCEPT on the next `poll` is piggybacked on ShareFetch
  (standalone ShareAcknowledge is still used for `commit` / `leave`).
  Incremental ShareFetch sends Java `ShareSessionHandler` forgotten
  partitions when the assignment shrinks or a topic id is replaced.
  `ShareGroup::subscribe` keeps the share session and heartbeats the
  new topic list instead of closing with FINAL epoch.
  `ShareGroup::accept` / `release` / `reject` are Java `acknowledge`
  (local until the next `poll`, `commit`, or `leave`; Renew stays an
  immediate ShareAcknowledge). Implicit mode refuses those calls
  (`Implicit acknowledgement of delivery is being used.`). Explicit mode
  refuses a later `poll` that would contact a broker until every
  previously delivered record is acknowledged
  (`All records must be acknowledged in explicit acknowledgement mode.`;
  draining an already-buffered ShareFetch is allowed).
  Implicit ACCEPT and queued explicit
  acknowledgements piggyback on the next ShareFetch.
  `ShareGroup::commit` / `commit_timeout` flush pending
  acknowledgements as `Result<()>`. `ShareGroup::commit_async` is Java
  `commitAsync` (queued standalone ShareAcknowledge on the next `poll` /
  `leave`). `ShareGroup::commit_sync` /
  `commit_sync_timeout` are Java `ShareConsumer.commitSync` /
  `commitSync(Duration)` (`Map<TopicIdPartition, Optional<KafkaException>>`).

- `ShareGroup::group_metadata` is Java `ShareConsumer.groupMetadata()`
  (`ConsumerGroupMetadata`; `generationId` is the share member epoch).

- `Error::unauthorized_topics` is Java
  `TopicAuthorizationException.unauthorizedTopics()` (non-empty only for
  `TOPIC_AUTHORIZATION_FAILED`; ShareFetch collect uses the partition
  topic, not ErrorMessage).

- `ShareGroup::leave` / `close` still complete after a metadata leader
  change: leftover acknowledgements whose acquisition node is no longer
  the Metadata leader finish as `NOT_LEADER_OR_FOLLOWER`, and ACCEPT is
  sent only for partitions still on that node (`commit()` stays
  first-error `Err`). When every leftover partition has moved, all of
  those acknowledgements complete `NOT_LEADER_OR_FOLLOWER` and close
  still proceeds. The acknowledgement commit callback fires once
  per partition with that partition's exception.

- ShareAcknowledge partition `NOT_LEADER_OR_FOLLOWER` /
  `FENCED_LEADER_EPOCH` / `UNKNOWN_TOPIC_OR_PARTITION` /
  `UNKNOWN_TOPIC_ID` / `INCONSISTENT_TOPIC_ID` complete without retry
  (`commit_sync` map; Java `testFatalErrorsAcknowledgementResponse` plus
  ownership-lost topic-id codes), including when `ErrorMessage` is
  empty or a non-empty broker `ErrorMessage` is preferred. Metadata is
  requested when that error has no usable CurrentLeader. A
  ShareAcknowledge partition `INCONSISTENT_TOPIC_ID` with a usable
  CurrentLeader does not update the next ShareFetch target (Java
  `updateLeaderInfoMap` omits that code). A ShareAcknowledge partition
  `INCONSISTENT_TOPIC_ID` with a usable CurrentLeader on one partition
  does not update the next ShareFetch target for a sibling whose error
  is none. A ShareAcknowledge partition `FENCED_LEADER_EPOCH` /
  `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` with a usable
  CurrentLeader plus NodeEndpoints updates the next ShareFetch target
  without a Metadata refresh (Java `updateLeaderInfoMap` / KIP-951).
  A ShareAcknowledge partition `FENCED_LEADER_EPOCH` with CurrentLeader
  `leaderId != -1` and `leaderEpoch == -1` does not update the next
  ShareFetch target and still requests Metadata (Java
  `updateLeaderInfoMap` requires both). A ShareAcknowledge partition
  `NOT_LEADER_OR_FOLLOWER` with CurrentLeader `leaderId != -1` and
  `leaderEpoch == -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `NOT_LEADER_OR_FOLLOWER` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_OR_PARTITION` with CurrentLeader `leaderId != -1` and
  `leaderEpoch == -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_ID` with CurrentLeader `leaderId != -1` and
  `leaderEpoch == -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_OR_PARTITION` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_ID` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` does not update the next ShareFetch target and
  still requests Metadata.
  A ShareAcknowledge partition `FENCED_LEADER_EPOCH` /
  `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` with a usable
  CurrentLeader on one partition updates that partition's next
  ShareFetch target and does not retarget a sibling whose error is
  none. A ShareAcknowledge partition `FENCED_LEADER_EPOCH` with
  CurrentLeader `leaderId != -1` and `leaderEpoch == -1` on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and still requests Metadata. A ShareAcknowledge
  partition `NOT_LEADER_OR_FOLLOWER` with CurrentLeader
  `leaderId != -1` and `leaderEpoch == -1` on one partition does not
  update the next ShareFetch target for a sibling whose error is none
  and still requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_OR_PARTITION` with CurrentLeader `leaderId != -1` and
  `leaderEpoch == -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareAcknowledge partition `UNKNOWN_TOPIC_ID`
  with CurrentLeader `leaderId != -1` and `leaderEpoch == -1` on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and still requests Metadata. A ShareAcknowledge
  partition `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId == -1`
  and `leaderEpoch != -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareAcknowledge partition
  `NOT_LEADER_OR_FOLLOWER` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareAcknowledge partition
  `UNKNOWN_TOPIC_OR_PARTITION` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareAcknowledge partition `UNKNOWN_TOPIC_ID`
  with CurrentLeader `leaderId == -1` and `leaderEpoch != -1` on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and still requests Metadata. A ShareAcknowledge
  partition `INVALID_RECORD_STATE` with a usable CurrentLeader does
  not update the next ShareFetch target
  (Java `updateLeaderInfoMap` omits that code). A ShareAcknowledge
  partition `INVALID_RECORD_STATE` with a usable CurrentLeader on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none. A successful
  ShareAcknowledge that omits a requested partition completes that
  partition with `INVALID_RECORD_STATE` (Java
  `testShareAcknowledgeInvalidResponse`). An omitted ShareAcknowledge
  partition does not fail a sibling partition that is present with
  error none.
  Unrequested ShareAcknowledge partitions, unknown topic ids, and
  known topic ids that were not in the request are skipped (Java
  `lookupTopicId` / `handleAcknowledgeErrorCode`). A non-empty
  ShareAcknowledge partition `ErrorMessage` is preferred over the
  crate fallback; an empty or missing `ErrorMessage` uses
  `ShareAcknowledge {topic}-{partition}`. A top-level ShareAcknowledge
  `ErrorMessage` is preferred the same way; empty uses
  `ShareAcknowledge`. A top-level ShareAcknowledge `UNKNOWN_TOPIC_ID`
  completes without retry and does not request Metadata, including
  when `ErrorMessage` is set or empty (Java
  `handleShareAcknowledgeSuccess` has no `metadata.requestUpdate`;
  unlike top-level ShareFetch `UNKNOWN_TOPIC_ID`). A top-level
  ShareAcknowledge `INCONSISTENT_TOPIC_ID` completes without retry and
  does not request Metadata, including when `ErrorMessage` is set or
  empty (Java `handleShareAcknowledgeSuccess` has no
  `metadata.requestUpdate`; a partition `INCONSISTENT_TOPIC_ID` still
  invalidates). A top-level ShareAcknowledge `FENCED_LEADER_EPOCH`
  completes without retry and does not request Metadata, including
  when `ErrorMessage` is set or empty (Java
  `handleShareAcknowledgeSuccess` has no `metadata.requestUpdate`; a
  partition `FENCED_LEADER_EPOCH` still invalidates when CurrentLeader
  is unused). A top-level ShareAcknowledge `NOT_LEADER_OR_FOLLOWER`
  completes without retry and does not request Metadata, including
  when `ErrorMessage` is set or empty (Java
  `handleShareAcknowledgeSuccess` has no `metadata.requestUpdate`; a
  partition `NOT_LEADER_OR_FOLLOWER` still invalidates when
  CurrentLeader is unused). A top-level ShareAcknowledge
  `UNKNOWN_TOPIC_OR_PARTITION` completes without retry and does not
  request Metadata, including when `ErrorMessage` is set or empty
  (Java `handleShareAcknowledgeSuccess` has no
  `metadata.requestUpdate`; a partition
  `UNKNOWN_TOPIC_OR_PARTITION` still invalidates when CurrentLeader is
  unused). A piggybacked ShareFetch
  `AcknowledgeErrorMessage` that is empty uses
  `ShareFetch acknowledgement {topic}-{partition}`. A piggybacked
  `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` /
  `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` /
  `INCONSISTENT_TOPIC_ID` still completes
  without retry, including when `AcknowledgeErrorMessage` is set or
  empty. A piggybacked `FENCED_LEADER_EPOCH` /
  `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION` /
  `UNKNOWN_TOPIC_ID` / `INCONSISTENT_TOPIC_ID` on one
  partition does
  not fail a sibling partition whose `AcknowledgeErrorCode` is none,
  including when `AcknowledgeErrorMessage` is set or empty.
  An omitted piggyback acknowledgement on one partition does not fail
  a sibling partition that is present with `AcknowledgeErrorCode` none.
  An unknown ShareFetch topic id on a piggyback response is skipped
  (Java `lookupTopicId`) and does not fail a requested partition whose
  `AcknowledgeErrorCode` is none. A known cluster topic id that was
  not in this piggyback ShareFetch is skipped the same way and does
  not deliver those records. A piggybacked `FENCED_LEADER_EPOCH` /
  `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION` /
  `UNKNOWN_TOPIC_ID` / `INCONSISTENT_TOPIC_ID`
  `AcknowledgeErrorCode` with a usable CurrentLeader does not update
  the next ShareFetch target (Java `handleShareFetchSuccess` applies
  CurrentLeader only for fetch `errorCode`). A piggybacked
  `INVALID_RECORD_STATE` `AcknowledgeErrorCode` with a usable
  CurrentLeader does not update the next ShareFetch target. A
  piggybacked `INVALID_RECORD_STATE` `AcknowledgeErrorCode` with a
  usable CurrentLeader on one partition does not update the next
  ShareFetch target for a sibling whose `AcknowledgeErrorCode` is
  none. A piggybacked `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` /
  `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` /
  `INCONSISTENT_TOPIC_ID`
  `AcknowledgeErrorCode` with a usable
  CurrentLeader on one partition does not update the next ShareFetch
  target for a sibling whose `AcknowledgeErrorCode` is none.
  ShareAcknowledge top-level `SHARE_SESSION_LIMIT_REACHED` completes
  without retry and advances the share session epoch (Java
  `handleResponse` `nextEpoch`), including when `ErrorMessage` is
  empty; `SHARE_SESSION_NOT_FOUND` /
  `INVALID_SHARE_SESSION_EPOCH` still reset, including when
  `ErrorMessage` is set or empty. A non-empty top-level `ErrorMessage` is
  preferred on that path; empty uses `ShareAcknowledge`. ShareFetch top-level
  `SHARE_SESSION_LIMIT_REACHED` / `INVALID_SHARE_SESSION_EPOCH` /
  `SHARE_SESSION_NOT_FOUND` still reset the session (Java
  `nextCloseExistingAttemptNew`) and complete piggybacked
  acknowledgements, including when `ErrorMessage` is empty (crate
  fallback `ShareFetch`).

- ShareFetch partition `INCONSISTENT_TOPIC_ID` / `UNKNOWN_TOPIC_OR_PARTITION`
  / `UNKNOWN_TOPIC_ID` / `REPLICA_NOT_AVAILABLE` / `OFFSET_NOT_AVAILABLE`
  / `KAFKA_STORAGE_ERROR` skip without throwing and request Metadata on
  the next poll; siblings still deliver (Java `handleInitializeErrors`).
  Top-level `UNKNOWN_TOPIC_ID` still fails that poll and still
  refreshes the topic id when `ErrorMessage` is set or empty (Java
  `testUnknownTopicIdError` / `handleShareFetchSuccess`). Top-level
  `INCONSISTENT_TOPIC_ID` still fails that poll without Metadata,
  including when `ErrorMessage` is set or empty (Java
  `handleShareFetchSuccess` only `requestUpdate`s for top-level
  `UNKNOWN_TOPIC_ID`). Top-level `FENCED_LEADER_EPOCH` still fails that
  poll without Metadata, including when `ErrorMessage` is set or empty
  (Java `handleShareFetchSuccess` only `requestUpdate`s for top-level
  `UNKNOWN_TOPIC_ID`; a partition `FENCED_LEADER_EPOCH` still skips
  and requests Metadata when CurrentLeader is unused). Top-level
  `UNKNOWN_LEADER_EPOCH` still fails that poll without Metadata,
  including when `ErrorMessage` is set or empty (Java
  `handleShareFetchSuccess` only `requestUpdate`s for top-level
  `UNKNOWN_TOPIC_ID`; a partition `UNKNOWN_LEADER_EPOCH` still skips
  without Metadata). Top-level `REPLICA_NOT_AVAILABLE` still fails that
  poll without Metadata, including when `ErrorMessage` is set or empty
  (Java `handleShareFetchSuccess` only `requestUpdate`s for top-level
  `UNKNOWN_TOPIC_ID`; a partition `REPLICA_NOT_AVAILABLE` still skips
  and requests Metadata). Top-level `OFFSET_NOT_AVAILABLE` still fails
  that poll without Metadata, including when `ErrorMessage` is set or
  empty (Java `handleShareFetchSuccess` only `requestUpdate`s for
  top-level `UNKNOWN_TOPIC_ID`; a partition `OFFSET_NOT_AVAILABLE`
  still skips and requests Metadata). Top-level `KAFKA_STORAGE_ERROR`
  still fails that poll without Metadata, including when
  `ErrorMessage` is set or empty (Java `handleShareFetchSuccess` only
  `requestUpdate`s for top-level `UNKNOWN_TOPIC_ID`; a partition
  `KAFKA_STORAGE_ERROR` still skips and requests Metadata). Top-level
  `UNKNOWN_SERVER_ERROR` still fails that poll without Metadata,
  including when `ErrorMessage` is set or empty (Java
  `handleShareFetchSuccess` only `requestUpdate`s for top-level
  `UNKNOWN_TOPIC_ID`; a partition `UNKNOWN_SERVER_ERROR` still skips
  without Metadata). A non-empty
  throwable partition or top-level ShareFetch `ErrorMessage` is
  preferred; empty uses `ShareFetch {topic}-{partition}` or
  `ShareFetch`. A sole partition
  `NOT_LEADER_OR_FOLLOWER` collects empty on that poll (Java
  `testFetchError`). `UNKNOWN_LEADER_EPOCH` / `UNKNOWN_SERVER_ERROR`
  still skip without Metadata, including when a partition
  `ErrorMessage` is present.   Sole `NOT_LEADER_OR_FOLLOWER` still
  collects empty and still requests Metadata when `ErrorMessage`
  is set. Partition `INCONSISTENT_TOPIC_ID` /
  `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` still skip and
  still request Metadata when `ErrorMessage` is set.
  `FENCED_LEADER_EPOCH` without CurrentLeader /
  `REPLICA_NOT_AVAILABLE` / `OFFSET_NOT_AVAILABLE` /
  `KAFKA_STORAGE_ERROR` still skip and still request Metadata when
  `ErrorMessage` is set.   A sibling `NOT_LEADER_OR_FOLLOWER` /
  `INCONSISTENT_TOPIC_ID` /
  `UNKNOWN_TOPIC_OR_PARTITION` / partition `UNKNOWN_TOPIC_ID` /
  `FENCED_LEADER_EPOCH` without CurrentLeader /
  `REPLICA_NOT_AVAILABLE` / `OFFSET_NOT_AVAILABLE` /
  `KAFKA_STORAGE_ERROR` with `ErrorMessage` still delivers the
  successful partition and still requests Metadata.   A sibling
  `UNKNOWN_LEADER_EPOCH` / `UNKNOWN_SERVER_ERROR` with `ErrorMessage`
  still delivers the successful partition and does not request
  Metadata.

- ShareFetch partition `FENCED_LEADER_EPOCH` without a usable
  CurrentLeader skips that partition, still delivers siblings, and
  requests Metadata (Java
  `testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation`).
  A usable CurrentLeader plus NodeEndpoints redirects the next
  ShareFetch without a Metadata refresh (Java
  `testWhenFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation`).
  A ShareFetch `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId != -1`
  and `leaderEpoch == -1` does not update the next ShareFetch target
  and still requests Metadata (Java `handleShareFetchSuccess` requires
  both). A ShareFetch `NOT_LEADER_OR_FOLLOWER` with CurrentLeader
  `leaderId != -1` and `leaderEpoch == -1` does not update the next
  ShareFetch target and still requests Metadata. A ShareFetch
  `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` does not update the next ShareFetch target and
  still requests Metadata. A ShareFetch `NOT_LEADER_OR_FOLLOWER` with
  CurrentLeader `leaderId == -1` and `leaderEpoch != -1` does not
  update the next ShareFetch target and still requests Metadata.
  A ShareFetch `FENCED_LEADER_EPOCH` with a usable CurrentLeader on
  one partition updates that partition's next ShareFetch target and
  does not retarget a sibling whose error is none. A ShareFetch
  `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId != -1` and
  `leaderEpoch == -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareFetch `NOT_LEADER_OR_FOLLOWER` with
  CurrentLeader `leaderId != -1` and `leaderEpoch == -1` on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and still requests Metadata. A ShareFetch
  `FENCED_LEADER_EPOCH` with CurrentLeader `leaderId == -1` and
  `leaderEpoch != -1` on one partition does not update the next
  ShareFetch target for a sibling whose error is none and still
  requests Metadata. A ShareFetch `NOT_LEADER_OR_FOLLOWER` with
  CurrentLeader `leaderId == -1` and `leaderEpoch != -1` on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and still requests Metadata. A ShareFetch
  `NOT_LEADER_OR_FOLLOWER` with a usable CurrentLeader on one
  partition updates that partition's next ShareFetch target and does
  not retarget a sibling whose error is none.
  A ShareFetch partition `INCONSISTENT_TOPIC_ID` with a usable
  CurrentLeader does not update the next ShareFetch target (Java
  `handleShareFetchSuccess` applies CurrentLeader only for
  `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH`) and still
  requests Metadata. A ShareFetch partition `INCONSISTENT_TOPIC_ID`
  with a usable CurrentLeader on one partition does not update the
  next ShareFetch target for a sibling whose error is none. A
  ShareFetch partition `UNKNOWN_TOPIC_OR_PARTITION` with a usable
  CurrentLeader does not update the next ShareFetch target and still
  requests Metadata. A ShareFetch partition
  `UNKNOWN_TOPIC_OR_PARTITION` with a usable CurrentLeader on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none. A ShareFetch partition `UNKNOWN_TOPIC_ID` with
  a usable CurrentLeader does not update the next ShareFetch target
  and still requests Metadata. A ShareFetch partition `UNKNOWN_TOPIC_ID`
  with a usable CurrentLeader on one partition does not update the
  next ShareFetch target for a sibling whose error is none. A
  ShareFetch `REPLICA_NOT_AVAILABLE` with a usable CurrentLeader does
  not update the next ShareFetch target and still requests Metadata.
  A ShareFetch `REPLICA_NOT_AVAILABLE` with a usable CurrentLeader on
  one partition does not update the next ShareFetch target for a
  sibling whose error is none. A ShareFetch `OFFSET_NOT_AVAILABLE`
  with a usable CurrentLeader does not update the next ShareFetch
  target and still requests Metadata. A ShareFetch
  `OFFSET_NOT_AVAILABLE` with a usable CurrentLeader on one partition
  does not update the next ShareFetch target for a sibling whose
  error is none. A ShareFetch `KAFKA_STORAGE_ERROR` with a usable
  CurrentLeader does not update the next ShareFetch target and still
  requests Metadata. A ShareFetch `KAFKA_STORAGE_ERROR` with a usable
  CurrentLeader on one partition does not update the next ShareFetch
  target for a sibling whose error is none. A ShareFetch
  `UNKNOWN_LEADER_EPOCH` with a usable CurrentLeader does not update
  the next ShareFetch target and does not request Metadata. A
  ShareFetch `UNKNOWN_LEADER_EPOCH` with a usable CurrentLeader on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and does not request Metadata. A ShareFetch
  `UNKNOWN_SERVER_ERROR` with a usable CurrentLeader does not update
  the next ShareFetch target and does not request Metadata. A
  ShareFetch `UNKNOWN_SERVER_ERROR` with a usable CurrentLeader on one
  partition does not update the next ShareFetch target for a sibling
  whose error is none and does not request Metadata.

- `ShareGroup::set_acknowledgement_commit_callback` is Java
  `ShareConsumer.setAcknowledgementCommitCallback`
  (`AcknowledgementCommitCallback.onComplete`). `commit_async` /
  ShareAcknowledge completion invokes the callback once per
  topic-partition (Java
  `testAcknowledgementCommitCallbackMultiplePartitionCommitAsync`);
  mixed per-partition outcomes keep that partition's exception.

- `examples/share.rs` accepts a comma-separated `KAFKA_TOPIC` list via
  `ShareGroup::join_topics`. `tests/share_live.rs` is the live Kafka 4.1+
  join-then-produce / `group_metadata` / multi-topic / `join_matching` /
  release-then-reject / two-member same-partition / implicit-ack /
  `commit_sync` probe plus live Admin `describe_share_groups` /
  `list_share_group_offsets` (`RUN_LIVE=1`).
  Produce after a warmup
  ShareFetch so `share.auto.offset.reset=latest` does not skip the first
  record.

- Classic JoinGroup now speaks v0 and v1. v0 omits RebalanceTimeoutMs
  and fills it from session timeout; v1 matches v2 on the request and
  omits response ThrottleTimeMs. A present `group.instance.id` on a
  v0/v1 peer is refused before any JoinGroup frame. v2–v9 cooperative
  and static behavior is unchanged (KL05-42).

- Public AlterShareGroupOffsets and DeleteShareGroupOffsets v0 bodies
  (start-offset 7, `INVALID_REQUEST` 42, topic-name-only delete, and
  `GROUP_ID_NOT_FOUND` 69) are checked against pinned Apache
  kafka-clients 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and parsers.
  Kafka 3.9.1 stays eight unsupported receipts. Public Admin replay
  lands on FindCoordinator `key_type=0` and uses those Apache bodies.
  The original `not_run` rows stay frozen (KL01-29).

- Finite VerifiableConsumer reset and autocommit profiles (`latest`
  delivers only the new offset and stores the next offset, `none`
  without a stored offset errors, `none` with a stored offset resumes,
  and `--enable-autocommit` suppresses `offsets_committed` while close
  does not auto-commit the final poll) are checked against pinned Apache
  kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 Fetch v12,
  ListOffsets v5, OffsetCommit v8 and OffsetFetch v8 builders and
  parsers. Public Consumer replay uses those Apache bodies. The original
  `local_consistency` rows stay frozen (KL01-27).

- The finite VerifiableProducer option profile (six records, throughput
  5, value-prefix 42, repeating-keys 2, create-time 1650000000000) is
  checked against pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2,
  4.2.1 and 4.3.1 Produce v9 builders and parsers. Public Producer
  replay sends the hashed `getValue` / `getKey` mapping. The original
  `local_consistency` row stays frozen (KL01-26).

- Produce v9 and Fetch v12 `ThrottleTimeMs` histories (zero, 250 ms
  Produce quota, negative unmute refusal, 80 ms Fetch quota, and empty
  throttled full reset to INITIAL) are checked against pinned Apache
  kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Public Producer/Consumer replay honors the hashed
  `shouldClientThrottle` gates. The original `not_run` row stays
  frozen (KL01-25).

- Incremental Fetch v12 session histories (full INITIAL, empty
  incremental, forgotten partition, `FETCH_SESSION_ID_NOT_FOUND` reset,
  and `INVALID_FETCH_SESSION_EPOCH` close-existing-attempt-new) are
  checked against pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1
  and 4.3.1 builders and parsers. Public Consumer replay speaks INITIAL
  rather than LEGACY. The original `not_run` row stays frozen (KL01-24).

- Ordinary Produce v9, Fetch v12, ListOffsets v5 and Metadata v9 bodies
  for a 25-record `pl-ordinary` topic are checked against pinned Apache
  kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Apache-shaped verifiable events follow the hashed 3.9.1
  `timestamp`/`name` contract. The original `local_consistency` rows stay
  frozen; KIP-848 and share-group cells stay nonapplicable (KL01-23).

- The five delivered-position fetch-boundary histories (A01–A05) are
  replayed against pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2,
  4.2.1 and 4.3.1 Fetch v12 bodies. OffsetCommit/OffsetFetch v8 bodies
  leftover-empty decode. The original `audit-regression-a0*` rows stay
  failed historical pins (KL01-22).

- ListOffsets v1–v11 request and response bodies are checked against pinned
  Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Kafka 3.9.1 v10/v11 and 4.1.x v11 stay unsupported receipts.
  The original registered local-consistency rows are not rewritten
  (KL01-21).

- Metadata v1–v13 request and response bodies are checked against pinned
  Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Kafka 3.9.1 v13 stays an unsupported receipt. The original
  registered local-consistency rows are not rewritten (KL01-20).

- Fetch v4–v18 request and response bodies are checked against pinned
  Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Kafka 3.9.1 v18 stays an unsupported receipt. The original
  registered local-consistency rows are not rewritten (KL01-19).

- Produce v3–v13 request and response bodies are checked against pinned
  Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders and
  parsers. Kafka 3.9.1 v12/v13 stay unsupported receipts. The original
  registered local-consistency rows are not rewritten (KL01-18).

- A frozen group-protocol churn campaign records separate classic,
  cooperative, KIP-848 and share-group histories for member loss,
  coordinator loss, pause, max-poll expiry and SASL reconnect. Timer
  budgets stay distinct; Kafka 3.9.1 KIP-848/share cells stay explicitly
  unsupported (KL03-21).

- A frozen consume-transform-produce crash and fencing campaign records
  named crash boundaries, replacement-producer fencing, and explicit abort
  on native Kafka 4.1.0. Independent `read_committed` partitionline and
  Java consumers must agree, and transaction timeout stays distinct from
  leader recovery (KL03-20).

- A frozen 1,000-seed nontransactional data-plane fault campaign now drives
  the three-broker KRaft fixture (native Kafka 4.1.0), injects leader moves,
  lost acks, stale metadata, disconnects and restarts, and checks unique-ID
  produce/consume histories (KL03-19).

- Consumer, ConsumerGroup, ShareGroup, and Admin exclusive roundtrips
  reauthenticate when a broker SaslAuthenticate session lifetime is due
  (KIP-368). One shared `BrokerConn::roundtrip` path honors the stored
  mechanism, including the shared OIDC token manager. SaslAuthenticate v0
  peers and failed reauthentication fail closed without sending the
  application request or falling back to plaintext (KL06-12).

- Auth smoke now exercises short broker session lifetimes, IdP outage and
  recovery, idle reconnect, and a JWKS-validating OAUTHBEARER listener
  distinct from the unsecured-JWT test validator (KL06-06). Mid-connection
  reauthentication sends SaslHandshake before SaslAuthenticate so a live
  Kafka broker enters KIP-368 renewal instead of treating Authenticate as
  an application request.

- Development and CI use the latest stable Rust only. Package manifests now
  require Rust 1.99; Rust 1.85 is no longer supported.
- The schema companion checks a bounded Apache Avro Rust 0.22.0 codec against
  Java Avro 1.12.1 in both directions (KL05-43). The bridge rejects truncated
  strings that the pinned backend can otherwise resolve as null.

- Consumer negotiates Fetch18 per broker. KIP-1166 HighWatermark tag1 is
  supported through added replica sidecar helpers while ordinary consumers omit
  its default; existing `FetchPartition` and encoder/decoder signatures remain.
  Lower-version fallbacks, sessions and maintained delivery/commit regressions
  retain coverage. This codec adds no follower acknowledgment behavior (KL05-12).

- Producer supports Produce v13 using nonzero Metadata topic IDs and maps replies
  through the identity snapshot sent with each request. Stale IDs refresh and
  retry within the original delivery deadline; v3–v12 fallback remains available.
  TransactionV1 stays at v11 unless the initialization snapshot finalizes
  `transaction.version >=2` and supports EndTxn5. Independent Apache
  4.1.2/4.2.1/4.3.1 fixtures and live Java decoding cover the wire delta (KL05-11).

- `Consumer::fetch` no longer decompresses fetched batches it cannot keep. Each
  batch is framed and CRC-checked when its response arrives but decoded only
  when applied, so batches past `buffer_memory` stay compressed and are fetched
  again later instead of being inflated and dropped. Against a local broker
  with real gzip, 8 MiB per partition and the default 32 MiB budget, client CPU
  fell from 15.1 to 4.5 µs per record and peak RSS from 1.33 GB to 308 MB.
  Framing, CRC and codec errors still fail the whole fetch; errors found only
  while decompressing now surface when that batch is applied (KL10-16).

- Consumer uses broker-local incremental Fetch sessions on v7–v18, retaining
  full v4–v6 fallback, changed/forgotten partitions and independent epochs.
  Session errors retry within the original deadline, response identities and
  partition sets are validated before records are applied, and topic recreation
  clears stale buffered records. Close retires known sessions within one remaining
  budget; wakeup notifications no longer leak into a later fetch. Request-byte
  reduction is measured against Apache serialization; no throughput gain is claimed.

- Consumer honors Fetch v8–v18 broker quotas while delivering from eligible peers
  and buffered records. Quota waits retain poll/request budgets and accept wakeup
  and caller cancellation; cancelling a one-shot poll preserves later settings.
  `ConsumerMetrics` adds `throttle` (`metrics::ThrottleStats`); exhaustive struct
  literals must add the field or use `..Default::default()`.

- Producer honors positive broker quotas for Produce v6–v13 across connection
  slots, retaining original delivery and close budgets while other brokers and
  in-flight acknowledgements make progress. `ProducerMetrics` adds `throttle`
  (`metrics::ThrottleStats`); callers using exhaustive struct literals must add
  the field or use `..Default::default()`.

- KIP-848 heartbeat responses now encode and decode a nullable assignment with
  Apache's signed byte marker. This fixes closing a consumer and receiving an
  unchanged assignment from real brokers. Truncated responses missing the final
  tag count are rejected.

- ListOffsets v11 exposes KIP-1023 earliest pending upload with
  `EARLIEST_PENDING_UPLOAD_TIMESTAMP` and `OffsetSpec::earliest_pending_upload`.
  Each selected leader negotiates its own range; unsupported selectors/isolation
  return `Error::Unsupported`. Metadata, connection, capability lookup and retries
  now share the caller deadline; wire TimeoutMs uses the remaining budget. This
  adds one ApiVersions control RPC per attempt. No live tiered-storage claim.

### Changed

- gzip now uses zlib-rs by default through the new default `zlib-rs` feature.
  On the json1k gzip cells this gives up to 1.7× compress and up to 2×
  decompress throughput on arm64 and x86_64. Decoding incompressible gzip on
  arm64 is 11.5% slower. `default-features = false` keeps miniz_oxide (which
  forbids `unsafe`; only `crc32fast`'s CRC-32 kernel uses it). flate2 `runtime_detection` is now on, so generic
  x86_64 and aarch64 Linux builds use hardware CRC-32. Compressed bytes differ
  from earlier releases; the wire format and decode limits are unchanged. See
  [docs/gzip-backend.md](docs/gzip-backend.md).

- DescribeLogDirs now supports v5's `IsCordoned` state, with broker routing and
  v1–v4 fallback preserved. The evolving `DescribeLogDirsResult` type adds
  `is_cordoned`; struct-literal callers must initialize it. Its existing
  five-argument `new` constructor stays available and defaults to false;
  `with_cordoned` sets the new field. Older wire versions omit it and decode false.

- Scripts: tip Verifiable quiet soft-latency recovery — after `latency gate failed (soft)`, `ci-tip-verifiable-broker` sleeps and re-runs integrity (`TIP_VERIFIABLE_QUIET_RETRIES` default 1, `TIP_VERIFIABLE_QUIET_SLEEP_SECS` default 8; `0` disables). Only a clean recheck may restore `ok`; still-soft stays PARTIAL/exit 2. `--self-test` + bars gate quiet retry (no greenwash).
- Scripts: tip Verifiable soft-latency honesty — soft latency miss is PARTIAL even when integrity-smoke prints `ok`; handoff `--self-test` gates day1 README/ADOPTION preserve on `LAND_PARKS=1`.
- Scripts/docs: cut-release `PUBLISH_LOCAL` auto-defaults to 1 when `CARGO_REGISTRY_TOKEN` is in-env; CIVILIZATION agent priority leads with WP-0.5 token cut.
- Scripts: `check-installable-preflight` runs honesty self-tests (`check-registry-token --self-test` + `ci-tip-verifiable-broker --self-test`) before `READY_EXCEPT_TOKEN`, so the one-screen Installable gate cannot skip PARTIAL/token-normalize units; bars require preflight wiring.
- Scripts: `owner-finish-installable` runs honesty self-tests before the token gate (`check-registry-token --self-test` + `ci-tip-verifiable-broker --self-test`) so the cut path cannot skip PARTIAL/token-normalize units; bars require finish wiring.
- Scripts: tip Verifiable soft-skip honesty is now executable — `ci-tip-verifiable-broker --self-test` proves finalize `ok`/PARTIAL exit 2/soft PARTIAL exit 0; wired into `ci-branch-lite` / `check-cut-path`; bars run the self-test (not grep-only).
- Scripts: tip Verifiable `PARTIAL` now exits **2** by default (was 0) so `set -e` tip proxies (`ci-branch-lite` / `check-cut-path`) cannot greenwash mid-chain soft-skips; `TIP_VERIFIABLE_SOFT=1` keeps PARTIAL exit 0 for constrained sandboxes. Bars audit requires `exit 2`.
- Scripts: tip Verifiable soft-skip honesty — `ci-tip-verifiable-broker` prints `ok` only when broker+auth+integrity all pass; mid-chain soft-skips print `PARTIAL` (not evidence). Capable envs (Java/openssl/keytool/python3 + Kafka) auto-set `REQUIRE_BROKER=1` / `REQUIRE_AUTH=1` unless `TIP_VERIFIABLE_SOFT=1`. Bars audit gates the pattern.
- Scripts: Verifiable bar now requires tip live-broker scripts (`ci-tip-verifiable-broker`, integrity/latency, `ensure-broker`) wired into `ci-branch-lite` / `check-cut-path`; `ci-civilization-check` no longer stops the shared native broker after broker-smoke (avoids integrity Connection refused / soft-miss greenwash). Opt-in old stop: `STOP_NATIVE_AFTER_BROKER=1`.
- Scripts: tip Verifiable (`ci-branch-lite` / `check-cut-path`) now runs `ci-tip-verifiable-broker` (ensure-broker → broker-smoke kip848+share → auth → integrity/latency) with soft-skip honesty when tooling/broker is absent — tip Verifiable is no longer fmt/clippy/lib-only while Installable waits.
- Scripts: `owner-finish-installable` missing-token path prints `owner-request-registry-token` inline so the publish-new Secrets ask is unavoidable.
- Scripts: `check-installable-preflight` surfaces `owner-request-registry-token` on READY_EXCEPT_TOKEN so the one-screen publish-new ask is the default next step.
- Scripts/docs: `owner-request-registry-token.sh` one-screen Installable token ask (Secrets deep link + publish-new scope + finish path); wired from `owner-status` / `owner-unblock`. CIVILIZATION post-cut parks tip ancestor stamp → `bb2506c`.
- Docs: same-day native Verifiable recheck on tip `67072a5` (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈150µs after under-load ≈380µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts: `check-registry-token --self-test` covers whitespace-only TOKEN / TOKEN_FILE normalize units (tip Verifiable + cut-path) before the fake-token network probe.
- Scripts: `check-registry-token` / `owner-finish-installable` treat whitespace-only `CARGO_REGISTRY_TOKEN` as unset (and trim leading/trailing space) via `pl_normalize_cargo_registry_token` / `pl_prepare_cargo_registry_token`, so a blank Secrets paste cannot look "set" while Installable stays blocked.
- Scripts: `lab-a-common` `prepare_broker` falls back to `ensure-broker` (native Kafka) when Docker overlay fails in nested Cloud Agent VMs, so direct Lab A integrity/produce/fetch no longer soft-skip without a broker.
- Scripts: `ci-broker-smoke` `SKIP_DOCKER=1` path now uses `ensure-broker` to start native Kafka when 9092 is down (agent Verifiable re-entry after auth/integrity) instead of hard-failing.
- Scripts: shared `scripts/lib/ensure-broker.sh` starts native Kafka when 9092 is down; `ci-latency-gate` uses it before benching, and `ci-integrity-smoke` no longer stops a shared native broker on EXIT (so agent Verifiable chains integrity → latency without Connection refused).
- Scripts: `ci-broker-smoke` auto-starts native Kafka (`ci-native-kafka.sh`) when Docker overlay mounts fail in nested Cloud Agent VMs, so local Verifiable does not soft-skip the broker gate.
- Docs: same-day native Verifiable recheck on tip `3a1b00a` (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈147–161µs after under-load miss ≈1007µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts/docs: `check-registry-token` / `owner-finish-installable` load `CARGO_REGISTRY_TOKEN_FILE` into the current shell and WARN on common misnamed env vars (`CARGO_TOKEN`, `CRATES_IO_TOKEN`, …) so Secrets UI typos cannot silently block Installable; shared helper `scripts/lib/cargo-registry-token.sh`.
- Scripts/docs: Cursor env Secrets deep link defaults to `.../secrets` so login redirect lands on the Secrets tab for `CARGO_REGISTRY_TOKEN` injection.
- Scripts: `check-actions-hygiene` probes for the GitHub `dependencies` label Dependabot expects and prints the owner `gh label create` one-shot (agents 403); surfaces in `owner-unblock` / cut-path hygiene.
- Tip: merge `main` `first-publish.yml` **publish-new** honesty; `check-post-cut-parks-stack` now requires tip↔main workflow match (not tip-soft-only) so tip-delta stays docs/scripts-only after the main Actions alternate landed.
- Scripts/docs: shared `scripts/lib/cursor-env-secrets-url.sh` prints the Cloud Agent Environments → Secrets deep link (overridable via `PARTITIONLINE_CURSOR_ENV_SECRETS_URL`); wired into `owner-finish-installable` / `owner-unblock` / RELEASE so token injection is one click away.
- Scripts: `ci-branch-lite` + `check-cut-path` run `DRY_RUN=1` `owner-enable-trusted-publishing` so tip Verifiable and cut-path rehearse the post-Installable OIDC UI checklist (crate may be absent); Installable preflight READY_EXCEPT_TOKEN messaging spells publish-new + Cursor/Actions secret paths.
- Scripts: `check-cut-path` runs `DRY_RUN=1` `day1-after-publish` (README flip rehearsal) and `check-actions-hygiene` (stale queue surface); tip Verifiable (`ci-branch-lite`) also surfaces Actions hygiene before Installable preflight.
- Scripts: `ci-branch-lite` + `check-cut-path` run `DRY_RUN=1` `owner-dispatch-first-publish` so tip Verifiable and cut-path prove `first-publish.yml` stays workflow_dispatch-visible on main (Actions-secret alternate) before the token cut.
- Docs: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈220µs after under-load miss ≈837µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts/docs: owner-finish / owner-unblock / RELEASE spell out Cursor Environments → Secrets for `CARGO_REGISTRY_TOKEN`; tip Verifiable + cut-path gate `check-merge-ready`.
- Scripts: `ci-branch-lite` + `check-cut-path` run `check-crate-metadata`; tip Verifiable also runs `check-installable-preflight` so READY_EXCEPT_TOKEN stays tip-gated before the token cut.
- Scripts: `ci-branch-lite` runs `cargo publish --dry-run` + `check-trusted-publishing-ready` so tip Verifiable proves upload shape and OIDC release.yml shape before the token cut.
- Scripts: `ci-branch-lite` + `check-cut-path` run `PRE_PUBLISH=1` `audit-civilization-bars` so five civilization bars are tip-gated before the token cut (Installable credentials may remain BLOCKED).
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-crate-consumer` so the packed `.crate` tarball is proven dependable (Installable packaging) before the token cut, not only via publish-ready.
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-deny` so the Independent bar (no C Kafka/OpenSSL/zstd defaults) is tip-gated, not only in publish-ready.
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-msrv` so Installable MSRV is exercised (compile+lib tests on declared rust-version), not only declared in Cargo.toml.
- Scripts: `check-post-cut-parks-stack` requires tip `first-publish.yml` to match `main` while Installable is unmet (main may already document publish-new) + park publish-new — so tip-delta stays docs/scripts-only.
- Scripts: `ci-branch-lite` + `check-cut-path` run `MODE=path` `verify-crates-io-consumer` so the day1 adopter compile proof is rehearsed before crates.io `0.1.0` exists.
- Scripts: `ci-branch-lite` rehearses `refresh-post-cut-parks` DRY_RUN (tip→Verifiable→SCRAM→lz4→checkout) before the parks stack gate.
- Scripts: `check-cut-path` rehearses `refresh-post-cut-parks` DRY_RUN (tip→Verifiable→SCRAM→lz4→checkout) before parks stack + publish dry-run.
- Docs: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈207µs after under-load miss ≈1049µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts: `refresh-post-cut-parks.sh` refreshes parks in land order (tip→Verifiable→SCRAM→lz4→checkout) so parallel tip merges cannot fork the tip⊆… chain; wired into owner-status / owner-unblock.
- Scripts: `check-post-cut-parks-stack` also gates park chain (Verifiable⊆SCRAM⊆lz4⊆checkout) so parallel tip refreshes cannot fork CHANGELOG histories that only conflict at stacked land time.
- Scripts/docs: rebuild lz4 + actions/checkout post-cut parks stacked on SCRAM (SCRAM⊆lz4⊆checkout) so tip→Verifiable→SCRAM→lz4→checkout DRY_RUN stays merge-clean after tip-is-ancestor gate; tip stays docs/scripts-only.
- Scripts: `check-post-cut-parks-stack` gates tip-is-ancestor of every post-cut park (catches parks lagging tip after docs/scripts commits) before stacked DRY_RUN merge + `cargo test --lib`.
- CI: bump `actions/checkout` to v7 on parked `dev/actions-checkout-bump-b686` (covers Dependabot #92; land after Installable).
- Dependencies: `lz4_flex` 0.11→0.14. Parked off tip until after Installable (lockfile breaks docs-only tip-delta).
- Dependencies: flate2 1.1.9→1.1.10 (gzip write header/footer infinite-loop fix; miniz_oxide 0.8→0.9). Parked off tip until after Installable (lockfile breaks docs-only tip-delta).
- Dependencies: SCRAM stack hmac 0.12→0.13, pbkdf2 0.12→0.13, sha2 0.10→0.11 (KeyInit import). Parked off tip until after Installable; auth-smoke green.
- Scripts/docs: `owner-enable-trusted-publishing` post-Installable helper verifies release.yml OIDC shape and prints exact crates.io Trusted Publishing UI steps; wired into day1 / owner-status / RELEASE.
- Scripts: `check-cut-path` runs `cargo publish --dry-run` so Installable rehearsal proves package upload shape before `CARGO_REGISTRY_TOKEN` arrives.
- Scripts/docs: park `actions/checkout` v7 on `dev/actions-checkout-bump-b686` and append it to post-cut land order (after Verifiable + SCRAM + lz4); tip stays docs/scripts-only until Installable.
- Scripts/docs: park `lz4_flex` 0.11→0.14 on `dev/lz4-flex-bump-b686` and wire it into post-cut parks land order (after Verifiable + SCRAM); tip stays docs/scripts-only until Installable.
- Docs/scripts: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈126–203µs after under-load miss) recorded unsigned; owner-status + publish-ready surface post-cut parks stack and Trusted Publishing shape.
- Scripts: `owner-land-post-cut-parks.sh` lands Verifiable + flate2 + SCRAM crypto parks after Installable; `owner-finish-installable` chains it by default.
- Scripts: READY_EXCEPT_TOKEN / owner-status / owner-unblock note that `owner-finish-installable` chains parked Verifiable merge by default.
- Scripts: `owner-finish-installable` defaults to chaining `owner-merge-parked-verifiable` after Installable (`MERGE_PARKED_VERIFIABLE=0` skips).
- Scripts: `owner-merge-parked-verifiable.sh` lands post-Installable Actions auth+integrity + ConsumerGroupHeartbeat fuzz from parked branch.
- Docs/scripts: post-cut Verifiable handoff for parked `dev/verifiable-auth-integrity-fuzz-b686` (Actions auth+integrity jobs + ConsumerGroupHeartbeat fuzz); tip stays docs/scripts-only until Installable.
- Docs: same-day native Verifiable recheck (broker kip848+share, auth matrix,
  integrity COUNT=2000, latency gate p99≈71–86µs) recorded in STATUS /
  ADOPTION / CIVILIZATION — unsigned; not a Suite HOLD lift.
- Changed: `Error::Closed` now displays as `client closed` (was `producer closed`) on all paths, including consumer/admin/net; match on the variant, never the text.

### Fixed

- Scripts: post-cut parks DRY_RUN cleanup uses globals under `set -u` so conflict exits do not abort worktree teardown.
- Docs: CIVILIZATION live tip SHA + [#86](https://github.com/mingley/partitionline/issues/86) cut path aligned to publish-new + `owner-finish-installable` (issue body no longer steers to publish-update-only / owner-cut-release).
- Scripts: `owner-status` / `audit-civilization-bars` / `check-merge-ready` probe crates.io publish-new auth (not token presence alone) so publish-update-only or garbage tokens cannot greenwash Installable readiness; `owner-unblock` surfaces both post-cut parks and the token probe.
- Docs: CIVILIZATION tip SHA + post-cut parks list (Verifiable + SCRAM/flate2) aligned with live cut path and structured registry-token probe.
- Scripts: `check-registry-token` probes crates.io publish-new auth via PUT `/api/v1/crates/new` with a structured empty-tarball body (not `/api/v1/me`, which 403s publish-only tokens; not an empty body, which 400s before auth and false-accepts); `--self-test` proves fake tokens fail; wired into preflight, cut-path, finish, and `ci-branch-lite`.
- Docs/scripts: first-cut token guidance requires crates.io `publish-new` (not only `publish-update`) so a wrong-scoped token cannot silently block Installable.
- Scripts/docs: `check-trusted-publishing-ready` validates release.yml OIDC shape; ADOPTION warns not to merge Dependabot flate2/SCRAM bumps onto tip before Installable (covered by post-cut SCRAM park); Verifiable park refreshed onto tip.
- Scripts: finish DRY_RUN hard-fails dirty post-cut parks stacks (no `|| true` greenwash); `check-cut-path` + owner-unblock surface tip→Verifiable→SCRAM stack gate.
- Scripts: post-cut parks DRY_RUN uses a disposable worktree (no `checkout -f` on tip) so tip WIP is not discarded; tip Verifiable proxy hard-fails on dirty tip→Verifiable→SCRAM stacks (`check-post-cut-parks-stack`).
- Scripts: tip Verifiable proxy (`ci-branch-lite` / `ci-publish-ready`) hard-fails when parked post-cut branches no longer stack-clean onto tip (`check-post-cut-parks-stack`); lander DRY_RUN prefers local tip when ahead of origin and honors `REQUIRE_PARKS=1`.
- Scripts: `owner-land-post-cut-parks` DRY_RUN now performs real stacked merges (per-park merge-tree vs bare target hid tip→Verifiable→SCRAM CHANGELOG conflicts); SCRAM/flate2 park rebased onto tip for clean post-cut land.
- Soft-skip honesty: optional kip848/share broker soft-skips only on Unsupported*/truncated-Protocol signals; civilization-check fails unexpected auth/integrity errors instead of SKIP-greenwashing.
- CI broker-smoke: when `REQUIRE_KIP848=0` / `REQUIRE_SHARE=0` (Kafka 3.9 matrix), soft-skip kip848/share on truncated `Protocol` decode errors instead of hard-failing — KIP-848/share remain required on 4.x. Optional kip848 also stops retrying immediately on `Protocol(need N bytes)` so 3.9 matrix cells do not wait out six timeouts.
- CI: stop injecting partial `KAFKA_*` env on `apache/kafka:4.x` Docker (was breaking KRaft format with missing `process.roles`); enable share via post-ready `share.version=1` upgrade only.
- CI latency gate: honor `LATENCY_LIMIT_US` absolute ceiling; Actions sets `5000` for GHA+Docker noise (local relative baseline unchanged).

- `release.yml`: add complementary `ghost-noop` job so branch-push evaluations
  that skip `publish` stay green (avoids empty-job / all-skipped red X on tip).
- `scripts/owner-cut-release.sh`: `DRY_RUN=1` allowed on non-`main` (civilization
  tip rehearsal); still refuses real cuts off `main` without
  `ALLOW_NON_MAIN_PUBLISH=1`.

### Added

- Verifiable: Actions `auth-smoke` + `integrity-smoke` jobs (REQUIRE_AUTH/REQUIRE_INTEGRITY); ConsumerGroupHeartbeat (KIP-848) decode fuzz target + decode-smoke coverage; auth-smoke self-bootstraps Kafka binaries when missing.
- `scripts/check-installable-preflight.sh`: one-shot pre-publish probe that
  exits `0` with `READY_EXCEPT_TOKEN` when merge-ready + metadata + main CI
  are green and only `CARGO_REGISTRY_TOKEN` / crates.io cut remains (exit `3`
  if main CI is still running; exit `2` if already Installable).
- `scripts/check-main-ci.sh`: probe whether `origin/main` HEAD has terminal
  green CI (exit 0/1/2). Wired into `owner-finish-installable` step 2b —
  refuses Installable cut on red main unless `ALLOW_RED_MAIN=1`; real cuts
  default `REQUIRE_MAIN_CI=1` (inconclusive also refuses); `DRY_RUN=1` keeps
  soft warn unless `REQUIRE_MAIN_CI` is set explicitly.
- `.github/workflows/first-publish.yml`: workflow_dispatch first crates.io
  cut when `CARGO_REGISTRY_TOKEN` is an Actions secret (confirm=publish);
  documented in ADOPTION / RELEASE / owner-finish-installable as the
  Actions-only alternate to the in-env finish script.
- `scripts/owner-dispatch-first-publish.sh`: owner helper to
  `gh workflow run first-publish.yml` after tip is on `main` (dispatch is
  only listed from the default branch).
- `scripts/owner-sync-main.sh`: explicit tip→main FF with `CONFIRM=1` (avoids cancel-in-progress thrash on main CI while Installable waits). Refuses when main HEAD CI is still running unless `ALLOW_BUSY_MAIN=1`. Also refuses docs/scripts-only tip→main while crates.io cut is still absent unless `ALLOW_DOCS_THRASH=1` (leave tip ahead; `owner-finish-installable` FF's once at cut).

- `owner-finish-installable.sh`: `DRY_RUN=1` rehearses merge/cut without a
  token; `check-merge-ready` next steps detect when HEAD already matches
  `main` (skip FF; point at first-publish dispatch).
- `scripts/lib/adopter-consumer-main.sh`: shared operator-surface consumer
  used by `ci-crate-consumer` and `verify-crates-io-consumer` so packed-crate
  and crates.io proofs cannot drift.
- `scripts/verify-crates-io-consumer.sh`: prove an adopter can compile against
  partitionline — `MODE=registry` (default) after crates.io publish; `MODE=path`
  pre-publish rehearsal wired into `ci-publish-ready` so day1 cannot fail on
  API drift. Registry mode wired into `day1-after-publish` +
  `owner-finish-installable`.
- `scripts/owner-finish-installable.sh`: one-shot Installable finish once
  `CARGO_REGISTRY_TOKEN` is in-env — FF-merge civilization → `main`, local
  `cargo publish` (bypasses starved Actions), day1, prove Installable; after
  success, best-effort `gh secret set CARGO_REGISTRY_TOKEN` for later tags.
  `owner-status` / `ci-publish-ready` / bars audit point at this path first.
- `scripts/lib/crates-io.sh`: shared Installable probe (crates.io API + sparse
  index fallback; required User-Agent — CDN returns empty 403 without one).
  Wired into `check-installable`, `check-merge-ready`, and `owner-status`.
- `scripts/check-merge-ready.sh`: `git merge-tree --write-tree` conflict probe
  vs `origin/main` (flush-left `changed in both` fallback; avoids doc/script
  false positives); owner next steps prefer `owner-cut-release.sh`.
- `scripts/check-actions-hygiene.sh`: classifies stale queued Actions (RC-tag
  release zombies vs tip CI leftovers); wired into `owner-status` /
  `owner-unblock`. `owner-cancel-stuck-runs` labels the same classes.
- `scripts/ci-crate-consumer.sh`: packed-crate downstream check now links the
  operator surface (Producer/Consumer/ConsumerGroup/ShareGroup/Admin +
  SASL/TLS config), not only Producer.
- `scripts/ci-civilization-check.sh`: Installable section uses shared crates.io
  probe, day1 README `DRY_RUN` preflight, merge-ready gate, and actions hygiene;
  ends with `PRE_PUBLISH=1` civilization-bars audit.
- `scripts/audit-civilization-bars.sh`: evidence audit for the six CIVILIZATION
  success bars (PASS/PARTIAL/BLOCKED/FAIL); wired into `owner-status`.
  `PRE_PUBLISH=1` treats Installable BLOCKED as expected so publish-ready can
  gate on the other five bars before the first crates.io cut.
- `scripts/check-crate-metadata.sh`: crates.io package-shape gate (version,
  description identity, keywords/categories, license files, include allowlist,
  packed-crate omits scripts/fuzz); wired into merge-ready and publish-ready.
- `scripts/owner-cut-release.sh`: best-effort Actions `CARGO_REGISTRY_TOKEN`
  preflight (`REQUIRE_ACTIONS_SECRET=1` to hard-fail); runs
  `audit-civilization-bars` after a successful cut.

- `examples/kip848`: KIP-848 next-gen consumer group join (`ConsumerGroup::join_consumer_topics`).
  Broker smoke runs it on Kafka 4.x (`REQUIRE_KIP848=1` default). Join now sends an
  **empty** `TopicPartitions` array (null was rejected: "must be empty when (re-)joining").
  ConsumerGroupHeartbeat decode accepts truncated error bodies that omit trailing
  tagged fields (Kafka 4.x `INVALID_REQUEST` path).

- Auth smoke covers **OIDC client_credentials** (local `scripts/oidc-token-stub.py`)
  and **mTLS** (SSL listener with `ssl.client.auth=required`; `examples/tls`
  accepts `TLS_CLIENT_CERT_PEM` / `TLS_CLIENT_KEY_PEM`). X.509 v3 client certs
  required for rustls. Wired into `scripts/ci-auth-smoke.sh` / civilization-check.

- `scripts/ci-auth-smoke.sh`: native Kafka SASL_SSL + SCRAM-SHA-256/512 +
  OAUTHBEARER (unsecured JWT) smoke (private CA, isolated ports,
  `examples/sasl` / `examples/oauth` with `TLS_CA_PEM`); TLS-only produce must
  fail closed. Wired into `scripts/ci-civilization-check.sh`.
- `examples/sasl` and `examples/oauth` accept optional `TLS_CA_PEM` /
  `TLS_SERVER_NAME` for the production SASL_SSL path.
- `scripts/owner-status.sh` prints Installable/Verifiable blocker status
  (token, crates.io, Actions tip/main); wired into civilization-check and
  publish-ready as an informational footer.
- CI concurrency cancels superseded runs on `main` too, so a stuck queued
  `main` job cannot permanently block the next push after merge.
- `scripts/owner-cancel-stuck-runs.sh` cancels queued Actions runs older than
  15 minutes (`DRY_RUN=1` supported); wired from `owner-status` / ADOPTION for
  the org-wide runner starvation case (agents get 403 — owner must run it).
- `scripts/owner-unblock.sh` one-shot owner checklist: status probe, dry-run
  cancel targets, merge → tag `v0.1.0` → day1 path after token + runners.
- `scripts/lab-a-common.sh` shared broker helpers; `scripts/lab-a-integrity.sh`
  combined produce→HW→fetch integrity; `scripts/ci-integrity-smoke.sh` small-
  count local/CI smoke (+ unsigned latency) wired into civilization-check.
  `lab-a-fetch.sh` now also requires HW delta == acked.
- `tests/fuzz_decode_smoke.rs` also hammers group / share / txn response
  decoders (not only fetch/produce/metadata). `scripts/ci-branch-lite.sh` and
  civilization-check now **run** that smoke (tip Verifiable no longer
  `--lib`-only); optional short libFuzzer smoke when nightly+g++ are present.
- CI: `dev/**` tip pushes no longer auto-queue (org runner starvation /
  perpetual tip re-queue); full matrix on PR/`main`/`workflow_dispatch` only.
- Git install pin `v0.1.0-rc.6` (README / ADOPTION / migrate guide) for
  adopters before crates.io; does not trigger `release.yml` (final `vX.Y.Z`
  only). Advances `v0.1.0-rc.5` with `owner-publish` → `day1-after-publish`
  chaining and a refreshed native Kafka 4.1 broker smoke (kip848/share).
- `scripts/check-adopter-pin.sh` (wired into branch-lite / publish-ready /
  owner-status) so git pins cannot silently lag tip.
- `scripts/post-publish-readme.sh` `DRY_RUN=1` preflight (wired into
  `ci-publish-ready`) so day-1 README flip cannot silently break before
  crates.io exists. `owner-unblock` points at release issue #86 and the
  `v0.1.0-rc.6` interim git pin.

### Changed

- `scripts/owner-cut-release.sh` one-shot owner cut on clean `main` (tag → Actions publish → crates.io wait → day1; `PUBLISH_LOCAL=1` / `DRY_RUN=1`). Wired into `owner-unblock` and RELEASE.md.
- `ci-publish-ready` and `owner-status` also run `check-merge-ready` so the
  merge→tag path is visible in the same probes as Installable/Verifiable.
- Tip re-verified auth smoke (SASL_SSL PLAIN+SCRAM+OAUTHBEARER+OIDC + mTLS fail-closed) on 2026-09-04.
- Tip re-verified native Kafka 4.1 broker smoke (`ci-native-kafka` + kip848/share required) on 2026-09-04.
- `scripts/check-merge-ready.sh` (wired into `owner-unblock`, documented in
  RELEASE.md) gates civilization → main → `vX.Y.Z` without requiring a token:
  final version + CHANGELOG section, release.yml OIDC/secret auth, workflow
  YAML, adopter pin, and tip-vs-`main` ancestry (`FULL=1` adds branch-lite).
- `release.yml` prefers crates.io **Trusted Publishing** (OIDC via
  `rust-lang/crates-io-auth-action`, `id-token: write`) and falls back to
  `CARGO_REGISTRY_TOKEN` for the first publish. RELEASE.md / owner-unblock /
  day1 document the post-0.1.0 migration off long-lived Actions secrets.
- Tip Verifiable gates (`ci-branch-lite` / `ci-publish-ready`) run
  `scripts/check-workflows.sh` so invalid workflow YAML (the flush-left
  `run: |` class that empty-failed `release` on branch pushes) cannot regress
  unnoticed. `owner-status` prefers Actions runs for the exact HEAD SHA.
- `release.yml` hardens the crates.io publish trigger: fix invalid YAML in the
  GitHub Release notes step (flush-left multiline string caused empty-job
  `release` failures on branch pushes), input-safe concurrency / checkout
  expressions, job-level skip unless final tag or `workflow_dispatch`, and an
  explicit `X.Y.Z` tag shape check. Still publishes on `v0.1.0`.
- `scripts/post-publish-readme.sh` also inserts crates.io + docs.rs badges on
  day-1 README flip (`DRY_RUN=1` asserts badges). `release.yml` creates a
  GitHub Release after a successful crates.io publish.
- `scripts/check-adopter-pin.sh` allows docs/scripts tip drift without a new rc;
  library/`Cargo.toml` changes since the pin still fail the gate.

- `scripts/owner-publish.sh` runs `day1-after-publish` by default after a
  successful manual `cargo publish` (`RUN_DAY1_AFTER_PUBLISH=0` to skip).
- Migration guide git pin → `v0.1.0-rc.6`; guide documents KIP-848 empty
  `TopicPartitions` join + `examples/kip848`. Cargo.toml lists remaining
  examples (`share`, `cooperative`, `metrics`, `offsets`, `pause`, `wakeup`)
  so the packaged crate surface matches README.
- ADOPTION owner unblock: `main` Actions recovered; remaining cancel targets
  are stale tip/tag queues (not an eternal `main` queue).

- Mock TLS fixtures use the `openssl` CLI instead of `rcgen`, dropping
  `time` from the dependency graph and clearing the `RUSTSEC-2026-0009`
  ignore in `deny.toml` / `.cargo/audit.toml` without raising MSRV.
- Auth smoke covers PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER over SASL_SSL;
  CI `test` jobs assert `openssl version` before `cargo test`.
- ADOPTION / CIVILIZATION record local civilization-check evidence
  (broker + SASL_SSL PLAIN + SCRAM-256/512 + OAUTHBEARER) while GitHub Actions
  remains queued.

### Fixed

- Mock TLS fixture reads cert/key PEMs via `File` + `Read` instead of
  `std::fs::read`, restoring `clippy::disallowed_methods` / publish-ready.
- `release.yml` only runs on final `vX.Y.Z` tags (not `v0.1.0-rc.*`), so git
  RC install pins do not queue crates.io publish jobs or burn Actions capacity.

## [0.1.0] - 2026-09-04

First crates.io release baseline (publish via `docs/RELEASE.md` / tag `v0.1.0`).

### Added

- Kafka client: produce, fetch, classic groups, cooperative-sticky,
  KIP-848 consumer groups, KIP-932 share groups, transactions / EOS, and
  Kafka 3.x/4.x admin APIs.
- TLS via rustls (no OpenSSL); SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER,
  OIDC client credentials.
- Compression: gzip, snappy, lz4 (zstd and Kerberos remain out of default
  features; see `docs/gaps.md` and `docs/zstd-spike.md`).
- Java-shaped builders and rustdoc naming matching Java client calls.
- Optional `tracing` feature (produce/fetch/poll/rejoin/txn spans).
- Examples: produce, consume, group, txn, eos, admin, tls, sasl, oauth, share,
  benches, interceptors, metrics (`FORMAT=prom`).
- Operator docs: guide, rdkafka migration, security, release policy,
  API stability, civilization plan.
- CI: mock tests (MSRV 1.85 + stable), clippy, audit, cargo-deny, `cargo package`,
  broker-smoke (Kafka 3.9.1 + 4.1.0), fuzz-smoke, latency-gate.
- libFuzzer targets under `fuzz/`; decode allocation DoS guards.
- TLS PEM parsing via `rustls-pki-types` (no archived `rustls-pemfile`).

### Fixed

- KIP-932 share groups on Kafka 4.1: join with a client Uuid member id, wait for
  a real heartbeat assignment (no zero topic id), decode null Assignment as
  INT8 `-1`, and use the group coordinator for membership. Share smoke needs
  `group.share.enable` and finalized `share.version=1`.

### Changed

- Lab A produce harness exits non-zero unless broker HW sum equals acked each run.
- Civilization/publish-ready gates verify a downstream crate can depend on the packed `.crate`.
- Release workflow accepts `workflow_dispatch` on an existing `v*` tag; rustdoc/`ci-docs` smoke is part of publish-ready.
- Fixed broken rustdoc `[Display]` intra-doc links (now `std::fmt::Display`).
- Cleared remaining unresolved rustdoc intra-doc links (module docs resolve in
  submodule scope; crate denies `rustdoc::broken_intra_doc_links`; `ci-docs`
  fails the gate if any reappear).
- CI: `dev/**` branch pushes run a single `branch-lite` job; full matrix on
  pull_request, `main`, and `workflow_dispatch` so scarce runners can finish.
- Lab A harness accepts `TOPIC=` as well as `KAFKA_TOPIC=`; STATUS notes a
  fresh unsigned latency-gate + HW==acked smoke sample (not a Suite HOLD lift).
- `scripts/check-installable.sh` probes crates.io for the Installable bar;
  ADOPTION notes Actions is stuck org-wide (`main` queued for hours).
- `ci-broker-smoke`: when Docker overlay fails but `$KAFKA_BOOTSTRAP` is already
  up, fall back to that broker automatically (same path as `SKIP_DOCKER=1`).
- Fuzz: Join/Sync/Heartbeat/OffsetCommit + ShareFetch decode targets; Lab A
  fetch integrity harness (`scripts/lab-a-fetch.sh`) requires consumed==seeded
  (unsigned; not a Suite HOLD lift).
- `examples/oauth` for OAUTHBEARER (unsecured JWT) and OIDC client-credentials;
  ADOPTION documents pinable `v0.1.0-rc.2` git install until crates.io lands.
- Broker smoke: Kafka CI matrix uses `apache/kafka:4.1.0`; Docker 4.x starts with
  share coordinator RF=1 and upgrades `share.version=1`; `REQUIRE_SHARE=1` fails
  the job if share cannot fetch records on 4.x. Civilization-check only counts
  broker smoke when the log contains `ci-broker-smoke: ok` (Docker soft-skips
  are not evidence).

### Security

- Reject array / tagged-field lengths that exceed remaining buffer bytes so
  malformed broker frames cannot force multi-gigabyte `Vec` allocations.

### Notes

- Not a drop-in for `rd_kafka_*` or rust-rdkafka types.
- Defaults that differ from Java: `auto.offset.reset=Earliest`,
  `allow.auto.create.topics=false`, shorter `delivery.timeout.ms` /
  `max.block.ms` (see README).
- Mock TLS fixtures use the `openssl` CLI (no `rcgen` / `time` in the graph).
