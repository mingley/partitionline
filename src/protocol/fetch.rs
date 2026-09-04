//! Fetch (api key 1). v4–v11 classic; v12–v17 flexible.

use std::collections::{HashMap, HashSet};
use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::api::MetadataResponse;
use super::buf;
use super::epoch::EpochEndOffset;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};

/// Java `FetchRequest.CONSUMER_REPLICA_ID`. ReplicaId is request-level
/// (untagged through v14; ReplicaState tagged field 1 at v15+).
pub const CONSUMER_REPLICA_ID: i32 = -1;
/// Java `FetchRequest.ORDINARY_CONSUMER_ID`.
pub const ORDINARY_CONSUMER_ID: i32 = -1;
/// Java `FetchRequest.DEBUGGING_CONSUMER_ID`.
pub const DEBUGGING_CONSUMER_ID: i32 = -2;
/// Java `FetchRequest.FUTURE_LOCAL_REPLICA_ID`.
pub const FUTURE_LOCAL_REPLICA_ID: i32 = -3;
/// Java `FetchRequest.INVALID_LOG_START_OFFSET`. Request partition
/// `log_start_offset`; the response copy is
/// [`FetchedPartition::INVALID_LOG_START_OFFSET`].
pub const INVALID_LOG_START_OFFSET: i64 = -1;
/// Java `FetchRequest.DEFAULT_RESPONSE_MAX_BYTES`.
///
/// Java `FetchRequest.Builder.build` uses this when the Fetch version is
/// below 3 (this crate speaks v4+).
pub const DEFAULT_RESPONSE_MAX_BYTES: i32 = i32::MAX;

/// Java `FetchRequest.isValidBrokerId`.
#[must_use]
pub const fn is_valid_broker_id(broker_id: i32) -> bool {
    broker_id >= 0
}

/// Java `FetchRequest.isFromFollower`.
#[must_use]
pub const fn is_from_follower(replica_id: i32) -> bool {
    is_valid_broker_id(replica_id)
}

/// Java `FetchRequest.isConsumer`.
#[must_use]
pub const fn is_consumer(replica_id: i32) -> bool {
    replica_id < 0 && replica_id != FUTURE_LOCAL_REPLICA_ID
}

/// Java `FetchRequest.describeReplicaId`.
#[must_use]
pub fn describe_replica_id(replica_id: i32) -> String {
    match replica_id {
        ORDINARY_CONSUMER_ID => "consumer".into(),
        DEBUGGING_CONSUMER_ID => "debug consumer".into(),
        FUTURE_LOCAL_REPLICA_ID => "future local replica".into(),
        id if is_valid_broker_id(id) => format!("replica [{id}]"),
        id => format!("invalid replica [{id}]"),
    }
}

/// Java `FetchRequest.replicaId()` (instance).
///
/// Below v15 this is the untagged ReplicaId. v15+ is ReplicaState.ReplicaId
/// (KIP-903). [`encode_fetch_request`] still writes [`CONSUMER_REPLICA_ID`]
/// through v14 and omits ReplicaState for consumers.
/// [`encode_fetch_request_with_replica_id`] writes the untagged field on
/// v4–v14. [`encode_fetch_request_with_replica_state`] writes ReplicaState
/// tagged field 1 on v15+. [`encode_fetch_request_with_cluster_id`] writes
/// ClusterId tagged field 0 on v12+.
#[must_use]
pub const fn replica_id(version: i16, replica_id: i32, replica_state_replica_id: i32) -> i32 {
    if version < 15 {
        replica_id
    } else {
        replica_state_replica_id
    }
}

/// Java `FetchRequest.replicaId(FetchRequestData)` (static).
///
/// Untagged ReplicaId when it is not `-1` ([`CONSUMER_REPLICA_ID`]);
/// otherwise ReplicaState.ReplicaId. Distinct from [`replica_id`]: v15+
/// with a non-`-1` untagged id still returns ReplicaState there, and v14
/// with `-1` still returns ReplicaState here.
#[must_use]
pub const fn replica_id_from_data(replica_id: i32, replica_state_replica_id: i32) -> i32 {
    if replica_id != CONSUMER_REPLICA_ID {
        replica_id
    } else {
        replica_state_replica_id
    }
}

/// Java `FetchMetadata` (incremental fetch session id and epoch).
///
/// [`encode_fetch_request`] writes [`Self::LEGACY`] (`session_id` 0 /
/// `epoch` `-1`): close any session and do not create one.
/// [`encode_fetch_request_with_session`] writes this value on v7+. Below
/// v7 SessionId / SessionEpoch are omitted even when this is not
/// LEGACY; decode fills [`Self::LEGACY`]. [`std::fmt::Display`] is Java `toString`
/// (`(sessionId=INVALID, epoch=FINAL)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchMetadata {
    session_id: i32,
    epoch: i32,
}

impl FetchMetadata {
    /// Java `FetchMetadata.INVALID_SESSION_ID`.
    pub const INVALID_SESSION_ID: i32 = 0;
    /// Java `FetchMetadata.INITIAL_EPOCH`.
    pub const INITIAL_EPOCH: i32 = 0;
    /// Java `FetchMetadata.FINAL_EPOCH`.
    pub const FINAL_EPOCH: i32 = -1;
    /// Java `FetchMetadata.INITIAL`.
    pub const INITIAL: Self = Self {
        session_id: Self::INVALID_SESSION_ID,
        epoch: Self::INITIAL_EPOCH,
    };
    /// Java `FetchMetadata.LEGACY`.
    pub const LEGACY: Self = Self {
        session_id: Self::INVALID_SESSION_ID,
        epoch: Self::FINAL_EPOCH,
    };

    /// Java `FetchMetadata(int, int)`.
    #[must_use]
    pub const fn new(session_id: i32, epoch: i32) -> Self {
        Self { session_id, epoch }
    }

    /// Java `FetchMetadata.sessionId`.
    #[must_use]
    pub const fn session_id(self) -> i32 {
        self.session_id
    }

    /// Java `FetchMetadata.epoch`.
    #[must_use]
    pub const fn epoch(self) -> i32 {
        self.epoch
    }

    /// Java `FetchMetadata.isFull`.
    #[must_use]
    pub const fn is_full(self) -> bool {
        self.epoch == Self::INITIAL_EPOCH || self.epoch == Self::FINAL_EPOCH
    }

    /// Java `FetchMetadata.nextEpoch`.
    #[must_use]
    pub const fn next_epoch(prev_epoch: i32) -> i32 {
        if prev_epoch < 0 {
            Self::FINAL_EPOCH
        } else if prev_epoch == i32::MAX {
            1
        } else {
            prev_epoch + 1
        }
    }

    /// Java `FetchMetadata.nextCloseExisting`.
    #[must_use]
    pub const fn next_close_existing(self) -> Self {
        Self::new(self.session_id, Self::FINAL_EPOCH)
    }

    /// Java `FetchMetadata.nextCloseExistingAttemptNew`.
    #[must_use]
    pub const fn next_close_existing_attempt_new(self) -> Self {
        Self::new(self.session_id, Self::INITIAL_EPOCH)
    }

    /// Java `FetchMetadata.newIncremental`.
    #[must_use]
    pub const fn new_incremental(session_id: i32) -> Self {
        Self::new(session_id, Self::next_epoch(Self::INITIAL_EPOCH))
    }

    /// Java `FetchMetadata.nextIncremental`.
    #[must_use]
    pub const fn next_incremental(self) -> Self {
        Self::new(self.session_id, Self::next_epoch(self.epoch))
    }
}

impl fmt::Display for FetchMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.session_id == Self::INVALID_SESSION_ID {
            f.write_str("(sessionId=INVALID, ")?;
        } else {
            write!(f, "(sessionId={}, ", self.session_id)?;
        }
        if self.epoch == Self::INITIAL_EPOCH {
            f.write_str("epoch=INITIAL)")
        } else if self.epoch == Self::FINAL_EPOCH {
            f.write_str("epoch=FINAL)")
        } else {
            write!(f, "epoch={})", self.epoch)
        }
    }
}

/// One partition in a Fetch request.
///
/// [`Self::partition_data`] is Java `FetchRequest.PartitionData(Uuid, long, long, int, Optional, Optional)`
/// (Java `topicId` is not stored; callers pass `partition`.
/// `Optional.empty` epoch is [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]).
#[derive(Debug, Clone)]
pub struct FetchPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch from Metadata, or
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub current_leader_epoch: i32,
    /// Next offset to fetch.
    pub fetch_offset: i64,
    /// Epoch of the last fetched record (v12+), or
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub last_fetched_epoch: i32,
    /// Earliest available offset of the follower replica (v5+).
    ///
    /// JSON `5+` INT64 after LastFetchedEpoch. Only used when the request
    /// is sent by a follower. Consumers send [`INVALID_LOG_START_OFFSET`].
    /// Below v5 encode omits the field even when the body is non-default;
    /// decode fills [`INVALID_LOG_START_OFFSET`]. Official Java
    /// `FetchRequest.PartitionData.logStartOffset`.
    pub log_start_offset: i64,
    /// Max bytes for this partition.
    pub partition_max_bytes: i32,
    /// Fetch v17+ ReplicaDirectoryId (partition tagged field 0).
    ///
    /// Kafka 4.0.0 FetchRequest.json `17+` UUID, default zeros, ignorable.
    /// Official Java `FetchRequestData.FetchPartition.replicaDirectoryId`.
    /// Consumers omit the tag (zeros). Below v17 encode omits the field
    /// even when non-zero; decode fills zeros.
    pub replica_directory_id: [u8; 16],
}

impl FetchPartition {
    /// Java `FetchRequest.PartitionData(Uuid, long, long, int, Optional, Optional)`.
    ///
    /// Java stores `topicId` on this object; this type stores `partition`
    /// (topic id lives on [`FetchTopic`]). `currentLeaderEpoch` /
    /// `lastFetchedEpoch` `None` is Java `Optional.empty`
    /// ([`RecordBatch::NO_PARTITION_LEADER_EPOCH`]). ReplicaDirectoryId
    /// stays zeros (JSON default). The five-argument Java constructor is
    /// this helper with empty `lastFetchedEpoch`. Encode still writes
    /// independently (below v5 omits LogStartOffset; decode fills
    /// [`INVALID_LOG_START_OFFSET`]; below v9 omits CurrentLeaderEpoch;
    /// decode fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; below v12
    /// omits LastFetchedEpoch; decode fills
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]). This crate speaks
    /// 4–17. This is not [`FetchRequest::fetch_data`] /
    /// [`FetchRequest::topics_from_fetch_data`] / ReplicaDirectoryId /
    /// replicaId encode.
    #[must_use]
    pub fn partition_data(
        partition: i32,
        fetch_offset: i64,
        log_start_offset: i64,
        max_bytes: i32,
        current_leader_epoch: Option<i32>,
        last_fetched_epoch: Option<i32>,
    ) -> Self {
        Self {
            partition,
            current_leader_epoch: current_leader_epoch
                .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
            fetch_offset,
            last_fetched_epoch: last_fetched_epoch
                .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
            log_start_offset,
            partition_max_bytes: max_bytes,
            replica_directory_id: [0; 16],
        }
    }
}

/// One topic in a Fetch request.
///
/// [`Self::error_result`] is Java `FetchRequest.getErrorResponse` one topic.
#[derive(Debug, Clone)]
pub struct FetchTopic {
    /// Topic name (v4–v12). Empty at v13+ (topic id on the wire).
    pub topic: String,
    /// Topic id (v13+). Zeros when the request uses a name.
    pub topic_id: [u8; 16],
    /// Partitions to fetch.
    pub partitions: Vec<FetchPartition>,
}

impl FetchTopic {
    /// Java `FetchRequest.getErrorResponse` one topic.
    ///
    /// Each partition is [`FetchedPartition::partition_response`]. Fetch v13
    /// and later omit partitions (top-level error only). Throttle on the
    /// response is the JSON default (`0`).
    #[must_use]
    pub fn error_result(&self, version: i16, error_code: i16) -> FetchedTopic {
        FetchedTopic {
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            partitions: if version < 13 {
                self.partitions
                    .iter()
                    .map(|p| FetchedPartition::partition_response(p.partition, error_code))
                    .collect()
            } else {
                Vec::new()
            },
        }
    }
}

/// One forgotten topic in a Fetch request (session increment).
///
/// Java `FetchRequestData.ForgottenTopic`. [`encode_fetch_request_with_forgotten`]
/// writes this list on v7+. Below v7 ForgottenTopicsData is omitted even
/// when the list is non-empty; decode fills empty.
/// [`encode_fetch_request`] / [`encode_fetch_request_with_session`] still
/// write an empty array. [`FetchRequest::forgotten_topics`] reads this
/// list and [`FetchRequest::forgotten_from_removed`] builds it from
/// removed+replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgottenTopic {
    /// Topic name (v4–v12). Empty at v13+ (topic id on the wire).
    pub topic: String,
    /// Topic id (v13+). Zeros when the request uses a name.
    pub topic_id: [u8; 16],
    /// Partitions to forget.
    pub partitions: Vec<i32>,
}

/// Java `FetchRequest` helpers.
pub struct FetchRequest;

impl FetchRequest {
    /// Java `FetchRequest.fetchData`.
    ///
    /// v4–v12 use each topic's name. v13+ looks up `topic_id` in
    /// `topic_names` (`None` when missing; Java still inserts that
    /// `TopicIdPartition`). A later partition overwrites the same
    /// `(topic_id, name, partition)` (Java `LinkedHashMap.put`).
    /// Each partition keeps [`FetchPartition::log_start_offset`] (official
    /// Java `FetchRequest.PartitionData.logStartOffset`).
    #[must_use]
    pub fn fetch_data(
        version: i16,
        topics: &[FetchTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> HashMap<([u8; 16], Option<String>, i32), FetchPartition> {
        let mut fetch_data = HashMap::new();
        for topic in topics {
            let name = if version < 13 {
                Some(topic.topic.clone())
            } else {
                topic_names.get(&topic.topic_id).cloned()
            };
            for partition in &topic.partitions {
                let _prev = fetch_data.insert(
                    (topic.topic_id, name.clone(), partition.partition),
                    partition.clone(),
                );
            }
        }
        fetch_data
    }

    /// Java `FetchRequest.forgottenTopics`.
    ///
    /// v4–v12 use each topic's name. v13+ looks up `topic_id` in
    /// `topic_names` (`None` when missing; Java still inserts that
    /// `TopicIdPartition`). Duplicate partitions are kept (`ArrayList`;
    /// unlike [`Self::fetch_data`]). [`encode_fetch_request_with_forgotten`]
    /// writes ForgottenTopicsData on v7+; [`encode_fetch_request`] still
    /// writes an empty array.
    #[must_use]
    pub fn forgotten_topics(
        version: i16,
        forgotten: &[ForgottenTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> Vec<([u8; 16], Option<String>, i32)> {
        let mut to_forget = Vec::new();
        for topic in forgotten {
            let name = if version < 13 {
                Some(topic.topic.clone())
            } else {
                topic_names.get(&topic.topic_id).cloned()
            };
            for partition in &topic.partitions {
                to_forget.push((topic.topic_id, name.clone(), *partition));
            }
        }
        to_forget
    }

    /// Java `FetchRequest.Builder.build` ForgottenTopicsData from removed
    /// and replaced.
    ///
    /// Grouped by topic name (Java `LinkedHashMap` keyed by
    /// `TopicIdPartition.topic()`). The first topic id for a name is kept;
    /// later partitions for that name append (`ArrayList`, duplicates
    /// kept). `replaced` is included only on v13+ (a same-name topic-id
    /// replacement is not forgotten below v13, so the newly added fetch
    /// partition is not removed). [`encode_fetch_request_with_forgotten`]
    /// writes ForgottenTopicsData on v7+; [`encode_fetch_request`] still
    /// writes an empty array. Distinct from [`Self::forgotten_topics`],
    /// which reads an already-built list and looks up names by id at v13+.
    #[must_use]
    pub fn forgotten_from_removed<'a, R, S>(
        version: i16,
        removed: R,
        replaced: S,
    ) -> Vec<ForgottenTopic>
    where
        R: IntoIterator<Item = ([u8; 16], &'a str, i32)>,
        S: IntoIterator<Item = ([u8; 16], &'a str, i32)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_name: HashMap<String, ForgottenTopic> = HashMap::new();
        add_to_forgotten_topic_map(&mut order, &mut by_name, removed);
        if version >= 13 {
            add_to_forgotten_topic_map(&mut order, &mut by_name, replaced);
        }
        order
            .into_iter()
            .filter_map(|name| by_name.remove(&name))
            .collect()
    }

    /// Java `FetchRequest.Builder.build` Topics from fetchData.
    ///
    /// Consecutive entries with the same topic name share one `FetchTopic`
    /// (first topic id is kept; later partitions append, duplicates kept).
    /// An intervening different name starts a new topic, even when a later
    /// entry repeats an earlier name (unlike [`FetchResponse::to_message`]
    /// matching by id, and unlike [`Self::forgotten_from_removed`], which
    /// merges by name across the whole list). Encode still writes the
    /// caller's Topics list as-is.
    #[must_use]
    pub fn topics_from_fetch_data<'a, I>(entries: I) -> Vec<FetchTopic>
    where
        I: IntoIterator<Item = (&'a str, [u8; 16], FetchPartition)>,
    {
        let mut topics = Vec::<FetchTopic>::new();
        for (name, topic_id, partition) in entries {
            match topics.last_mut() {
                Some(topic) if topic.topic == name => {
                    topic.partitions.push(partition);
                }
                _ => {
                    topics.push(FetchTopic {
                        topic: name.to_string(),
                        topic_id,
                        partitions: vec![partition],
                    });
                }
            }
        }
        topics
    }

    /// Java `FetchRequest.getErrorResponse`.
    ///
    /// Below v13 each request topic is [`FetchTopic::error_result`]. v13+
    /// Responses is empty (top-level error only; unlike `error_result`,
    /// which still keeps a topic with empty partitions). Official Java
    /// also sets `throttleTimeMs`, top-level `ErrorCode`, and `SessionId`
    /// from the request; this helper is the Responses body. Encode with
    /// those fields through [`Self::encode_error_response`]. Convenience
    /// encode still writes ThrottleTimeMs `0`, ErrorCode `0`, and SessionId
    /// [`FetchMetadata::INVALID_SESSION_ID`]. ErrorCode / SessionId are
    /// v7+; below v7 encode omits them even when the body is non-zero and
    /// decode fills `0`.
    #[must_use]
    pub fn error_response(
        version: i16,
        topics: &[FetchTopic],
        error_code: i16,
    ) -> Vec<FetchedTopic> {
        if version < 13 {
            topics
                .iter()
                .map(|topic| topic.error_result(version, error_code))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Java `FetchRequest.getErrorResponse` encode.
    ///
    /// Responses from [`Self::error_response`]. ThrottleTimeMs is written
    /// on every spoken version from `throttle_time_ms` (JSON `1+`; this
    /// crate speaks 4–17). ErrorCode and SessionId are written on v7+
    /// from `error_code` / `session_id`; below v7 they are omitted even
    /// when non-zero and decode fills `0` /
    /// [`FetchMetadata::INVALID_SESSION_ID`]. NodeEndpoints stay empty.
    /// Convenience encode still writes throttle `0`, ErrorCode `0`, and
    /// SessionId [`FetchMetadata::INVALID_SESSION_ID`]. This crate speaks
    /// 4–17. This is not [`Self::error_response`] leftover /
    /// [`encode_fetch_response_with_throttle`] leftover /
    /// [`encode_fetch_response_with_endpoints`] leftover / SimpleBuilder
    /// leftover.
    pub fn encode_error_response(
        buf: &mut BytesMut,
        version: i16,
        topics: &[FetchTopic],
        error_code: i16,
        session_id: i32,
        throttle_time_ms: i32,
    ) -> Result<()> {
        let responses = Self::error_response(version, topics, error_code);
        encode_fetch_response_fields(
            buf,
            version,
            &responses,
            error_code,
            session_id,
            &[],
            throttle_time_ms,
        )
    }

    /// Java `FetchRequest.Builder(short minVersion, short maxVersion, int replicaId, long replicaEpoch, ...)`.
    ///
    /// Oldest allowed version is `min_version`. Latest is `max_version`.
    /// ReplicaId and ReplicaEpoch are the arguments. Isolation defaults
    /// to READ_UNCOMMITTED (`0`) and MaxBytes defaults to
    /// [`DEFAULT_RESPONSE_MAX_BYTES`]; pass those on encode separately.
    /// MaxWaitMs, MinBytes, and Topics are the caller's values.
    /// [`Self::for_consumer`] is this helper with oldest 4, ReplicaId
    /// [`CONSUMER_REPLICA_ID`], ReplicaEpoch `-1`. [`Self::for_replica`]
    /// is this helper with min=max=`allowed_version`. Encode still writes
    /// ReplicaId independently of this Builder range. This crate speaks
    /// 4–17. This is not [`Self::forgotten_from_removed`] /
    /// [`Self::topics_from_fetch_data`] / replicaId encode /
    /// [`Self::simple_build`] / [`Self::replica_for_build`].
    #[must_use]
    pub const fn builder(
        min_version: i16,
        max_version: i16,
        replica_id: i32,
        replica_epoch: i64,
    ) -> (i16, i16, i32, i64) {
        (min_version, max_version, replica_id, replica_epoch)
    }

    /// Java `FetchRequest.Builder.forConsumer`.
    ///
    /// Oldest allowed version is 4 (Java `ApiKeys.FETCH.oldestVersion()`
    /// on Kafka 4.0, matching this crate's spoken floor). Latest is
    /// `max_version`. ReplicaId is [`CONSUMER_REPLICA_ID`]. ReplicaEpoch
    /// is `-1`. This is [`Self::builder`] with those values. Isolation
    /// defaults to READ_UNCOMMITTED (`0`) and MaxBytes defaults to
    /// [`DEFAULT_RESPONSE_MAX_BYTES`]; pass those on
    /// [`encode_fetch_request`] separately. MaxWaitMs, MinBytes, and Topics
    /// are the caller's values. Replica epoch lives on Java `ReplicaState`
    /// (v15+ tagged field 1); consumers omit that field. This helper still
    /// returns the Java constructor value so callers can keep it next to
    /// replica id. Encode still writes ReplicaId independently of this
    /// Builder range. This crate speaks 4–17. This is not
    /// [`Self::forgotten_from_removed`] / [`Self::topics_from_fetch_data`] /
    /// replicaId encode / ShareFetch `forConsumer`.
    #[must_use]
    pub const fn for_consumer(max_version: i16) -> (i16, i16, i32, i64) {
        Self::builder(4, max_version, CONSUMER_REPLICA_ID, -1)
    }

    /// Java `FetchRequest.Builder.forReplica`.
    ///
    /// Oldest and latest allowed versions are both `allowed_version`.
    /// ReplicaId and ReplicaEpoch are the arguments. This is
    /// [`Self::builder`] with min=max=`allowed_version`. Isolation defaults
    /// to READ_UNCOMMITTED (`0`) and MaxBytes defaults to
    /// [`DEFAULT_RESPONSE_MAX_BYTES`]; pass those on
    /// [`encode_fetch_request_with_replica_id`] separately. MaxWaitMs,
    /// MinBytes, and Topics are the caller's values. Replica epoch lives
    /// on Java `ReplicaState` (v15+ tagged field 1); this crate does not
    /// write that field. Encode still writes untagged ReplicaId on v4–v14
    /// and omits it on v15+. This crate speaks 4–17. This is not
    /// [`Self::for_consumer`] / [`Self::forgotten_from_removed`] /
    /// [`Self::topics_from_fetch_data`] / replicaId encode / ListOffsets
    /// `forReplica`.
    #[must_use]
    pub const fn for_replica(
        allowed_version: i16,
        replica_id: i32,
        replica_epoch: i64,
    ) -> (i16, i16, i32, i64) {
        Self::builder(allowed_version, allowed_version, replica_id, replica_epoch)
    }

    /// Java `FetchRequest.SimpleBuilder.build`.
    ///
    /// Untagged ReplicaId must be `< 0` (Java `IllegalStateException`
    /// `"The replica id should be placed in the replicaState of a fetchRequestData"`
    /// otherwise). Below v15 the returned untagged ReplicaId is
    /// `replica_state_replica_id` and ReplicaState.ReplicaId is reset to
    /// [`CONSUMER_REPLICA_ID`] (`new ReplicaState()`). v15+ leaves both as
    /// the caller passed them. [`encode_fetch_request_with_replica_id`]
    /// still writes untagged ReplicaId on v4–v14 and omits ReplicaState on
    /// v15+. This crate speaks 4–17. This is not [`replica_id`] /
    /// [`replica_id_from_data`] / [`Self::builder`] / [`Self::for_consumer`] /
    /// [`Self::for_replica`].
    pub fn simple_build(
        version: i16,
        replica_id: i32,
        replica_state_replica_id: i32,
    ) -> Result<(i32, i32)> {
        if replica_id >= 0 {
            return Err(Error::protocol(
                "The replica id should be placed in the replicaState of a fetchRequestData",
            ));
        }
        if version < 15 {
            Ok((replica_state_replica_id, CONSUMER_REPLICA_ID))
        } else {
            Ok((replica_id, replica_state_replica_id))
        }
    }

    /// Java `FetchRequest.Builder.build` ReplicaId / ReplicaState.
    ///
    /// Below v15 untagged ReplicaId is `replica_id` and ReplicaState stays
    /// at JSON defaults ([`CONSUMER_REPLICA_ID`], `-1`). v15+ untagged
    /// ReplicaId stays [`CONSUMER_REPLICA_ID`] and ReplicaState takes
    /// `replica_id` and `replica_epoch`. MaxBytes below v3 is
    /// [`DEFAULT_RESPONSE_MAX_BYTES`] (this crate speaks v4+, so that
    /// rewrite is a no-op). ForgottenTopicsData is
    /// [`Self::forgotten_from_removed`]. Topics are
    /// [`Self::topics_from_fetch_data`].
    /// [`encode_fetch_request_with_replica_id`] still writes untagged
    /// ReplicaId on v4–v14 and omits ReplicaState on v15+. This crate
    /// speaks 4–17. This is not [`replica_id`] / [`replica_id_from_data`]
    /// / [`Self::simple_build`] / [`Self::builder`] / [`Self::for_consumer`] /
    /// [`Self::for_replica`].
    #[must_use]
    pub const fn replica_for_build(
        version: i16,
        replica_id: i32,
        replica_epoch: i64,
    ) -> (i32, i32, i64) {
        if version < 15 {
            (replica_id, CONSUMER_REPLICA_ID, -1)
        } else {
            (CONSUMER_REPLICA_ID, replica_id, replica_epoch)
        }
    }
}

fn add_to_forgotten_topic_map<'a, I>(
    order: &mut Vec<String>,
    by_name: &mut HashMap<String, ForgottenTopic>,
    to_forget: I,
) where
    I: IntoIterator<Item = ([u8; 16], &'a str, i32)>,
{
    for (topic_id, topic, partition) in to_forget {
        by_name
            .entry(topic.to_string())
            .or_insert_with(|| {
                order.push(topic.to_string());
                ForgottenTopic {
                    topic: topic.to_string(),
                    topic_id,
                    partitions: Vec::new(),
                }
            })
            .partitions
            .push(partition);
    }
}

/// One partition in a Fetch response.
///
/// [`Self::INVALID_HIGH_WATERMARK`] / [`Self::INVALID_LAST_STABLE_OFFSET`] /
/// [`Self::INVALID_LOG_START_OFFSET`] / [`Self::INVALID_PREFERRED_REPLICA_ID`]
/// are Java `FetchResponse` sentinels (`-1`).
/// [`Self::partition_response`] is Java `FetchResponse.partitionResponse`.
/// [`Self::preferred_read_replica()`] / [`Self::is_preferred_replica`] /
/// [`Self::diverging_epoch()`] / [`Self::is_diverging_epoch`] are Java
/// `FetchResponse.preferredReadReplica` / `isPreferredReplica` /
/// `divergingEpoch` / `isDivergingEpoch`. [`Self::snapshot_id()`] /
/// [`Self::is_snapshot_id`] are the JSON `SnapshotId` tagged field
/// (Apache `FetchResponse.java` has no `snapshotId` helper). Omitted v12+
/// CurrentLeader fills [`MetadataResponse::NO_LEADER_ID`] /
/// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; omitted DivergingEpoch fills
/// [`EpochEndOffset::UNDEFINED_EPOCH`] /
/// [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`]; omitted SnapshotId fills
/// the same offset / epoch defaults (JSON `-1` / `-1`). This is not the
/// FetchSnapshot API and does not start those RPCs.
#[derive(Debug, Clone)]
pub struct FetchedPartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// High watermark.
    pub high_watermark: i64,
    /// Last stable offset (transactions).
    pub last_stable_offset: i64,
    /// Log start offset.
    pub log_start_offset: i64,
    /// `(producer_id, first_offset)` for aborted transactions (Fetch isolation=1).
    pub aborted_transactions: Vec<(i64, i64)>,
    /// Broker id to fetch from next, or [`Self::INVALID_PREFERRED_REPLICA_ID`].
    pub preferred_read_replica: i32,
    /// Fetch v12+ CurrentLeader `LeaderId` (tagged field 1), or
    /// [`MetadataResponse::NO_LEADER_ID`] when omitted (JSON default).
    pub current_leader_id: i32,
    /// Fetch v12+ CurrentLeader `LeaderEpoch` (tagged field 1), or
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] when omitted (JSON default).
    pub current_leader_epoch: i32,
    /// Fetch v12+ DivergingEpoch `Epoch` (tagged field 0), or
    /// [`EpochEndOffset::UNDEFINED_EPOCH`] when omitted (JSON default).
    pub diverging_epoch: i32,
    /// Fetch v12+ DivergingEpoch `EndOffset` (tagged field 0), or
    /// [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] when omitted (JSON default).
    pub diverging_end_offset: i64,
    /// Fetch v12+ SnapshotId `EndOffset` (tagged field 2), or
    /// [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] when omitted (JSON default).
    /// JSON field order is EndOffset then Epoch (the reverse of DivergingEpoch).
    pub snapshot_end_offset: i64,
    /// Fetch v12+ SnapshotId `Epoch` (tagged field 2), or
    /// [`EpochEndOffset::UNDEFINED_EPOCH`] when omitted (JSON default).
    pub snapshot_epoch: i32,
    /// Record batches for this partition.
    pub records: Vec<RecordBatch>,
}

impl FetchedPartition {
    /// Java `FetchResponse.INVALID_HIGH_WATERMARK`.
    pub const INVALID_HIGH_WATERMARK: i64 = -1;
    /// Java `FetchResponse.INVALID_LAST_STABLE_OFFSET`.
    pub const INVALID_LAST_STABLE_OFFSET: i64 = -1;
    /// Java `FetchResponse.INVALID_LOG_START_OFFSET`.
    pub const INVALID_LOG_START_OFFSET: i64 = -1;
    /// Java `FetchResponse.INVALID_PREFERRED_REPLICA_ID`.
    pub const INVALID_PREFERRED_REPLICA_ID: i32 = -1;

    /// Java `FetchResponse.partitionResponse(int, Errors)`.
    ///
    /// Sets [`Self::INVALID_HIGH_WATERMARK`] and empty records. Other
    /// fields are Apache JSON defaults (`LastStableOffset` /
    /// `LogStartOffset` / `PreferredReadReplica` / omitted
    /// `CurrentLeader` / omitted `DivergingEpoch` / omitted `SnapshotId`).
    #[must_use]
    pub fn partition_response(partition: i32, error_code: i16) -> Self {
        Self {
            partition,
            error_code,
            high_watermark: Self::INVALID_HIGH_WATERMARK,
            last_stable_offset: Self::INVALID_LAST_STABLE_OFFSET,
            log_start_offset: Self::INVALID_LOG_START_OFFSET,
            aborted_transactions: Vec::new(),
            preferred_read_replica: Self::INVALID_PREFERRED_REPLICA_ID,
            current_leader_id: MetadataResponse::NO_LEADER_ID,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            diverging_epoch: EpochEndOffset::UNDEFINED_EPOCH,
            diverging_end_offset: EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
            snapshot_end_offset: EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
            snapshot_epoch: EpochEndOffset::UNDEFINED_EPOCH,
            records: Vec::new(),
        }
    }

    /// Java `FetchResponse.preferredReadReplica` (`None` is empty
    /// `Optional`).
    #[must_use]
    pub fn preferred_read_replica(&self) -> Option<i32> {
        (self.preferred_read_replica != Self::INVALID_PREFERRED_REPLICA_ID)
            .then_some(self.preferred_read_replica)
    }

    /// Java `FetchResponse.isPreferredReplica`.
    #[must_use]
    pub fn is_preferred_replica(&self) -> bool {
        self.preferred_read_replica().is_some()
    }

    /// Java `FetchResponse.divergingEpoch` (`None` is empty `Optional`).
    ///
    /// Epoch `< 0` is empty (JSON default [`EpochEndOffset::UNDEFINED_EPOCH`]).
    /// The pair is `(epoch, end_offset)`.
    #[must_use]
    pub fn diverging_epoch(&self) -> Option<(i32, i64)> {
        (self.diverging_epoch >= 0).then_some((self.diverging_epoch, self.diverging_end_offset))
    }

    /// Java `FetchResponse.isDivergingEpoch`.
    #[must_use]
    pub fn is_diverging_epoch(&self) -> bool {
        self.diverging_epoch().is_some()
    }

    /// JSON `SnapshotId` tagged field 2 (`None` when both fields are the
    /// JSON defaults). Apache `FetchResponse.java` has no `snapshotId`
    /// helper (generated `FetchResponseData.PartitionData` only).
    ///
    /// The pair is `(end_offset, epoch)` matching JSON field order
    /// (`EndOffset` INT64 then `Epoch` INT32; the reverse of
    /// [`Self::diverging_epoch()`]). This is not the FetchSnapshot API
    /// and does not start those RPCs.
    #[must_use]
    pub fn snapshot_id(&self) -> Option<(i64, i32)> {
        (self.snapshot_end_offset != EpochEndOffset::UNDEFINED_EPOCH_OFFSET
            || self.snapshot_epoch != EpochEndOffset::UNDEFINED_EPOCH)
            .then_some((self.snapshot_end_offset, self.snapshot_epoch))
    }

    /// JSON `SnapshotId` tagged field 2 is present (not both JSON defaults).
    #[must_use]
    pub fn is_snapshot_id(&self) -> bool {
        self.snapshot_id().is_some()
    }

    /// Java `FetchResponse.recordsSize`.
    ///
    /// `0` when records are empty (Java `null` or `MemoryRecords.EMPTY`).
    /// Otherwise the encoded size of the records blob.
    pub fn records_size(&self) -> Result<i32> {
        if self.records.is_empty() {
            return Ok(0);
        }
        let mut recs = BytesMut::new();
        for batch in &self.records {
            records::encode_record_batch(&mut recs, batch)?;
        }
        buf::i32_from_usize(recs.len())
    }
}

/// Java `FetchResponse` helpers.
pub struct FetchResponse;

impl FetchResponse {
    /// Java `FetchResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 8
    }

    /// Java `FetchResponse.topicIds`.
    ///
    /// Topic ids that are not zeros. Fetch versions below 13 return an empty
    /// set (those responses use names; `topic_id` is zeros).
    #[must_use]
    pub fn topic_ids(topics: &[FetchedTopic]) -> HashSet<[u8; 16]> {
        topics
            .iter()
            .map(|topic| topic.topic_id)
            .filter(|id| *id != [0u8; 16])
            .collect()
    }

    /// Java `FetchResponse.responseData`.
    ///
    /// v4–v12 use each topic's name. v13+ looks up `topic_id` in
    /// `topic_names` and skips a topic whose id is missing (Java `name
    /// != null`). A later partition overwrites the same pair (Java
    /// `LinkedHashMap.put`).
    #[must_use]
    pub fn response_data(
        version: i16,
        topics: &[FetchedTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> HashMap<(String, i32), FetchedPartition> {
        let mut response_data = HashMap::new();
        for topic in topics {
            let name = if version < 13 {
                Some(topic.topic.as_str())
            } else {
                topic_names.get(&topic.topic_id).map(String::as_str)
            };
            let Some(name) = name else {
                continue;
            };
            for partition in &topic.partitions {
                let _prev = response_data
                    .insert((name.to_string(), partition.partition), partition.clone());
            }
        }
        response_data
    }

    /// Java `FetchResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`) plus each
    /// partition-level code (including `NONE`). Decode returns the
    /// top-level code; [`encode_fetch_response`] writes `0`.
    #[must_use]
    pub fn error_counts(error_code: i16, topics: &[FetchedTopic]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(error_code).or_insert(0);
        *count += 1;
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }

    /// Java `FetchResponse.toMessage` Responses grouping used by public `of`.
    ///
    /// `entries` are `(topic_id, topic, partition)` plus a body. Java
    /// `setPartitionIndex` copies the key partition onto each body.
    /// Consecutive entries batch into one topic when `matchingTopic`: a
    /// non-zero previous `topic_id` matches by id; otherwise the previous
    /// name matches. A later entry for the same topic after a different
    /// topic starts a new group (unlike ShareFetch `LinkedHashMap` by
    /// id). Throttle, top-level error, session id, and NodeEndpoints
    /// stay with crate encode (`0` / empty).
    #[must_use]
    pub fn to_message(entries: &[([u8; 16], &str, i32, FetchedPartition)]) -> Vec<FetchedTopic> {
        let mut topics: Vec<FetchedTopic> = Vec::new();
        for (topic_id, topic, partition, body) in entries {
            let mut body = body.clone();
            body.partition = *partition;
            if let Some(prev) = topics.last_mut() {
                let matches = if prev.topic_id != [0u8; 16] {
                    prev.topic_id == *topic_id
                } else {
                    prev.topic == *topic
                };
                if matches {
                    prev.partitions.push(body);
                    continue;
                }
            }
            topics.push(FetchedTopic {
                topic: (*topic).to_string(),
                topic_id: *topic_id,
                partitions: vec![body],
            });
        }
        topics
    }

    /// Java `FetchResponse.sizeOf`.
    ///
    /// `4` plus the encoded body from [`Self::to_message`] then
    /// [`encode_fetch_response`] (Java `toMessage(NONE, 0,
    /// INVALID_SESSION_ID, iterator, empty)` then `data.size` plus the
    /// INT32 size prefix). Throttle / ErrorCode / SessionId stay
    /// convenience-encode values (`0` / [`FetchMetadata::INVALID_SESSION_ID`]);
    /// those fields have fixed width so the values do not change the
    /// size. Empty NodeEndpoints. This crate speaks 4–17. This is not
    /// [`Self::to_message`] / [`FetchedPartition::records_size`] /
    /// [`FetchRequest::encode_error_response`].
    pub fn size_of(
        version: i16,
        entries: &[([u8; 16], &str, i32, FetchedPartition)],
    ) -> Result<i32> {
        let topics = Self::to_message(entries);
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, version, &topics)?;
        let n = buf
            .len()
            .checked_add(4)
            .ok_or_else(|| Error::protocol("FetchResponse.sizeOf overflow"))?;
        buf::i32_from_usize(n)
    }

    /// Java `FetchResponse.of` (empty NodeEndpoints).
    ///
    /// Responses from [`Self::to_message`]. ThrottleTimeMs, ErrorCode, and
    /// SessionId are the arguments. ErrorCode and SessionId are written on
    /// v7+; below v7 they are omitted even when non-zero and decode fills
    /// `0` / [`FetchMetadata::INVALID_SESSION_ID`]. NodeEndpoints stay
    /// empty ([`Self::of_with_endpoints`] is the five-argument Java `of`
    /// with `nodeEndpoints`). Convenience encode still writes throttle
    /// `0`, ErrorCode `0`, SessionId
    /// [`FetchMetadata::INVALID_SESSION_ID`]. This crate speaks 4–17.
    /// This is not [`Self::to_message`] / [`Self::size_of`] /
    /// [`encode_fetch_response_with_throttle`] /
    /// [`encode_fetch_response_with_endpoints`] /
    /// [`FetchRequest::encode_error_response`].
    pub fn of(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
        session_id: i32,
        entries: &[([u8; 16], &str, i32, FetchedPartition)],
    ) -> Result<()> {
        Self::of_with_endpoints(
            buf,
            version,
            error_code,
            throttle_time_ms,
            session_id,
            entries,
            &[],
        )
    }

    /// Java `FetchResponse.of` with NodeEndpoints.
    ///
    /// Responses from [`Self::to_message`]. ThrottleTimeMs, ErrorCode,
    /// SessionId, and NodeEndpoints are the arguments. ErrorCode and
    /// SessionId are written on v7+; below v7 they are omitted even when
    /// non-zero. NodeEndpoints are written on v16+ (tagged field 0);
    /// below v16 they are omitted even when non-empty and decode fills
    /// empty. [`Self::of`] is this helper with empty NodeEndpoints.
    /// Convenience encode still writes throttle `0`, ErrorCode `0`,
    /// SessionId [`FetchMetadata::INVALID_SESSION_ID`], empty
    /// NodeEndpoints. This crate speaks 4–17. This is not [`Self::of`] /
    /// [`Self::to_message`] / [`Self::size_of`] /
    /// [`encode_fetch_response_with_throttle`] /
    /// [`encode_fetch_response_with_endpoints`] /
    /// [`FetchRequest::encode_error_response`].
    pub fn of_with_endpoints(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
        session_id: i32,
        entries: &[([u8; 16], &str, i32, FetchedPartition)],
        endpoints: &[super::api::NodeEndpoint],
    ) -> Result<()> {
        let topics = Self::to_message(entries);
        encode_fetch_response_fields(
            buf,
            version,
            &topics,
            error_code,
            session_id,
            endpoints,
            throttle_time_ms,
        )
    }
}

/// One topic in a Fetch response.
#[derive(Debug, Clone)]
pub struct FetchedTopic {
    /// Topic name (v4–v12). Empty at v13+ (topic id on the wire).
    pub topic: String,
    /// Topic id (v13+). Zeros when the response uses a name.
    pub topic_id: [u8; 16],
    /// Partition bodies.
    pub partitions: Vec<FetchedPartition>,
}

/// Fetch v4–v11 (classic) or v12–v17 (flexible). LastFetchedEpoch is v12+.
/// SessionId / SessionEpoch / ForgottenTopicsData are v7+. LogStartOffset
/// is JSON `5+` (encode writes [`FetchPartition::log_start_offset`]).
/// CurrentLeaderEpoch is v9+. RackId is v11+. Session is
/// [`FetchMetadata::LEGACY`]. ReplicaId is JSON `0-14` (still
/// [`CONSUMER_REPLICA_ID`]). MaxWaitMs is JSON `0+` (decode returns it).
/// MinBytes is JSON `0+` (decode returns it; encode already takes `min_bytes`).
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, and rack together"
)]
pub fn encode_fetch_request(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
) -> crate::error::Result<()> {
    encode_fetch_request_with_session(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        FetchMetadata::LEGACY,
    )
}

/// Encode Fetch v4–v17 with [`FetchMetadata`].
///
/// SessionId / SessionEpoch are v7+. Below v7 they are omitted even when
/// `session` is not [`FetchMetadata::LEGACY`]. Decode fills
/// [`FetchMetadata::LEGACY`]. ForgottenTopicsData stays empty. Kafka 4.0
/// `validVersions` is `4-17`.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, and session together"
)]
pub fn encode_fetch_request_with_session(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    session: FetchMetadata,
) -> crate::error::Result<()> {
    encode_fetch_request_with_forgotten(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        session,
        &[],
    )
}

/// Encode Fetch v4–v17 with [`FetchMetadata`] and ForgottenTopicsData.
///
/// SessionId / SessionEpoch / ForgottenTopicsData are v7+. Below v7 they
/// are omitted even when `session` is not [`FetchMetadata::LEGACY`] or
/// `forgotten` is non-empty. Decode fills [`FetchMetadata::LEGACY`] and
/// an empty forgotten list. v13+ ForgottenTopics use TopicId. Kafka 4.0
/// `validVersions` is `4-17`.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, session, and forgotten together"
)]
pub fn encode_fetch_request_with_forgotten(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    session: FetchMetadata,
    forgotten: &[ForgottenTopic],
) -> crate::error::Result<()> {
    encode_fetch_request_body(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        session,
        forgotten,
        CONSUMER_REPLICA_ID,
        CONSUMER_REPLICA_ID,
        -1,
        None,
    )
}

/// Encode Fetch v4–v17 with ReplicaId.
///
/// ReplicaId is JSON `0-14` (untagged INT32; default `-1`). Official Java
/// `FetchRequestData.replicaId` / `FetchRequest.replicaId()`. v15+ omits
/// the untagged field even when `replica_id` is not [`CONSUMER_REPLICA_ID`]
/// (ReplicaState tagged field 1 is not written). Decode fills
/// [`CONSUMER_REPLICA_ID`]. [`encode_fetch_request`] still writes
/// [`CONSUMER_REPLICA_ID`]. This is not ReplicaState / ListOffsets ReplicaId
/// / OffsetForLeaderEpoch ReplicaId.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, and replica id together"
)]
pub fn encode_fetch_request_with_replica_id(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    replica_id: i32,
) -> crate::error::Result<()> {
    encode_fetch_request_body(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        FetchMetadata::LEGACY,
        &[],
        replica_id,
        CONSUMER_REPLICA_ID,
        -1,
        None,
    )
}

/// Encode Fetch v4–v17 with ReplicaId / ReplicaState.
///
/// Below v15 this is untagged ReplicaId (JSON `0-14`). v15+ omits the
/// untagged field and writes ReplicaState tagged field 1 when ReplicaId
/// or ReplicaEpoch is not the JSON default (`-1` / `-1`). Official Java
/// `FetchRequest.Builder.build` ReplicaId / ReplicaState (KIP-903).
/// [`encode_fetch_request_with_replica_id`] still omits ReplicaState.
/// [`encode_fetch_request`] still writes consumer defaults. This crate
/// speaks 4–17. This is not ClusterId tagged field 0 / partition
/// ReplicaDirectoryId / ListOffsets ReplicaId.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, replica id, and replica epoch together"
)]
pub fn encode_fetch_request_with_replica_state(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    replica_id: i32,
    replica_epoch: i64,
) -> crate::error::Result<()> {
    let (untagged, state_id, state_epoch) =
        FetchRequest::replica_for_build(version, replica_id, replica_epoch);
    encode_fetch_request_body(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        FetchMetadata::LEGACY,
        &[],
        untagged,
        state_id,
        state_epoch,
        None,
    )
}

/// Encode Fetch v4–v17 with ClusterId and ReplicaId / ReplicaState.
///
/// Kafka 4.0.0 FetchRequest.json ClusterId is versions `12+` tagged
/// field 0 (nullable compact STRING, default `null`, ignorable). Official
/// Java `FetchRequestData.clusterId`. Consumers omit it. Below v12 the
/// field is omitted even when `Some`. [`encode_fetch_request`] /
/// [`encode_fetch_request_with_replica_id`] /
/// [`encode_fetch_request_with_replica_state`] still omit ClusterId.
/// ReplicaId / ReplicaState match [`encode_fetch_request_with_replica_state`].
/// This crate speaks 4–17. This is not partition ReplicaDirectoryId.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, replica id, replica epoch, and cluster id together"
)]
pub fn encode_fetch_request_with_cluster_id(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    replica_id: i32,
    replica_epoch: i64,
    cluster_id: Option<&str>,
) -> crate::error::Result<()> {
    let (untagged, state_id, state_epoch) =
        FetchRequest::replica_for_build(version, replica_id, replica_epoch);
    encode_fetch_request_body(
        buf,
        version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
        rack_id,
        FetchMetadata::LEGACY,
        &[],
        untagged,
        state_id,
        state_epoch,
        cluster_id,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, rack, session, forgotten, replica id, replica epoch, and cluster id together"
)]
fn encode_fetch_request_body(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
    session: FetchMetadata,
    forgotten: &[ForgottenTopic],
    replica_id: i32,
    replica_state_replica_id: i32,
    replica_state_replica_epoch: i64,
    cluster_id: Option<&str>,
) -> crate::error::Result<()> {
    let flexible = fetch_flexible(version)?;
    // ReplicaId is untagged only through v14. v15+ uses ReplicaState tagged
    // field 1 (KIP-903). Consumers omit it (ReplicaId / ReplicaEpoch default
    // -1 / -1).
    if version <= 14 {
        buf.put_i32(replica_id);
    }
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    buf.put_i8(isolation_level);
    if version >= 7 {
        buf.put_i32(session.session_id());
        buf.put_i32(session.epoch());
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        put_fetch_topic_identity(buf, version, flexible, &t.topic, &t.topic_id)?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            if version >= 9 {
                buf.put_i32(p.current_leader_epoch);
            }
            buf.put_i64(p.fetch_offset);
            if version >= 12 {
                buf.put_i32(p.last_fetched_epoch);
            }
            if version >= 5 {
                buf.put_i64(p.log_start_offset);
            }
            buf.put_i32(p.partition_max_bytes);
            if flexible {
                // v17+ ReplicaDirectoryId is partition tagged field 0
                // (KIP-853). Consumers omit it (JSON default zeros).
                if version >= 17 && p.replica_directory_id != [0; 16] {
                    buf::put_tagged_fields(
                        buf,
                        &[(0, Bytes::copy_from_slice(&p.replica_directory_id))],
                    )?;
                } else {
                    buf::put_empty_tagged_fields(buf);
                }
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 7 {
        buf::put_array_len(buf, flexible, Some(forgotten.len()))?;
        for t in forgotten {
            put_fetch_topic_identity(buf, version, flexible, &t.topic, &t.topic_id)?;
            buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(*p);
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    if version >= 11 {
        // Fetch v11 RackId is STRING, not nullable (Apache JSON / kafka-protocol
        // 0.18.0). Kafka 3.9.1 rejects a null rackId. v12 is compact STRING.
        buf::put_string(buf, flexible, Some(rack_id.unwrap_or("")))?;
    }
    if flexible {
        let mut tags: Vec<(u32, Bytes)> = Vec::new();
        if version >= 12 {
            if let Some(cluster_id) = cluster_id {
                tags.push((0, encode_cluster_id(cluster_id)?));
            }
        }
        if version >= 15
            && (replica_state_replica_id != CONSUMER_REPLICA_ID
                || replica_state_replica_epoch != -1)
        {
            tags.push((
                1,
                encode_replica_state(replica_state_replica_id, replica_state_replica_epoch),
            ));
        }
        if tags.is_empty() {
            buf::put_empty_tagged_fields(buf);
        } else {
            buf::put_tagged_fields(buf, &tags)?;
        }
    }
    Ok(())
}

/// ReplicaState inside Fetch request tagged field 1 (13 bytes when
/// present: INT32 ReplicaId + INT64 ReplicaEpoch + empty nested tagged
/// fields).
fn encode_replica_state(replica_id: i32, replica_epoch: i64) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(replica_id);
    inner.put_i64(replica_epoch);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

/// ClusterId inside Fetch request tagged field 0 (compact STRING).
fn encode_cluster_id(cluster_id: &str) -> Result<Bytes> {
    let mut inner = BytesMut::new();
    buf::put_compact_string(&mut inner, Some(cluster_id))?;
    Ok(inner.freeze())
}

fn decode_replica_state(value: &Bytes) -> Result<(i32, i64)> {
    let mut cur = value.as_ref();
    let replica_id = buf::get_i32(&mut cur)?;
    let replica_epoch = buf::get_i64(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("ReplicaState leftover bytes"));
    }
    Ok((replica_id, replica_epoch))
}

fn decode_cluster_id(value: &Bytes) -> Result<Option<String>> {
    let mut cur = value.as_ref();
    let cluster_id = buf::get_compact_string(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("ClusterId leftover bytes"));
    }
    Ok(cluster_id)
}

fn decode_replica_directory_id(value: &Bytes) -> Result<[u8; 16]> {
    let mut cur = value.as_ref();
    let directory_id = buf::get_uuid(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("ReplicaDirectoryId leftover bytes"));
    }
    Ok(directory_id)
}

fn decode_fetch_request_partition_tags<B: Buf>(buf: &mut B, version: i16) -> Result<[u8; 16]> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut directory_id = [0u8; 16];
    for (tag, value) in tags {
        match tag {
            0 if version >= 17 => {
                directory_id = decode_replica_directory_id(&value)?;
            }
            _ => {}
        }
    }
    Ok(directory_id)
}

fn decode_fetch_request_tags<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i32, i64, Option<String>)> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut replica_id = CONSUMER_REPLICA_ID;
    let mut replica_epoch = -1i64;
    let mut cluster_id = None;
    for (tag, value) in tags {
        match tag {
            0 if version >= 12 => {
                cluster_id = decode_cluster_id(&value)?;
            }
            1 if version >= 15 => {
                (replica_id, replica_epoch) = decode_replica_state(&value)?;
            }
            _ => {}
        }
    }
    Ok((replica_id, replica_epoch, cluster_id))
}

/// `true` when Fetch `version` is flexible (v12+).
///
/// v4–v11 are classic. v12–v15 are compact arrays/strings/bytes plus tagged
/// fields (Apache JSON `flexibleVersions: "12+"`). Kafka 4.0 removed
/// v0–v3. v13 replaces topic names with topic ids (KIP-516). v14 is the
/// same layout as v13 (`OffsetMovedToTieredStorageException`). v15 drops
/// untagged ReplicaId and adds ReplicaState tagged field 1 (KIP-903;
/// consumers omit it; [`encode_fetch_request_with_replica_state`] writes
/// it). ClusterId tagged field 0 is decoded at v12+ (consumers omit it;
/// [`encode_fetch_request_with_cluster_id`] writes it). v16 is the same request as v15 (KIP-951). v17 is
/// the same consumer request as v16 when ReplicaDirectoryId is zeros
/// (partition tagged field 0, KIP-853; a non-zero directory id is written). This crate speaks 4–17. Partition
/// CurrentLeader tagged field 1, DivergingEpoch tagged field 0, and
/// SnapshotId tagged field 2 (`EndOffset` INT64 then `Epoch` INT32) are
/// decoded (v12+). Below v12 SnapshotId is omitted even when the body is
/// non-default; decode fills [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] /
/// [`EpochEndOffset::UNDEFINED_EPOCH`]. This is not the FetchSnapshot API
/// and does not start those RPCs. Top-level NodeEndpoints tagged field 0
/// is decoded at v16+ so unknown CurrentLeader brokers can be inserted
/// before apply. v18+ (KIP-1166 HighWatermark) is not spoken.
fn fetch_flexible(version: i16) -> Result<bool> {
    match version {
        4..=11 => Ok(false),
        12..=17 => Ok(true),
        other => Err(Error::protocol(format!(
            "Fetch version {other} is not implemented"
        ))),
    }
}

fn put_fetch_topic_identity(
    buf: &mut BytesMut,
    version: i16,
    flexible: bool,
    name: &str,
    topic_id: &[u8; 16],
) -> Result<()> {
    if version >= 13 {
        buf.extend_from_slice(topic_id);
        Ok(())
    } else {
        buf::put_string(buf, flexible, Some(name))
    }
}

fn get_fetch_topic_identity<B: Buf>(
    buf: &mut B,
    version: i16,
    flexible: bool,
) -> Result<(String, [u8; 16])> {
    if version >= 13 {
        Ok((String::new(), buf::get_uuid(buf)?))
    } else {
        Ok((
            buf::get_string(buf, flexible)?.unwrap_or_default(),
            [0u8; 16],
        ))
    }
}

/// EpochEndOffset inside Fetch partition tagged field 0 (13 bytes when
/// present: INT32 + INT64 + empty nested tagged fields).
fn encode_diverging_epoch(epoch: i32, end_offset: i64) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(epoch);
    inner.put_i64(end_offset);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_diverging_epoch(value: &Bytes) -> Result<(i32, i64)> {
    let mut cur = value.as_ref();
    let epoch = buf::get_i32(&mut cur)?;
    let end_offset = buf::get_i64(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("DivergingEpoch leftover bytes"));
    }
    Ok((epoch, end_offset))
}

/// LeaderIdAndEpoch inside Fetch partition tagged field 1 (9 bytes when
/// present: INT32 + INT32 + empty nested tagged fields).
fn encode_current_leader(leader_id: i32, leader_epoch: i32) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(leader_id);
    inner.put_i32(leader_epoch);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_current_leader(value: &Bytes) -> Result<(i32, i32)> {
    let mut cur = value.as_ref();
    let leader_id = buf::get_i32(&mut cur)?;
    let leader_epoch = buf::get_i32(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("CurrentLeader leftover bytes"));
    }
    Ok((leader_id, leader_epoch))
}

/// SnapshotId inside Fetch partition tagged field 2 (13 bytes when
/// present: INT64 EndOffset + INT32 Epoch + empty nested tagged fields).
/// JSON field order is the reverse of DivergingEpoch (tag 0: Epoch then
/// EndOffset).
fn encode_snapshot_id(end_offset: i64, epoch: i32) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i64(end_offset);
    inner.put_i32(epoch);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_snapshot_id(value: &Bytes) -> Result<(i64, i32)> {
    let mut cur = value.as_ref();
    let end_offset = buf::get_i64(&mut cur)?;
    let epoch = buf::get_i32(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("SnapshotId leftover bytes"));
    }
    Ok((end_offset, epoch))
}

fn encode_fetch_partition_tags(
    buf: &mut BytesMut,
    diverging_epoch: i32,
    diverging_end_offset: i64,
    current_leader_id: i32,
    current_leader_epoch: i32,
    snapshot_end_offset: i64,
    snapshot_epoch: i32,
) -> Result<()> {
    let mut fields: Vec<(u32, Bytes)> = Vec::new();
    if diverging_epoch >= 0 {
        fields.push((
            0,
            encode_diverging_epoch(diverging_epoch, diverging_end_offset),
        ));
    }
    if current_leader_id >= 0 {
        fields.push((
            1,
            encode_current_leader(current_leader_id, current_leader_epoch),
        ));
    }
    if snapshot_end_offset != EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        || snapshot_epoch != EpochEndOffset::UNDEFINED_EPOCH
    {
        fields.push((2, encode_snapshot_id(snapshot_end_offset, snapshot_epoch)));
    }
    if fields.is_empty() {
        buf::put_empty_tagged_fields(buf);
        Ok(())
    } else {
        buf::put_tagged_fields(buf, &fields)
    }
}

fn decode_fetch_partition_tags<B: Buf>(buf: &mut B) -> Result<(i32, i64, i32, i32, i64, i32)> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut diverging_epoch = EpochEndOffset::UNDEFINED_EPOCH;
    let mut diverging_end_offset = EpochEndOffset::UNDEFINED_EPOCH_OFFSET;
    let mut current_leader_id = MetadataResponse::NO_LEADER_ID;
    let mut current_leader_epoch = RecordBatch::NO_PARTITION_LEADER_EPOCH;
    let mut snapshot_end_offset = EpochEndOffset::UNDEFINED_EPOCH_OFFSET;
    let mut snapshot_epoch = EpochEndOffset::UNDEFINED_EPOCH;
    for (tag, value) in tags {
        match tag {
            0 => (diverging_epoch, diverging_end_offset) = decode_diverging_epoch(&value)?,
            1 => (current_leader_id, current_leader_epoch) = decode_current_leader(&value)?,
            2 => (snapshot_end_offset, snapshot_epoch) = decode_snapshot_id(&value)?,
            _ => {}
        }
    }
    Ok((
        diverging_epoch,
        diverging_end_offset,
        current_leader_id,
        current_leader_epoch,
        snapshot_end_offset,
        snapshot_epoch,
    ))
}

/// Decode Fetch: `(isolation_level, max_bytes, topics, rack_id, session,
/// forgotten, max_wait_ms, min_bytes, replica_id, replica_epoch, cluster_id)`.
///
/// `last_fetched_epoch` is [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]
/// below v12. `current_leader_epoch` is the same below v9. SessionId /
/// SessionEpoch / ForgottenTopicsData are v7+. Below v7 SessionId /
/// SessionEpoch are omitted; decode fills [`FetchMetadata::LEGACY`] and
/// an empty forgotten list. LogStartOffset is JSON `5+` (INT64 after
/// LastFetchedEpoch; official Java `FetchRequest.PartitionData.logStartOffset`;
/// below v5 decode fills [`INVALID_LOG_START_OFFSET`]). RackId is v11+;
/// below v11 decode fills empty. MaxWaitMs is JSON `0+` (INT32 after ReplicaId
/// on v4–v14; first untagged field on v15+; official Java
/// `FetchRequest.maxWait`). MinBytes is JSON `0+` (INT32 after MaxWaitMs;
/// official Java `FetchRequest.minBytes`). ReplicaId is JSON `0-14`
/// (untagged INT32; official Java `FetchRequest.replicaId()`; v15+ omitted
/// and filled from ReplicaState tagged field 1, or
/// [`CONSUMER_REPLICA_ID`] when that field is omitted). ReplicaEpoch is
/// ReplicaState.ReplicaEpoch (JSON `15+` default `-1`; omitted below v15
/// and when consumers skip the tag). ClusterId is JSON `12+` tagged field 0
/// (nullable compact STRING, default `null`; omitted below v12 and when
/// consumers skip the tag).
#[expect(
    clippy::type_complexity,
    reason = "Fetch request decode returns isolation, max bytes, topics, rack, session, forgotten, max wait, min bytes, replica id, replica epoch, and cluster id together"
)]
pub fn decode_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    i8,
    i32,
    Vec<FetchTopic>,
    String,
    FetchMetadata,
    Vec<ForgottenTopic>,
    i32,
    i32,
    i32,
    i64,
    Option<String>,
)> {
    let flexible = fetch_flexible(version)?;
    let untagged_replica_id = if version <= 14 {
        buf::get_i32(buf)?
    } else {
        CONSUMER_REPLICA_ID
    };
    let max_wait_ms = buf::get_i32(buf)?;
    let min_bytes = buf::get_i32(buf)?;
    let max_bytes = buf::get_i32(buf)?;
    let isolation = buf::get_i8(buf)?;
    let session = if version >= 7 {
        FetchMetadata::new(buf::get_i32(buf)?, buf::get_i32(buf)?)
    } else {
        FetchMetadata::LEGACY
    };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let (topic, topic_id) = get_fetch_topic_identity(buf, version, flexible)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let current_leader_epoch = if version >= 9 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let fetch_offset = buf::get_i64(buf)?;
            let last_fetched_epoch = if version >= 12 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let log_start_offset = if version >= 5 {
                buf::get_i64(buf)?
            } else {
                INVALID_LOG_START_OFFSET
            };
            let partition_max_bytes = buf::get_i32(buf)?;
            let replica_directory_id = if flexible {
                decode_fetch_request_partition_tags(buf, version)?
            } else {
                [0; 16]
            };
            partitions.push(FetchPartition {
                partition,
                current_leader_epoch,
                fetch_offset,
                last_fetched_epoch,
                log_start_offset,
                partition_max_bytes,
                replica_directory_id,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(FetchTopic {
            topic,
            topic_id,
            partitions,
        });
    }
    let mut forgotten_out = Vec::new();
    if version >= 7 {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        forgotten_out.reserve(n);
        for _ in 0..n {
            let (topic, topic_id) = get_fetch_topic_identity(buf, version, flexible)?;
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            forgotten_out.push(ForgottenTopic {
                topic,
                topic_id,
                partitions,
            });
        }
    }
    let rack = if version >= 11 {
        buf::get_string(buf, flexible)?.unwrap_or_default()
    } else {
        String::new()
    };
    let (replica_state_replica_id, replica_epoch, cluster_id) = if flexible {
        decode_fetch_request_tags(buf, version)?
    } else {
        (CONSUMER_REPLICA_ID, -1, None)
    };
    let replica_id = replica_id(version, untagged_replica_id, replica_state_replica_id);
    Ok((
        isolation,
        max_bytes,
        topics,
        rack,
        session,
        forgotten_out,
        max_wait_ms,
        min_bytes,
        replica_id,
        replica_epoch,
        cluster_id,
    ))
}

/// Encode a Fetch v4–v11 (classic) or v12–v17 (flexible) response.
///
/// ThrottleTimeMs is the JSON default (`0`) on v1+ (JSON `1+`).
/// Top-level ErrorCode is `0` and SessionId is
/// [`FetchMetadata::INVALID_SESSION_ID`]. Those fields are v7+; below v7
/// they are omitted. LogStartOffset is v5+. PreferredReadReplica is v11+.
/// Below those versions the fields are omitted even when the body is
/// non-default; decode fills [`FetchedPartition::INVALID_LOG_START_OFFSET`]
/// / [`FetchedPartition::INVALID_PREFERRED_REPLICA_ID`]. SnapshotId tagged
/// field 2 is v12+; below v12 it is omitted even when the body is
/// non-default; decode fills [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] /
/// [`EpochEndOffset::UNDEFINED_EPOCH`]. This is not the FetchSnapshot API
/// and does not start those RPCs.
pub fn encode_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[FetchedTopic],
) -> Result<()> {
    encode_fetch_response_with_endpoints(
        buf,
        version,
        topics,
        0,
        FetchMetadata::INVALID_SESSION_ID,
        &[],
    )
}

/// Encode Fetch v4–v17 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `1+`: written first on every spoken version
/// (this crate speaks 4–17). v4–v11 are classic. v12–v17 are flexible.
/// Kafka 4.0 `validVersions` is `4-17`. v18+ is not spoken. Official Java
/// `getErrorResponse` sets `throttleTimeMs` from the argument. Convenience
/// encode still writes `0`. Top-level ErrorCode is at bytes 4–5 on v7+.
/// ErrorCode / SessionId stay `0` / [`FetchMetadata::INVALID_SESSION_ID`]
/// (use [`encode_fetch_response_with_endpoints`]).
pub fn encode_fetch_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[FetchedTopic],
    throttle_time_ms: i32,
) -> Result<()> {
    encode_fetch_response_fields(
        buf,
        version,
        topics,
        0,
        FetchMetadata::INVALID_SESSION_ID,
        &[],
        throttle_time_ms,
    )
}

/// Encode Fetch plus top-level ErrorCode, SessionId, and NodeEndpoints
/// (v16+ tagged field 0).
///
/// ErrorCode and SessionId are v7+. Below v7 they are omitted even when
/// the body is non-zero; decode fills `0`. ThrottleTimeMs is the JSON
/// default (`0`).
pub fn encode_fetch_response_with_endpoints(
    buf: &mut BytesMut,
    version: i16,
    topics: &[FetchedTopic],
    error_code: i16,
    session_id: i32,
    endpoints: &[super::api::NodeEndpoint],
) -> Result<()> {
    encode_fetch_response_fields(buf, version, topics, error_code, session_id, endpoints, 0)
}

fn encode_fetch_response_fields(
    buf: &mut BytesMut,
    version: i16,
    topics: &[FetchedTopic],
    error_code: i16,
    session_id: i32,
    endpoints: &[super::api::NodeEndpoint],
    throttle_time_ms: i32,
) -> Result<()> {
    let flexible = fetch_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    if version >= 7 {
        buf.put_i16(error_code);
        buf.put_i32(session_id);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        put_fetch_topic_identity(buf, version, flexible, &t.topic, &t.topic_id)?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf.put_i64(p.high_watermark);
            buf.put_i64(p.last_stable_offset);
            if version >= 5 {
                buf.put_i64(p.log_start_offset);
            }
            buf::put_array_len(buf, flexible, Some(p.aborted_transactions.len()))?;
            for (pid, first) in &p.aborted_transactions {
                buf.put_i64(*pid);
                buf.put_i64(*first);
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
            if version >= 11 {
                buf.put_i32(p.preferred_read_replica);
            }
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            if recs.is_empty() {
                buf::put_bytes(buf, flexible, None)?;
            } else {
                buf::put_bytes(buf, flexible, Some(&recs))?;
            }
            if flexible {
                encode_fetch_partition_tags(
                    buf,
                    p.diverging_epoch,
                    p.diverging_end_offset,
                    p.current_leader_id,
                    p.current_leader_epoch,
                    p.snapshot_end_offset,
                    p.snapshot_epoch,
                )?;
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        super::api::encode_top_level_node_endpoints(buf, version >= 16, endpoints)?;
    }
    Ok(())
}

/// Decode a Fetch v4–v11 (classic) or v12–v17 (flexible) response:
/// `(topics, node_endpoints, error_code, session_id, throttle_time_ms)`.
///
/// ThrottleTimeMs is JSON `1+`; this crate speaks 4–17 so the field is
/// always on the wire. Below v7 ErrorCode and SessionId are omitted;
/// decode fills `0`.
/// LogStartOffset is v5+; PreferredReadReplica is v11+; below those
/// versions decode fills [`FetchedPartition::INVALID_LOG_START_OFFSET`] /
/// [`FetchedPartition::INVALID_PREFERRED_REPLICA_ID`]. SnapshotId tagged
/// field 2 is v12+; below v12 decode fills
/// [`EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] /
/// [`EpochEndOffset::UNDEFINED_EPOCH`].
#[expect(
    clippy::type_complexity,
    reason = "Fetch response decode returns topics, endpoints, error, session, and throttle together"
)]
pub fn decode_fetch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    Vec<FetchedTopic>,
    Vec<super::api::NodeEndpoint>,
    i16,
    i32,
    i32,
)> {
    let flexible = fetch_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = if version >= 7 { buf::get_i16(buf)? } else { 0 };
    let session_id = if version >= 7 {
        buf::get_i32(buf)?
    } else {
        FetchMetadata::INVALID_SESSION_ID
    };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let (topic, topic_id) = get_fetch_topic_identity(buf, version, flexible)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let high_watermark = buf::get_i64(buf)?;
            let last_stable_offset = buf::get_i64(buf)?;
            let log_start_offset = if version >= 5 {
                buf::get_i64(buf)?
            } else {
                FetchedPartition::INVALID_LOG_START_OFFSET
            };
            let aborted_len = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut aborted_transactions = Vec::with_capacity(aborted_len);
            for _ in 0..aborted_len {
                let pid = buf::get_i64(buf)?;
                let first = buf::get_i64(buf)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                aborted_transactions.push((pid, first));
            }
            let preferred_read_replica = if version >= 11 {
                buf::get_i32(buf)?
            } else {
                FetchedPartition::INVALID_PREFERRED_REPLICA_ID
            };
            let rec_bytes = if flexible {
                buf::take_compact_bytes(buf)?.unwrap_or_else(Bytes::new)
            } else {
                buf::take_classic_bytes(buf)?.unwrap_or_else(Bytes::new)
            };
            let records = if rec_bytes.is_empty() {
                Vec::new()
            } else {
                let mut rec_buf = rec_bytes;
                records::decode_record_batches(&mut rec_buf)?
            };
            let (
                diverging_epoch,
                diverging_end_offset,
                current_leader_id,
                current_leader_epoch,
                snapshot_end_offset,
                snapshot_epoch,
            ) = if flexible {
                decode_fetch_partition_tags(buf)?
            } else {
                (
                    EpochEndOffset::UNDEFINED_EPOCH,
                    EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
                    MetadataResponse::NO_LEADER_ID,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
                    EpochEndOffset::UNDEFINED_EPOCH,
                )
            };
            partitions.push(FetchedPartition {
                partition,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                preferred_read_replica,
                current_leader_id,
                current_leader_epoch,
                diverging_epoch,
                diverging_end_offset,
                snapshot_end_offset,
                snapshot_epoch,
                records,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(FetchedTopic {
            topic,
            topic_id,
            partitions,
        });
    }
    let endpoints = if flexible {
        super::api::decode_top_level_node_endpoints(buf, version >= 16)?
    } else {
        Vec::new()
    };
    Ok((topics, endpoints, error_code, session_id, throttle_time_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::{Buf, BufMut, Bytes};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn fetched_partition_invalid_sentinels_match_java() {
        assert_eq!(FetchedPartition::INVALID_HIGH_WATERMARK, -1);
        assert_eq!(FetchedPartition::INVALID_LAST_STABLE_OFFSET, -1);
        assert_eq!(FetchedPartition::INVALID_LOG_START_OFFSET, -1);
        assert_eq!(FetchedPartition::INVALID_PREFERRED_REPLICA_ID, -1);
        assert_eq!(MetadataResponse::NO_LEADER_ID, -1);
        assert_eq!(RecordBatch::NO_PARTITION_LEADER_EPOCH, -1);
        assert_eq!(EpochEndOffset::UNDEFINED_EPOCH, -1);
        assert_eq!(EpochEndOffset::UNDEFINED_EPOCH_OFFSET, -1);
        assert!(!FetchResponse::should_client_throttle(7));
        assert!(FetchResponse::should_client_throttle(8));
        let none = FetchedPartition::partition_response(0, 0);
        assert_eq!(
            none.high_watermark,
            FetchedPartition::INVALID_HIGH_WATERMARK
        );
        assert_eq!(
            none.last_stable_offset,
            FetchedPartition::INVALID_LAST_STABLE_OFFSET
        );
        assert_eq!(
            none.log_start_offset,
            FetchedPartition::INVALID_LOG_START_OFFSET
        );
        assert_eq!(
            none.preferred_read_replica,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        );
        assert_eq!(none.current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            none.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(none.diverging_epoch, EpochEndOffset::UNDEFINED_EPOCH);
        assert_eq!(
            none.diverging_end_offset,
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        );
        assert_eq!(
            none.snapshot_end_offset,
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        );
        assert_eq!(none.snapshot_epoch, EpochEndOffset::UNDEFINED_EPOCH);
        assert!(none.records.is_empty());
        assert_eq!(none.preferred_read_replica(), None);
        assert!(!none.is_preferred_replica());
        assert_eq!(none.diverging_epoch(), None);
        assert!(!none.is_diverging_epoch());
        assert_eq!(none.snapshot_id(), None);
        assert!(!none.is_snapshot_id());
        assert_eq!(none.records_size().unwrap(), 0);
        let unknown =
            FetchedPartition::partition_response(3, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(unknown.partition, 3);
        assert_eq!(unknown.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            unknown.high_watermark,
            FetchedPartition::INVALID_HIGH_WATERMARK
        );
        assert!(unknown.records.is_empty());
        let topic = FetchTopic {
            topic: "t".into(),
            topic_id: [1u8; 16],
            partitions: vec![
                FetchPartition {
                    partition: 0,
                    current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    fetch_offset: 0,
                    last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    log_start_offset: INVALID_LOG_START_OFFSET,
                    partition_max_bytes: 1,
                    replica_directory_id: [0; 16],
                },
                FetchPartition {
                    partition: 3,
                    current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    fetch_offset: 1,
                    last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    log_start_offset: INVALID_LOG_START_OFFSET,
                    partition_max_bytes: 1,
                    replica_directory_id: [0; 16],
                },
            ],
        };
        let v12 = topic.error_result(12, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(v12.topic, "t");
        assert_eq!(v12.topic_id, [1u8; 16]);
        assert_eq!(v12.partitions.len(), 2);
        let first = v12.partitions.first().expect("v12 partition");
        assert_eq!(first.partition, 0);
        assert_eq!(first.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            first.high_watermark,
            FetchedPartition::INVALID_HIGH_WATERMARK
        );
        assert!(first.records.is_empty());
        let third = v12.partitions.get(1).expect("v12 second partition");
        assert_eq!(third.partition, 3);
        let v13 = topic.error_result(13, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(v13.topic, "t");
        assert_eq!(v13.topic_id, [1u8; 16]);
        assert!(v13.partitions.is_empty());
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 12, std::slice::from_ref(&v12)).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 12).unwrap();
        assert!(
            !cur.has_remaining(),
            "Fetch getErrorResponse v12 leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        assert_eq!(decoded.len(), 1);
        let decoded_topic = decoded.first().expect("one topic");
        assert_eq!(decoded_topic.topic, "t");
        assert_eq!(decoded_topic.partitions.len(), 2);
        let mut v13_buf = BytesMut::new();
        encode_fetch_response(&mut v13_buf, 13, std::slice::from_ref(&v13)).unwrap();
        let mut cur = v13_buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 13).unwrap();
        assert!(
            !cur.has_remaining(),
            "Fetch getErrorResponse v13 leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        assert_eq!(decoded.len(), 1);
        assert!(decoded.first().expect("one topic").partitions.is_empty());
        let mut pref = none;
        pref.preferred_read_replica = 2;
        assert_eq!(pref.preferred_read_replica(), Some(2));
        assert!(pref.is_preferred_replica());
        pref.diverging_epoch = 3;
        pref.diverging_end_offset = 12;
        assert_eq!(pref.diverging_epoch(), Some((3, 12)));
        assert!(pref.is_diverging_epoch());
        pref.snapshot_end_offset = 20;
        pref.snapshot_epoch = 3;
        assert_eq!(pref.snapshot_id(), Some((20, 3)));
        assert!(pref.is_snapshot_id());
    }

    #[test]
    fn fetch_response_records_size_matches_java() {
        // Java FetchResponse.recordsSize: 0 when records are null or
        // MemoryRecords.EMPTY. Otherwise sizeInBytes of the records blob.
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let mut part = FetchedPartition::partition_response(0, 0);
        assert_eq!(part.records_size().unwrap(), 0);
        part.records = vec![RecordBatch::from_records(vec![rec])];
        let size = part.records_size().unwrap();
        assert!(size > 0, "non-empty recordsSize must be the blob length");
        let mut recs = BytesMut::new();
        let batch = part.records.first().expect("one batch");
        records::encode_record_batch(&mut recs, batch).unwrap();
        assert_eq!(
            size,
            buf::i32_from_usize(recs.len()).unwrap(),
            "recordsSize must match encoded records blob"
        );
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![part.clone()],
        }];
        for version in [11_i16, 12] {
            let mut buf = BytesMut::new();
            encode_fetch_response(&mut buf, version, &topics).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} recordsSize leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            let got = decoded
                .first()
                .and_then(|t| t.partitions.first())
                .expect("one partition");
            assert_eq!(
                got.records_size().unwrap(),
                size,
                "v{version} decoded recordsSize must match"
            );
        }
    }

    #[test]
    fn fetch_request_replica_id_sentinels_match_java() {
        assert_eq!(CONSUMER_REPLICA_ID, -1);
        assert_eq!(ORDINARY_CONSUMER_ID, -1);
        assert_eq!(DEBUGGING_CONSUMER_ID, -2);
        assert_eq!(FUTURE_LOCAL_REPLICA_ID, -3);
        assert_eq!(INVALID_LOG_START_OFFSET, -1);
        assert_eq!(
            INVALID_LOG_START_OFFSET,
            FetchedPartition::INVALID_LOG_START_OFFSET
        );
        assert!(is_consumer(CONSUMER_REPLICA_ID));
        assert!(is_consumer(ORDINARY_CONSUMER_ID));
        assert!(is_consumer(DEBUGGING_CONSUMER_ID));
        assert!(!is_consumer(FUTURE_LOCAL_REPLICA_ID));
        assert!(!is_consumer(1));
        assert!(is_valid_broker_id(0));
        assert!(is_valid_broker_id(1));
        assert!(!is_valid_broker_id(ORDINARY_CONSUMER_ID));
        assert_eq!(describe_replica_id(ORDINARY_CONSUMER_ID), "consumer");
        assert_eq!(describe_replica_id(DEBUGGING_CONSUMER_ID), "debug consumer");
        assert_eq!(
            describe_replica_id(FUTURE_LOCAL_REPLICA_ID),
            "future local replica"
        );
        assert_eq!(describe_replica_id(3), "replica [3]");
        assert_eq!(describe_replica_id(-4), "invalid replica [-4]");
        assert_eq!(DEFAULT_RESPONSE_MAX_BYTES, i32::MAX);
        assert!(is_from_follower(0));
        assert!(is_from_follower(1));
        assert!(!is_from_follower(CONSUMER_REPLICA_ID));
        assert!(!is_from_follower(DEBUGGING_CONSUMER_ID));
        assert!(!is_from_follower(FUTURE_LOCAL_REPLICA_ID));
        assert_eq!(is_from_follower(1), is_valid_broker_id(1));
    }

    #[test]
    fn fetch_request_replica_id_matches_java() {
        // Java FetchRequest.replicaId() uses untagged ReplicaId below v15
        // and ReplicaState.ReplicaId on v15+. Static replicaId(data) uses
        // untagged when it is not -1. Encode still writes CONSUMER_REPLICA_ID
        // through v14 and omits ReplicaState for consumers.
        assert_eq!(replica_id(14, CONSUMER_REPLICA_ID, 7), CONSUMER_REPLICA_ID);
        assert_eq!(replica_id(15, CONSUMER_REPLICA_ID, 7), 7);
        assert_eq!(replica_id(14, 3, 7), 3);
        assert_eq!(replica_id(15, 3, 7), 7);
        assert_eq!(replica_id_from_data(CONSUMER_REPLICA_ID, 7), 7);
        assert_eq!(replica_id_from_data(3, 7), 3);
        assert_eq!(
            replica_id_from_data(DEBUGGING_CONSUMER_ID, 7),
            DEBUGGING_CONSUMER_ID
        );
        assert_eq!(replica_id(15, DEBUGGING_CONSUMER_ID, 7), 7);
        assert_eq!(
            replica_id(14, DEBUGGING_CONSUMER_ID, 7),
            DEBUGGING_CONSUMER_ID
        );

        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [14_i16, 15] {
            let selected = replica_id(version, CONSUMER_REPLICA_ID, CONSUMER_REPLICA_ID);
            assert_eq!(selected, CONSUMER_REPLICA_ID);
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &topics, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, decoded, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(decoded.len(), 1);
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} replicaId leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [14_i16, 17] {
            let from_data = replica_id_from_data(CONSUMER_REPLICA_ID, CONSUMER_REPLICA_ID);
            assert_eq!(from_data, CONSUMER_REPLICA_ID);
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &[], None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, decoded, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(decoded.is_empty());
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} replicaId empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }

        // Kafka 4.0 FetchRequest.json ReplicaId is versions 0-14 (untagged
        // INT32; default -1). Official Java FetchRequestData.replicaId /
        // FetchRequest.replicaId() read it. Encode previously always wrote
        // CONSUMER_REPLICA_ID; decode discarded it. v15+ omits the untagged
        // field even when non-default (ReplicaState tagged field 1 is not
        // written). This crate speaks 4–17. This is not ReplicaState /
        // ListOffsets ReplicaId / OffsetForLeaderEpoch ReplicaId.
        for version in [4_i16, 11, 12, 14, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_replica_id(
                &mut buf, version, 10, 1, 1024, 0, &topics, None, 7,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., replica, _, _) = decode_fetch_request(&mut cur, version).unwrap();
            if version <= 14 {
                assert_eq!(replica, 7);
            } else {
                assert_eq!(replica, CONSUMER_REPLICA_ID);
            }
            assert!(
                cur.is_empty(),
                "Fetch request v{version} ReplicaId leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_request_with_replica_id(&mut with, 4, 10, 1, 1024, 0, &topics, None, 7)
            .unwrap();
        let mut consumer = BytesMut::new();
        encode_fetch_request(&mut consumer, 4, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &consumer[..],
            "v4 ReplicaId is not always CONSUMER_REPLICA_ID"
        );
        let (.., replica, _, _) = decode_fetch_request(&mut consumer.as_ref(), 4).unwrap();
        assert_eq!(replica, CONSUMER_REPLICA_ID);

        let mut v15_with = BytesMut::new();
        encode_fetch_request_with_replica_id(&mut v15_with, 15, 10, 1, 1024, 0, &topics, None, 7)
            .unwrap();
        let mut v15_consumer = BytesMut::new();
        encode_fetch_request(&mut v15_consumer, 15, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &v15_with[..],
            &v15_consumer[..],
            "v15+ omits untagged ReplicaId even when the body is non-default"
        );
    }

    #[test]
    fn fetch_request_max_wait_ms_matches_java() {
        // Kafka 4.0 FetchRequest.json MaxWaitMs is versions 0+ (INT32 after
        // ReplicaId on v0–v14; first untagged field on v15+). Official Java
        // FetchRequest.maxWait reads it. Encode already takes max_wait_ms;
        // decode previously discarded it. This crate speaks 4–17. This is
        // not MinBytes / MaxBytes / ShareFetch MaxWaitMs.
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [4_i16, 11, 12, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 3_600_000, 1, 1024, 0, &topics, None).unwrap();
            let mut cur = buf.as_ref();
            let (iso, max_bytes, got, rack, session, forgotten, max_wait, ..) =
                decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(iso, 0);
            assert_eq!(max_bytes, 1024);
            assert_eq!(got.len(), 1);
            assert!(forgotten.is_empty());
            assert_eq!(max_wait, 3_600_000);
            if version >= 11 {
                assert!(rack.is_empty());
            }
            if version >= 7 {
                assert_eq!(session, FetchMetadata::LEGACY);
            }
            assert!(
                cur.is_empty(),
                "Fetch request v{version} MaxWaitMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_request(&mut with, 4, 3_600_000, 1, 1024, 0, &topics, None).unwrap();
        let mut ten = BytesMut::new();
        encode_fetch_request(&mut ten, 4, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(&with[..], &ten[..], "v4 MaxWaitMs is not always 10");
        let mut cur = ten.as_ref();
        let (.., max_wait, _, _, _, _) = decode_fetch_request(&mut cur, 4).unwrap();
        assert_eq!(max_wait, 10);

        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 3_600_000, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &v15[..],
            "v4–v14 write ReplicaId then MaxWaitMs; v15+ MaxWaitMs is first untagged"
        );
    }

    #[test]
    fn fetch_request_min_bytes_matches_java() {
        // Kafka 4.0 FetchRequest.json MinBytes is versions 0+ (INT32 after
        // MaxWaitMs). Official Java FetchRequest.minBytes reads it. Encode
        // already takes min_bytes; decode previously discarded it. This
        // crate speaks 4–17. This is not MaxBytes / MaxWaitMs / ShareFetch
        // MinBytes.
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [4_i16, 11, 12, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 3_600_000, 1024, 0, &topics, None).unwrap();
            let mut cur = buf.as_ref();
            let (iso, max_bytes, got, rack, session, forgotten, max_wait, min_bytes, ..) =
                decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(iso, 0);
            assert_eq!(max_bytes, 1024);
            assert_eq!(got.len(), 1);
            assert!(forgotten.is_empty());
            assert_eq!(max_wait, 10);
            assert_eq!(min_bytes, 3_600_000);
            if version >= 11 {
                assert!(rack.is_empty());
            }
            if version >= 7 {
                assert_eq!(session, FetchMetadata::LEGACY);
            }
            assert!(
                cur.is_empty(),
                "Fetch request v{version} MinBytes leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_request(&mut with, 4, 10, 3_600_000, 1024, 0, &topics, None).unwrap();
        let mut one = BytesMut::new();
        encode_fetch_request(&mut one, 4, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(&with[..], &one[..], "v4 MinBytes is not always 1");
        let mut cur = one.as_ref();
        let (.., min_bytes, _, _, _) = decode_fetch_request(&mut cur, 4).unwrap();
        assert_eq!(min_bytes, 1);

        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 10, 3_600_000, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &v15[..],
            "v4 and v15 both write MinBytes (JSON 0+); v15 omits untagged ReplicaId"
        );
    }

    #[test]
    fn fetch_request_log_start_offset_matches_java() {
        // Kafka 4.0 FetchRequest.json partition LogStartOffset is versions
        // 5+ (INT64 after LastFetchedEpoch; default -1; ignorable). Official
        // Java FetchRequest.PartitionData.logStartOffset / Builder.build
        // setLogStartOffset. Encode previously hardcoded
        // INVALID_LOG_START_OFFSET; decode discarded it. This crate speaks
        // 4–17. This is not response LogStartOffset / Produce LogStartOffset
        // / ReplicaId.
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: 42,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [4_i16, 5, 11, 12, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &topics, None).unwrap();
            let mut cur = buf.as_ref();
            let (iso, max_bytes, got, rack, session, forgotten, ..) =
                decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(iso, 0);
            assert_eq!(max_bytes, 1024);
            assert_eq!(got.len(), 1);
            assert!(forgotten.is_empty());
            let part = got
                .first()
                .and_then(|t| t.partitions.first())
                .expect("fetch partition");
            if version >= 5 {
                assert_eq!(part.log_start_offset, 42);
            } else {
                assert_eq!(part.log_start_offset, INVALID_LOG_START_OFFSET);
            }
            if version >= 11 {
                assert!(rack.is_empty());
            }
            if version >= 7 {
                assert_eq!(session, FetchMetadata::LEGACY);
            }
            assert!(
                cur.is_empty(),
                "Fetch request v{version} LogStartOffset leftover-empty"
            );
        }

        let mut omitted = topics.clone();
        omitted
            .first_mut()
            .and_then(|t| t.partitions.first_mut())
            .expect("fetch partition")
            .log_start_offset = INVALID_LOG_START_OFFSET;

        let mut v4_with = BytesMut::new();
        encode_fetch_request(&mut v4_with, 4, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v4_omit = BytesMut::new();
        encode_fetch_request(&mut v4_omit, 4, 10, 1, 1024, 0, &omitted, None).unwrap();
        assert_eq!(
            &v4_with[..],
            &v4_omit[..],
            "v4 encode omits LogStartOffset even when the body is non-default"
        );

        let mut v5_with = BytesMut::new();
        encode_fetch_request(&mut v5_with, 5, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v5_omit = BytesMut::new();
        encode_fetch_request(&mut v5_omit, 5, 10, 1, 1024, 0, &omitted, None).unwrap();
        assert_ne!(
            &v5_with[..],
            &v5_omit[..],
            "v5 LogStartOffset is not always INVALID_LOG_START_OFFSET"
        );
        assert_ne!(
            &v4_with[..],
            &v5_with[..],
            "v5 adds request LogStartOffset even when SessionId is still omitted"
        );
    }

    #[test]
    fn fetch_response_topic_ids_matches_java() {
        fn topic(topic_id: [u8; 16]) -> FetchedTopic {
            FetchedTopic {
                topic: String::new(),
                topic_id,
                partitions: Vec::new(),
            }
        }
        assert!(FetchResponse::topic_ids(&[]).is_empty());
        assert!(FetchResponse::topic_ids(std::slice::from_ref(&topic([0; 16]))).is_empty());
        assert_eq!(
            FetchResponse::topic_ids(&[topic([0; 16]), topic([1; 16]), topic([2; 16])]),
            HashSet::from([[1; 16], [2; 16]])
        );
    }

    #[test]
    fn fetch_response_response_data_matches_java() {
        // Java FetchResponse.responseData: v4–v12 use topic(). v13+
        // looks up topicId in topicNames and skips a missing name
        // (name != null). LinkedHashMap.put overwrites the same pair.
        let p0 = FetchedPartition::partition_response(0, 0);
        let p1 = FetchedPartition::partition_response(1, crate::error::NOT_LEADER_OR_FOLLOWER);
        let overwrite =
            FetchedPartition::partition_response(0, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let named = vec![
            FetchedTopic {
                topic: "t".into(),
                topic_id: [0; 16],
                partitions: vec![p0.clone(), p1.clone()],
            },
            FetchedTopic {
                topic: "t".into(),
                topic_id: [0; 16],
                partitions: vec![overwrite.clone()],
            },
        ];
        let empty_names = HashMap::new();
        assert!(FetchResponse::response_data(12, &[], &empty_names).is_empty());
        let v12 = FetchResponse::response_data(12, &named, &empty_names);
        assert_eq!(v12.len(), 2);
        assert_eq!(
            v12.get(&("t".into(), 0)).map(|p| p.error_code),
            Some(crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
        assert_eq!(
            v12.get(&("t".into(), 1)).map(|p| p.error_code),
            Some(crate::error::NOT_LEADER_OR_FOLLOWER)
        );
        let topic_id = [1u8; 16];
        let id_topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id,
            partitions: vec![p0.clone(), p1.clone()],
        }];
        assert!(FetchResponse::response_data(13, &id_topics, &empty_names).is_empty());
        let names = HashMap::from([(topic_id, "resolved".into())]);
        let v13 = FetchResponse::response_data(13, &id_topics, &names);
        assert_eq!(v13.len(), 2);
        assert_eq!(
            v13.get(&("resolved".into(), 0)).map(|p| p.partition),
            Some(0)
        );
        assert_eq!(
            v13.get(&("resolved".into(), 1)).map(|p| p.partition),
            Some(1)
        );
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &named).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 11).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v11 responseData leftover-empty; leftover {} bytes",
            cur.len()
        );
        let decoded_map = FetchResponse::response_data(11, &decoded, &empty_names);
        assert_eq!(decoded_map.len(), 2);
        assert_eq!(
            decoded_map.get(&("t".into(), 0)).map(|p| p.error_code),
            Some(crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
        buf.clear();
        encode_fetch_response(&mut buf, 13, &id_topics).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 13).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v13 responseData leftover-empty; leftover {} bytes",
            cur.len()
        );
        let decoded_map = FetchResponse::response_data(13, &decoded, &names);
        assert_eq!(decoded_map.len(), 2);
        assert_eq!(
            decoded_map
                .get(&("resolved".into(), 0))
                .map(|p| p.error_code),
            Some(0)
        );
    }

    #[test]
    fn fetch_response_error_counts_matches_java() {
        assert_eq!(FetchResponse::error_counts(0, &[]), HashMap::from([(0, 1)]));
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0u8; 16],
            partitions: vec![
                FetchedPartition::partition_response(0, 0),
                FetchedPartition::partition_response(1, crate::error::NOT_LEADER_OR_FOLLOWER),
            ],
        }];
        assert_eq!(
            FetchResponse::error_counts(0, &topics),
            HashMap::from([(0, 2), (crate::error::NOT_LEADER_OR_FOLLOWER, 1)])
        );
        assert_eq!(
            FetchResponse::error_counts(crate::error::FETCH_SESSION_TOPIC_ID_ERROR, &[]),
            HashMap::from([(crate::error::FETCH_SESSION_TOPIC_ID_ERROR, 1)])
        );
        let same = FetchResponse::error_counts(
            crate::error::NOT_LEADER_OR_FOLLOWER,
            std::slice::from_ref(&FetchedTopic {
                topic: "t".into(),
                topic_id: [0u8; 16],
                partitions: vec![FetchedPartition::partition_response(
                    0,
                    crate::error::NOT_LEADER_OR_FOLLOWER,
                )],
            }),
        );
        assert_eq!(
            same,
            HashMap::from([(crate::error::NOT_LEADER_OR_FOLLOWER, 2)])
        );
    }

    #[test]
    fn fetch_response_to_message_matches_java() {
        // Java FetchResponse.toMessage: consecutive matchingTopic only
        // (non-zero previous topicId matches by id, else by name).
        // Non-adjacent same topic starts a new group (unlike ShareFetch).
        assert!(FetchResponse::to_message(&[]).is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let z = [0u8; 16];
        let body0 = FetchedPartition::partition_response(99, 0);
        let body1 =
            FetchedPartition::partition_response(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let body2 = FetchedPartition::partition_response(2, 0);
        let grouped = FetchResponse::to_message(&[
            (a, "alpha", 0, body0.clone()),
            (b, "beta", 1, body1.clone()),
            (a, "alpha", 3, body2.clone()),
        ]);
        assert_eq!(grouped.len(), 3, "non-adjacent same topicId stays split");
        let first = grouped.first().expect("first topic");
        assert_eq!(first.topic, "alpha");
        assert_eq!(first.topic_id, a);
        assert_eq!(first.partitions.len(), 1);
        assert_eq!(
            first.partitions.first().map(|part| part.partition),
            Some(0),
            "setPartitionIndex copies the key partition onto the body"
        );
        assert_eq!(body0.partition, 99);
        let second = grouped.get(1).expect("second topic");
        assert_eq!(second.topic, "beta");
        assert_eq!(second.topic_id, b);
        assert_eq!(
            second.partitions.first().map(|part| part.error_code),
            Some(crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
        let third = grouped.get(2).expect("third topic");
        assert_eq!(third.topic, "alpha");
        assert_eq!(third.topic_id, a);
        assert_eq!(third.partitions.first().map(|part| part.partition), Some(3));
        let consecutive = FetchResponse::to_message(&[
            (a, "alpha", 0, body0.clone()),
            (a, "other", 2, body2.clone()),
        ]);
        assert_eq!(consecutive.len(), 1, "non-zero topicId matches by id");
        let only = consecutive.first().expect("one topic");
        assert_eq!(only.topic, "alpha");
        assert_eq!(only.partitions.len(), 2);
        assert_eq!(only.partitions.get(1).map(|part| part.partition), Some(2));
        let by_name = FetchResponse::to_message(&[
            (z, "t", 0, body0.clone()),
            (a, "t", 4, body2.clone()),
            (z, "u", 5, body1.clone()),
        ]);
        assert_eq!(by_name.len(), 2, "zero previous topicId matches by name");
        let named_t = by_name.first().expect("t");
        assert_eq!(named_t.topic, "t");
        assert_eq!(named_t.topic_id, z);
        assert_eq!(named_t.partitions.len(), 2);
        assert_eq!(
            named_t.partitions.get(1).map(|part| part.partition),
            Some(4)
        );
        let named_u = by_name.get(1).expect("u");
        assert_eq!(named_u.topic, "u");
        assert_eq!(named_u.partitions.len(), 1);
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &by_name).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 11).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v11 toMessage leftover-empty; leftover {} bytes",
            cur.len()
        );
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.first().map(|topic| topic.topic.as_str()), Some("t"));
        assert_eq!(decoded.get(1).map(|topic| topic.topic.as_str()), Some("u"));
        buf.clear();
        encode_fetch_response(&mut buf, 13, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut cur, 13).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v13 toMessage leftover-empty; leftover {} bytes",
            cur.len()
        );
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.first().map(|topic| topic.topic_id), Some(a));
        assert_eq!(decoded.get(1).map(|topic| topic.topic_id), Some(b));
        assert_eq!(decoded.get(2).map(|topic| topic.topic_id), Some(a));
        assert_eq!(
            decoded
                .get(2)
                .and_then(|topic| topic.partitions.first())
                .map(|part| part.partition),
            Some(3)
        );
    }

    #[test]
    fn fetch_response_size_of_matches_java() {
        // Java 4.0 FetchResponse.sizeOf: 4 plus
        // toMessage(NONE, 0, INVALID_SESSION_ID, iterator, empty).size.
        // Official Java FetchResponse.sizeOf. Convenience encode still
        // writes throttle 0, ErrorCode 0, SessionId INVALID. This crate
        // speaks 4-17. This is not toMessage leftover / recordsSize
        // leftover / Request.getErrorResponse leftover.
        let empty = FetchResponse::size_of(4, &[]).unwrap();
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 4, &[]).unwrap();
        let body_len = buf::i32_from_usize(buf.len()).unwrap();
        assert_eq!(empty, body_len + 4, "sizeOf is 4 plus the encoded body");
        assert_ne!(empty, body_len);
        leftover_fetch_size_of(4, &[]);

        let a = [1u8; 16];
        let b = [2u8; 16];
        let body = FetchedPartition::partition_response(99, 0);
        let entries = [
            (a, "alpha", 0, body.clone()),
            (b, "beta", 1, body.clone()),
            (a, "alpha", 3, body.clone()),
        ];
        let topics = FetchResponse::to_message(&entries);
        assert_eq!(topics.len(), 3, "non-adjacent same topicId stays split");
        for version in [4_i16, 7, 12, 13, 17] {
            let size = FetchResponse::size_of(version, &entries).unwrap();
            buf.clear();
            encode_fetch_response(&mut buf, version, &topics).unwrap();
            let encoded_len = buf::i32_from_usize(buf.len()).unwrap();
            assert_eq!(size, encoded_len + 4);
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_fetch_response(&mut cur, version).unwrap();
            leftover_fetch_size_of_cur(version, cur);
            assert_eq!(decoded.len(), 3);
        }

        leftover_fetch_size_of(4, &[]);
        leftover_fetch_size_of(7, &[]);
        leftover_fetch_size_of(12, &[]);
        leftover_fetch_size_of(13, &[]);
        leftover_fetch_size_of(4, &topics);
        leftover_fetch_size_of(7, &topics);
        leftover_fetch_size_of(12, &topics);
        leftover_fetch_size_of(13, &topics);

        let consecutive = [(a, "alpha", 0, body.clone()), (a, "alpha", 3, body.clone())];
        let split = FetchResponse::size_of(12, &entries).unwrap();
        let merged = FetchResponse::size_of(12, &consecutive).unwrap();
        assert!(
            split > merged,
            "non-adjacent grouping is larger than consecutive matchingTopic"
        );

        buf.clear();
        encode_fetch_response_with_throttle(&mut buf, 7, &topics, 3_600_000).unwrap();
        let mut with_throttle = BytesMut::new();
        encode_fetch_response(&mut with_throttle, 7, &topics).unwrap();
        assert_eq!(
            buf.len(),
            with_throttle.len(),
            "ThrottleTimeMs is a fixed-width INT32"
        );
        buf.clear();
        encode_fetch_response_with_endpoints(
            &mut buf,
            7,
            &topics,
            crate::error::FETCH_SESSION_ID_NOT_FOUND,
            9,
            &[],
        )
        .unwrap();
        assert_eq!(
            buf.len(),
            with_throttle.len(),
            "ErrorCode and SessionId are fixed width on v7+"
        );
    }

    fn leftover_fetch_size_of(version: i16, topics: &[FetchedTopic]) {
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, version, topics).unwrap();
        let mut cur = buf.as_ref();
        drop(decode_fetch_response(&mut cur, version).unwrap());
        leftover_fetch_size_of_cur(version, cur);
    }

    fn leftover_fetch_size_of_cur(version: i16, cur: &[u8]) {
        let msg = match (version, cur.is_empty()) {
            (4, true) => "Fetch v4 Response.sizeOf leftover-empty",
            (7, true) => "Fetch v7 Response.sizeOf leftover-empty",
            (12, true) => "Fetch v12 Response.sizeOf leftover-empty",
            (13, true) => "Fetch v13 Response.sizeOf leftover-empty",
            _ => "Fetch Response.sizeOf leftover-empty",
        };
        assert!(cur.is_empty(), "{msg}; leftover {} bytes", cur.len());
    }

    #[test]
    fn fetch_response_of_matches_java() {
        // Java 4.0 FetchResponse.of: toMessage(error, throttleTimeMs,
        // sessionId, iterator, empty). Official Java FetchResponse.of
        // (empty NodeEndpoints). Convenience encode still writes throttle
        // 0, ErrorCode 0, SessionId INVALID. This crate speaks 4-17. This
        // is not toMessage leftover / sizeOf leftover / with_throttle
        // leftover / with_endpoints leftover / Request.getErrorResponse
        // leftover.
        let a = [1u8; 16];
        let b = [2u8; 16];
        let body = FetchedPartition::partition_response(99, 0);
        let entries = [
            (a, "alpha", 0, body.clone()),
            (b, "beta", 1, body.clone()),
            (a, "alpha", 3, body.clone()),
        ];
        let topics = FetchResponse::to_message(&entries);
        assert_eq!(topics.len(), 3, "non-adjacent same topicId stays split");

        let mut none = BytesMut::new();
        FetchResponse::of(
            &mut none,
            7,
            0,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &entries,
        )
        .unwrap();
        let mut conv = BytesMut::new();
        encode_fetch_response(&mut conv, 7, &topics).unwrap();
        assert_eq!(
            none, conv,
            "of(NONE, 0, INVALID) matches convenience encode"
        );

        let err = crate::error::FETCH_SESSION_ID_NOT_FOUND;
        let session = 9;
        let throttle = 3_600_000;
        for version in [4_i16, 7, 12, 13, 17] {
            let mut buf = BytesMut::new();
            FetchResponse::of(&mut buf, version, err, throttle, session, &entries).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, endpoints, error_code, session_id, throttle_time_ms) =
                decode_fetch_response(&mut cur, version).unwrap();
            leftover_fetch_of(version, cur);
            assert_eq!(decoded.len(), 3);
            assert!(endpoints.is_empty());
            assert_eq!(throttle_time_ms, throttle);
            if version >= 7 {
                assert_eq!(error_code, err);
                assert_eq!(session_id, session);
            } else {
                assert_eq!(error_code, 0);
                assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
            }
        }

        leftover_fetch_of_encode(4, err, throttle, session, &entries);
        leftover_fetch_of_encode(7, err, throttle, session, &entries);
        leftover_fetch_of_encode(12, err, throttle, session, &entries);
        leftover_fetch_of_encode(13, err, throttle, session, &entries);
        leftover_fetch_of_encode(4, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[]);
        leftover_fetch_of_encode(7, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[]);
        leftover_fetch_of_encode(12, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[]);
        leftover_fetch_of_encode(13, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[]);

        let mut of_buf = BytesMut::new();
        FetchResponse::of(&mut of_buf, 7, err, throttle, session, &entries).unwrap();
        let mut with_throttle = BytesMut::new();
        encode_fetch_response_with_throttle(&mut with_throttle, 7, &topics, throttle).unwrap();
        assert_ne!(
            of_buf, with_throttle,
            "of writes ErrorCode / SessionId; with_throttle writes 0 / INVALID"
        );
        let mut with_endpoints = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut with_endpoints, 7, &topics, err, session, &[])
            .unwrap();
        assert_ne!(
            of_buf, with_endpoints,
            "of writes ThrottleTimeMs from the argument; with_endpoints writes 0"
        );

        let mut v4_of = BytesMut::new();
        FetchResponse::of(&mut v4_of, 4, err, throttle, session, &entries).unwrap();
        let mut v4_throttle = BytesMut::new();
        encode_fetch_response_with_throttle(&mut v4_throttle, 4, &topics, throttle).unwrap();
        assert_eq!(
            v4_of, v4_throttle,
            "Fetch v4 omits ErrorCode / SessionId even when the body is non-zero"
        );

        let mut sized = BytesMut::new();
        FetchResponse::of(
            &mut sized,
            12,
            0,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &entries,
        )
        .unwrap();
        assert_eq!(
            FetchResponse::size_of(12, &entries).unwrap(),
            buf::i32_from_usize(sized.len()).unwrap() + 4
        );
    }

    fn leftover_fetch_of_encode(
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
        session_id: i32,
        entries: &[([u8; 16], &str, i32, FetchedPartition)],
    ) {
        let mut buf = BytesMut::new();
        FetchResponse::of(
            &mut buf,
            version,
            error_code,
            throttle_time_ms,
            session_id,
            entries,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        drop(decode_fetch_response(&mut cur, version).unwrap());
        leftover_fetch_of(version, cur);
    }

    fn leftover_fetch_of(version: i16, cur: &[u8]) {
        let msg = match (version, cur.is_empty()) {
            (4, true) => "Fetch v4 Response.of leftover-empty",
            (7, true) => "Fetch v7 Response.of leftover-empty",
            (12, true) => "Fetch v12 Response.of leftover-empty",
            (13, true) => "Fetch v13 Response.of leftover-empty",
            _ => "Fetch Response.of leftover-empty",
        };
        assert!(cur.is_empty(), "{msg}; leftover {} bytes", cur.len());
    }

    #[test]
    fn fetch_response_of_with_endpoints_matches_java() {
        // Java 4.0 FetchResponse.of(error, throttleTimeMs, sessionId,
        // responseData, nodeEndpoints). Official Java FetchResponse.of
        // with NodeEndpoints. The four-argument of is this helper with
        // empty endpoints. NodeEndpoints are v16+ tagged field 0; below
        // v16 encode omits them even when non-empty. This crate speaks
        // 4-17. This is not of leftover / toMessage leftover / sizeOf
        // leftover / with_throttle leftover / with_endpoints leftover /
        // Request.getErrorResponse leftover.
        let a = [1u8; 16];
        let b = [2u8; 16];
        let body = FetchedPartition::partition_response(99, 0);
        let entries = [
            (a, "alpha", 0, body.clone()),
            (b, "beta", 1, body.clone()),
            (a, "alpha", 3, body.clone()),
        ];
        let topics = FetchResponse::to_message(&entries);
        let endpoint = crate::protocol::api::NodeEndpoint::new(1, "h", 9092, None);
        let endpoints = std::slice::from_ref(&endpoint);
        let err = crate::error::FETCH_SESSION_ID_NOT_FOUND;
        let session = 9;
        let throttle = 3_600_000;

        let mut empty = BytesMut::new();
        FetchResponse::of_with_endpoints(
            &mut empty,
            16,
            0,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &entries,
            &[],
        )
        .unwrap();
        let mut of_empty = BytesMut::new();
        FetchResponse::of(
            &mut of_empty,
            16,
            0,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &entries,
        )
        .unwrap();
        assert_eq!(empty, of_empty, "of_with_endpoints empty list matches of");

        let mut v12 = BytesMut::new();
        FetchResponse::of_with_endpoints(&mut v12, 12, err, throttle, session, &entries, endpoints)
            .unwrap();
        let mut v12_of = BytesMut::new();
        FetchResponse::of(&mut v12_of, 12, err, throttle, session, &entries).unwrap();
        assert_eq!(
            v12, v12_of,
            "Fetch v12 omits NodeEndpoints even when non-empty"
        );
        leftover_fetch_of_endpoints_encode(4, err, throttle, session, &entries, endpoints);
        leftover_fetch_of_endpoints_encode(12, err, throttle, session, &entries, endpoints);
        leftover_fetch_of_endpoints_encode(16, err, throttle, session, &entries, endpoints);
        leftover_fetch_of_endpoints_encode(17, err, throttle, session, &entries, endpoints);
        leftover_fetch_of_endpoints_encode(16, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[], &[]);
        leftover_fetch_of_endpoints_encode(17, 0, 0, FetchMetadata::INVALID_SESSION_ID, &[], &[]);

        for version in [16_i16, 17] {
            let mut buf = BytesMut::new();
            FetchResponse::of_with_endpoints(
                &mut buf, version, err, throttle, session, &entries, endpoints,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, got_endpoints, error_code, session_id, throttle_time_ms) =
                decode_fetch_response(&mut cur, version).unwrap();
            leftover_fetch_of_endpoints(version, cur);
            assert_eq!(decoded.len(), 3);
            assert_eq!(got_endpoints, endpoints);
            assert_eq!(error_code, err);
            assert_eq!(session_id, session);
            assert_eq!(throttle_time_ms, throttle);
        }

        let mut of_ep = BytesMut::new();
        FetchResponse::of_with_endpoints(
            &mut of_ep, 16, err, throttle, session, &entries, endpoints,
        )
        .unwrap();
        let mut of_only = BytesMut::new();
        FetchResponse::of(&mut of_only, 16, err, throttle, session, &entries).unwrap();
        assert_ne!(
            of_ep, of_only,
            "of_with_endpoints writes NodeEndpoints on v16+"
        );
        let mut with_ep = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut with_ep, 16, &topics, err, session, endpoints)
            .unwrap();
        assert_ne!(
            of_ep, with_ep,
            "of_with_endpoints writes ThrottleTimeMs from the argument; with_endpoints writes 0"
        );
        let mut with_throttle = BytesMut::new();
        encode_fetch_response_with_throttle(&mut with_throttle, 16, &topics, throttle).unwrap();
        assert_ne!(
            of_ep, with_throttle,
            "of_with_endpoints writes ErrorCode / SessionId / NodeEndpoints"
        );
    }

    fn leftover_fetch_of_endpoints_encode(
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
        session_id: i32,
        entries: &[([u8; 16], &str, i32, FetchedPartition)],
        endpoints: &[crate::protocol::api::NodeEndpoint],
    ) {
        let mut buf = BytesMut::new();
        FetchResponse::of_with_endpoints(
            &mut buf,
            version,
            error_code,
            throttle_time_ms,
            session_id,
            entries,
            endpoints,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        drop(decode_fetch_response(&mut cur, version).unwrap());
        leftover_fetch_of_endpoints(version, cur);
    }

    fn leftover_fetch_of_endpoints(version: i16, cur: &[u8]) {
        let msg = match (version, cur.is_empty()) {
            (4, true) => "Fetch v4 Response.of endpoints leftover-empty",
            (12, true) => "Fetch v12 Response.of endpoints leftover-empty",
            (16, true) => "Fetch v16 Response.of endpoints leftover-empty",
            (17, true) => "Fetch v17 Response.of endpoints leftover-empty",
            _ => "Fetch Response.of endpoints leftover-empty",
        };
        assert!(cur.is_empty(), "{msg}; leftover {} bytes", cur.len());
    }

    #[test]
    fn fetch_request_fetch_data_matches_java() {
        // Java FetchRequest.fetchData: v4–v12 use topic(). v13+ looks up
        // topicId in topicNames and still inserts when the name is null.
        // LinkedHashMap.put overwrites the same TopicIdPartition.
        fn part(partition: i32, fetch_offset: i64) -> FetchPartition {
            FetchPartition {
                partition,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }
        }
        let p0 = part(0, 10);
        let p1 = part(1, 20);
        let overwrite = part(0, 99);
        let zeros = [0u8; 16];
        let named = vec![
            FetchTopic {
                topic: "t".into(),
                topic_id: zeros,
                partitions: vec![p0.clone(), p1.clone()],
            },
            FetchTopic {
                topic: "t".into(),
                topic_id: zeros,
                partitions: vec![overwrite.clone()],
            },
        ];
        let empty_names = HashMap::new();
        assert!(FetchRequest::fetch_data(12, &[], &empty_names).is_empty());
        let v12 = FetchRequest::fetch_data(12, &named, &empty_names);
        assert_eq!(v12.len(), 2);
        assert_eq!(
            v12.get(&(zeros, Some("t".into()), 0))
                .map(|p| p.fetch_offset),
            Some(99)
        );
        assert_eq!(
            v12.get(&(zeros, Some("t".into()), 1))
                .map(|p| p.fetch_offset),
            Some(20)
        );
        let topic_id = [1u8; 16];
        let id_topics = vec![FetchTopic {
            topic: String::new(),
            topic_id,
            partitions: vec![p0.clone(), p1.clone()],
        }];
        let unresolved = FetchRequest::fetch_data(13, &id_topics, &empty_names);
        assert_eq!(unresolved.len(), 2, "v13 missing name is still inserted");
        assert_eq!(
            unresolved.get(&(topic_id, None, 0)).map(|p| p.fetch_offset),
            Some(10)
        );
        let names = HashMap::from([(topic_id, "resolved".into())]);
        let v13 = FetchRequest::fetch_data(13, &id_topics, &names);
        assert_eq!(v13.len(), 2);
        assert_eq!(
            v13.get(&(topic_id, Some("resolved".into()), 0))
                .map(|p| p.partition),
            Some(0)
        );
        assert_eq!(
            v13.get(&(topic_id, Some("resolved".into()), 1))
                .map(|p| p.partition),
            Some(1)
        );
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &named, None).unwrap();
        let mut cur = buf.as_ref();
        let (_iso, _max, decoded, ..) = decode_fetch_request(&mut cur, 11).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v11 fetchData leftover-empty; leftover {} bytes",
            cur.len()
        );
        let decoded_map = FetchRequest::fetch_data(11, &decoded, &empty_names);
        assert_eq!(decoded_map.len(), 2);
        assert_eq!(
            decoded_map
                .get(&(zeros, Some("t".into()), 0))
                .map(|p| p.fetch_offset),
            Some(99)
        );
        buf.clear();
        encode_fetch_request(&mut buf, 13, 10, 1, 1024, 0, &id_topics, None).unwrap();
        let mut cur = buf.as_ref();
        let (_iso, _max, decoded, ..) = decode_fetch_request(&mut cur, 13).unwrap();
        assert!(
            cur.is_empty(),
            "Fetch v13 fetchData leftover-empty; leftover {} bytes",
            cur.len()
        );
        let decoded_map = FetchRequest::fetch_data(13, &decoded, &names);
        assert_eq!(decoded_map.len(), 2);
        assert_eq!(
            decoded_map
                .get(&(topic_id, Some("resolved".into()), 0))
                .map(|p| p.fetch_offset),
            Some(10)
        );
    }

    #[test]
    fn fetch_request_forgotten_topics_matches_java() {
        // Java FetchRequest.forgottenTopics: v4–v12 use topic(). v13+
        // looks up topicId in topicNames and still inserts when the name
        // is null. ArrayList keeps duplicate partitions.
        let zeros = [0u8; 16];
        let named = vec![
            ForgottenTopic {
                topic: "t".into(),
                topic_id: zeros,
                partitions: vec![0, 1],
            },
            ForgottenTopic {
                topic: "t".into(),
                topic_id: zeros,
                partitions: vec![0],
            },
        ];
        let empty_names = HashMap::new();
        assert!(FetchRequest::forgotten_topics(12, &[], &empty_names).is_empty());
        let v12 = FetchRequest::forgotten_topics(12, &named, &empty_names);
        assert_eq!(
            v12,
            vec![
                (zeros, Some("t".into()), 0),
                (zeros, Some("t".into()), 1),
                (zeros, Some("t".into()), 0),
            ]
        );
        let topic_id = [1u8; 16];
        let id_topics = vec![ForgottenTopic {
            topic: String::new(),
            topic_id,
            partitions: vec![0, 1],
        }];
        let unresolved = FetchRequest::forgotten_topics(13, &id_topics, &empty_names);
        assert_eq!(
            unresolved,
            vec![(topic_id, None, 0), (topic_id, None, 1)],
            "v13 missing name is still inserted"
        );
        let names = HashMap::from([(topic_id, "resolved".into())]);
        let v13 = FetchRequest::forgotten_topics(13, &id_topics, &names);
        assert_eq!(
            v13,
            vec![
                (topic_id, Some("resolved".into()), 0),
                (topic_id, Some("resolved".into()), 1),
            ]
        );

        let fetch = vec![FetchTopic {
            topic: "t".into(),
            topic_id: zeros,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 10,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [11_i16, 12] {
            let forgotten = FetchRequest::forgotten_topics(version, &named, &empty_names);
            assert_eq!(forgotten.len(), 3);
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &fetch, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} forgottenTopics leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [11_i16, 13] {
            let forgotten = FetchRequest::forgotten_topics(version, &[], &empty_names);
            assert!(forgotten.is_empty());
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &fetch, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} forgottenTopics empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn fetch_request_forgotten_from_removed_matches_java() {
        // Java FetchRequest.Builder.build ForgottenTopicsData: LinkedHashMap
        // keyed by topic name; first topicId for that name wins; partitions
        // append (duplicates kept). replaced is included only on v13+.
        // Encode still writes empty ForgottenTopicsData.
        let none: Vec<([u8; 16], &str, i32)> = Vec::new();
        assert!(FetchRequest::forgotten_from_removed(12, none.clone(), none.clone()).is_empty());
        assert!(FetchRequest::forgotten_from_removed(13, none.clone(), none.clone()).is_empty());

        let id_a = [1u8; 16];
        let id_b = [2u8; 16];
        let id_a2 = [3u8; 16];
        let id_c = [4u8; 16];
        let removed = [
            (id_a, "a", 0i32),
            (id_b, "b", 1),
            (id_a2, "a", 2),
            (id_a, "a", 0),
        ];
        let replaced = [(id_a2, "a", 3i32), (id_c, "c", 0)];

        let v12 = FetchRequest::forgotten_from_removed(12, removed, replaced);
        assert_eq!(
            v12,
            vec![
                ForgottenTopic {
                    topic: "a".into(),
                    topic_id: id_a,
                    partitions: vec![0, 2, 0],
                },
                ForgottenTopic {
                    topic: "b".into(),
                    topic_id: id_b,
                    partitions: vec![1],
                },
            ],
            "below v13 replaced is omitted; first topicId for a name is kept"
        );
        let v13 = FetchRequest::forgotten_from_removed(13, removed, replaced);
        assert_eq!(
            v13,
            vec![
                ForgottenTopic {
                    topic: "a".into(),
                    topic_id: id_a,
                    partitions: vec![0, 2, 0, 3],
                },
                ForgottenTopic {
                    topic: "b".into(),
                    topic_id: id_b,
                    partitions: vec![1],
                },
                ForgottenTopic {
                    topic: "c".into(),
                    topic_id: id_c,
                    partitions: vec![0],
                },
            ]
        );
        assert_eq!(
            FetchRequest::forgotten_topics(13, &v13, &HashMap::new()).len(),
            6,
            "Builder list flattened by forgottenTopics keeps duplicates"
        );

        let fetch = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 10,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        for version in [12_i16, 13] {
            let forgotten = FetchRequest::forgotten_from_removed(version, removed, replaced);
            assert!(!forgotten.is_empty());
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &fetch, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Builder.build forgotten leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [12_i16, 17] {
            let forgotten =
                FetchRequest::forgotten_from_removed(version, none.clone(), none.clone());
            assert!(forgotten.is_empty());
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &fetch, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Builder.build forgotten empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn fetch_request_topics_from_fetch_data_matches_java() {
        // Java FetchRequest.Builder.build Topics: consecutive same topic()
        // share one FetchTopic (first topicId kept). An intervening name
        // starts a new topic even when a later entry repeats an earlier
        // name. Encode still writes the caller's Topics as-is.
        assert!(FetchRequest::topics_from_fetch_data(std::iter::empty::<(
            &str,
            [u8; 16],
            FetchPartition,
        )>())
        .is_empty());

        let id_a = [1u8; 16];
        let id_a2 = [9u8; 16];
        let id_b = [2u8; 16];
        let p0 = FetchPartition {
            partition: 0,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            fetch_offset: 10,
            last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            log_start_offset: INVALID_LOG_START_OFFSET,
            partition_max_bytes: 1024,
            replica_directory_id: [0; 16],
        };
        let p1 = FetchPartition {
            partition: 1,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            fetch_offset: 11,
            last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            log_start_offset: INVALID_LOG_START_OFFSET,
            partition_max_bytes: 1024,
            replica_directory_id: [0; 16],
        };
        let p2 = FetchPartition {
            partition: 2,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            fetch_offset: 12,
            last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            log_start_offset: INVALID_LOG_START_OFFSET,
            partition_max_bytes: 1024,
            replica_directory_id: [0; 16],
        };
        let p0_dup = FetchPartition {
            partition: 0,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            fetch_offset: 99,
            last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            log_start_offset: INVALID_LOG_START_OFFSET,
            partition_max_bytes: 1,
            replica_directory_id: [0; 16],
        };

        let consecutive = FetchRequest::topics_from_fetch_data([
            ("a", id_a, p0.clone()),
            ("a", id_a2, p1.clone()),
            ("a", id_a, p0_dup.clone()),
        ]);
        assert_eq!(consecutive.len(), 1);
        let only = consecutive.first().expect("one topic");
        assert_eq!(only.topic, "a");
        assert_eq!(
            only.topic_id, id_a,
            "first topicId for a consecutive name is kept"
        );
        assert_eq!(only.partitions.len(), 3, "duplicate partitions are kept");
        assert_eq!(
            only.partitions
                .iter()
                .map(|part| part.partition)
                .collect::<Vec<_>>(),
            vec![0, 1, 0]
        );

        let split = FetchRequest::topics_from_fetch_data([
            ("a", id_a, p0.clone()),
            ("b", id_b, p1.clone()),
            ("a", id_a, p2.clone()),
        ]);
        assert_eq!(
            split.len(),
            3,
            "intervening name stays split (unlike forgotten_from_removed)"
        );
        assert_eq!(split.first().map(|topic| topic.topic.as_str()), Some("a"));
        assert_eq!(split.get(1).map(|topic| topic.topic.as_str()), Some("b"));
        assert_eq!(split.get(2).map(|topic| topic.topic.as_str()), Some("a"));
        assert_eq!(split.first().map(|topic| topic.partitions.len()), Some(1));

        for version in [12_i16, 13] {
            let grouped = FetchRequest::topics_from_fetch_data([("a", id_a, p0.clone())]);
            assert_eq!(grouped.len(), 1);
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &grouped, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Builder.build Topics leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [12_i16, 17] {
            let grouped = FetchRequest::topics_from_fetch_data(std::iter::empty::<(
                &str,
                [u8; 16],
                FetchPartition,
            )>());
            assert!(grouped.is_empty());
            let mut buf = BytesMut::new();
            encode_fetch_request(&mut buf, version, 10, 1, 1024, 0, &grouped, None).unwrap();
            let mut cur = buf.as_ref();
            let (_iso, _max, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Builder.build Topics empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn fetch_request_for_consumer_matches_java() {
        // Java 4.0 FetchRequest.Builder.forConsumer: oldest allowed
        // version is ApiKeys.FETCH.oldestVersion() (4 on Kafka 4.0);
        // latest is maxVersion; ReplicaId is CONSUMER_REPLICA_ID;
        // ReplicaEpoch is -1. Isolation defaults to READ_UNCOMMITTED
        // (0) and MaxBytes to DEFAULT_RESPONSE_MAX_BYTES. Official Java
        // FetchRequest.Builder.forConsumer. MaxWaitMs, MinBytes, and
        // Topics are the caller's values. Replica epoch lives on Java
        // ReplicaState (v15+ tagged field 1); consumers omit that field.
        // Encode still writes ReplicaId independently. This crate speaks
        // 4-17. This is not forgotten_from_removed /
        // topics_from_fetch_data / replicaId encode / ShareFetch
        // forConsumer.
        let (oldest, latest, replica_id, replica_epoch) = FetchRequest::for_consumer(17);
        assert_eq!(oldest, 4);
        assert_eq!(latest, 17);
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(replica_epoch, -1);
        assert_eq!(
            FetchRequest::for_consumer(4),
            (4, 4, CONSUMER_REPLICA_ID, -1)
        );
        assert!(is_consumer(replica_id));
        assert!(!is_from_follower(replica_id));
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_for_consumer(oldest, replica_id, &topics);
        leftover_for_consumer(oldest, replica_id, &[]);
        leftover_for_consumer(12, replica_id, &topics);
        leftover_for_consumer(12, replica_id, &[]);
        leftover_for_consumer(latest, replica_id, &topics);
        leftover_for_consumer(latest, replica_id, &[]);
    }

    fn leftover_for_consumer(version: i16, replica_id: i32, topics: &[FetchTopic]) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_epoch, -1);
        assert_eq!(got_replica, replica_id);
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} Builder.forConsumer {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_for_replica_matches_java() {
        // Java 4.0 FetchRequest.Builder.forReplica: oldest and latest
        // allowed versions are both allowedVersion; ReplicaId and
        // ReplicaEpoch are the arguments. Isolation defaults to
        // READ_UNCOMMITTED (0) and MaxBytes to
        // DEFAULT_RESPONSE_MAX_BYTES. Official Java
        // FetchRequest.Builder.forReplica. MaxWaitMs, MinBytes, and
        // Topics are the caller's values. Replica epoch lives on Java
        // ReplicaState (v15+ tagged field 1); this crate does not write
        // that field. Encode still writes ReplicaId independently on
        // v4-v14. This crate speaks 4-17. This is not forConsumer /
        // forgotten_from_removed / topics_from_fetch_data / replicaId
        // encode / ListOffsets forReplica.
        let (oldest, latest, replica_id, replica_epoch) = FetchRequest::for_replica(17, 7, 3);
        assert_eq!(oldest, 17);
        assert_eq!(latest, 17);
        assert_eq!(replica_id, 7);
        assert_eq!(replica_epoch, 3);
        assert_eq!(FetchRequest::for_replica(4, 7, 3), (4, 4, 7, 3));
        assert_eq!(
            FetchRequest::for_replica(4, CONSUMER_REPLICA_ID, -1),
            (4, 4, CONSUMER_REPLICA_ID, -1)
        );
        assert!(is_from_follower(replica_id));
        assert!(!is_consumer(replica_id));
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_for_replica(4, replica_id, &topics);
        leftover_for_replica(4, replica_id, &[]);
        leftover_for_replica(12, replica_id, &topics);
        leftover_for_replica(12, replica_id, &[]);
        leftover_for_replica(latest, replica_id, &topics);
        leftover_for_replica(latest, replica_id, &[]);
    }

    #[test]
    fn fetch_request_builder_matches_java() {
        // Java 4.0 FetchRequest.Builder(short minVersion, short maxVersion,
        // int replicaId, long replicaEpoch, ...): oldest is minVersion;
        // latest is maxVersion; ReplicaId and ReplicaEpoch are the
        // arguments. Isolation defaults to READ_UNCOMMITTED (0) and
        // MaxBytes to DEFAULT_RESPONSE_MAX_BYTES. Official Java
        // FetchRequest.Builder(short, short, int, long, ...). forConsumer
        // is this helper with oldest 4, ReplicaId CONSUMER_REPLICA_ID,
        // ReplicaEpoch -1. forReplica is this helper with min=max.
        // Encode still writes ReplicaId independently of this Builder
        // range. This crate speaks 4-17. This is not forConsumer /
        // forReplica / SimpleBuilder.build / replica_for_build /
        // forgotten_from_removed / topics_from_fetch_data.
        let (oldest, latest, replica_id, replica_epoch) = FetchRequest::builder(4, 17, 7, 3);
        assert_eq!(oldest, 4);
        assert_eq!(latest, 17);
        assert_eq!(replica_id, 7);
        assert_eq!(replica_epoch, 3);
        assert_eq!(
            FetchRequest::for_consumer(17),
            FetchRequest::builder(4, 17, CONSUMER_REPLICA_ID, -1)
        );
        assert_eq!(
            FetchRequest::for_replica(17, 7, 3),
            FetchRequest::builder(17, 17, 7, 3)
        );
        assert_eq!(
            FetchRequest::builder(13, 1, CONSUMER_REPLICA_ID, -1),
            (13, 1, CONSUMER_REPLICA_ID, -1)
        );
        assert!(is_from_follower(replica_id));
        assert!(!is_consumer(replica_id));
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_fetch_builder(4, replica_id, &topics);
        leftover_fetch_builder(4, replica_id, &[]);
        leftover_fetch_builder(12, replica_id, &topics);
        leftover_fetch_builder(12, replica_id, &[]);
        leftover_fetch_builder(latest, replica_id, &topics);
        leftover_fetch_builder(latest, replica_id, &[]);
    }

    fn leftover_fetch_builder(version: i16, replica_id: i32, topics: &[FetchTopic]) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_replica_id(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            replica_id,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_epoch, -1);
        if version <= 14 {
            assert_eq!(got_replica, replica_id);
        } else {
            assert_eq!(got_replica, CONSUMER_REPLICA_ID);
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} Builder.minVersion.maxVersion {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_partition_data_matches_java() {
        // Java 4.0 FetchRequest.PartitionData(Uuid, long, long, int,
        // Optional, Optional): fetchOffset / logStartOffset / maxBytes /
        // currentLeaderEpoch / lastFetchedEpoch from the arguments.
        // Official Java FetchRequest.PartitionData. This type stores
        // partitionIndex; Java topicId lives on FetchTopic. ReplicaDirectoryId
        // stays zeros. The five-argument Java constructor is this helper
        // with empty lastFetchedEpoch. Encode still writes independently
        // (below v5 omits LogStartOffset; decode fills
        // INVALID_LOG_START_OFFSET; below v9 omits CurrentLeaderEpoch;
        // decode fills NO_PARTITION_LEADER_EPOCH; below v12 omits
        // LastFetchedEpoch; decode fills NO_PARTITION_LEADER_EPOCH). This
        // crate speaks 4-17. This is not fetch_data /
        // topics_from_fetch_data / ReplicaDirectoryId / replicaId encode.
        let none = FetchPartition::partition_data(0, 0, INVALID_LOG_START_OFFSET, 1, None, None);
        assert_eq!(none.partition, 0);
        assert_eq!(none.fetch_offset, 0);
        assert_eq!(none.log_start_offset, INVALID_LOG_START_OFFSET);
        assert_eq!(none.partition_max_bytes, 1);
        assert_eq!(
            none.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(
            none.last_fetched_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(none.replica_directory_id, [0; 16]);
        let with = FetchPartition::partition_data(3, 42, 10, 1024, Some(8), Some(7));
        assert_eq!(with.partition, 3);
        assert_eq!(with.fetch_offset, 42);
        assert_eq!(with.log_start_offset, 10);
        assert_eq!(with.partition_max_bytes, 1024);
        assert_eq!(with.current_leader_epoch, 8);
        assert_eq!(with.last_fetched_epoch, 7);
        assert_eq!(with.replica_directory_id, [0; 16]);
        let five = FetchPartition::partition_data(3, 42, 10, 1024, Some(8), None);
        assert_eq!(five.partition, 3);
        assert_eq!(five.fetch_offset, 42);
        assert_eq!(five.log_start_offset, 10);
        assert_eq!(five.partition_max_bytes, 1024);
        assert_eq!(five.current_leader_epoch, 8);
        assert_eq!(
            five.last_fetched_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(five.replica_directory_id, [0; 16]);
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![with],
        }];
        leftover_fetch_partition_data(4, &topics);
        leftover_fetch_partition_data(4, &[]);
        leftover_fetch_partition_data(9, &topics);
        leftover_fetch_partition_data(9, &[]);
        leftover_fetch_partition_data(12, &topics);
        leftover_fetch_partition_data(12, &[]);
        leftover_fetch_partition_data(17, &topics);
        leftover_fetch_partition_data(17, &[]);
    }

    fn leftover_fetch_partition_data(version: i16, topics: &[FetchTopic]) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (isolation, max_bytes, decoded, rack, ..) =
            decode_fetch_request(&mut cur, version).unwrap();
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            let want = topic.partitions.first().expect("one partition");
            let got_part = got.partitions.first().expect("one partition");
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(got_part.partition, want.partition);
            assert_eq!(got_part.fetch_offset, want.fetch_offset);
            assert_eq!(got_part.partition_max_bytes, want.partition_max_bytes);
            assert_eq!(got_part.replica_directory_id, [0; 16]);
            if version >= 5 {
                assert_eq!(got_part.log_start_offset, want.log_start_offset);
            } else {
                assert_eq!(got_part.log_start_offset, INVALID_LOG_START_OFFSET);
            }
            if version >= 9 {
                assert_eq!(got_part.current_leader_epoch, want.current_leader_epoch);
            } else {
                assert_eq!(
                    got_part.current_leader_epoch,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH
                );
            }
            if version >= 12 {
                assert_eq!(got_part.last_fetched_epoch, want.last_fetched_epoch);
            } else {
                assert_eq!(
                    got_part.last_fetched_epoch,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH
                );
            }
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} PartitionData {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    fn leftover_for_replica(version: i16, replica_id: i32, topics: &[FetchTopic]) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_replica_id(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            replica_id,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_epoch, -1);
        if version <= 14 {
            assert_eq!(got_replica, replica_id);
        } else {
            assert_eq!(got_replica, CONSUMER_REPLICA_ID);
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} Builder.forReplica {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_simple_builder_build_matches_java() {
        // Java 4.0 FetchRequest.SimpleBuilder.build: untagged ReplicaId
        // must be < 0 (IllegalStateException otherwise). Below v15 the
        // untagged ReplicaId becomes ReplicaState.ReplicaId and
        // ReplicaState is reset to new ReplicaState() (ReplicaId -1).
        // v15+ leaves both as passed. Official Java
        // FetchRequest.SimpleBuilder.build. Encode still writes untagged
        // ReplicaId on v4-v14 and omits ReplicaState on v15+. This crate
        // speaks 4-17. This is not replicaId() / replicaId(data) /
        // forConsumer / forReplica.
        let placed = FetchRequest::simple_build(14, 0, 7).unwrap_err();
        assert!(
            matches!(placed, Error::Protocol(_)),
            "untagged ReplicaId >= 0 is Java IllegalStateException, got {placed}"
        );
        assert!(
            placed.to_string().contains(
                "The replica id should be placed in the replicaState of a fetchRequestData"
            ),
            "got {placed}"
        );
        let broker = FetchRequest::simple_build(15, 7, 3).unwrap_err();
        assert!(
            matches!(broker, Error::Protocol(_)),
            "v15+ still rejects untagged ReplicaId >= 0, got {broker}"
        );
        assert_eq!(
            FetchRequest::simple_build(14, CONSUMER_REPLICA_ID, 7).unwrap(),
            (7, CONSUMER_REPLICA_ID)
        );
        assert_eq!(
            FetchRequest::simple_build(4, DEBUGGING_CONSUMER_ID, 7).unwrap(),
            (7, CONSUMER_REPLICA_ID)
        );
        assert_eq!(
            FetchRequest::simple_build(15, CONSUMER_REPLICA_ID, 7).unwrap(),
            (CONSUMER_REPLICA_ID, 7)
        );
        assert_eq!(
            FetchRequest::simple_build(17, FUTURE_LOCAL_REPLICA_ID, 7).unwrap(),
            (FUTURE_LOCAL_REPLICA_ID, 7)
        );
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_simple_build(4, CONSUMER_REPLICA_ID, 7, &topics);
        leftover_simple_build(4, CONSUMER_REPLICA_ID, 7, &[]);
        leftover_simple_build(14, DEBUGGING_CONSUMER_ID, 7, &topics);
        leftover_simple_build(14, DEBUGGING_CONSUMER_ID, 7, &[]);
        leftover_simple_build(17, CONSUMER_REPLICA_ID, 7, &topics);
        leftover_simple_build(17, CONSUMER_REPLICA_ID, 7, &[]);
    }

    fn leftover_simple_build(
        version: i16,
        replica_id: i32,
        replica_state_replica_id: i32,
        topics: &[FetchTopic],
    ) {
        let (untagged, _) =
            FetchRequest::simple_build(version, replica_id, replica_state_replica_id).unwrap();
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_replica_id(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            untagged,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_epoch, -1);
        if version <= 14 {
            assert_eq!(got_replica, untagged);
        } else {
            assert_eq!(got_replica, CONSUMER_REPLICA_ID);
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} SimpleBuilder.build {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_replica_for_build_matches_java() {
        // Java 4.0 FetchRequest.Builder.build ReplicaId / ReplicaState:
        // below v15 untagged ReplicaId is the builder replica id and
        // ReplicaState stays at JSON defaults (-1, -1). v15+ untagged
        // ReplicaId stays -1 and ReplicaState takes replicaId and
        // replicaEpoch. Official Java FetchRequest.Builder.build.
        // MaxBytes below v3 is DEFAULT_RESPONSE_MAX_BYTES (crate speaks
        // v4+). Encode still writes untagged ReplicaId on v4-v14 and
        // omits ReplicaState on v15+. This crate speaks 4-17. This is
        // not replicaId() / replicaId(data) / SimpleBuilder.build /
        // forConsumer / forReplica.
        assert_eq!(
            FetchRequest::replica_for_build(4, CONSUMER_REPLICA_ID, -1),
            (CONSUMER_REPLICA_ID, CONSUMER_REPLICA_ID, -1)
        );
        assert_eq!(
            FetchRequest::replica_for_build(4, 7, 3),
            (7, CONSUMER_REPLICA_ID, -1)
        );
        assert_eq!(
            FetchRequest::replica_for_build(14, 7, 3),
            (7, CONSUMER_REPLICA_ID, -1)
        );
        assert_eq!(
            FetchRequest::replica_for_build(17, 7, 3),
            (CONSUMER_REPLICA_ID, 7, 3)
        );
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_replica_for_build(4, 7, 3, &topics);
        leftover_replica_for_build(4, 7, 3, &[]);
        leftover_replica_for_build(14, 7, 3, &topics);
        leftover_replica_for_build(14, 7, 3, &[]);
        leftover_replica_for_build(17, 7, 3, &topics);
        leftover_replica_for_build(17, 7, 3, &[]);
    }

    fn leftover_replica_for_build(
        version: i16,
        replica_id: i32,
        replica_epoch: i64,
        topics: &[FetchTopic],
    ) {
        let (untagged, ..) = FetchRequest::replica_for_build(version, replica_id, replica_epoch);
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_replica_id(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            untagged,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_epoch, -1);
        if version <= 14 {
            assert_eq!(got_replica, untagged);
        } else {
            assert_eq!(got_replica, CONSUMER_REPLICA_ID);
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} Builder.build replica {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_replica_state_matches_java() {
        // Kafka 4.0.0 FetchRequest.json ReplicaState is versions 15+
        // tagged field 1 (ReplicaId INT32 default -1, ReplicaEpoch INT64
        // default -1). Official Java FetchRequest.Builder.build writes
        // ReplicaState on v15+ and untagged ReplicaId below v15. Encode
        // previously omitted the tag even when ReplicaId was not -1.
        // encode_fetch_request_with_replica_id still omits ReplicaState.
        // This crate speaks 4-17. This is not ClusterId tagged field 0 /
        // partition ReplicaDirectoryId / replicaId() / forReplica leftover.
        let replica_id = 7;
        let replica_epoch = 3i64;
        assert_eq!(
            FetchRequest::replica_for_build(14, replica_id, replica_epoch),
            (replica_id, CONSUMER_REPLICA_ID, -1)
        );
        assert_eq!(
            FetchRequest::replica_for_build(15, replica_id, replica_epoch),
            (CONSUMER_REPLICA_ID, replica_id, replica_epoch)
        );
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_replica_state(4, replica_id, replica_epoch, &topics);
        leftover_replica_state(4, replica_id, replica_epoch, &[]);
        leftover_replica_state(14, replica_id, replica_epoch, &topics);
        leftover_replica_state(14, replica_id, replica_epoch, &[]);
        leftover_replica_state(15, replica_id, replica_epoch, &topics);
        leftover_replica_state(15, replica_id, replica_epoch, &[]);
        leftover_replica_state(17, replica_id, replica_epoch, &topics);
        leftover_replica_state(17, replica_id, replica_epoch, &[]);
        leftover_replica_state(15, CONSUMER_REPLICA_ID, -1, &topics);
        leftover_replica_state(15, CONSUMER_REPLICA_ID, -1, &[]);

        let mut with = BytesMut::new();
        encode_fetch_request_with_replica_state(
            &mut with,
            15,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            replica_id,
            replica_epoch,
        )
        .unwrap();
        let mut omitted = BytesMut::new();
        encode_fetch_request_with_replica_id(
            &mut omitted,
            15,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            replica_id,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &omitted[..],
            "v15 ReplicaState tagged field 1 is not omitted when ReplicaId is not -1"
        );
        let mut consumer = BytesMut::new();
        encode_fetch_request(&mut consumer, 15, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &omitted[..],
            &consumer[..],
            "encode_fetch_request_with_replica_id still omits ReplicaState on v15+"
        );
    }

    fn leftover_replica_state(
        version: i16,
        replica_id: i32,
        replica_epoch: i64,
        topics: &[FetchTopic],
    ) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_replica_state(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            replica_id,
            replica_epoch,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert!(got_cluster.is_none());
        assert_eq!(got_replica, replica_id);
        if version >= 15 {
            assert_eq!(got_epoch, replica_epoch);
        } else {
            assert_eq!(got_epoch, -1);
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} ReplicaState {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_cluster_id_matches_java() {
        // Kafka 4.0.0 FetchRequest.json ClusterId is versions 12+ tagged
        // field 0 (nullable compact STRING, default null, ignorable).
        // Official Java FetchRequestData.clusterId. Consumers omit it.
        // encode_fetch_request and encode_fetch_request_with_replica_state
        // still omit ClusterId. This crate speaks 4-17. This is not
        // ReplicaState tagged field 1 / partition ReplicaDirectoryId /
        // replicaId encode leftover.
        let replica_id = 7;
        let replica_epoch = 3i64;
        let topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        leftover_cluster_id(4, Some("mk"), CONSUMER_REPLICA_ID, -1, &topics);
        leftover_cluster_id(4, Some("mk"), CONSUMER_REPLICA_ID, -1, &[]);
        leftover_cluster_id(11, Some("mk"), CONSUMER_REPLICA_ID, -1, &topics);
        leftover_cluster_id(11, Some("mk"), CONSUMER_REPLICA_ID, -1, &[]);
        leftover_cluster_id(12, Some("mk"), CONSUMER_REPLICA_ID, -1, &topics);
        leftover_cluster_id(12, Some("mk"), CONSUMER_REPLICA_ID, -1, &[]);
        leftover_cluster_id(12, None, CONSUMER_REPLICA_ID, -1, &topics);
        leftover_cluster_id(12, Some(""), CONSUMER_REPLICA_ID, -1, &topics);
        leftover_cluster_id(15, Some("mk"), replica_id, replica_epoch, &topics);
        leftover_cluster_id(15, Some("mk"), replica_id, replica_epoch, &[]);
        leftover_cluster_id(15, None, replica_id, replica_epoch, &topics);
        leftover_cluster_id(17, Some("mk"), replica_id, replica_epoch, &topics);
        leftover_cluster_id(17, Some("mk"), replica_id, replica_epoch, &[]);
        leftover_cluster_id(17, None, CONSUMER_REPLICA_ID, -1, &topics);

        let mut with = BytesMut::new();
        encode_fetch_request_with_cluster_id(
            &mut with,
            12,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            CONSUMER_REPLICA_ID,
            -1,
            Some("mk"),
        )
        .unwrap();
        let mut omitted = BytesMut::new();
        encode_fetch_request(&mut omitted, 12, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &omitted[..],
            "v12 ClusterId tagged field 0 is not omitted when set"
        );
        let mut none = BytesMut::new();
        encode_fetch_request_with_cluster_id(
            &mut none,
            12,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            CONSUMER_REPLICA_ID,
            -1,
            None,
        )
        .unwrap();
        assert_eq!(
            &none[..],
            &omitted[..],
            "v12 ClusterId tagged field 0 is omitted when null"
        );
        let mut below = BytesMut::new();
        encode_fetch_request_with_cluster_id(
            &mut below,
            11,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            CONSUMER_REPLICA_ID,
            -1,
            Some("mk"),
        )
        .unwrap();
        let mut v11 = BytesMut::new();
        encode_fetch_request(&mut v11, 11, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &below[..],
            &v11[..],
            "below v12 ClusterId is omitted even when Some"
        );
        let mut both = BytesMut::new();
        encode_fetch_request_with_cluster_id(
            &mut both,
            15,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            replica_id,
            replica_epoch,
            Some("mk"),
        )
        .unwrap();
        let mut replica_only = BytesMut::new();
        encode_fetch_request_with_replica_state(
            &mut replica_only,
            15,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            replica_id,
            replica_epoch,
        )
        .unwrap();
        assert_ne!(
            &both[..],
            &replica_only[..],
            "v15 ClusterId tagged field 0 is written before ReplicaState tagged field 1"
        );
    }

    fn leftover_cluster_id(
        version: i16,
        cluster_id: Option<&str>,
        replica_id: i32,
        replica_epoch: i64,
        topics: &[FetchTopic],
    ) {
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request_with_cluster_id(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            topics,
            None,
            replica_id,
            replica_epoch,
            cluster_id,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (
            isolation,
            max_bytes,
            decoded,
            rack,
            session,
            forgotten,
            got_wait,
            got_min,
            got_replica,
            got_epoch,
            got_cluster,
        ) = decode_fetch_request(&mut cur, version).unwrap();
        assert_eq!(got_replica, replica_id);
        if version >= 15 {
            assert_eq!(got_epoch, replica_epoch);
        } else {
            assert_eq!(got_epoch, -1);
        }
        if version >= 12 {
            assert_eq!(got_cluster.as_deref(), cluster_id);
        } else {
            assert!(got_cluster.is_none());
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(got_wait, max_wait_ms);
        assert_eq!(got_min, min_bytes);
        assert_eq!(decoded.len(), topics.len());
        if let Some(topic) = topics.first() {
            let got = decoded.first().expect("one topic");
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
            assert_eq!(got.partitions.len(), topic.partitions.len());
            assert_eq!(
                got.partitions.first().map(|p| p.partition),
                topic.partitions.first().map(|p| p.partition)
            );
        }
        assert!(forgotten.is_empty());
        if version >= 7 {
            assert_eq!(session, FetchMetadata::LEGACY);
        }
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} ClusterId {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_replica_directory_id_matches_java() {
        // Kafka 4.0.0 FetchRequest.json ReplicaDirectoryId is versions 17+
        // partition tagged field 0 (UUID, default zeros, ignorable).
        // Official Java FetchRequestData.FetchPartition.replicaDirectoryId
        // (KIP-853). Consumers omit the tag. Below v17 encode omits even
        // when non-zero; decode fills zeros. This crate speaks 4-17. This
        // is not ClusterId tagged field 0 / ReplicaState tagged field 1 /
        // response DivergingEpoch tagged field 0.
        let directory = [9u8; 16];
        let zeros = [0u8; 16];
        let mut topics = [FetchTopic {
            topic: "t".into(),
            topic_id: [7u8; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 0,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: directory,
            }],
        }];
        leftover_replica_directory_id(4, directory, &topics);
        leftover_replica_directory_id(4, directory, &[]);
        leftover_replica_directory_id(16, directory, &topics);
        leftover_replica_directory_id(16, directory, &[]);
        leftover_replica_directory_id(17, directory, &topics);
        leftover_replica_directory_id(17, directory, &[]);
        leftover_replica_directory_id(17, zeros, &topics);
        leftover_replica_directory_id(17, zeros, &[]);

        let mut with = BytesMut::new();
        encode_fetch_request(&mut with, 17, 10, 1, 1024, 0, &topics, None).unwrap();
        topics
            .first_mut()
            .expect("topic")
            .partitions
            .first_mut()
            .expect("partition")
            .replica_directory_id = zeros;
        let mut omitted = BytesMut::new();
        encode_fetch_request(&mut omitted, 17, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &omitted[..],
            "v17 ReplicaDirectoryId tagged field 0 is not omitted when non-zero"
        );
        let mut v16 = BytesMut::new();
        encode_fetch_request(&mut v16, 16, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &omitted[..],
            &v16[..],
            "v17 zeros ReplicaDirectoryId matches v16 consumer request"
        );
        topics
            .first_mut()
            .expect("topic")
            .partitions
            .first_mut()
            .expect("partition")
            .replica_directory_id = directory;
        let mut v16_with = BytesMut::new();
        encode_fetch_request(&mut v16_with, 16, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &v16_with[..],
            &v16[..],
            "below v17 ReplicaDirectoryId is omitted even when non-zero"
        );
    }

    fn leftover_replica_directory_id(
        version: i16,
        replica_directory_id: [u8; 16],
        topics: &[FetchTopic],
    ) {
        let owned: Vec<FetchTopic> = topics
            .iter()
            .map(|topic| FetchTopic {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| FetchPartition {
                        replica_directory_id,
                        ..partition.clone()
                    })
                    .collect(),
            })
            .collect();
        let max_wait_ms = 500;
        let min_bytes = 1;
        let mut buf = BytesMut::new();
        encode_fetch_request(
            &mut buf,
            version,
            max_wait_ms,
            min_bytes,
            DEFAULT_RESPONSE_MAX_BYTES,
            0,
            &owned,
            None,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (isolation, max_bytes, decoded, rack, ..) =
            decode_fetch_request(&mut cur, version).unwrap();
        assert_eq!(decoded.len(), owned.len());
        let expected = if version >= 17 {
            replica_directory_id
        } else {
            [0; 16]
        };
        if let Some(topic) = owned.first() {
            let got = decoded.first().expect("one topic");
            assert_eq!(
                got.partitions.first().map(|p| p.replica_directory_id),
                Some(expected)
            );
            if version < 13 {
                assert_eq!(got.topic, topic.topic);
            } else {
                assert!(got.topic.is_empty());
                assert_eq!(got.topic_id, topic.topic_id);
            }
        }
        assert_eq!(isolation, 0, "Java IsolationLevel.READ_UNCOMMITTED");
        assert_eq!(max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        if version >= 11 {
            assert!(rack.is_empty());
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "Fetch v{version} ReplicaDirectoryId {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_error_response_matches_java() {
        // Java FetchRequest.getErrorResponse: below v13 each topic is
        // FetchResponse.partitionResponse per partition. v13+ Responses
        // is empty (top-level error only). FetchTopic.error_result still
        // keeps a topic with empty partitions on v13+.
        let topic = FetchTopic {
            topic: "t".into(),
            topic_id: [1u8; 16],
            partitions: vec![
                FetchPartition {
                    partition: 0,
                    current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    fetch_offset: 0,
                    last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    log_start_offset: INVALID_LOG_START_OFFSET,
                    partition_max_bytes: 1,
                    replica_directory_id: [0; 16],
                },
                FetchPartition {
                    partition: 3,
                    current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    fetch_offset: 1,
                    last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    log_start_offset: INVALID_LOG_START_OFFSET,
                    partition_max_bytes: 1,
                    replica_directory_id: [0; 16],
                },
            ],
        };
        let err = crate::error::UNKNOWN_TOPIC_OR_PARTITION;
        let v12 = FetchRequest::error_response(12, std::slice::from_ref(&topic), err);
        assert_eq!(v12.len(), 1);
        let v12_topic = v12.first().expect("v12 topic");
        assert_eq!(v12_topic.topic, "t");
        assert_eq!(v12_topic.topic_id, [1u8; 16]);
        assert_eq!(v12_topic.partitions.len(), 2);
        let first = v12_topic.partitions.first().expect("v12 partition");
        assert_eq!(first.partition, 0);
        assert_eq!(first.error_code, err);
        let v13 = FetchRequest::error_response(13, std::slice::from_ref(&topic), err);
        assert!(
            v13.is_empty(),
            "v13+ getErrorResponse Responses is empty, got {v13:?}"
        );
        let shell = topic.error_result(13, err);
        assert_eq!(shell.topic, "t");
        assert!(shell.partitions.is_empty());

        for version in [11_i16, 12] {
            let responses =
                FetchRequest::error_response(version, std::slice::from_ref(&topic), err);
            assert_eq!(responses.len(), 1);
            let mut buf = BytesMut::new();
            encode_fetch_response_with_endpoints(
                &mut buf,
                version,
                &responses,
                err,
                FetchMetadata::INVALID_SESSION_ID,
                &[],
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, _endpoints, error_code, session, ..) =
                decode_fetch_response(&mut cur, version).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded.first().map(|t| t.partitions.len()), Some(2));
            assert_eq!(error_code, err);
            assert_eq!(session, FetchMetadata::INVALID_SESSION_ID);
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Request.getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [13_i16, 17] {
            let responses =
                FetchRequest::error_response(version, std::slice::from_ref(&topic), err);
            assert!(responses.is_empty());
            let mut buf = BytesMut::new();
            encode_fetch_response_with_endpoints(
                &mut buf,
                version,
                &responses,
                err,
                FetchMetadata::INVALID_SESSION_ID,
                &[],
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, _endpoints, error_code, session, ..) =
                decode_fetch_response(&mut cur, version).unwrap();
            assert!(decoded.is_empty());
            assert_eq!(error_code, err);
            assert_eq!(session, FetchMetadata::INVALID_SESSION_ID);
            assert!(
                !cur.has_remaining(),
                "Fetch v{version} Request.getErrorResponse empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn fetch_request_encode_error_response_matches_java() {
        // Java 4.0 FetchRequest.getErrorResponse writes Responses from
        // the request (empty on v13+), ThrottleTimeMs from the argument,
        // and v7+ ErrorCode / SessionId. NodeEndpoints stay empty.
        // Official Java FetchRequest.getErrorResponse. Convenience encode
        // still writes throttle 0, ErrorCode 0, SessionId INVALID.
        // This crate speaks 4–17. This is not error_response leftover /
        // with_throttle leftover / with_endpoints leftover / ErrorCode
        // leftover / SessionId leftover / ThrottleTimeMs leftover.
        let topic = FetchTopic {
            topic: "t".into(),
            topic_id: [1u8; 16],
            partitions: vec![FetchPartition::partition_data(
                0,
                0,
                INVALID_LOG_START_OFFSET,
                1,
                None,
                None,
            )],
        };
        let err = crate::error::UNKNOWN_TOPIC_OR_PARTITION;
        let session = 7;
        for version in [4_i16, 7, 12, 13] {
            let mut buf = BytesMut::new();
            FetchRequest::encode_error_response(
                &mut buf,
                version,
                std::slice::from_ref(&topic),
                err,
                session,
                3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, endpoints, error_code, session_id, throttle) =
                decode_fetch_response(&mut cur, version).unwrap();
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 3_600_000);
            if version < 13 {
                assert_eq!(decoded.len(), 1);
                assert_eq!(decoded.first().map(|t| t.partitions.len()), Some(1));
            } else {
                assert!(decoded.is_empty(), "v13+ Responses must be empty");
            }
            if version >= 7 {
                assert_eq!(error_code, err);
                assert_eq!(session_id, session);
            } else {
                assert_eq!(error_code, 0);
                assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
            }
            leftover_fetch_encode_error_response(version, cur);
        }

        let responses = FetchRequest::error_response(7, std::slice::from_ref(&topic), err);
        let mut with_throttle = BytesMut::new();
        encode_fetch_response_with_throttle(&mut with_throttle, 7, &responses, 3_600_000).unwrap();
        let mut with = BytesMut::new();
        FetchRequest::encode_error_response(
            &mut with,
            7,
            std::slice::from_ref(&topic),
            err,
            session,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &with_throttle[..],
            "Fetch Request.getErrorResponse encode must write ErrorCode / SessionId"
        );
        let mut with_endpoints = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut with_endpoints, 7, &responses, err, session, &[])
            .unwrap();
        assert_ne!(
            &with[..],
            &with_endpoints[..],
            "Fetch Request.getErrorResponse encode must write the throttleTimeMs argument"
        );

        let mut v4_with = BytesMut::new();
        FetchRequest::encode_error_response(
            &mut v4_with,
            4,
            std::slice::from_ref(&topic),
            err,
            session,
            3_600_000,
        )
        .unwrap();
        let v4_body = FetchRequest::error_response(4, std::slice::from_ref(&topic), err);
        let mut v4_throttle = BytesMut::new();
        encode_fetch_response_with_throttle(&mut v4_throttle, 4, &v4_body, 3_600_000).unwrap();
        assert_eq!(
            &v4_with[..],
            &v4_throttle[..],
            "Fetch v4 omits ErrorCode / SessionId even when the body is non-zero"
        );
    }

    fn leftover_fetch_encode_error_response(version: i16, cur: &[u8]) {
        let msg = match version {
            4 => "Fetch v4 Request.getErrorResponse encode leftover-empty",
            7 => "Fetch v7 Request.getErrorResponse encode leftover-empty",
            12 => "Fetch v12 Request.getErrorResponse encode leftover-empty",
            13 => "Fetch v13 Request.getErrorResponse encode leftover-empty",
            _ => "Fetch Request.getErrorResponse encode leftover-empty",
        };
        assert!(cur.is_empty(), "{msg}; leftover {} bytes", cur.len());
    }

    #[test]
    fn fetch_error_code_session_id_matches_java() {
        let err = crate::error::FETCH_SESSION_ID_NOT_FOUND;
        let session = 9;
        for version in [7_i16, 11, 12, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_response_with_endpoints(&mut buf, version, &[], err, session, &[])
                .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, endpoints, error_code, session_id, ..) =
                decode_fetch_response(&mut cur, version).unwrap();
            assert!(decoded.is_empty());
            assert!(endpoints.is_empty());
            assert_eq!(error_code, err);
            assert_eq!(session_id, session);
            assert!(cur.is_empty(), "Fetch v{version} ErrorCode leftover-empty");
        }

        let mut buf = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut buf, 4, &[], err, session, &[]).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, error_code, session_id, ..) = decode_fetch_response(&mut cur, 4).unwrap();
        assert!(cur.is_empty(), "Fetch v4 ErrorCode leftover-empty");
        assert_eq!(
            error_code, 0,
            "Fetch v4 omits ErrorCode even when the body has a non-zero value"
        );
        assert_eq!(
            session_id,
            FetchMetadata::INVALID_SESSION_ID,
            "Fetch v4 omits SessionId even when the body has a non-zero value"
        );

        let mut with = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut with, 7, &[], err, session, &[]).unwrap();
        let mut zero = BytesMut::new();
        encode_fetch_response_with_endpoints(
            &mut zero,
            7,
            &[],
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &[],
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v7 ErrorCode / SessionId are not always the JSON default 0"
        );
        let mut v4_nonzero = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut v4_nonzero, 4, &[], err, session, &[]).unwrap();
        let mut v4_zero = BytesMut::new();
        encode_fetch_response_with_endpoints(
            &mut v4_zero,
            4,
            &[],
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &[],
        )
        .unwrap();
        assert_eq!(
            &v4_nonzero[..],
            &v4_zero[..],
            "v4 encode omits ErrorCode / SessionId even when the body has a non-zero value"
        );
        let mut v6 = BytesMut::new();
        encode_fetch_response_with_endpoints(&mut v6, 6, &[], err, session, &[]).unwrap();
        assert_eq!(
            &v4_nonzero[..],
            &v6[..],
            "v4–v6 Fetch responses omit ErrorCode / SessionId"
        );
        assert_ne!(
            &v6[..],
            &with[..],
            "v7 adds ErrorCode and SessionId after ThrottleTimeMs"
        );
    }

    #[test]
    fn fetch_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 FetchResponse.json ThrottleTimeMs is versions 1+
        // (INT32 first field; ignorable). Crate speaks 4–17 so the field
        // is on the wire for every spoken version. Official Java
        // FetchRequest.getErrorResponse sets throttleTimeMs from the
        // argument. encode_fetch_response still writes 0.
        // Empty-Responses v4 == v5 == v6 (classic; ErrorCode / SessionId
        // are v7+; LogStartOffset / PreferredReadReplica are on
        // partitions); v7 == v8 == v9 == v10 == v11 (ErrorCode / SessionId
        // after throttle); v12 == v13 == v14 == v15 == v16 == v17
        // (compact; NodeEndpoints tagged field 0 is omitted when empty).
        // Top-level ErrorCode is at bytes 4–5 on v7+. This crate speaks
        // 4–17. This is not Produce / Metadata / OffsetForLeaderEpoch
        // ThrottleTimeMs.
        let topics: Vec<FetchedTopic> = vec![];
        for version in 4..=17 {
            let mut buf = BytesMut::new();
            encode_fetch_response_with_throttle(&mut buf, version, &topics, 3_600_000).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, endpoints, error_code, session_id, throttle) =
                decode_fetch_response(&mut cur, version).unwrap();
            assert!(decoded.is_empty());
            assert!(endpoints.is_empty());
            assert_eq!(error_code, 0);
            assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "Fetch v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_response_with_throttle(&mut with, 4, &topics, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_fetch_response_with_throttle(&mut zero, 4, &topics, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v4 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_fetch_response(&mut conv, 4, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_fetch_response still writes ThrottleTimeMs 0"
        );
        assert_eq!(
            &with[..4],
            &3_600_000i32.to_be_bytes(),
            "empty-Responses ThrottleTimeMs is the first field"
        );

        for version in 5..=6 {
            let mut buf = BytesMut::new();
            encode_fetch_response_with_throttle(&mut buf, version, &topics, 3_600_000).unwrap();
            assert_eq!(
                &with[..],
                &buf[..],
                "empty-Responses ThrottleTimeMs bodies: v4 == v{version}"
            );
        }
        let mut v7_with = BytesMut::new();
        encode_fetch_response_with_throttle(&mut v7_with, 7, &topics, 3_600_000).unwrap();
        assert_ne!(
            &with[..],
            &v7_with[..],
            "v7 adds ErrorCode and SessionId after ThrottleTimeMs"
        );
        for version in 8..=11 {
            let mut buf = BytesMut::new();
            encode_fetch_response_with_throttle(&mut buf, version, &topics, 3_600_000).unwrap();
            assert_eq!(
                &v7_with[..],
                &buf[..],
                "empty-Responses ThrottleTimeMs bodies: v7 == v{version}"
            );
        }
        let mut v12_with = BytesMut::new();
        encode_fetch_response_with_throttle(&mut v12_with, 12, &topics, 3_600_000).unwrap();
        assert_ne!(
            &v7_with[..],
            &v12_with[..],
            "v12 adds compact tagged fields after Responses"
        );
        for version in 13..=17 {
            let mut buf = BytesMut::new();
            encode_fetch_response_with_throttle(&mut buf, version, &topics, 3_600_000).unwrap();
            assert_eq!(
                &v12_with[..],
                &buf[..],
                "empty-Responses ThrottleTimeMs bodies: v12 == v{version}"
            );
        }
    }

    #[test]
    fn fetch_json_version_gates_match_java() {
        let req = [FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut v8 = BytesMut::new();
        encode_fetch_request(&mut v8, 8, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut v8_default = BytesMut::new();
        encode_fetch_request(
            &mut v8_default,
            8,
            10,
            1,
            1024,
            0,
            &[FetchTopic {
                topic: "t".into(),
                topic_id: [0; 16],
                partitions: vec![FetchPartition {
                    partition: 0,
                    current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    fetch_offset: 3,
                    last_fetched_epoch: -1,
                    log_start_offset: INVALID_LOG_START_OFFSET,
                    partition_max_bytes: 1024,
                    replica_directory_id: [0; 16],
                }],
            }],
            None,
        )
        .unwrap();
        assert_eq!(
            &v8[..],
            &v8_default[..],
            "v8 encode omits CurrentLeaderEpoch and RackId even when the body has values"
        );
        let mut cur = v8.as_ref();
        let (_, _, decoded, rack, ..) = decode_fetch_request(&mut cur, 8).unwrap();
        assert_eq!(
            decoded[0].partitions[0].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(rack.is_empty());
        assert!(cur.is_empty(), "Fetch v8 CurrentLeaderEpoch leftover-empty");

        let mut v9 = BytesMut::new();
        encode_fetch_request(&mut v9, 9, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut cur = v9.as_ref();
        let (_, _, decoded, rack, ..) = decode_fetch_request(&mut cur, 9).unwrap();
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert!(rack.is_empty());
        assert!(cur.is_empty(), "Fetch v9 CurrentLeaderEpoch leftover-empty");
        assert_ne!(
            &v8[..],
            &v9[..],
            "v9 adds CurrentLeaderEpoch even when RackId is still omitted"
        );

        let mut v10 = BytesMut::new();
        encode_fetch_request(&mut v10, 10, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut v10_none = BytesMut::new();
        encode_fetch_request(&mut v10_none, 10, 10, 1, 1024, 0, &req, None).unwrap();
        assert_eq!(
            &v10[..],
            &v10_none[..],
            "v10 encode omits RackId even when the body has a rack"
        );
        let mut cur = v10.as_ref();
        let (_, _, _, rack, ..) = decode_fetch_request(&mut cur, 10).unwrap();
        assert!(rack.is_empty());
        assert!(cur.is_empty(), "Fetch v10 RackId leftover-empty");

        let mut v11 = BytesMut::new();
        encode_fetch_request(&mut v11, 11, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut cur = v11.as_ref();
        let (_, _, decoded, rack, ..) = decode_fetch_request(&mut cur, 11).unwrap();
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(rack, "az1");
        assert!(cur.is_empty(), "Fetch v11 RackId leftover-empty");
        assert_ne!(
            &v10[..],
            &v11[..],
            "v11 adds RackId even when CurrentLeaderEpoch is already present"
        );

        let mut v4 = BytesMut::new();
        encode_fetch_request(&mut v4, 4, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut v5 = BytesMut::new();
        encode_fetch_request(&mut v5, 5, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut v6 = BytesMut::new();
        encode_fetch_request(&mut v6, 6, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut v7 = BytesMut::new();
        encode_fetch_request(&mut v7, 7, 10, 1, 1024, 0, &req, Some("az1")).unwrap();
        let mut cur = v4.as_ref();
        let (_, _, decoded, ..) = decode_fetch_request(&mut cur, 4).unwrap();
        assert_eq!(
            decoded[0].partitions[0].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "Fetch v4 SessionId leftover-empty");
        assert_ne!(
            &v4[..],
            &v5[..],
            "v5 adds request LogStartOffset even when SessionId is still omitted"
        );
        assert_eq!(
            &v5[..],
            &v6[..],
            "v5–v6 Fetch requests omit SessionId / ForgottenTopicsData"
        );
        assert_ne!(
            &v6[..],
            &v7[..],
            "v7 adds SessionId / SessionEpoch / ForgottenTopicsData"
        );
        let mut cur = v7.as_ref();
        let (_, _, decoded, ..) = decode_fetch_request(&mut cur, 7).unwrap();
        assert_eq!(
            decoded[0].partitions[0].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "Fetch v7 SessionId leftover-empty");

        let part = |log_start: i64, replica: i32| FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: log_start,
                aborted_transactions: Vec::new(),
                preferred_read_replica: replica,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: Vec::new(),
            }],
        };
        let with = [part(5, 3)];
        let defaults = [part(
            FetchedPartition::INVALID_LOG_START_OFFSET,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID,
        )];
        let mut v4_resp = BytesMut::new();
        encode_fetch_response(&mut v4_resp, 4, &with).unwrap();
        let mut v4_def = BytesMut::new();
        encode_fetch_response(&mut v4_def, 4, &defaults).unwrap();
        assert_eq!(
            &v4_resp[..],
            &v4_def[..],
            "v4 encode omits LogStartOffset and PreferredReadReplica even when the body has values"
        );
        let mut cur = v4_resp.as_ref();
        let (decoded, ..) = decode_fetch_response(&mut cur, 4).unwrap();
        assert_eq!(
            decoded[0].partitions[0].log_start_offset,
            FetchedPartition::INVALID_LOG_START_OFFSET
        );
        assert_eq!(
            decoded[0].partitions[0].preferred_read_replica,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        );
        assert!(cur.is_empty(), "Fetch v4 LogStartOffset leftover-empty");

        let mut v5_resp = BytesMut::new();
        encode_fetch_response(&mut v5_resp, 5, &with).unwrap();
        let mut cur = v5_resp.as_ref();
        let (decoded, ..) = decode_fetch_response(&mut cur, 5).unwrap();
        assert_eq!(decoded[0].partitions[0].log_start_offset, 5);
        assert_eq!(
            decoded[0].partitions[0].preferred_read_replica,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        );
        assert!(cur.is_empty(), "Fetch v5 LogStartOffset leftover-empty");
        assert_ne!(
            &v4_resp[..],
            &v5_resp[..],
            "v5 adds LogStartOffset even when PreferredReadReplica is still omitted"
        );

        let mut v10_resp = BytesMut::new();
        encode_fetch_response(&mut v10_resp, 10, &with).unwrap();
        let mut v10_def = BytesMut::new();
        encode_fetch_response(
            &mut v10_def,
            10,
            std::slice::from_ref(&part(5, FetchedPartition::INVALID_PREFERRED_REPLICA_ID)),
        )
        .unwrap();
        assert_eq!(
            &v10_resp[..],
            &v10_def[..],
            "v10 encode omits PreferredReadReplica even when the body has a replica"
        );
        let mut v11_resp = BytesMut::new();
        encode_fetch_response(&mut v11_resp, 11, &with).unwrap();
        let mut cur = v11_resp.as_ref();
        let (decoded, ..) = decode_fetch_response(&mut cur, 11).unwrap();
        assert_eq!(decoded[0].partitions[0].log_start_offset, 5);
        assert_eq!(decoded[0].partitions[0].preferred_read_replica, 3);
        assert!(
            cur.is_empty(),
            "Fetch v11 PreferredReadReplica leftover-empty"
        );
        assert_ne!(
            &v10_resp[..],
            &v11_resp[..],
            "v11 adds PreferredReadReplica after LogStartOffset"
        );
    }

    #[test]
    fn fetch_metadata_matches_java() {
        assert_eq!(FetchMetadata::INVALID_SESSION_ID, 0);
        assert_eq!(FetchMetadata::INITIAL_EPOCH, 0);
        assert_eq!(FetchMetadata::FINAL_EPOCH, -1);
        assert_eq!(FetchMetadata::INITIAL.session_id(), 0);
        assert_eq!(FetchMetadata::INITIAL.epoch(), 0);
        assert_eq!(FetchMetadata::LEGACY.session_id(), 0);
        assert_eq!(FetchMetadata::LEGACY.epoch(), -1);
        assert!(FetchMetadata::INITIAL.is_full());
        assert!(FetchMetadata::LEGACY.is_full());
        assert!(!FetchMetadata::new_incremental(9).is_full());
        assert_eq!(FetchMetadata::next_epoch(-1), FetchMetadata::FINAL_EPOCH);
        assert_eq!(FetchMetadata::next_epoch(FetchMetadata::INITIAL_EPOCH), 1);
        assert_eq!(FetchMetadata::next_epoch(i32::MAX), 1);
        assert_eq!(
            FetchMetadata::new(9, 4).next_close_existing(),
            FetchMetadata::new(9, FetchMetadata::FINAL_EPOCH)
        );
        assert_eq!(
            FetchMetadata::new(9, 4).next_close_existing_attempt_new(),
            FetchMetadata::new(9, FetchMetadata::INITIAL_EPOCH)
        );
        assert_eq!(FetchMetadata::new_incremental(9), FetchMetadata::new(9, 1));
        assert_eq!(
            FetchMetadata::new(9, 1).next_incremental(),
            FetchMetadata::new(9, 2)
        );
        assert_eq!(
            FetchMetadata::INITIAL.to_string(),
            "(sessionId=INVALID, epoch=INITIAL)"
        );
        assert_eq!(
            FetchMetadata::LEGACY.to_string(),
            "(sessionId=INVALID, epoch=FINAL)"
        );
        assert_eq!(
            FetchMetadata::new(12, 3).to_string(),
            "(sessionId=12, epoch=3)"
        );
    }

    #[test]
    fn fetch_request_session_matches_java() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                fetch_offset: 3,
                last_fetched_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let session = FetchMetadata::new(12, 3);
        for version in [7_i16, 8, 11, 12, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_session(
                &mut buf, version, 10, 1, 1024, 0, &topics, None, session,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (_, _, _, _, got, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(got, session);
            assert!(
                cur.is_empty(),
                "Fetch v{version} SessionId / SessionEpoch leftover-empty"
            );
        }

        for version in [4_i16, 5, 6] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_session(
                &mut buf, version, 10, 1, 1024, 0, &topics, None, session,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (_, _, _, _, got, ..) = decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(
                got,
                FetchMetadata::LEGACY,
                "Fetch v{version} omits SessionId / SessionEpoch even when the body is non-LEGACY"
            );
            assert!(
                cur.is_empty(),
                "Fetch v{version} SessionId / SessionEpoch leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_request_with_session(&mut with, 7, 10, 1, 1024, 0, &topics, None, session)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_fetch_request_with_session(
            &mut zero,
            7,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v7 SessionId / SessionEpoch are not always LEGACY"
        );
        let mut conv = BytesMut::new();
        encode_fetch_request(&mut conv, 7, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_fetch_request still writes FetchMetadata LEGACY"
        );

        let mut v6_with = BytesMut::new();
        encode_fetch_request_with_session(&mut v6_with, 6, 10, 1, 1024, 0, &topics, None, session)
            .unwrap();
        let mut v6_legacy = BytesMut::new();
        encode_fetch_request_with_session(
            &mut v6_legacy,
            6,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
        )
        .unwrap();
        assert_eq!(
            &v6_with[..],
            &v6_legacy[..],
            "v6 encode omits SessionId / SessionEpoch even when the body is non-LEGACY"
        );
        let mut v8_with = BytesMut::new();
        encode_fetch_request_with_session(&mut v8_with, 8, 10, 1, 1024, 0, &topics, None, session)
            .unwrap();
        assert_eq!(
            &with[..],
            &v8_with[..],
            "v7 and v8 both write SessionId / SessionEpoch; do not confuse with v9 CurrentLeaderEpoch, v11 RackId, or v12 flexible"
        );
        assert_ne!(
            &v6_with[..],
            &with[..],
            "v7 adds SessionId / SessionEpoch / ForgottenTopicsData"
        );
    }

    #[test]
    fn fetch_request_forgotten_matches_java() {
        let topics: Vec<FetchTopic> = Vec::new();
        let named = [ForgottenTopic {
            topic: "u".into(),
            topic_id: [0; 16],
            partitions: vec![1, 1],
        }];
        let by_id = [ForgottenTopic {
            topic: String::new(),
            topic_id: [7u8; 16],
            partitions: vec![1, 1],
        }];
        for version in [7_i16, 8, 11, 12] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_forgotten(
                &mut buf,
                version,
                10,
                1,
                1024,
                0,
                &topics,
                None,
                FetchMetadata::LEGACY,
                &named,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., forgotten, _, _, _, _, _) = decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(forgotten.as_slice(), named.as_slice());
            assert!(
                cur.is_empty(),
                "Fetch v{version} ForgottenTopicsData leftover-empty"
            );
        }
        for version in [13_i16, 15, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_forgotten(
                &mut buf,
                version,
                10,
                1,
                1024,
                0,
                &topics,
                None,
                FetchMetadata::LEGACY,
                &by_id,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., forgotten, _, _, _, _, _) = decode_fetch_request(&mut cur, version).unwrap();
            assert_eq!(forgotten.as_slice(), by_id.as_slice());
            assert!(
                cur.is_empty(),
                "Fetch v{version} ForgottenTopicsData leftover-empty"
            );
        }

        for version in [4_i16, 5, 6] {
            let mut buf = BytesMut::new();
            encode_fetch_request_with_forgotten(
                &mut buf,
                version,
                10,
                1,
                1024,
                0,
                &topics,
                None,
                FetchMetadata::LEGACY,
                &named,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., forgotten, _, _, _, _, _) = decode_fetch_request(&mut cur, version).unwrap();
            assert!(
                forgotten.is_empty(),
                "Fetch v{version} omits ForgottenTopicsData even when the body is non-empty"
            );
            assert!(
                cur.is_empty(),
                "Fetch v{version} ForgottenTopicsData leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_fetch_request_with_forgotten(
            &mut with,
            7,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
            &named,
        )
        .unwrap();
        let mut empty = BytesMut::new();
        encode_fetch_request_with_session(
            &mut empty,
            7,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "v7 ForgottenTopicsData is not always empty"
        );
        let mut conv = BytesMut::new();
        encode_fetch_request(&mut conv, 7, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_fetch_request still writes empty ForgottenTopicsData"
        );

        let mut v6_with = BytesMut::new();
        encode_fetch_request_with_forgotten(
            &mut v6_with,
            6,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
            &named,
        )
        .unwrap();
        let mut v6_empty = BytesMut::new();
        encode_fetch_request(&mut v6_empty, 6, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            &v6_with[..],
            &v6_empty[..],
            "v6 encode omits ForgottenTopicsData even when the body is non-empty"
        );
        let mut v8_with = BytesMut::new();
        encode_fetch_request_with_forgotten(
            &mut v8_with,
            8,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
            &named,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v8_with[..],
            "v7 and v8 both write ForgottenTopicsData; do not confuse with v12 flexible or v13 TopicId"
        );
        assert_ne!(
            &v6_with[..],
            &with[..],
            "v7 adds SessionId / SessionEpoch / ForgottenTopicsData"
        );
        let mut v12_with = BytesMut::new();
        encode_fetch_request_with_forgotten(
            &mut v12_with,
            12,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
            &named,
        )
        .unwrap();
        let mut v13_with = BytesMut::new();
        encode_fetch_request_with_forgotten(
            &mut v13_with,
            13,
            10,
            1,
            1024,
            0,
            &topics,
            None,
            FetchMetadata::LEGACY,
            &named,
        )
        .unwrap();
        assert_ne!(
            &v12_with[..],
            &v13_with[..],
            "v13 ForgottenTopics use TopicId instead of Name"
        );
    }

    #[test]
    fn fetch_request_sends_current_leader_epoch() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut session = &buf[..];
        assert_eq!(session.get_i32(), CONSUMER_REPLICA_ID);
        assert_eq!(session.get_i32(), 10);
        assert_eq!(session.get_i32(), 1);
        assert_eq!(session.get_i32(), 1024);
        assert_eq!(session.get_i8(), 0);
        assert_eq!(session.get_i32(), FetchMetadata::LEGACY.session_id());
        assert_eq!(session.get_i32(), FetchMetadata::LEGACY.epoch());
        let mut cur = &buf[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 11).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(
            decoded[0].partitions[0].last_fetched_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(decoded[0].partitions[0].partition_max_bytes, 1024);
        assert!(rack.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch v11 request leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_rack_id_is_empty_string_not_null() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, None).unwrap();
        let tail = buf.get(buf.len().saturating_sub(2)..).unwrap();
        assert_eq!(
            tail,
            [0, 0],
            "v11 RackId must be empty STRING, not null i16=-1"
        );
    }

    #[test]
    fn fetch_request_sends_rack_id() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, Some("az1")).unwrap();
        let (_iso, _max_bytes, _decoded, rack, ..) =
            decode_fetch_request(&mut &buf[..], 11).unwrap();
        assert_eq!(rack, "az1");
    }

    #[test]
    fn fetch_v11_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 0);
        assert!(decoded[0].partitions[0].aborted_transactions.is_empty());
    }

    #[test]
    fn fetch_response_preserves_aborted_transactions() {
        let rec = Record {
            offset: 1,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"aborted")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec]);
        batch.producer_id = 1000;
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 2,
                last_stable_offset: 2,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(
            decoded[0].partitions[0].aborted_transactions,
            vec![(1000, 1)]
        );
        assert_eq!(decoded[0].partitions[0].records[0].producer_id, 1000);
    }

    #[test]
    fn decode_fetch_response_keeps_log_start_on_offset_out_of_range() {
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: crate::error::OFFSET_OUT_OF_RANGE,
                high_watermark: 20,
                last_stable_offset: 20,
                log_start_offset: 10,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(
            decoded[0].partitions[0].error_code,
            crate::error::OFFSET_OUT_OF_RANGE
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 10);
        assert!(decoded[0].partitions[0].records.is_empty());
    }

    #[test]
    fn decode_fetch_response_uses_record_batch_decoder_on_partition_bytes() {
        let rec = |v: &'static [u8]| Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(v)),
            headers: vec![],
        };
        let mut recs = BytesMut::new();
        records::encode_record_batch(&mut recs, &RecordBatch::from_records(vec![rec(b"one")]))
            .unwrap();
        records::encode_record_batch(&mut recs, &RecordBatch::from_records(vec![rec(b"two")]))
            .unwrap();
        recs.extend_from_slice(&[0u8; 8]);
        let mut body = BytesMut::new();
        body.put_i32(0);
        body.put_i16(0);
        body.put_i32(0);
        crate::protocol::buf::put_array_len(&mut body, false, Some(1)).unwrap();
        crate::protocol::buf::put_classic_nullable_string(&mut body, Some("t")).unwrap();
        crate::protocol::buf::put_array_len(&mut body, false, Some(1)).unwrap();
        body.put_i32(0);
        body.put_i16(0);
        body.put_i64(2);
        body.put_i64(2);
        body.put_i64(0);
        body.put_i32(-1);
        body.put_i32(-1);
        crate::protocol::buf::put_classic_bytes(&mut body, Some(&recs)).unwrap();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut &body[..], 11).unwrap();
        assert_eq!(decoded[0].partitions[0].records.len(), 2);
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(
            decoded[0].partitions[0].records[1].records[0]
                .value
                .as_deref(),
            Some(&b"two"[..])
        );
    }

    #[test]
    fn decode_fetch_response_from_bytes_shares_record_value() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"view-me")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec]);
        batch.base_offset = 20;
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 21,
                last_stable_offset: 21,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let frozen = buf.freeze();
        let (decoded, _endpoints, ..) = decode_fetch_response(&mut frozen.clone(), 11).unwrap();
        let got = &decoded[0].partitions[0].records[0].records[0];
        assert_eq!(got.offset, 20);
        assert_eq!(got.value.as_deref(), Some(&b"view-me"[..]));
        let start = frozen.as_ptr();
        let end = start.wrapping_add(frozen.len());
        let value = got.value.as_ref().unwrap();
        assert!(
            value.as_ptr() >= start && value.as_ptr() < end,
            "fetch record value must be a view into the response frame"
        );
    }

    #[test]
    fn fetch_v12_roundtrip_is_leftover_empty() {
        let req_topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: 4,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 12, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 12).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, 4);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v12 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 12, &topics).unwrap();
        let mut cur = &resp[..];
        let (got, _endpoints, ..) = decode_fetch_response(&mut cur, 12).unwrap();
        assert_eq!(got[0].topic, "t");
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v12 response must consume compact tagged fields"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v12_request_matches_compact_layout() {
        // ReplicaId -1, MaxWait 10, MinBytes 1, MaxBytes 1024, isolation 0,
        // session 0 / -1, compact Topics {Name "t", compact Partitions
        // {0, epoch 0, offset 0, lastFetched -1, logStart -1, maxBytes
        // 1024, tagged}, tagged}, empty forgotten, empty RackId, tagged.
        const REQ: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x02, 0x02, 0x74,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        ];
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 12, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    const SAMPLE_TOPIC_ID: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];

    fn sample_v13_topic() -> FetchTopic {
        FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }
    }

    #[test]
    fn fetch_v14_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 14, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 14).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert!(decoded[0].topic.is_empty());
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, -1);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v14 request must consume TopicId plus compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 14, &topics).unwrap();
        let mut cur = &resp[..];
        let (got, _endpoints, ..) = decode_fetch_response(&mut cur, 14).unwrap();
        assert!(got[0].topic.is_empty());
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v14 response must consume TopicId plus compact tagged fields"
        );
        let mut v13 = BytesMut::new();
        encode_fetch_request(&mut v13, 13, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v13[..],
            &req[..],
            "Fetch v13 and v14 request layout must match"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v14_request_matches_topic_id_layout() {
        // Same as v12 compact layout except Topics uses TopicId UUID
        // instead of compact Name "t" (0x02 0x74).
        const REQ: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x02, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        ];
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut v12 = BytesMut::new();
        encode_fetch_request(&mut v12, 12, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v14 = BytesMut::new();
        encode_fetch_request(&mut v14, 14, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &v14[..],
            &v12[..],
            "Fetch v14 TopicId bytes must not equal v12 compact name"
        );
        assert_eq!(&v14[..], REQ);
    }

    #[test]
    fn fetch_v14_forgotten_topic_id_is_leftover_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        buf.put_i32(10);
        buf.put_i32(1);
        buf.put_i32(1024);
        buf.put_i8(0);
        buf.put_i32(0);
        buf.put_i32(-1);
        crate::protocol::buf::put_array_len(&mut buf, true, Some(0)).unwrap();
        crate::protocol::buf::put_array_len(&mut buf, true, Some(1)).unwrap();
        buf.extend_from_slice(&SAMPLE_TOPIC_ID);
        crate::protocol::buf::put_array_len(&mut buf, true, Some(1)).unwrap();
        buf.put_i32(0);
        crate::protocol::buf::put_empty_tagged_fields(&mut buf);
        crate::protocol::buf::put_string(&mut buf, true, Some("")).unwrap();
        crate::protocol::buf::put_empty_tagged_fields(&mut buf);
        let mut cur = &buf[..];
        let (iso, max_bytes, topics, rack, ..) = decode_fetch_request(&mut cur, 14).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(max_bytes, 1024);
        assert!(topics.is_empty());
        assert!(rack.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch v14 forgotten TopicId must be consumed"
        );
    }

    #[test]
    fn fetch_v15_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 15, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 15).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert!(decoded[0].topic.is_empty());
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, -1);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v15 request must omit untagged ReplicaId and consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 15, &topics).unwrap();
        let mut cur = &resp[..];
        let (got, _endpoints, ..) = decode_fetch_response(&mut cur, 15).unwrap();
        assert!(got[0].topic.is_empty());
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v15 response must match the v14 compact layout"
        );
        let mut v14_resp = BytesMut::new();
        encode_fetch_response(&mut v14_resp, 14, &topics).unwrap();
        assert_eq!(
            &v14_resp[..],
            &resp[..],
            "Fetch v14 and v15 response layout must match"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v15_request_omits_untagged_replica_id() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: INVALID_LOG_START_OFFSET,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }];
        let mut v14 = BytesMut::new();
        encode_fetch_request(&mut v14, 14, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            v14.get(..4),
            Some([0xff, 0xff, 0xff, 0xff].as_slice()),
            "Fetch v14 starts with untagged ReplicaId -1"
        );
        assert_eq!(
            v15.as_ref(),
            v14.get(4..).unwrap(),
            "Fetch v15 request is v14 without untagged ReplicaId"
        );
    }

    #[test]
    fn fetch_v16_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut v16 = BytesMut::new();
        encode_fetch_request(&mut v16, 16, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v15[..],
            &v16[..],
            "Fetch v16 request layout must match v15"
        );
        let mut cur = &v16[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 16).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v16 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp15 = BytesMut::new();
        encode_fetch_response(&mut resp15, 15, &topics).unwrap();
        let mut resp16 = BytesMut::new();
        encode_fetch_response(&mut resp16, 16, &topics).unwrap();
        assert_eq!(
            &resp15[..],
            &resp16[..],
            "Fetch v16 empty CurrentLeader / NodeEndpoints must match v15"
        );
        let mut cur = &resp16[..];
        let (got, endpoints, ..) = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].current_leader_id,
            MetadataResponse::NO_LEADER_ID
        );
        assert_eq!(
            got[0].partitions[0].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(endpoints.is_empty());
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert!(
            cur.is_empty(),
            "Fetch v16 response must consume compact tagged fields"
        );
        v16.clear();
        assert!(
            encode_fetch_request(&mut v16, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v16_current_leader_tagged_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let mut with_leader = FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 6,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: 2,
                current_leader_epoch: 7,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        };
        let topics = vec![with_leader.clone()];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 16, &topics).unwrap();
        let mut cur = &buf[..];
        let (got, endpoints, ..) = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].partitions[0].current_leader_id, 2);
        assert_eq!(got[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(
            got[0].partitions[0].diverging_epoch,
            EpochEndOffset::UNDEFINED_EPOCH
        );
        assert_eq!(
            got[0].partitions[0].diverging_end_offset,
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        );
        assert!(endpoints.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch CurrentLeader tagged field 1 must consume nested tagged fields"
        );
        let mut v15 = BytesMut::new();
        encode_fetch_response(&mut v15, 15, &topics).unwrap();
        assert_eq!(
            &v15[..],
            &buf[..],
            "Fetch v12+ CurrentLeader layout is unchanged at v16"
        );
        with_leader.partitions[0].current_leader_id = MetadataResponse::NO_LEADER_ID;
        with_leader.partitions[0].current_leader_epoch = RecordBatch::NO_PARTITION_LEADER_EPOCH;
        let mut omitted = BytesMut::new();
        encode_fetch_response(&mut omitted, 16, &[with_leader]).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "CurrentLeader tagged field 1 must not equal empty tags"
        );
    }

    #[test]
    fn fetch_v16_diverging_epoch_tagged_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let mut with_div = FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: 3,
                diverging_end_offset: 12,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        };
        let topics = vec![with_div.clone()];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 16, &topics).unwrap();
        let mut cur = &buf[..];
        let (got, endpoints, ..) = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].partitions[0].diverging_epoch, 3);
        assert_eq!(got[0].partitions[0].diverging_end_offset, 12);
        assert_eq!(got[0].partitions[0].diverging_epoch(), Some((3, 12)));
        assert!(got[0].partitions[0].is_diverging_epoch());
        assert_eq!(
            got[0].partitions[0].current_leader_id,
            MetadataResponse::NO_LEADER_ID
        );
        assert!(endpoints.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch DivergingEpoch tagged field 0 must consume nested tagged fields"
        );
        let mut v15 = BytesMut::new();
        encode_fetch_response(&mut v15, 15, &topics).unwrap();
        assert_eq!(
            &v15[..],
            &buf[..],
            "Fetch v12+ DivergingEpoch layout is unchanged at v16"
        );
        with_div.partitions[0].diverging_epoch = EpochEndOffset::UNDEFINED_EPOCH;
        with_div.partitions[0].diverging_end_offset = EpochEndOffset::UNDEFINED_EPOCH_OFFSET;
        let mut omitted = BytesMut::new();
        encode_fetch_response(&mut omitted, 16, &[with_div]).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "DivergingEpoch tagged field 0 must not equal empty tags"
        );
    }

    #[test]
    fn fetch_response_snapshot_id_matches_java() {
        // Kafka 4.0.0 FetchResponse.json SnapshotId is partition tagged
        // field 2 on v12+ (`taggedVersions: "12+"`). Nested order is
        // EndOffset INT64 then Epoch INT32 (the reverse of DivergingEpoch
        // tag 0). Apache FetchResponse.java has no snapshotId helper.
        // This is not the FetchSnapshot API and does not start those RPCs.
        let snapshot_topic = |end_offset: i64, epoch: i32| FetchedTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: end_offset,
                snapshot_epoch: epoch,
                records: Vec::new(),
            }],
        };
        let with = [snapshot_topic(20, 3)];
        let omitted = [snapshot_topic(
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
            EpochEndOffset::UNDEFINED_EPOCH,
        )];
        let diverging = {
            let mut topic = snapshot_topic(
                EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
                EpochEndOffset::UNDEFINED_EPOCH,
            );
            topic.partitions[0].diverging_epoch = 3;
            topic.partitions[0].diverging_end_offset = 20;
            [topic]
        };

        for version in [12_i16, 13, 15, 16, 17] {
            let mut buf = BytesMut::new();
            encode_fetch_response(&mut buf, version, &with).unwrap();
            let mut cur = buf.as_ref();
            let (got, endpoints, ..) = decode_fetch_response(&mut cur, version).unwrap();
            let part = got
                .first()
                .and_then(|t| t.partitions.first())
                .expect("one partition");
            assert_eq!(part.snapshot_end_offset, 20);
            assert_eq!(part.snapshot_epoch, 3);
            assert_eq!(part.snapshot_id(), Some((20, 3)));
            assert!(part.is_snapshot_id());
            assert_eq!(part.diverging_epoch, EpochEndOffset::UNDEFINED_EPOCH);
            assert_eq!(
                part.diverging_end_offset,
                EpochEndOffset::UNDEFINED_EPOCH_OFFSET
            );
            assert_eq!(part.current_leader_id, MetadataResponse::NO_LEADER_ID);
            assert!(!part.is_diverging_epoch());
            assert!(endpoints.is_empty());
            assert!(cur.is_empty(), "Fetch v{version} SnapshotId leftover-empty");
        }

        for version in [4_i16, 5, 6, 7, 8, 11] {
            let mut buf = BytesMut::new();
            encode_fetch_response(&mut buf, version, &with).unwrap();
            let mut omitted_buf = BytesMut::new();
            encode_fetch_response(&mut omitted_buf, version, &omitted).unwrap();
            assert_eq!(
                &buf[..],
                &omitted_buf[..],
                "Fetch v{version} omits SnapshotId even when the body is non-default"
            );
            let mut cur = buf.as_ref();
            let (got, ..) = decode_fetch_response(&mut cur, version).unwrap();
            let part = got
                .first()
                .and_then(|t| t.partitions.first())
                .expect("one partition");
            assert_eq!(
                part.snapshot_end_offset,
                EpochEndOffset::UNDEFINED_EPOCH_OFFSET
            );
            assert_eq!(part.snapshot_epoch, EpochEndOffset::UNDEFINED_EPOCH);
            assert_eq!(part.snapshot_id(), None);
            assert!(!part.is_snapshot_id());
            assert!(cur.is_empty(), "Fetch v{version} SnapshotId leftover-empty");
        }

        let mut v12_with = BytesMut::new();
        encode_fetch_response(&mut v12_with, 12, &with).unwrap();
        let mut v12_omitted = BytesMut::new();
        encode_fetch_response(&mut v12_omitted, 12, &omitted).unwrap();
        assert_ne!(
            &v12_with[..],
            &v12_omitted[..],
            "v12 SnapshotId tagged field 2 is not always omitted"
        );
        let mut v11_with = BytesMut::new();
        encode_fetch_response(&mut v11_with, 11, &with).unwrap();
        let mut v11_omitted = BytesMut::new();
        encode_fetch_response(&mut v11_omitted, 11, &omitted).unwrap();
        assert_eq!(
            &v11_with[..],
            &v11_omitted[..],
            "v11 encode omits SnapshotId even when the body is non-default"
        );
        assert_ne!(
            &v11_with[..],
            &v12_with[..],
            "v12 adds SnapshotId tagged field 2; do not confuse with v11 PreferredReadReplica"
        );

        let mut v15 = BytesMut::new();
        encode_fetch_response(&mut v15, 15, &with).unwrap();
        let mut v16 = BytesMut::new();
        encode_fetch_response(&mut v16, 16, &with).unwrap();
        assert_eq!(
            &v15[..],
            &v16[..],
            "Fetch v12+ SnapshotId layout is unchanged at v16"
        );

        let mut snap = BytesMut::new();
        encode_fetch_response(&mut snap, 16, &with).unwrap();
        let mut div = BytesMut::new();
        encode_fetch_response(&mut div, 16, &diverging).unwrap();
        assert_ne!(
            &snap[..],
            &div[..],
            "SnapshotId tag 2 is EndOffset then Epoch; DivergingEpoch tag 0 is Epoch then EndOffset"
        );
        let mut leader = BytesMut::new();
        let mut with_leader = snapshot_topic(
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
            EpochEndOffset::UNDEFINED_EPOCH,
        );
        with_leader.partitions[0].current_leader_id = 2;
        with_leader.partitions[0].current_leader_epoch = 7;
        encode_fetch_response(&mut leader, 16, std::slice::from_ref(&with_leader)).unwrap();
        assert_ne!(
            &snap[..],
            &leader[..],
            "SnapshotId tagged field 2 must not equal CurrentLeader tagged field 1"
        );
    }

    #[test]
    fn fetch_v16_node_endpoints_tagged_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 6,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: 3,
                current_leader_epoch: 1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let endpoints = [crate::protocol::api::NodeEndpoint {
            node_id: 3,
            host: "h".into(),
            port: 1,
            rack: None,
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response_with_endpoints(
            &mut buf,
            16,
            &topics,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &endpoints,
        )
        .unwrap();
        let mut cur = &buf[..];
        let (got, eps, ..) = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].partitions[0].current_leader_id, 3);
        assert_eq!(eps, endpoints);
        assert!(
            cur.is_empty(),
            "Fetch NodeEndpoints tagged field 0 must consume nested tagged fields"
        );
        let mut omitted = BytesMut::new();
        encode_fetch_response(&mut omitted, 16, &topics).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "NodeEndpoints tagged field 0 must not equal empty tags"
        );
        let mut v15 = BytesMut::new();
        encode_fetch_response_with_endpoints(
            &mut v15,
            15,
            &topics,
            0,
            FetchMetadata::INVALID_SESSION_ID,
            &endpoints,
        )
        .unwrap();
        let mut empty = BytesMut::new();
        encode_fetch_response(&mut empty, 15, &topics).unwrap();
        assert_eq!(&v15[..], &empty[..], "Fetch v15 must omit NodeEndpoints");
    }

    #[test]
    fn fetch_v17_roundtrip_matches_v16() {
        let req_topics = vec![sample_v13_topic()];
        let mut v16 = BytesMut::new();
        encode_fetch_request(&mut v16, 16, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut v17 = BytesMut::new();
        encode_fetch_request(&mut v17, 17, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v16[..],
            &v17[..],
            "Fetch v17 consumer request must omit ReplicaDirectoryId and match v16"
        );
        let mut cur = &v17[..];
        let (iso, max_bytes, decoded, rack, ..) = decode_fetch_request(&mut cur, 17).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v17 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                snapshot_end_offset: -1,
                snapshot_epoch: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp16 = BytesMut::new();
        encode_fetch_response(&mut resp16, 16, &topics).unwrap();
        let mut resp17 = BytesMut::new();
        encode_fetch_response(&mut resp17, 17, &topics).unwrap();
        assert_eq!(
            &resp16[..],
            &resp17[..],
            "Fetch v17 response layout must match v16"
        );
        let mut cur = &resp17[..];
        let (got, endpoints, ..) = decode_fetch_response(&mut cur, 17).unwrap();
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert!(endpoints.is_empty());
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert!(
            cur.is_empty(),
            "Fetch v17 response must consume compact tagged fields"
        );
        v17.clear();
        assert!(
            encode_fetch_request(&mut v17, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }
}
