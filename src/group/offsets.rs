use super::ConsumerGroup;
use crate::admin::AdminConfig;
use crate::consumer::{OffsetAndMetadata, TopicPartition};
use crate::error::{Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::offsets::{OffsetClient, OffsetOptions};
use crate::protocol::group::*;
use std::collections::HashMap;
use std::time::Duration;

impl ConsumerGroup {
    /// Commit delivered offsets while metadata for `names` reloads.
    ///
    /// OffsetCommit uses the coordinator. Metadata uses the member socket.
    /// The commit outcome is returned first.
    pub(super) async fn commit_offsets_while_refreshing(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        names: &[String],
        timeout: Duration,
    ) -> Result<()> {
        if offsets.is_empty() {
            return self
                .consumer
                .refresh_metadata_timeout(Some(names), timeout)
                .await;
        }
        if self.coord.offset_commit_version == 10 {
            return self
                .commit_v10_while_refreshing(offsets, names, timeout)
                .await;
        }
        let mut commit_topics = std::mem::take(&mut self.classic_commit_topics);
        super::refill_offset_topics(&mut commit_topics, offsets);
        let group_id = self.group_id.as_str();
        let member_id = self.member_id.as_str();
        let generation_id = self.generation_id;
        let instance = self.cfg.group_instance_id.as_deref();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let rpc = super::coord_roundtrip(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |version, buf| {
                encode_offset_commit_request(
                    buf,
                    version,
                    group_id,
                    generation_id,
                    member_id,
                    instance,
                    DEFAULT_RETENTION_TIME,
                    &commit_topics,
                )
            },
            timeout,
        );
        let refreshed = consumer.refresh_metadata_timeout(Some(names), timeout);
        let (rpc, refreshed) = tokio::join!(rpc, refreshed);
        self.classic_commit_topics = commit_topics;
        let (version, body) = match rpc {
            Ok(response) => response,
            Err(error) => {
                self.note_commit(false);
                return Err(error);
            }
        };
        let err = match decode_offset_commit_response(&mut body.clone(), version) {
            Ok(err) => err,
            Err(error) => {
                self.note_commit(false);
                return Err(error);
            }
        };
        if err != 0 {
            self.note_commit(false);
            return Err(Error::broker(err, "OffsetCommit"));
        }
        self.cfg.interceptors.on_commit(offsets);
        self.note_commit(true);
        refreshed
    }

    async fn commit_v10_while_refreshing(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        names: &[String],
        timeout: Duration,
    ) -> Result<()> {
        let mut request = std::mem::take(&mut self.commit_v10);
        if let Err(error) = self.fill_explicit_commit(&mut request, offsets, None) {
            self.commit_v10 = request;
            return Err(error);
        }
        let limits = OffsetLimits::default();
        let group_id = self.group_id.as_str();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let sent = coord_offsets_v10(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |buf| encode_offset_commit_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            timeout,
        );
        let refreshed = consumer.refresh_metadata_timeout(Some(names), timeout);
        let (sent, refreshed) = tokio::join!(sent, refreshed);
        self.commit_v10 = request;
        let body = match sent {
            Ok(body) => body,
            Err(error) => {
                self.note_commit(false);
                return Err(error);
            }
        };
        let response = match decode_offset_commit_response_data(&body, 10, limits) {
            Ok(response) => response,
            Err(error) => {
                self.note_commit(false);
                return Err(error);
            }
        };
        if let Some(code) = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| *code != 0)
        {
            self.note_commit(false);
            return Err(Error::broker(code, "OffsetCommit"));
        }
        self.cfg.interceptors.on_commit(offsets);
        self.note_commit(true);
        refreshed
    }

    pub(super) fn snapshot_delivered_positions(&mut self) {
        let mut positions = self.last_delivered_positions.take().unwrap_or_default();
        self.consumer
            .fill_delivered_positions_shared(&mut positions);
        let mut ids = std::mem::take(&mut self.last_delivered_topic_ids);
        super::clear_delivered_topic_ids(&mut ids);
        for (topic, partition, _) in &positions {
            if let Some(id) = self.consumer.assigned_topic_id(topic, *partition) {
                super::remember_shared_topic_id(&mut ids, topic, *partition, id);
            }
        }
        ids.retain(|_, parts| !parts.is_empty());
        self.last_delivered_topic_ids = ids;
        self.last_delivered_positions = Some(positions);
    }

    pub(super) fn queue_async_commit(
        &mut self,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
        callback: Option<super::AsyncOffsetCommitCallback>,
    ) {
        let metadata = self.consumer.topic_name_ids();
        let mut ids = HashMap::new();
        for (tp, _) in &offsets {
            if let Some(id) = self
                .consumer
                .assigned_topic_id(&tp.topic, tp.partition)
                .or_else(|| metadata.get(tp.topic.as_str()).copied())
                .filter(|id| *id != [0; 16])
            {
                super::remember_topic_id(&mut ids, &tp.topic, tp.partition, id);
            }
        }
        self.pending_async_commits.push((offsets, callback, ids));
    }

    fn offset_client(&self) -> Result<OffsetClient> {
        let cfg = &self.cfg;
        OffsetClient::new(AdminConfig {
            bootstrap: cfg.bootstrap.clone(),
            client_id: cfg.client_id.clone(),
            request_timeout: cfg.request_timeout,
            connect_timeout: cfg.connect_timeout,
            reconnect_backoff: cfg.reconnect_backoff,
            reconnect_backoff_max: cfg.reconnect_backoff_max,
            connections_max_idle: cfg.connections_max_idle,
            retry_backoff: cfg.retry_backoff,
            retry_backoff_max: cfg.retry_backoff_max,
            sasl_plain: cfg.sasl_plain.clone(),
            sasl_scram: cfg.sasl_scram.clone(),
            sasl_scram_sha512: cfg.sasl_scram_sha512.clone(),
            sasl_oauthbearer: cfg.sasl_oauthbearer.clone(),
            sasl_oauthbearer_oidc: cfg.sasl_oauthbearer_oidc.clone(),
            tls: cfg.tls.clone(),
        })
    }

    /// Commit explicit topic identities using this member's group identity.
    /// The request must match [`Self::group_metadata`]. Per-partition errors
    /// remain in the typed response. UUIDs require v10; no name fallback occurs.
    /// The caller remains responsible for choosing the offsets to commit.
    pub async fn commit_offsets_by_identity(
        &mut self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetCommitResponseData> {
        self.adopt_kip848_member_id();
        let code = self.hb_err.load(std::sync::atomic::Ordering::SeqCst);
        if code != 0 {
            return Err(Error::broker(code, "group membership"));
        }
        if request.group_id != self.group_id
            || request.member_id != self.member_id
            || request.generation_id_or_member_epoch != self.generation_id
            || request.group_instance_id != self.cfg.group_instance_id
        {
            return Err(Error::protocol(
                "offset commit membership differs from this group",
            ));
        }
        self.offset_client()?.commit(request, options).await
    }

    /// Fetch typed offsets for this group, preserving UUIDs and nullable user
    /// metadata. Exactly one group is accepted. KIP-848 membership fields must
    /// match this member; classic requests use null member ID and epoch -1.
    pub async fn committed_offsets_by_identity(
        &mut self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetFetchResponseData> {
        self.adopt_kip848_member_id();
        let group = request
            .groups
            .first()
            .filter(|_| request.groups.len() == 1)
            .ok_or_else(|| Error::protocol("member offset fetch requires one group"))?;
        let (member, epoch) = if self.kip848 {
            (Some(self.member_id.as_str()), self.generation_id)
        } else {
            (None, -1)
        };
        if group.group_id != self.group_id
            || group.member_id.as_deref() != member
            || group.member_epoch != epoch
        {
            return Err(Error::protocol(
                "offset fetch membership differs from this group",
            ));
        }
        self.offset_client()?.fetch(request, options).await
    }

    pub(super) async fn commit_shared_v10(
        &mut self,
        positions: &[(std::sync::Arc<str>, i32, i64)],
        timeout: Duration,
    ) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        let mut request = std::mem::take(&mut self.commit_v10);
        let filled = self.fill_shared_commit(&mut request, positions);
        if let Err(error) = filled {
            self.commit_v10 = request;
            return Err(error);
        }
        self.finish_commit_v10(request, timeout).await
    }

    fn fill_shared_commit(
        &self,
        request: &mut OffsetCommitRequestData,
        positions: &[(std::sync::Arc<str>, i32, i64)],
    ) -> Result<()> {
        copy_commit_identity(
            request,
            &self.group_id,
            &self.member_id,
            self.generation_id,
            self.cfg.group_instance_id.as_deref(),
        );
        let metadata = self.consumer.topic_name_ids();
        let mut used = 0usize;
        for (topic, partition, offset) in positions {
            let id = self
                .consumer
                .assigned_topic_id(topic, *partition)
                .or_else(|| metadata.get(topic.as_ref()).copied())
                .filter(|id| *id != [0; 16])
                .ok_or_else(|| {
                    Error::Unsupported(
                        "OffsetCommit v10 requires a captured nonzero topic UUID".into(),
                    )
                })?;
            push_commit_part(
                request,
                &mut used,
                id,
                OffsetCommitPartitionData {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: self.consumer.position_epoch(topic, *partition),
                    committed_metadata: Some(String::new()),
                },
            );
        }
        request.topics.truncate(used);
        Ok(())
    }

    pub(super) async fn commit_offsets_v10(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        timeout: Duration,
        snapshot: Option<&super::DeliveredTopicIds>,
    ) -> Result<()> {
        let mut request = std::mem::take(&mut self.commit_v10);
        let filled = self.fill_explicit_commit(&mut request, offsets, snapshot);
        if let Err(error) = filled {
            self.commit_v10 = request;
            return Err(error);
        }
        self.finish_commit_v10(request, timeout).await?;
        self.cfg.interceptors.on_commit(offsets);
        Ok(())
    }

    fn fill_explicit_commit(
        &self,
        request: &mut OffsetCommitRequestData,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        snapshot: Option<&super::DeliveredTopicIds>,
    ) -> Result<()> {
        copy_commit_identity(
            request,
            &self.group_id,
            &self.member_id,
            self.generation_id,
            self.cfg.group_instance_id.as_deref(),
        );
        let metadata = self.consumer.topic_name_ids();
        let mut used = 0usize;
        for (tp, md) in offsets {
            let id = match snapshot {
                Some(ids) => super::nested_topic_id(ids, &tp.topic, tp.partition),
                None => self
                    .consumer
                    .assigned_topic_id(&tp.topic, tp.partition)
                    .or_else(|| metadata.get(tp.topic.as_str()).copied()),
            }
            .filter(|id| *id != [0; 16])
            .ok_or_else(|| {
                Error::Unsupported("OffsetCommit v10 requires a captured nonzero topic UUID".into())
            })?;
            push_commit_part(
                request,
                &mut used,
                id,
                OffsetCommitPartitionData {
                    partition_index: tp.partition,
                    committed_offset: md.offset,
                    committed_leader_epoch: md.leader_epoch.unwrap_or(-1),
                    committed_metadata: Some(md.metadata.clone()),
                },
            );
        }
        request.topics.truncate(used);
        Ok(())
    }

    async fn finish_commit_v10(
        &mut self,
        request: OffsetCommitRequestData,
        timeout: Duration,
    ) -> Result<()> {
        let limits = OffsetLimits::default();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let sent = coord_offsets_v10(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            super::COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |buf| encode_offset_commit_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            timeout,
        )
        .await;
        self.commit_v10 = request;
        let body = sent?;
        let response = decode_offset_commit_response_data(&body, 10, limits)?;
        if let Some(code) = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| *code != 0)
        {
            return Err(Error::broker(code, "OffsetCommit"));
        }
        Ok(())
    }

    pub(super) async fn fetch_offsets_v10(
        &mut self,
        topics: &[OffsetFetchTopic],
        timeout: Duration,
    ) -> Result<Vec<FetchedOffsetTopic>> {
        let mut request = std::mem::take(&mut self.fetch_v10);
        let mut reverse = std::mem::take(&mut self.fetch_names);
        if let Err(error) = self.fill_fetch_v10(&mut request, &mut reverse, topics) {
            self.fetch_v10 = request;
            self.fetch_names = reverse;
            return Err(error);
        }
        let limits = OffsetLimits::default();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let sent = coord_offsets_v10(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            super::COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_FETCH,
            |buf| encode_offset_fetch_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            timeout,
        )
        .await;
        self.fetch_v10 = request;
        let body = match sent {
            Ok(body) => body,
            Err(error) => {
                self.fetch_names = reverse;
                return Err(error);
            }
        };
        let response = match decode_offset_fetch_response_data(&body, 10, limits) {
            Ok(response) => response,
            Err(error) => {
                self.fetch_names = reverse;
                return Err(error);
            }
        };
        let group = match response.groups.into_iter().next() {
            Some(group) => group,
            None => {
                self.fetch_names = reverse;
                return Err(Error::protocol("missing fetched group"));
            }
        };
        if group.error_code != 0 {
            self.fetch_names = reverse;
            return Err(Error::broker(group.error_code, "OffsetFetch"));
        }
        let fetched = group
            .topics
            .into_iter()
            .map(|topic| {
                let OffsetTopicIdentity::Id(id) = topic.identity else {
                    return Err(Error::protocol("v10 returned a topic name"));
                };
                let name = reverse
                    .remove(&id)
                    .ok_or_else(|| Error::protocol("unknown fetched topic UUID"))?;
                Ok(FetchedOffsetTopic {
                    topic: name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| FetchedOffset {
                            partition: partition.partition_index,
                            offset: partition.committed_offset,
                            leader_epoch: partition.committed_leader_epoch,
                            metadata: partition.metadata.unwrap_or_default(),
                            error_code: partition.error_code,
                        })
                        .collect(),
                })
            })
            .collect();
        self.fetch_names = reverse;
        fetched
    }

    fn fill_fetch_v10(
        &self,
        request: &mut OffsetFetchRequestData,
        reverse: &mut HashMap<[u8; 16], String>,
        topics: &[OffsetFetchTopic],
    ) -> Result<()> {
        request.require_stable = self.cfg.isolation_level == crate::IsolationLevel::ReadCommitted;
        if request.groups.is_empty() {
            request.groups.push(OffsetFetchGroupData {
                group_id: String::new(),
                member_id: None,
                member_epoch: -1,
                topics: Some(Vec::new()),
            });
        }
        request.groups.truncate(1);
        let Some(group) = request.groups.first_mut() else {
            return Err(Error::protocol("missing offset fetch group"));
        };
        copy_text(&mut group.group_id, &self.group_id);
        if self.kip848 {
            match &mut group.member_id {
                Some(slot) => copy_text(slot, &self.member_id),
                None => group.member_id = Some(self.member_id.clone()),
            }
            group.member_epoch = self.generation_id;
        } else {
            group.member_id = None;
            group.member_epoch = -1;
        }
        let slots = group.topics.get_or_insert_with(Vec::new);
        let names = self.consumer.topic_name_ids();
        refill_fetch_topics(slots, topics, |topic| names.get(topic).copied(), reverse)
    }
}

fn refill_fetch_topics(
    slots: &mut Vec<OffsetFetchTopicData>,
    topics: &[OffsetFetchTopic],
    mut id_of: impl FnMut(&str) -> Option<[u8; 16]>,
    reverse: &mut HashMap<[u8; 16], String>,
) -> Result<()> {
    reverse.clear();
    let mut used = 0usize;
    for topic in topics {
        let id = id_of(topic.topic.as_str())
            .filter(|id| *id != [0; 16])
            .ok_or_else(|| {
                Error::Unsupported("OffsetFetch v10 requires a captured nonzero topic UUID".into())
            })?;
        if reverse.insert(id, topic.topic.clone()).is_some() {
            return Err(Error::protocol(
                "offset metadata maps multiple names to one UUID",
            ));
        }
        if let Some(slot) = slots.get_mut(used) {
            slot.identity = OffsetTopicIdentity::Id(id);
            slot.partition_indexes.clear();
            slot.partition_indexes.extend_from_slice(&topic.partitions);
        } else {
            slots.push(OffsetFetchTopicData {
                identity: OffsetTopicIdentity::Id(id),
                partition_indexes: topic.partitions.clone(),
            });
        }
        used = used.saturating_add(1);
    }
    slots.truncate(used);
    Ok(())
}

fn copy_text(slot: &mut String, text: &str) {
    if slot != text {
        slot.clear();
        slot.push_str(text);
    }
}

fn copy_commit_identity(
    request: &mut OffsetCommitRequestData,
    group_id: &str,
    member_id: &str,
    generation: i32,
    instance: Option<&str>,
) {
    copy_text(&mut request.group_id, group_id);
    copy_text(&mut request.member_id, member_id);
    request.generation_id_or_member_epoch = generation;
    match instance {
        Some(id) => {
            if request.group_instance_id.as_deref() != Some(id) {
                request.group_instance_id = Some(id.to_owned());
            }
        }
        None => request.group_instance_id = None,
    }
}

fn push_commit_part(
    request: &mut OffsetCommitRequestData,
    used: &mut usize,
    id: [u8; 16],
    part: OffsetCommitPartitionData,
) {
    let existing =
        request.topics.iter().take(*used).position(
            |slot| matches!(slot.identity, OffsetTopicIdentity::Id(stored) if stored == id),
        );
    if let Some(index) = existing {
        if let Some(slot) = request.topics.get_mut(index) {
            slot.partitions.push(part);
        }
        return;
    }
    if let Some(slot) = request.topics.get_mut(*used) {
        slot.identity = OffsetTopicIdentity::Id(id);
        slot.partitions.clear();
        slot.partitions.push(part);
    } else {
        request.topics.push(OffsetCommitTopicData {
            identity: OffsetTopicIdentity::Id(id),
            partitions: vec![part],
        });
    }
    *used = used.saturating_add(1);
}

fn offset_commit_v10_code(body: &[u8]) -> Option<i16> {
    let response = decode_offset_commit_response_data(body, 10, OffsetLimits::default()).ok()?;
    response
        .topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.error_code)
        .find(|code| *code != 0)
}

fn offset_fetch_v10_code(body: &[u8]) -> Option<i16> {
    let response = decode_offset_fetch_response_data(body, 10, OffsetLimits::default()).ok()?;
    if let Some(code) = response
        .groups
        .iter()
        .map(|group| group.error_code)
        .find(|code| *code != 0)
    {
        return Some(code);
    }
    response
        .groups
        .iter()
        .flat_map(|group| &group.topics)
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.error_code)
        .find(|code| *code != 0)
}

fn v10_coordinator_code(api: i16, body: &[u8]) -> Option<i16> {
    if api == crate::protocol::api_keys::OFFSET_FETCH {
        offset_fetch_v10_code(body)
    } else {
        offset_commit_v10_code(body)
    }
}

fn require_copied_coord_version(coord: &BrokerConn, api: i16, version: i16) -> Result<()> {
    let spoken = super::negotiated_coord_version(coord, api)?;
    if spoken < version {
        Err(Error::Unsupported(
            "moved coordinator cannot represent the name-based offset request".into(),
        ))
    } else {
        Ok(())
    }
}

/// OffsetCommit or OffsetFetch v10 on the group's coordinator.
///
/// The body is the typed UUID request. A coordinator error rediscovers that
/// broker on the caller's deadline. Name-only v2–v9 stays on
/// [`coord_offsets_legacy`].
#[expect(
    clippy::too_many_arguments,
    reason = "v10 offset RPC carries the learned bootstrap versions"
)]
pub(super) async fn coord_offsets_v10(
    coord: &mut BrokerConn,
    cfg: &crate::ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api: i16,
    mut encode: impl FnMut(&mut bytes::BytesMut) -> Result<()>,
    bootstrap_keys: &[crate::protocol::api::ApiVersion],
    timeout: Duration,
) -> Result<bytes::Bytes> {
    let target = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Timeout)?;
    let deadline = Deadline::from_std(target);
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        *coord = super::reopen_known_coord(&bounded, coord).await?;
        require_copied_coord_version(coord, api, 10)?;
    }
    let body = match coord
        .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(Error::Io(_) | Error::Timeout) => {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            *coord = super::reopen_known_coord(&bounded, coord).await?;
            require_copied_coord_version(coord, api, 10)?;
            coord
                .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
                .await?
        }
        Err(err) => return Err(err),
    };
    if v10_coordinator_code(api, &body).is_some_and(crate::error::coordinator_retriable) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        let known = super::KnownCoord::capture(coord).with_bootstrap_keys(bootstrap_keys);
        let mut prior = std::collections::HashMap::new();
        *coord = Box::pin(super::rediscover_known_coord(
            &bounded, group_id, key_type, &known, &mut prior,
        ))
        .await?;
        let picked = if api == crate::protocol::api_keys::OFFSET_FETCH {
            coord.offset_fetch_version
        } else {
            coord.offset_commit_version
        };
        if picked < 10 {
            return Err(Error::Unsupported(
                "moved coordinator cannot represent the offset request".into(),
            ));
        }
        coord
            .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
            .await
    } else {
        Ok(body)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "legacy offset RPC and coordinator discovery identity"
)]
pub(super) async fn coord_offsets_legacy(
    coord: &mut BrokerConn,
    cfg: &crate::ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api: i16,
    version: i16,
    mut encode: impl FnMut(&mut bytes::BytesMut) -> Result<()>,
    bootstrap_keys: &[crate::protocol::api::ApiVersion],
    timeout: Duration,
) -> Result<bytes::Bytes> {
    if version >= 10 {
        return Err(Error::Unsupported(
            "name-only offset RPC cannot use v10".into(),
        ));
    }
    let target = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Timeout)?;
    let deadline = Deadline::from_std(target);
    {
        if coord.idle_expired(cfg.connections_max_idle) {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            *coord = super::reopen_known_coord(&bounded, coord).await?;
            require_copied_coord_version(coord, api, version)?;
        }
        let body = match coord
            .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
            .await
        {
            Ok(body) => body,
            Err(Error::Io(_) | Error::Timeout) => {
                let bounded = bounded_consumer_config(cfg, deadline)?;
                *coord = super::reopen_known_coord(&bounded, coord).await?;
                require_copied_coord_version(coord, api, version)?;
                coord
                    .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
                    .await?
            }
            Err(err) => return Err(err),
        };
        if super::coordinator_error(api, version, &body)
            .is_some_and(crate::error::coordinator_retriable)
        {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            let known = super::KnownCoord::capture(coord).with_bootstrap_keys(bootstrap_keys);
            let mut prior = std::collections::HashMap::new();
            *coord = Box::pin(super::rediscover_known_coord(
                &bounded, group_id, key_type, &known, &mut prior,
            ))
            .await?;
            require_copied_coord_version(coord, api, version)?;
            coord
                .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
                .await
        } else {
            Ok(body)
        }
    }
}

fn bounded_consumer_config(
    cfg: &crate::ConsumerConfig,
    deadline: Deadline,
) -> Result<crate::ConsumerConfig> {
    let mut bounded = cfg.clone();
    bounded.request_timeout = deadline.remaining()?;
    bounded.connect_timeout = bounded.connect_timeout.min(bounded.request_timeout);
    Ok(bounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn a_repeated_v10_fetch_keeps_the_group_name() {
        let mut group_id = String::new();
        copy_text(&mut group_id, "orders");
        let group = group_id.as_ptr();
        copy_text(&mut group_id, "orders");
        assert_eq!(group_id.as_ptr(), group);
        let mut slots = Vec::new();
        let mut reverse = HashMap::new();
        let orders = [7u8; 16];
        let topics = [OffsetFetchTopic {
            topic: "orders".to_string(),
            partitions: vec![0, 1],
        }];
        refill_fetch_topics(&mut slots, &topics, |_| Some(orders), &mut reverse).unwrap();
        let group_slot = slots.first().expect("topic");
        let indexes = group_slot.partition_indexes.as_ptr();
        let capacity = group_slot.partition_indexes.capacity();
        refill_fetch_topics(&mut slots, &topics, |_| Some(orders), &mut reverse).unwrap();
        let group_slot = slots.first().expect("topic");
        assert_eq!(group_slot.partition_indexes.as_ptr(), indexes);
        assert!(group_slot.partition_indexes.capacity() >= capacity);
        assert_eq!(group_slot.partition_indexes, [0, 1]);
        assert!(matches!(
            group_slot.identity,
            OffsetTopicIdentity::Id(id) if id == orders
        ));
        assert_eq!(reverse.get(&orders).map(String::as_str), Some("orders"));
    }

    #[test]
    fn a_repeated_v10_commit_keeps_the_group_name() {
        let mut request = OffsetCommitRequestData::default();
        let id = [4u8; 16];
        let mut used = 0usize;
        copy_commit_identity(&mut request, "orders", "m-1", 2, None);
        push_commit_part(
            &mut request,
            &mut used,
            id,
            OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 1,
                committed_leader_epoch: -1,
                committed_metadata: Some(String::new()),
            },
        );
        request.topics.truncate(used);
        let group = request.group_id.as_ptr();
        let member = request.member_id.as_ptr();
        let capacity = request
            .topics
            .first()
            .map(|topic| topic.partitions.capacity())
            .unwrap_or(0);
        used = 0;
        copy_commit_identity(&mut request, "orders", "m-1", 2, None);
        push_commit_part(
            &mut request,
            &mut used,
            id,
            OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 8,
                committed_leader_epoch: -1,
                committed_metadata: Some(String::new()),
            },
        );
        request.topics.truncate(used);
        assert_eq!(request.group_id.as_ptr(), group);
        assert_eq!(request.member_id.as_ptr(), member);
        let topic = request.topics.first().expect("topic");
        assert!(topic.partitions.capacity() >= capacity);
        assert_eq!(
            topic.partitions.first().map(|part| part.committed_offset),
            Some(8)
        );
        assert!(matches!(topic.identity, OffsetTopicIdentity::Id(stored) if stored == id));
    }

    #[test]
    fn a_v10_group_commit_names_topic_ids() {
        let id = [9u8; 16];
        let request = OffsetCommitRequestData {
            group_id: "g".into(),
            generation_id_or_member_epoch: 3,
            member_id: "m-1".into(),
            group_instance_id: None,
            topics: vec![OffsetCommitTopicData {
                identity: OffsetTopicIdentity::Id(id),
                partitions: vec![OffsetCommitPartitionData {
                    partition_index: 1,
                    committed_offset: 4,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                }],
            }],
        };
        let limits = OffsetLimits::default();
        let bytes = encode_offset_commit_request_data(&request, 10, limits).unwrap();
        let mut direct = bytes::BytesMut::new();
        encode_offset_commit_request_into(&mut direct, &request, 10, limits).unwrap();
        assert_eq!(&direct[..], &bytes[..]);
        let wide = OffsetCommitRequestData {
            group_id: "g".into(),
            generation_id_or_member_epoch: 3,
            member_id: "m-1".into(),
            group_instance_id: None,
            topics: vec![OffsetCommitTopicData {
                identity: OffsetTopicIdentity::Id(id),
                partitions: (0..8)
                    .map(|partition| OffsetCommitPartitionData {
                        partition_index: partition,
                        committed_offset: 4,
                        committed_leader_epoch: -1,
                        committed_metadata: Some("cursor".into()),
                    })
                    .collect(),
            }],
        };
        let mut sized = bytes::BytesMut::new();
        encode_offset_commit_request_into(&mut sized, &wide, 10, limits).unwrap();
        let len = sized.len();
        let capacity = sized.capacity();
        assert!(capacity >= len);
        sized.clear();
        encode_offset_commit_request_into(&mut sized, &wide, 10, limits).unwrap();
        assert_eq!(sized.len(), len);
        assert_eq!(sized.capacity(), capacity);
        let names = ["orders".to_string()];
        let metadata = encode_offset_metadata_names(Some(&names), 12, limits).unwrap();
        validate_offset_metadata_names(Some(&names), 12, limits).unwrap();
        let mut metadata_direct = bytes::BytesMut::new();
        encode_offset_metadata_names_into(&mut metadata_direct, Some(&names), 12, limits).unwrap();
        assert_eq!(&metadata_direct[..], &metadata[..]);
        let decoded = decode_offset_commit_request_data(&bytes, 10, limits).unwrap();
        assert_eq!(decoded.group_id, "g");
        assert_eq!(decoded.member_id, "m-1");
        let topic = decoded.topics.first().expect("topic");
        assert_eq!(topic.identity, OffsetTopicIdentity::Id(id));
        assert!(!bytes.windows(8).any(|window| window == b"payments"));
    }

    #[test]
    fn a_v10_commit_response_reports_a_coordinator_error() {
        let limits = OffsetLimits::default();
        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponseData {
                identity: OffsetTopicIdentity::Id([1u8; 16]),
                partitions: vec![OffsetCommitPartitionResponseData {
                    partition_index: 0,
                    error_code: crate::error::NOT_COORDINATOR,
                }],
            }],
        };
        let bytes = encode_offset_commit_response_data(&response, 10, limits).unwrap();
        assert_eq!(
            offset_commit_v10_code(&bytes),
            Some(crate::error::NOT_COORDINATOR)
        );
        assert_eq!(
            v10_coordinator_code(crate::protocol::api_keys::OFFSET_COMMIT, &bytes),
            Some(crate::error::NOT_COORDINATOR)
        );
        let empty =
            encode_offset_commit_response_data(&OffsetCommitResponseData::default(), 10, limits)
                .unwrap();
        assert_eq!(offset_commit_v10_code(&empty), None);
        let fetched = OffsetFetchResponseData {
            throttle_time_ms: 0,
            groups: vec![OffsetFetchGroupResponseData {
                group_id: "g".into(),
                topics: Vec::new(),
                error_code: crate::error::NOT_COORDINATOR,
            }],
        };
        let fetched = encode_offset_fetch_response_data(&fetched, 10, limits).unwrap();
        assert_eq!(
            offset_fetch_v10_code(&fetched),
            Some(crate::error::NOT_COORDINATOR)
        );
        assert_eq!(
            v10_coordinator_code(crate::protocol::api_keys::OFFSET_FETCH, &fetched),
            Some(crate::error::NOT_COORDINATOR)
        );
    }
}
