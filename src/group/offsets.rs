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
        super::refill_offset_topics_cooperative(&mut commit_topics, offsets).await;
        let group_id = self.group_id.as_str();
        let member_id = self.member_id.as_str();
        let generation_id = self.generation_id;
        let instance = self.cfg.group_instance_id.as_deref();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let request = LegacyOffsetRequest::Commit {
            group_id,
            generation: generation_id,
            member_id,
            instance,
            topics: &commit_topics,
        };
        let rpc = coord_offsets_typed(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            &request,
            &bootstrap_keys,
            &address_keys,
            prior,
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
        let err = match decode_offset_commit_response_cooperative(&mut body.clone(), version).await
        {
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
        let address_keys = self.consumer.copy_learned_broker_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let sent = coord_offsets_v10(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |buf| encode_offset_commit_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            &address_keys,
            prior,
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
        super::refresh_delivered_topic_ids(&mut ids, &positions, |topic, partition| {
            self.consumer.assigned_topic_id(topic, partition)
        });
        self.last_delivered_topic_ids = ids;
        self.last_delivered_positions = Some(positions);
    }

    pub(super) async fn snapshot_delivered_positions_cooperative(&mut self) {
        let mut positions = self.last_delivered_positions.take().unwrap_or_default();
        self.consumer
            .fill_delivered_positions_shared_cooperative(&mut positions)
            .await;
        let mut ids = std::mem::take(&mut self.last_delivered_topic_ids);
        super::refresh_delivered_topic_ids_cooperative(&mut ids, &positions, |topic, partition| {
            self.consumer.assigned_topic_id(topic, partition)
        })
        .await;
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
        self.pending_async_commits
            .get_mut()
            .push((offsets, callback, ids));
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
        let address_keys = self.consumer.copy_learned_broker_keys();
        let sent = coord_offsets_v10(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            super::COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |buf| encode_offset_commit_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
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
        let address_keys = self.consumer.copy_learned_broker_keys();
        let sent = coord_offsets_v10(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            super::COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_FETCH,
            |buf| encode_offset_fetch_request_into(buf, &request, 10, limits),
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
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
    let range = if api == crate::protocol::api_keys::OFFSET_FETCH {
        coord.offset_fetch_range
    } else if api == crate::protocol::api_keys::OFFSET_COMMIT {
        coord.offset_commit_range
    } else {
        return Err(Error::protocol("unexpected offset coordinator API"));
    };
    copied_offset_version(range, version)
}

fn copied_offset_version(range: Option<(i16, i16)>, needed: i16) -> Result<()> {
    if range.is_some_and(|(min, max)| (min..=max).contains(&needed)) {
        Ok(())
    } else {
        Err(Error::Unsupported(
            "moved coordinator cannot represent the offset request".into(),
        ))
    }
}

/// OffsetCommit or OffsetFetch v10 on the group's coordinator.
///
/// The body is the typed UUID request. A coordinator error rediscovers that
/// broker on the caller's deadline. Name-only v2–v9 stays on
/// [`coord_offsets_typed`].
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
    mut encode: impl FnMut(&mut bytes::BytesMut) -> Result<()> + Send,
    bootstrap_keys: &[crate::protocol::api::ApiVersion],
    address_keys: &std::collections::HashMap<String, Vec<crate::protocol::api::ApiVersion>>,
    prior: &mut std::collections::HashMap<String, super::KnownCoord>,
    timeout: Duration,
) -> Result<bytes::Bytes> {
    let target = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Timeout)?;
    let deadline = Deadline::from_std(target);
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        let replacement = super::reopen_known_coord(&bounded, coord).await?;
        let previous = std::mem::replace(coord, replacement);
        super::retire_lookup_conn(previous).await;
        require_copied_coord_version(coord, api, 10)?;
    }
    let mut body = match coord
        .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(Error::Io(_) | Error::Timeout | Error::Closed) => {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            let replacement = super::reopen_known_coord(&bounded, coord).await?;
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
            require_copied_coord_version(coord, api, 10)?;
            coord
                .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
                .await?
        }
        Err(err) => return Err(err),
    };
    let mut coordinator = v10_coordinator_code(api, &body);
    if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
        match coord
            .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
            .await
        {
            Ok(retried) => {
                body = retried;
                coordinator = v10_coordinator_code(api, &body);
                if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
                    return Ok(body);
                }
            }
            Err(Error::Io(_) | Error::Timeout | Error::Closed) => {}
            Err(error) => return Err(error),
        }
    }
    if coordinator.is_some_and(crate::error::coordinator_retriable) {
        let known = super::KnownCoord::capture(coord)
            .with_bootstrap_keys(bootstrap_keys)
            .with_address_keys(address_keys);
        if let Some(replacement) = Box::pin(super::rediscover_with_live_bootstrap(
            coord, cfg, group_id, key_type, &known, prior, deadline,
        ))
        .await?
        {
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
            require_copied_coord_version(coord, api, 10)?;
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
    mut version: i16,
    mut encode: impl FnMut(i16, &mut bytes::BytesMut) -> Result<()> + Send,
    bootstrap_keys: &[crate::protocol::api::ApiVersion],
    address_keys: &std::collections::HashMap<String, Vec<crate::protocol::api::ApiVersion>>,
    prior: &mut std::collections::HashMap<String, super::KnownCoord>,
    timeout: Duration,
) -> Result<(i16, bytes::Bytes)> {
    if version >= 10 {
        return Err(Error::Unsupported(
            "name-only offset RPC cannot use v10".into(),
        ));
    }
    let target = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Timeout)?;
    let deadline = Deadline::from_std(target);
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        let replacement = super::reopen_known_coord(&bounded, coord).await?;
        let previous = std::mem::replace(coord, replacement);
        super::retire_lookup_conn(previous).await;
        version = legacy_coord_version(coord, api)?;
    }
    let mut body = match coord
        .roundtrip_deadline(api, version, |buf| encode(version, buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(Error::Io(_) | Error::Timeout | Error::Closed) => {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            let replacement = super::reopen_known_coord(&bounded, coord).await?;
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
            version = legacy_coord_version(coord, api)?;
            coord
                .roundtrip_deadline(api, version, |buf| encode(version, buf), deadline)
                .await?
        }
        Err(err) => return Err(err),
    };
    let mut coordinator = if api == crate::protocol::api_keys::OFFSET_FETCH {
        super::offset_fetch_error(version, &body).await
    } else if api == crate::protocol::api_keys::OFFSET_COMMIT {
        super::offset_commit_error(version, &body).await
    } else {
        super::coordinator_error(api, version, &body)
    };
    if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
        match coord
            .roundtrip_deadline(api, version, |buf| encode(version, buf), deadline)
            .await
        {
            Ok(retried) => {
                body = retried;
                coordinator = if api == crate::protocol::api_keys::OFFSET_FETCH {
                    super::offset_fetch_error(version, &body).await
                } else if api == crate::protocol::api_keys::OFFSET_COMMIT {
                    super::offset_commit_error(version, &body).await
                } else {
                    super::coordinator_error(api, version, &body)
                };
                if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
                    return Ok((version, body));
                }
            }
            Err(Error::Io(_) | Error::Timeout | Error::Closed) => {}
            Err(error) => return Err(error),
        }
    }
    if coordinator.is_some_and(crate::error::coordinator_retriable) {
        let known = super::KnownCoord::capture(coord)
            .with_bootstrap_keys(bootstrap_keys)
            .with_address_keys(address_keys);
        if let Some(replacement) = Box::pin(super::rediscover_with_live_bootstrap(
            coord, cfg, group_id, key_type, &known, prior, deadline,
        ))
        .await?
        {
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
        }
        version = legacy_coord_version(coord, api)?;
        let body = coord
            .roundtrip_deadline(api, version, |buf| encode(version, buf), deadline)
            .await?;
        Ok((version, body))
    } else {
        Ok((version, body))
    }
}

/// Select a name-based version from the replacement socket's own range.
fn legacy_coord_version(coord: &BrokerConn, api: i16) -> Result<i16> {
    let (range, minimum) = if api == crate::protocol::api_keys::OFFSET_FETCH {
        (coord.offset_fetch_range, 1)
    } else if api == crate::protocol::api_keys::OFFSET_COMMIT {
        (coord.offset_commit_range, 2)
    } else {
        return Err(Error::protocol("unexpected offset coordinator API"));
    };
    name_based_offset_version(range, minimum)
}

fn name_based_offset_version(range: Option<(i16, i16)>, minimum: i16) -> Result<i16> {
    range
        .and_then(|(min, max)| super::pick_version(min, max, minimum, 9))
        .ok_or_else(|| {
            Error::Unsupported("moved coordinator does not support name-based offsets".into())
        })
}

/// Check old-version field compatibility without a long uninterrupted scan.
async fn require_commit_fields_cooperative(
    version: i16,
    instance: Option<&str>,
    topics: &[OffsetTopic],
) -> Result<()> {
    if version < 7 && instance.is_some() {
        return Err(Error::Unsupported(
            "OffsetCommit version cannot carry group instance ID".into(),
        ));
    }
    if version < 6 {
        let mut walked = 0usize;
        for topic in topics {
            for partition in &topic.partitions {
                if partition.leader_epoch != -1 {
                    return Err(Error::Unsupported(
                        "OffsetCommit version cannot carry leader epochs".into(),
                    ));
                }
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
                }
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    Ok(())
}

/// Preserve stable-read and KIP-848 membership fields when negotiating.
pub(super) fn require_fetch_fields(
    version: i16,
    member: Option<&str>,
    epoch: i32,
    stable: bool,
) -> Result<()> {
    if stable && version < 7 {
        return Err(Error::Unsupported(
            "OffsetFetch version cannot require stable offsets".into(),
        ));
    }
    if version < 9 && (member.is_some() || epoch != -1) {
        return Err(Error::Unsupported(
            "OffsetFetch version cannot carry member identity and epoch".into(),
        ));
    }
    Ok(())
}

/// Name-based offset request whose body can be encoded for a newly negotiated version.
pub(super) enum LegacyOffsetRequest<'a> {
    Commit {
        group_id: &'a str,
        generation: i32,
        member_id: &'a str,
        instance: Option<&'a str>,
        topics: &'a [OffsetTopic],
    },
    Fetch {
        group_id: &'a str,
        member_id: Option<&'a str>,
        member_epoch: i32,
        require_stable: bool,
        topics: Option<&'a [OffsetFetchTopic]>,
    },
}

impl LegacyOffsetRequest<'_> {
    fn api(&self) -> i16 {
        match self {
            Self::Commit { .. } => crate::protocol::api_keys::OFFSET_COMMIT,
            Self::Fetch { .. } => crate::protocol::api_keys::OFFSET_FETCH,
        }
    }

    async fn encode(&self, version: i16) -> Result<bytes::BytesMut> {
        let mut body = bytes::BytesMut::new();
        match self {
            Self::Commit {
                group_id,
                generation,
                member_id,
                instance,
                topics,
            } => {
                require_commit_fields_cooperative(version, *instance, topics).await?;
                encode_offset_commit_request_cooperative(
                    &mut body,
                    version,
                    group_id,
                    *generation,
                    member_id,
                    *instance,
                    DEFAULT_RETENTION_TIME,
                    topics,
                )
                .await?;
            }
            Self::Fetch {
                group_id,
                member_id,
                member_epoch,
                require_stable,
                topics,
            } => {
                require_fetch_fields(version, *member_id, *member_epoch, *require_stable)?;
                encode_offset_fetch_request_cooperative(
                    &mut body,
                    version,
                    group_id,
                    *member_id,
                    *member_epoch,
                    *require_stable,
                    *topics,
                )
                .await?;
            }
        }
        Ok(body)
    }
}

async fn send_typed_offset(
    coord: &mut BrokerConn,
    request: &LegacyOffsetRequest<'_>,
    version: i16,
    cached: &mut Option<(i16, bytes::BytesMut)>,
    deadline: Deadline,
) -> Result<bytes::Bytes> {
    if cached
        .as_ref()
        .is_none_or(|(cached_version, _)| *cached_version != version)
    {
        let body = tokio::time::timeout_at(deadline.target(), request.encode(version))
            .await
            .map_err(|_| Error::Timeout)??;
        *cached = Some((version, body));
    }
    let Some((_, body)) = cached.as_ref() else {
        return Err(Error::protocol("missing encoded offset request"));
    };
    coord
        .roundtrip_preencoded_cooperative(request.api(), version, body, deadline)
        .await
}

/// Execute a typed name-based offset request, re-encoding for each replacement
/// coordinator. Encoding and frame assembly both observe the original deadline.
#[expect(
    clippy::too_many_arguments,
    reason = "offset RPC includes coordinator discovery state"
)]
pub(super) async fn coord_offsets_typed(
    coord: &mut BrokerConn,
    cfg: &crate::ConsumerConfig,
    group_id: &str,
    key_type: i8,
    request: &LegacyOffsetRequest<'_>,
    bootstrap_keys: &[crate::protocol::api::ApiVersion],
    address_keys: &std::collections::HashMap<String, Vec<crate::protocol::api::ApiVersion>>,
    prior: &mut std::collections::HashMap<String, super::KnownCoord>,
    timeout: Duration,
) -> Result<(i16, bytes::Bytes)> {
    let deadline = Deadline::from_std(
        std::time::Instant::now()
            .checked_add(timeout)
            .ok_or(Error::Timeout)?,
    );
    let api = request.api();
    if coord.idle_expired(cfg.connections_max_idle) {
        let replacement =
            super::reopen_known_coord(&bounded_consumer_config(cfg, deadline)?, coord).await?;
        let previous = std::mem::replace(coord, replacement);
        super::retire_lookup_conn(previous).await;
    }
    let mut version = legacy_coord_version(coord, api)?;
    let mut cached = None;
    let mut body = match send_typed_offset(coord, request, version, &mut cached, deadline).await {
        Ok(body) => body,
        Err(Error::Io(_) | Error::Timeout | Error::Closed) => {
            let replacement =
                super::reopen_known_coord(&bounded_consumer_config(cfg, deadline)?, coord).await?;
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
            version = legacy_coord_version(coord, api)?;
            send_typed_offset(coord, request, version, &mut cached, deadline).await?
        }
        Err(error) => return Err(error),
    };
    let mut coordinator = if api == crate::protocol::api_keys::OFFSET_FETCH {
        super::offset_fetch_error(version, &body).await
    } else {
        super::offset_commit_error(version, &body).await
    };
    if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
        match send_typed_offset(coord, request, version, &mut cached, deadline).await {
            Ok(retried) => {
                body = retried;
                coordinator = if api == crate::protocol::api_keys::OFFSET_FETCH {
                    super::offset_fetch_error(version, &body).await
                } else {
                    super::offset_commit_error(version, &body).await
                };
                // One live-socket load retry is enough. A different retriable
                // coordinator error still needs rediscovery below.
                if coordinator == Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS) {
                    return Ok((version, body));
                }
            }
            Err(Error::Io(_) | Error::Timeout | Error::Closed) => {}
            Err(error) => return Err(error),
        }
    }
    if coordinator.is_some_and(crate::error::coordinator_retriable) {
        let known = super::KnownCoord::capture(coord)
            .with_bootstrap_keys(bootstrap_keys)
            .with_address_keys(address_keys);
        if let Some(replacement) = Box::pin(super::rediscover_with_live_bootstrap(
            coord, cfg, group_id, key_type, &known, prior, deadline,
        ))
        .await?
        {
            let previous = std::mem::replace(coord, replacement);
            super::retire_lookup_conn(previous).await;
        }
        version = legacy_coord_version(coord, api)?;
        let body = send_typed_offset(coord, request, version, &mut cached, deadline).await?;
        Ok((version, body))
    } else {
        Ok((version, body))
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
mod reconnect_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn name_based_offsets_follow_both_bounds_of_the_new_socket() {
        assert_eq!(
            super::name_based_offset_version(Some((2, 8)), 2).unwrap(),
            8
        );
        assert_eq!(
            super::name_based_offset_version(Some((9, 10)), 2).unwrap(),
            9
        );
        assert!(matches!(
            super::name_based_offset_version(Some((10, 10)), 2),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            super::name_based_offset_version(None, 1),
            Err(Error::Unsupported(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn offset_downgrade_does_not_drop_identity_or_stability_fields() {
        let mut topic = OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 1)],
        };
        assert!(
            super::require_commit_fields_cooperative(6, Some("static"), &[topic.clone()])
                .await
                .is_err()
        );
        assert!(
            super::require_commit_fields_cooperative(7, Some("static"), &[topic.clone()])
                .await
                .is_ok()
        );
        topic.partitions[0].leader_epoch = 3;
        assert!(
            super::require_commit_fields_cooperative(5, None, &[topic.clone()])
                .await
                .is_err()
        );
        assert!(super::require_commit_fields_cooperative(6, None, &[topic])
            .await
            .is_ok());
        assert!(super::require_fetch_fields(6, None, -1, true).is_err());
        assert!(super::require_fetch_fields(7, None, -1, true).is_ok());
        assert!(super::require_fetch_fields(8, Some("member"), 3, false).is_err());
        assert!(super::require_fetch_fields(9, Some("member"), 3, false).is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn large_offset_reencode_yields_before_completion() {
        use std::future::Future;
        use std::task::Poll;

        let topics: Vec<_> = (0..20_000)
            .map(|index| OffsetTopic {
                topic: format!("topic-{index}"),
                partitions: vec![OffsetPartition::new(0, 1)],
            })
            .collect();
        let request = super::LegacyOffsetRequest::Commit {
            group_id: "group",
            generation: 1,
            member_id: "member",
            instance: None,
            topics: &topics,
        };
        let mut encoded = Box::pin(request.encode(5));
        let first = std::future::poll_fn(|cx| Poll::Ready(encoded.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "large offset validation or encoding must yield"
        );
        let body = encoded.await.unwrap();
        let (group, member, decoded, _, _, generation) =
            decode_offset_commit_request(&mut body.as_ref(), 5).unwrap();
        assert_eq!(group, "group");
        assert_eq!(member, "member");
        assert_eq!(generation, 1);
        assert_eq!(decoded.len(), topics.len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn typed_offset_reconnect_reencodes_for_both_new_range_bounds() {
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_COMMIT, OFFSET_FETCH};
        use crate::protocol::header::{decode_request_header, encode_response_header};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for api in [OFFSET_COMMIT, OFFSET_FETCH] {
            for (old_version, new_version) in [(9i16, 8i16), (8, 9)] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap().to_string();
                let peer = tokio::spawn(async move {
                    let (_old, _) = listener.accept().await.unwrap();
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let size = socket.read_i32().await.unwrap();
                    let mut bytes = vec![0; usize::try_from(size).unwrap()];
                    let _read = socket.read_exact(&mut bytes).await.unwrap();
                    let header = decode_request_header(&mut bytes.as_slice()).unwrap();
                    assert_eq!(header.api_key, 18);
                    let versions = ApiVersionsResponse {
                        api_keys: vec![
                            ApiVersion {
                                api_key: FIND_COORDINATOR,
                                min_version: 0,
                                max_version: 1,
                            },
                            ApiVersion {
                                api_key: api,
                                min_version: new_version,
                                max_version: new_version,
                            },
                        ],
                        ..ApiVersionsResponse::default()
                    };
                    let mut response = bytes::BytesMut::new();
                    encode_response_header(
                        &mut response,
                        18,
                        header.api_version,
                        header.correlation_id,
                    )
                    .unwrap();
                    encode_api_versions_response(&mut response, header.api_version, &versions)
                        .unwrap();
                    socket
                        .write_i32(i32::try_from(response.len()).unwrap())
                        .await
                        .unwrap();
                    socket.write_all(&response).await.unwrap();

                    let size = socket.read_i32().await.unwrap();
                    let mut bytes = vec![0; usize::try_from(size).unwrap()];
                    let _read = socket.read_exact(&mut bytes).await.unwrap();
                    let mut input = bytes.as_slice();
                    let header = decode_request_header(&mut input).unwrap();
                    assert_eq!((header.api_key, header.api_version), (api, new_version));
                    let mut response = bytes::BytesMut::new();
                    encode_response_header(&mut response, api, new_version, header.correlation_id)
                        .unwrap();
                    if api == OFFSET_COMMIT {
                        let (group, member, topics, _, instance, generation) =
                            decode_offset_commit_request(&mut input, new_version).unwrap();
                        assert_eq!(
                            (group.as_str(), member.as_str(), generation),
                            ("group", "member", 3)
                        );
                        assert_eq!(instance.as_deref(), Some("static"));
                        assert_eq!(topics[0].partitions[0].offset, 42);
                        assert_eq!(topics[0].partitions[0].leader_epoch, 6);
                        encode_offset_commit_response(&mut response, new_version, &topics, 0)
                            .unwrap();
                    } else {
                        let (group, topics, stable) =
                            decode_offset_fetch_request(&mut input, new_version).unwrap();
                        assert_eq!(group, "group");
                        assert!(stable);
                        assert_eq!(topics.unwrap()[0].partitions, vec![0]);
                        encode_offset_fetch_response(&mut response, new_version, "group", &[], 0)
                            .unwrap();
                    }
                    socket
                        .write_i32(i32::try_from(response.len()).unwrap())
                        .await
                        .unwrap();
                    socket.write_all(&response).await.unwrap();
                });
                let mut coord =
                    BrokerConn::connect(&addr, "offset-reencode", Duration::from_secs(1))
                        .await
                        .unwrap();
                coord.offset_commit_version = old_version;
                coord.offset_fetch_version = old_version;
                let mut cfg = crate::ConsumerConfig::bootstrap([addr]);
                cfg.request_timeout = Duration::from_secs(1);
                cfg.connect_timeout = Duration::from_secs(1);
                cfg.connections_max_idle = Duration::from_nanos(1);
                tokio::time::sleep(Duration::from_millis(2)).await;
                let commits = [OffsetTopic {
                    topic: "t".into(),
                    partitions: vec![OffsetPartition {
                        partition: 0,
                        offset: 42,
                        leader_epoch: 6,
                        metadata: "metadata".into(),
                    }],
                }];
                let fetches = [OffsetFetchTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }];
                let request = if api == OFFSET_COMMIT {
                    super::LegacyOffsetRequest::Commit {
                        group_id: "group",
                        generation: 3,
                        member_id: "member",
                        instance: Some("static"),
                        topics: &commits,
                    }
                } else {
                    super::LegacyOffsetRequest::Fetch {
                        group_id: "group",
                        member_id: None,
                        member_epoch: -1,
                        require_stable: true,
                        topics: Some(&fetches),
                    }
                };
                let (version, body) = super::coord_offsets_typed(
                    &mut coord,
                    &cfg,
                    "group",
                    COORDINATOR_GROUP,
                    &request,
                    &[],
                    &HashMap::new(),
                    &mut HashMap::new(),
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                assert_eq!(version, new_version);
                if api == OFFSET_COMMIT {
                    assert_eq!(
                        decode_offset_commit_response_cooperative(&mut body.clone(), version)
                            .await
                            .unwrap(),
                        0
                    );
                } else {
                    assert!(
                        decode_offset_fetch_response_cooperative(&mut body.clone(), version)
                            .await
                            .unwrap()
                            .is_empty()
                    );
                }
                peer.await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn typed_offset_reconnect_rejects_unrepresentable_fields_before_wire() {
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_COMMIT, OFFSET_FETCH};
        use crate::protocol::header::{decode_request_header, encode_response_header};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // v10-only cannot accept a name-based request. A v5 commit cannot
        // retain static membership, and v6/v8 fetches cannot retain stable
        // reads or KIP-848 identity respectively.
        for (api, min, max, static_member, stable, kip) in [
            (OFFSET_COMMIT, 10, 10, false, false, false),
            (OFFSET_COMMIT, 2, 5, true, false, false),
            (OFFSET_FETCH, 1, 6, false, true, false),
            (OFFSET_FETCH, 1, 8, false, false, true),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let peer = tokio::spawn(async move {
                let (_old, _) = listener.accept().await.unwrap();
                let (mut socket, _) = listener.accept().await.unwrap();
                let size = socket.read_i32().await.unwrap();
                let mut request = vec![0; usize::try_from(size).unwrap()];
                let _read = socket.read_exact(&mut request).await.unwrap();
                let header = decode_request_header(&mut request.as_slice()).unwrap();
                let versions = ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: FIND_COORDINATOR,
                            min_version: 0,
                            max_version: 1,
                        },
                        ApiVersion {
                            api_key: api,
                            min_version: min,
                            max_version: max,
                        },
                    ],
                    ..ApiVersionsResponse::default()
                };
                let mut response = bytes::BytesMut::new();
                encode_response_header(
                    &mut response,
                    18,
                    header.api_version,
                    header.correlation_id,
                )
                .unwrap();
                encode_api_versions_response(&mut response, header.api_version, &versions).unwrap();
                socket
                    .write_i32(i32::try_from(response.len()).unwrap())
                    .await
                    .unwrap();
                socket.write_all(&response).await.unwrap();
                let mut byte = [0u8; 1];
                tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap()
            });
            let mut coord = BrokerConn::connect(&addr, "offset-reject", Duration::from_secs(1))
                .await
                .unwrap();
            coord.offset_commit_version = 9;
            coord.offset_fetch_version = 9;
            let mut cfg = crate::ConsumerConfig::bootstrap([addr]);
            cfg.request_timeout = Duration::from_secs(1);
            cfg.connect_timeout = Duration::from_secs(1);
            cfg.connections_max_idle = Duration::from_nanos(1);
            tokio::time::sleep(Duration::from_millis(2)).await;
            let commits = [OffsetTopic {
                topic: "t".into(),
                partitions: vec![OffsetPartition::new(0, 42)],
            }];
            let fetches = [OffsetFetchTopic {
                topic: "t".into(),
                partitions: vec![0],
            }];
            let request = if api == OFFSET_COMMIT {
                super::LegacyOffsetRequest::Commit {
                    group_id: "group",
                    generation: 3,
                    member_id: "member",
                    instance: static_member.then_some("static"),
                    topics: &commits,
                }
            } else {
                super::LegacyOffsetRequest::Fetch {
                    group_id: "group",
                    member_id: kip.then_some("member"),
                    member_epoch: if kip { 3 } else { -1 },
                    require_stable: stable,
                    topics: Some(&fetches),
                }
            };
            let result = super::coord_offsets_typed(
                &mut coord,
                &cfg,
                "group",
                COORDINATOR_GROUP,
                &request,
                &[],
                &HashMap::new(),
                &mut HashMap::new(),
                Duration::from_secs(1),
            )
            .await;
            assert!(
                matches!(result, Err(Error::Unsupported(_))),
                "unexpected {result:?}"
            );
            drop(coord);
            assert_eq!(
                peer.await.unwrap(),
                0,
                "unrepresentable offset request was sent"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn offset_encoding_deadline_expires_before_any_request_is_sent() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut conn =
            BrokerConn::connect(&addr.to_string(), "offset-deadline", Duration::from_secs(1))
                .await
                .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let topics: Vec<_> = (0..20_000)
            .map(|index| OffsetTopic {
                topic: format!("topic-{index}"),
                partitions: vec![OffsetPartition::new(0, 1)],
            })
            .collect();
        let request = super::LegacyOffsetRequest::Commit {
            group_id: "group",
            generation: 1,
            member_id: "member",
            instance: None,
            topics: &topics,
        };
        let result = super::send_typed_offset(
            &mut conn,
            &request,
            5,
            &mut None,
            Deadline::from_timeout(Duration::ZERO),
        )
        .await;
        assert!(matches!(result, Err(Error::Timeout)));
        drop(conn);
        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0, "timed-out encoder sent a partial request");
    }

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

    #[tokio::test(flavor = "current_thread")]
    async fn idle_offset_reconnect_rejects_a_raised_minimum_before_sending() {
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_COMMIT, OFFSET_FETCH};
        use crate::protocol::group::COORDINATOR_GROUP;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for api in [OFFSET_FETCH, OFFSET_COMMIT] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let peer = tokio::spawn(async move {
                let (_old, _) = listener.accept().await.unwrap();
                let (mut socket, _) = listener.accept().await.unwrap();
                let length = socket.read_i32().await.unwrap();
                assert!((8..=4096).contains(&length));
                let mut request = vec![0; usize::try_from(length).unwrap()];
                let _read = socket.read_exact(&mut request).await.unwrap();
                assert_eq!(i16::from_be_bytes(request[0..2].try_into().unwrap()), 18);
                assert_eq!(i16::from_be_bytes(request[2..4].try_into().unwrap()), 4);
                let versions = ApiVersionsResponse {
                    error_code: 0,
                    api_keys: vec![
                        ApiVersion {
                            api_key: FIND_COORDINATOR,
                            min_version: 0,
                            max_version: 1,
                        },
                        ApiVersion {
                            api_key: api,
                            min_version: 10,
                            max_version: 10,
                        },
                    ],
                    throttle_time_ms: 0,
                    supported_features: Vec::new(),
                    finalized_features_epoch: None,
                    finalized_features: Vec::new(),
                    zk_migration_ready: false,
                };
                let mut body = bytes::BytesMut::new();
                encode_api_versions_response(&mut body, 4, &versions).unwrap();
                socket
                    .write_i32(i32::try_from(4 + body.len()).unwrap())
                    .await
                    .unwrap();
                socket.write_all(&request[4..8]).await.unwrap();
                socket.write_all(&body).await.unwrap();
                let mut first_byte = [0u8; 1];
                socket.read(&mut first_byte).await.unwrap_or(0) != 0
            });
            let mut coord = BrokerConn::connect(&addr, "offset-range", Duration::from_secs(1))
                .await
                .unwrap();
            coord.offset_fetch_version = 8;
            coord.offset_commit_version = 8;
            let mut cfg = crate::ConsumerConfig::bootstrap([addr]);
            cfg.request_timeout = Duration::from_secs(1);
            cfg.connect_timeout = Duration::from_secs(1);
            cfg.connections_max_idle = Duration::from_nanos(1);
            tokio::time::sleep(Duration::from_millis(2)).await;
            let mut prior = HashMap::new();
            let result = coord_offsets_legacy(
                &mut coord,
                &cfg,
                "group",
                COORDINATOR_GROUP,
                api,
                8,
                |_version, _buf| Ok(()),
                &[],
                &HashMap::new(),
                &mut prior,
                Duration::from_secs(1),
            )
            .await;
            assert!(
                matches!(result, Err(Error::Unsupported(_))),
                "unexpected result: {result:?}"
            );
            drop(coord);
            let wrote_offset = tokio::time::timeout(Duration::from_secs(1), peer)
                .await
                .unwrap()
                .unwrap();
            assert!(
                !wrote_offset,
                "client sent a name-based offset API {api} outside the negotiated v10-only range"
            );
        }
    }

    #[test]
    fn a_reopened_v10_offset_connection_still_covers_v10() {
        assert!(super::copied_offset_version(Some((1, 10)), 10).is_ok());
        assert!(super::copied_offset_version(Some((1, 10)), 8).is_ok());
        assert!(super::copied_offset_version(Some((1, 9)), 10).is_err());
        assert!(super::copied_offset_version(Some((9, 10)), 8).is_err());
        assert!(super::copied_offset_version(None, 8).is_err());
    }
}
