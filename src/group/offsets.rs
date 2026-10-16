use super::ConsumerGroup;
use crate::admin::AdminConfig;
use crate::consumer::{OffsetAndMetadata, TopicPartition};
use crate::error::{Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::offsets::{OffsetClient, OffsetOptions};
use crate::protocol::group::*;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

impl ConsumerGroup {
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
        let metadata = self.consumer.topic_name_ids();
        let mut topics = BTreeMap::<[u8; 16], Vec<OffsetCommitPartitionData>>::new();
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
            topics
                .entry(id)
                .or_default()
                .push(OffsetCommitPartitionData {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: self.consumer.position_epoch(topic, *partition),
                    committed_metadata: Some(String::new()),
                });
        }
        if topics.is_empty() {
            return Ok(());
        }
        let request = OffsetCommitRequestData {
            group_id: self.group_id.clone(),
            generation_id_or_member_epoch: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: self.cfg.group_instance_id.clone(),
            topics: topics
                .into_iter()
                .map(|(id, partitions)| OffsetCommitTopicData {
                    identity: OffsetTopicIdentity::Id(id),
                    partitions,
                })
                .collect(),
        };
        self.finish_commit_v10(request, timeout).await
    }

    pub(super) async fn commit_offsets_v10(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        timeout: Duration,
        snapshot: Option<&super::DeliveredTopicIds>,
    ) -> Result<()> {
        let metadata = self.consumer.topic_name_ids();
        let mut topics = BTreeMap::<[u8; 16], Vec<OffsetCommitPartitionData>>::new();
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
            topics
                .entry(id)
                .or_default()
                .push(OffsetCommitPartitionData {
                    partition_index: tp.partition,
                    committed_offset: md.offset,
                    committed_leader_epoch: md.leader_epoch.unwrap_or(-1),
                    committed_metadata: Some(md.metadata.clone()),
                });
        }
        let request = OffsetCommitRequestData {
            group_id: self.group_id.clone(),
            generation_id_or_member_epoch: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: self.cfg.group_instance_id.clone(),
            topics: topics
                .into_iter()
                .map(|(id, partitions)| OffsetCommitTopicData {
                    identity: OffsetTopicIdentity::Id(id),
                    partitions,
                })
                .collect(),
        };
        self.finish_commit_v10(request, timeout).await?;
        self.cfg.interceptors.on_commit(offsets);
        Ok(())
    }

    async fn finish_commit_v10(
        &mut self,
        request: OffsetCommitRequestData,
        timeout: Duration,
    ) -> Result<()> {
        let limits = OffsetLimits::default();
        let encoded = encode_offset_commit_request_data(&request, 10, limits)?;
        let body = coord_offsets_v10(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            super::COORDINATOR_GROUP,
            crate::protocol::api_keys::OFFSET_COMMIT,
            |buf| {
                buf.extend_from_slice(&encoded);
                Ok(())
            },
            timeout,
        )
        .await?;
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
        let names = self.consumer.topic_name_ids();
        let mut reverse = HashMap::new();
        let topics = topics
            .iter()
            .map(|topic| {
                let id = names
                    .get(topic.topic.as_str())
                    .copied()
                    .filter(|id| *id != [0; 16])
                    .ok_or_else(|| {
                        Error::Unsupported(
                            "OffsetFetch v10 requires a captured nonzero topic UUID".into(),
                        )
                    })?;
                if reverse.insert(id, topic.topic.clone()).is_some() {
                    return Err(Error::protocol(
                        "offset metadata maps multiple names to one UUID",
                    ));
                }
                Ok(OffsetFetchTopicData {
                    identity: OffsetTopicIdentity::Id(id),
                    partition_indexes: topic.partitions.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchGroupData {
                group_id: self.group_id.clone(),
                member_id: self.kip848.then(|| self.member_id.clone()),
                member_epoch: if self.kip848 { self.generation_id } else { -1 },
                topics: Some(topics),
            }],
            require_stable: self.cfg.isolation_level == crate::IsolationLevel::ReadCommitted,
        };
        let response = self
            .offset_client()?
            .fetch(
                &request,
                &OffsetOptions {
                    topic_ids: true,
                    timeout: Some(timeout),
                    ..Default::default()
                },
            )
            .await?;
        let group = response
            .groups
            .into_iter()
            .next()
            .ok_or_else(|| Error::protocol("missing fetched group"))?;
        if group.error_code != 0 {
            return Err(Error::broker(group.error_code, "OffsetFetch"));
        }
        group
            .topics
            .into_iter()
            .map(|topic| {
                let OffsetTopicIdentity::Id(id) = topic.identity else {
                    return Err(Error::protocol("v10 returned a topic name"));
                };
                let name = reverse
                    .get(&id)
                    .cloned()
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
            .collect()
    }
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

/// OffsetCommit v10 on the group's coordinator.
///
/// The body is the typed UUID request. A coordinator error rediscovers that
/// broker on the caller's deadline. Name-only v2–v9 stays on
/// [`coord_offsets_legacy`].
pub(super) async fn coord_offsets_v10(
    coord: &mut BrokerConn,
    cfg: &crate::ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api: i16,
    mut encode: impl FnMut(&mut bytes::BytesMut) -> Result<()>,
    timeout: Duration,
) -> Result<bytes::Bytes> {
    let target = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Timeout)?;
    let deadline = Deadline::from_std(target);
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        *coord = super::open_coord_supporting(&bounded, coord.addr(), api, 10).await?;
    }
    let body = match coord
        .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(Error::Io(_) | Error::Timeout) => {
            let bounded = bounded_consumer_config(cfg, deadline)?;
            *coord = super::open_coord_supporting(&bounded, coord.addr(), api, 10).await?;
            coord
                .roundtrip_deadline(api, 10, |buf| encode(buf), deadline)
                .await?
        }
        Err(err) => return Err(err),
    };
    if offset_commit_v10_code(&body).is_some_and(crate::error::coordinator_retriable) {
        let bounded = bounded_consumer_config(cfg, deadline)?;
        *coord = super::discover_coord_supporting(&bounded, group_id, key_type, api, 10).await?;
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
            *coord = super::open_coord_supporting(&bounded, coord.addr(), api, version).await?;
        }
        let body = match coord
            .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
            .await
        {
            Ok(body) => body,
            Err(Error::Io(_) | Error::Timeout) => {
                let bounded = bounded_consumer_config(cfg, deadline)?;
                *coord = super::open_coord_supporting(&bounded, coord.addr(), api, version).await?;
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
            *coord = super::discover_coord_supporting(&bounded, group_id, key_type, api, version)
                .await?;
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
        let empty =
            encode_offset_commit_response_data(&OffsetCommitResponseData::default(), 10, limits)
                .unwrap();
        assert_eq!(offset_commit_v10_code(&empty), None);
    }
}
