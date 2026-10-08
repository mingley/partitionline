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
    pub(super) fn record_delivered_positions(&mut self, positions: Vec<(TopicPartition, i64)>) {
        self.last_delivered_topic_ids = positions
            .iter()
            .filter_map(|(tp, _)| {
                self.consumer
                    .assigned_topic_id(&tp.topic, tp.partition)
                    .map(|id| ((tp.topic.clone(), tp.partition), id))
            })
            .collect();
        self.last_delivered_positions = Some(positions);
    }

    pub(super) fn queue_async_commit(
        &mut self,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
        callback: Option<super::AsyncOffsetCommitCallback>,
    ) {
        let metadata = self.consumer.topic_name_ids();
        let ids = offsets
            .iter()
            .filter_map(|(tp, _)| {
                self.consumer
                    .assigned_topic_id(&tp.topic, tp.partition)
                    .or_else(|| metadata.get(&tp.topic).copied())
                    .filter(|id| *id != [0; 16])
                    .map(|id| ((tp.topic.clone(), tp.partition), id))
            })
            .collect();
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

    pub(super) async fn commit_offsets_v10(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        timeout: Duration,
        snapshot: Option<&HashMap<(String, i32), [u8; 16]>>,
    ) -> Result<()> {
        let metadata = self.consumer.topic_name_ids();
        let mut topics = BTreeMap::<[u8; 16], Vec<OffsetCommitPartitionData>>::new();
        for (tp, md) in offsets {
            let id = match snapshot {
                Some(ids) => ids.get(&(tp.topic.clone(), tp.partition)).copied(),
                None => self
                    .consumer
                    .assigned_topic_id(&tp.topic, tp.partition)
                    .or_else(|| metadata.get(&tp.topic).copied()),
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
        let response = self
            .offset_client()?
            .commit(
                &request,
                &OffsetOptions {
                    topic_ids: true,
                    timeout: Some(timeout),
                    ..Default::default()
                },
            )
            .await?;
        if let Some(code) = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| *code != 0)
        {
            return Err(Error::broker(code, "OffsetCommit"));
        }
        self.cfg.interceptors.on_commit(offsets);
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
                    .get(&topic.topic)
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
    encode: impl Fn(&mut bytes::BytesMut) -> Result<()>,
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
    deadline
        .run(async {
            if coord.idle_expired(cfg.connections_max_idle) {
                let bounded = bounded_consumer_config(cfg, deadline)?;
                *coord = super::open_coord(&bounded, coord.addr()).await?;
                verify_version(coord, api, version, deadline).await?;
            }
            let body = match coord
                .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
                .await
            {
                Ok(body) => body,
                Err(Error::Io(_) | Error::Timeout) => {
                    let bounded = bounded_consumer_config(cfg, deadline)?;
                    *coord = super::open_coord(&bounded, coord.addr()).await?;
                    verify_version(coord, api, version, deadline).await?;
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
                *coord = super::discover_coord(&bounded, group_id, key_type).await?;
                verify_version(coord, api, version, deadline).await?;
                coord
                    .roundtrip_deadline(api, version, |buf| encode(buf), deadline)
                    .await
            } else {
                Ok(body)
            }
        })
        .await
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

async fn verify_version(
    conn: &mut BrokerConn,
    api: i16,
    version: i16,
    deadline: Deadline,
) -> Result<()> {
    let versions =
        crate::protocol::api::negotiate_api_versions(conn, deadline.remaining()?).await?;
    if !versions
        .api_version(api)
        .is_some_and(|range| range.min_version <= version && range.max_version >= version)
    {
        return Err(Error::Unsupported(
            "moved coordinator cannot represent the name-based offset request".into(),
        ));
    }
    Ok(())
}
