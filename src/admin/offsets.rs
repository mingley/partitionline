use super::Admin;
use crate::error::{Error, Result};
use crate::net::Deadline;
use crate::offsets::{OffsetClient, OffsetOptions};
use crate::protocol::api_keys::{OFFSET_COMMIT, OFFSET_FETCH};
use crate::protocol::group::{
    OffsetCommitRequestData, OffsetCommitResponseData, OffsetFetchRequestData,
    OffsetFetchResponseData,
};

/// Capabilities stay with the socket that negotiated them and disappear when
/// that socket is removed, rather than following a reused broker node ID.
pub(super) struct AdminOffsetConn {
    pub(super) inner: crate::net::BrokerConn,
    commit: Option<(i16, i16)>,
    fetch: Option<(i16, i16)>,
}

impl AdminOffsetConn {
    pub(super) fn new(
        inner: crate::net::BrokerConn,
        versions: &crate::protocol::api::ApiVersionsResponse,
    ) -> Self {
        Self {
            inner,
            commit: versions
                .api_version(OFFSET_COMMIT)
                .map(|range| (range.min_version, range.max_version)),
            fetch: versions
                .api_version(OFFSET_FETCH)
                .map(|range| (range.min_version, range.max_version)),
        }
    }

    pub(super) fn offset_version(&self, api: i16, min: i16, max: i16) -> Result<i16> {
        let range = match api {
            OFFSET_COMMIT => self.commit,
            OFFSET_FETCH => self.fetch,
            _ => None,
        };
        range
            .and_then(|(low, high)| crate::protocol::api_keys::pick_version(low, high, min, max))
            .ok_or_else(|| {
                Error::Unsupported("actual coordinator cannot represent the offset request".into())
            })
    }
}

impl std::ops::Deref for AdminOffsetConn {
    type Target = crate::net::BrokerConn;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for AdminOffsetConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Admin {
    pub(super) async fn fetch_named_groups_v10(
        &self,
        jobs: &[(String, super::ListConsumerGroupOffsetsSpec)],
        require_stable: bool,
        deadline: Deadline,
    ) -> Result<
        Vec<(
            String,
            Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
        )>,
    > {
        use crate::protocol::group::*;
        let limits = OffsetLimits::default();
        let mut ids = std::collections::HashSet::new();
        let mut groups = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        let mut all = false;
        for (id, spec) in jobs {
            if spec.partitions.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }
            if !ids.insert(id) {
                return Err(Error::protocol("duplicate named offset group"));
            }
            let topics = super::offset_fetch_topics_for_spec(spec);
            if let Some(topics) = &topics {
                for topic in topics {
                    let _inserted = names.insert(topic.topic.clone());
                }
            } else {
                all = true;
            }
            groups.push(OffsetFetchGroupData {
                group_id: id.clone(),
                topics: topics.map(|topics| {
                    topics
                        .into_iter()
                        .map(|topic| OffsetFetchTopicData {
                            identity: OffsetTopicIdentity::Name(topic.topic),
                            partition_indexes: topic.partitions,
                        })
                        .collect()
                }),
                ..Default::default()
            });
        }
        let mut request = OffsetFetchRequestData {
            groups,
            require_stable,
        };
        validate_offset_fetch_request_data(&request, 9, limits)?;
        if request.groups.is_empty() {
            return Ok(jobs
                .iter()
                .map(|(id, _)| (id.clone(), Vec::new()))
                .collect());
        }
        let names = names.into_iter().collect::<Vec<_>>();
        let mut client = OffsetClient::new(self.cfg.clone())?;
        client.set_stats(std::sync::Arc::clone(&self.stats));
        let bindings = client
            .resolve_names(
                if all { None } else { Some(&names) },
                OFFSET_FETCH,
                deadline,
                limits,
            )
            .await?;
        let reverse = bindings
            .iter()
            .map(|(name, id)| (*id, name))
            .collect::<std::collections::HashMap<_, _>>();
        for group in &mut request.groups {
            if let Some(topics) = &mut group.topics {
                for topic in topics {
                    let OffsetTopicIdentity::Name(name) = &topic.identity else {
                        return Err(Error::protocol("missing named group offset intent"));
                    };
                    topic.identity = OffsetTopicIdentity::Id(
                        *bindings
                            .get(name)
                            .ok_or_else(|| Error::protocol("missing named group topic UUID"))?,
                    );
                }
            }
        }
        let response = client
            .fetch(
                &request,
                &OffsetOptions {
                    topic_ids: true,
                    timeout: Some(deadline.remaining()?),
                    limits,
                },
            )
            .await?;
        let mut returned = std::collections::HashMap::new();
        let mut name_bytes = 0usize;
        for group in response.groups {
            if group.error_code != 0 {
                return Err(Error::broker(group.error_code, "OffsetFetch"));
            }
            let topics = group
                .topics
                .into_iter()
                .map(|topic| {
                    let OffsetTopicIdentity::Id(id) = topic.identity else {
                        return Err(Error::protocol("v10 returned a topic name"));
                    };
                    let name = reverse.get(&id).ok_or_else(|| {
                        Error::protocol("fetched group UUID is absent from captured metadata")
                    })?;
                    name_bytes = name_bytes
                        .checked_add(name.len())
                        .filter(|bytes| *bytes <= limits.total_string_bytes)
                        .ok_or_else(|| Error::protocol("projected offset names exceed limit"))?;
                    Ok(FetchedOffsetTopic {
                        topic: (*name).clone(),
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
                .collect::<Result<Vec<_>>>()?;
            let _previous = returned.insert(group.group_id, topics);
        }
        let result = jobs
            .iter()
            .map(|(id, spec)| {
                let offsets = if spec.partitions.as_ref().is_some_and(Vec::is_empty) {
                    Vec::new()
                } else {
                    super::listed_group_offsets(
                        spec,
                        &returned.remove(id).ok_or_else(|| {
                            Error::protocol("missing named group offset response")
                        })?,
                    )?
                };
                Ok((id.clone(), offsets))
            })
            .collect();
        deadline.check_expired()?;
        result
    }

    pub(super) async fn commit_named_offsets_v10(
        &self,
        group_id: &str,
        offsets: &[(crate::TopicPartition, crate::OffsetAndMetadata)],
        deadline: Deadline,
    ) -> Result<()> {
        let limits = crate::protocol::group::OffsetLimits::default();
        let topics = crate::group::group_offset_topics(offsets);
        let mut request = OffsetCommitRequestData {
            group_id: group_id.to_owned(),
            topics: topics
                .into_iter()
                .map(|topic| crate::protocol::group::OffsetCommitTopicData {
                    identity: crate::protocol::group::OffsetTopicIdentity::Name(topic.topic),
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(
                            |partition| crate::protocol::group::OffsetCommitPartitionData {
                                partition_index: partition.partition,
                                committed_offset: partition.offset,
                                committed_leader_epoch: partition.leader_epoch,
                                committed_metadata: Some(partition.metadata),
                            },
                        )
                        .collect(),
                })
                .collect(),
            ..Default::default()
        };
        crate::protocol::group::validate_offset_commit_request_data(&request, 9, limits)?;
        let names = request
            .topics
            .iter()
            .filter_map(|topic| match &topic.identity {
                crate::protocol::group::OffsetTopicIdentity::Name(name) => Some(name.clone()),
                crate::protocol::group::OffsetTopicIdentity::Id(_) => None,
            })
            .collect::<Vec<_>>();
        let mut client = OffsetClient::new(self.cfg.clone())?;
        client.set_stats(std::sync::Arc::clone(&self.stats));
        let bindings = client
            .resolve_names(Some(&names), OFFSET_COMMIT, deadline, limits)
            .await?;
        for topic in &mut request.topics {
            let crate::protocol::group::OffsetTopicIdentity::Name(name) = &topic.identity else {
                return Err(Error::protocol("missing named offset intent"));
            };
            let id = bindings
                .get(name)
                .copied()
                .ok_or_else(|| Error::protocol("missing offset topic UUID"))?;
            topic.identity = crate::protocol::group::OffsetTopicIdentity::Id(id);
        }
        let result = client
            .commit(
                &request,
                &OffsetOptions {
                    topic_ids: true,
                    timeout: Some(deadline.remaining()?),
                    limits,
                },
            )
            .await?;
        if let Some(code) = result
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| *code != 0)
        {
            return Err(Error::broker(code, "OffsetCommit"));
        }
        deadline.check_expired()
    }

    pub(super) async fn fetch_named_offsets_v10(
        &self,
        group_id: &str,
        topics: Option<&[crate::protocol::group::OffsetFetchTopic]>,
        require_stable: bool,
        deadline: Deadline,
    ) -> Result<Vec<crate::protocol::group::FetchedOffsetTopic>> {
        use crate::protocol::group::*;
        let limits = OffsetLimits::default();
        let mut request = OffsetFetchRequestData {
            groups: vec![OffsetFetchGroupData {
                group_id: group_id.to_owned(),
                topics: topics.map(|topics| {
                    topics
                        .iter()
                        .map(|topic| OffsetFetchTopicData {
                            identity: OffsetTopicIdentity::Name(topic.topic.clone()),
                            partition_indexes: topic.partitions.clone(),
                        })
                        .collect()
                }),
                ..Default::default()
            }],
            require_stable,
        };
        validate_offset_fetch_request_data(&request, 9, limits)?;
        let names = topics.map(|topics| {
            topics
                .iter()
                .map(|topic| topic.topic.clone())
                .collect::<Vec<_>>()
        });
        let mut client = OffsetClient::new(self.cfg.clone())?;
        client.set_stats(std::sync::Arc::clone(&self.stats));
        let bindings = client
            .resolve_names(names.as_deref(), OFFSET_FETCH, deadline, limits)
            .await?;
        let reverse = bindings
            .iter()
            .map(|(name, id)| (*id, name))
            .collect::<std::collections::HashMap<_, _>>();
        if let Some(topics) = request
            .groups
            .first_mut()
            .and_then(|group| group.topics.as_mut())
        {
            for topic in topics {
                let OffsetTopicIdentity::Name(name) = &topic.identity else {
                    return Err(Error::protocol("missing fetched name intent"));
                };
                topic.identity = OffsetTopicIdentity::Id(
                    *bindings
                        .get(name)
                        .ok_or_else(|| Error::protocol("missing fetched topic UUID"))?,
                );
            }
        }
        let result = client
            .fetch(
                &request,
                &OffsetOptions {
                    topic_ids: true,
                    timeout: Some(deadline.remaining()?),
                    limits,
                },
            )
            .await?;
        let group = result
            .groups
            .into_iter()
            .next()
            .ok_or_else(|| Error::protocol("missing fetched group"))?;
        if group.error_code != 0 {
            return Err(Error::broker(group.error_code, "OffsetFetch"));
        }
        let result = group
            .topics
            .into_iter()
            .map(|topic| {
                let OffsetTopicIdentity::Id(id) = topic.identity else {
                    return Err(Error::protocol("v10 returned a topic name"));
                };
                let name = reverse.get(&id).ok_or_else(|| {
                    Error::protocol("fetched UUID is absent from the captured metadata")
                })?;
                Ok(FetchedOffsetTopic {
                    topic: (*name).clone(),
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
        deadline.check_expired()?;
        result
    }

    /// Commit explicit name or UUID identities with caller-supplied membership
    /// fields. For administrative commits, use generation -1 and an empty
    /// member ID. The typed response retains every partition error.
    ///
    /// UUIDs require v10 on the actual coordinator. This operation owns its
    /// sockets, authenticates with the Admin configuration and never resolves
    /// an unknown UUID to a topic name. A timeout may follow a successful commit.
    pub async fn commit_group_offsets(
        &mut self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetCommitResponseData> {
        let mut client = OffsetClient::new(self.cfg.clone())?;
        client.set_stats(std::sync::Arc::clone(&self.stats));
        client.commit(request, options).await
    }

    /// Fetch typed offsets for one or more groups. Null topic selection means
    /// all topics; an empty selection means none. UUID responses remain UUIDs.
    /// Groups sharing a coordinator are batched, with caller group order restored.
    pub async fn fetch_group_offsets(
        &mut self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetFetchResponseData> {
        let mut client = OffsetClient::new(self.cfg.clone())?;
        client.set_stats(std::sync::Arc::clone(&self.stats));
        client.fetch(request, options).await
    }
}
