//! Independent Apache fixture sentinel models; source preparation only.
use partitionline::protocol::streams::*;
pub(super) fn streams_group_heartbeat_request_expected() -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "StreamsGroupHeartbeatRequest.GroupId-詳細".into(),
        member_id: "StreamsGroupHeartbeatRequest.MemberId-詳細".into(),
        member_epoch: -1000003,
        endpoint_information_epoch: -1000004,
        instance_id: Some("StreamsGroupHeartbeatRequest.InstanceId-詳細".into()),
        rack_id: Some("StreamsGroupHeartbeatRequest.RackId-詳細".into()),
        rebalance_timeout_ms: -1000007,
        topology: Some(HeartbeatTopology {
            epoch: -1000008,
            subtopologies: vec![HeartbeatSubtopology {
                subtopology_id: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SubtopologyId-詳細".into(),
                source_topics: vec!["StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopics[0]-詳細".into(), "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopics[1]-詳細".into()],
                source_topic_regex: vec!["StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopicRegex[0]-詳細".into(), "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopicRegex[1]-詳細".into()],
                state_changelog_topics: vec![TopicInfo {
                    name: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].Name-詳細".into(),
                    partitions: -1000015,
                    replication_factor: -9984,
                    topic_configs: vec![KeyValue {
                        key: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Key-詳細".into(),
                        value: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Value-詳細".into(),
                    }],
                }],
                repartition_sink_topics: vec!["StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSinkTopics[0]-詳細".into(), "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSinkTopics[1]-詳細".into()],
                repartition_source_topics: vec![TopicInfo {
                    name: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].Name-詳細".into(),
                    partitions: -1000022,
                    replication_factor: -9977,
                    topic_configs: vec![KeyValue {
                        key: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Key-詳細".into(),
                        value: "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Value-詳細".into(),
                    }],
                }],
                copartition_groups: vec![CopartitionGroup {
                    source_topics: vec![-9974, -9973],
                    source_topic_regex: vec![-9972, -9971],
                    repartition_source_topics: vec![-9970, -9969],
                }],
            }],
        }),
        active_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatRequest.ActiveTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000033, -1000034],
        }]),
        standby_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatRequest.StandbyTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000036, -1000037],
        }]),
        warmup_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatRequest.WarmupTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000039, -1000040],
        }]),
        process_id: Some("StreamsGroupHeartbeatRequest.ProcessId-詳細".into()),
        user_endpoint: Some(Endpoint {
            host: "StreamsGroupHeartbeatRequest.UserEndpoint.Host-詳細".into(),
            port: 65492,
        }),
        client_tags: Some(vec![KeyValue {
            key: "StreamsGroupHeartbeatRequest.ClientTags[0].Key-詳細".into(),
            value: "StreamsGroupHeartbeatRequest.ClientTags[0].Value-詳細".into(),
        }]),
        task_offsets: Some(vec![TaskOffset {
            subtopology_id: "StreamsGroupHeartbeatRequest.TaskOffsets[0].SubtopologyId-詳細".into(),
            partition: -1000047,
            offset: i64::MIN + 48,
        }]),
        task_end_offsets: Some(vec![TaskOffset {
            subtopology_id: "StreamsGroupHeartbeatRequest.TaskEndOffsets[0].SubtopologyId-詳細".into(),
            partition: -1000050,
            offset: i64::MIN + 51,
        }]),
        shutdown_application: true,
    }
}

pub(super) fn streams_group_heartbeat_response_expected() -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        throttle_time_ms: -1000053,
        error_code: -9946,
        error_message: Some("StreamsGroupHeartbeatResponse.ErrorMessage-詳細".into()),
        member_id: "StreamsGroupHeartbeatResponse.MemberId-詳細".into(),
        member_epoch: -1000057,
        heartbeat_interval_ms: -1000058,
        acceptable_recovery_lag: -1000059,
        task_offset_interval_ms: -1000060,
        status: Some(vec![Status {
            status_code: -59,
            status_detail: "StreamsGroupHeartbeatResponse.Status[0].StatusDetail-詳細".into(),
        }]),
        active_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatResponse.ActiveTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000064, -1000065],
        }]),
        standby_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatResponse.StandbyTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000067, -1000068],
        }]),
        warmup_tasks: Some(vec![TaskIds {
            subtopology_id: "StreamsGroupHeartbeatResponse.WarmupTasks[0].SubtopologyId-詳細".into(),
            partitions: vec![-1000070, -1000071],
        }]),
        endpoint_information_epoch: -1000072,
        partitions_by_user_endpoint: Some(vec![EndpointToPartitions {
            user_endpoint: Endpoint {
                host: "StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].UserEndpoint.Host-詳細".into(),
                port: 65461,
            },
            active_partitions: vec![TopicPartition {
                topic: "StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].ActivePartitions[0].Topic-詳細".into(),
                partitions: vec![-1000076, -1000077],
            }],
            standby_partitions: vec![TopicPartition {
                topic: "StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].StandbyPartitions[0].Topic-詳細".into(),
                partitions: vec![-1000079, -1000080],
            }],
        }]),
    }
}

pub(super) fn streams_group_describe_request_expected() -> StreamsGroupDescribeRequest {
    StreamsGroupDescribeRequest {
        group_ids: vec![
            "StreamsGroupDescribeRequest.GroupIds[0]-詳細".into(),
            "StreamsGroupDescribeRequest.GroupIds[1]-詳細".into(),
        ],
        include_authorized_operations: true,
    }
}

pub(super) fn streams_group_describe_response_expected() -> StreamsGroupDescribeResponse {
    StreamsGroupDescribeResponse {
        throttle_time_ms: -1000084,
        groups: vec![DescribedStreamsGroup {
            error_code: -9915,
            error_message: Some("StreamsGroupDescribeResponse.Groups[0].ErrorMessage-詳細".into()),
            group_id: "StreamsGroupDescribeResponse.Groups[0].GroupId-詳細".into(),
            group_state: "StreamsGroupDescribeResponse.Groups[0].GroupState-詳細".into(),
            group_epoch: -1000089,
            assignment_epoch: -1000090,
            topology: Some(DescribedTopology {
                epoch: -1000091,
                subtopologies: Some(vec![DescribedSubtopology {
                    subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SubtopologyId-詳細".into(),
                    source_topics: vec!["StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SourceTopics[0]-詳細".into(), "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SourceTopics[1]-詳細".into()],
                    repartition_sink_topics: vec!["StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSinkTopics[0]-詳細".into(), "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSinkTopics[1]-詳細".into()],
                    state_changelog_topics: vec![TopicInfo {
                        name: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].Name-詳細".into(),
                        partitions: -1000098,
                        replication_factor: -9901,
                        topic_configs: vec![KeyValue {
                            key: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Key-詳細".into(),
                            value: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Value-詳細".into(),
                        }],
                    }],
                    repartition_source_topics: vec![TopicInfo {
                        name: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].Name-詳細".into(),
                        partitions: -1000103,
                        replication_factor: -9896,
                        topic_configs: vec![KeyValue {
                            key: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Key-詳細".into(),
                            value: "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Value-詳細".into(),
                        }],
                    }],
                }]),
            }),
            members: vec![StreamsMember {
                member_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].MemberId-詳細".into(),
                member_epoch: -1000108,
                instance_id: Some("StreamsGroupDescribeResponse.Groups[0].Members[0].InstanceId-詳細".into()),
                rack_id: Some("StreamsGroupDescribeResponse.Groups[0].Members[0].RackId-詳細".into()),
                client_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].ClientId-詳細".into(),
                client_host: "StreamsGroupDescribeResponse.Groups[0].Members[0].ClientHost-詳細".into(),
                topology_epoch: -1000113,
                process_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].ProcessId-詳細".into(),
                user_endpoint: Some(Endpoint {
                    host: "StreamsGroupDescribeResponse.Groups[0].Members[0].UserEndpoint.Host-詳細".into(),
                    port: 65419,
                }),
                client_tags: vec![KeyValue {
                    key: "StreamsGroupDescribeResponse.Groups[0].Members[0].ClientTags[0].Key-詳細".into(),
                    value: "StreamsGroupDescribeResponse.Groups[0].Members[0].ClientTags[0].Value-詳細".into(),
                }],
                task_offsets: vec![TaskOffset {
                    subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].TaskOffsets[0].SubtopologyId-詳細".into(),
                    partition: -1000120,
                    offset: i64::MIN + 121,
                }],
                task_end_offsets: vec![TaskOffset {
                    subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].TaskEndOffsets[0].SubtopologyId-詳細".into(),
                    partition: -1000123,
                    offset: i64::MIN + 124,
                }],
                assignment: Assignment {
                    active_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.ActiveTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000126, -1000127],
                    }],
                    standby_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.StandbyTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000129, -1000130],
                    }],
                    warmup_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.WarmupTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000132, -1000133],
                    }],
                },
                target_assignment: Assignment {
                    active_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.ActiveTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000135, -1000136],
                    }],
                    standby_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.StandbyTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000138, -1000139],
                    }],
                    warmup_tasks: vec![TaskIds {
                        subtopology_id: "StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.WarmupTasks[0].SubtopologyId-詳細".into(),
                        partitions: vec![-1000141, -1000142],
                    }],
                },
                is_classic: true,
            }],
            authorized_operations: -1000144,
        }],
    }
}
