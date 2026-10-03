//! Bounded Streams API88/API89 codec checks. Independent Apache interoperability is separate.

mod heartbeat {
    //! Local bounded-codec checks for API88 only; independent Apache fixtures follow separately.
    use partitionline::protocol::streams::{
        decode_streams_group_heartbeat_request as decode_request,
        decode_streams_group_heartbeat_response as decode_response,
        encode_streams_group_heartbeat_request as encode_request,
        encode_streams_group_heartbeat_response as encode_response, CopartitionGroup, Endpoint,
        EndpointToPartitions, HeartbeatSubtopology, HeartbeatTopology, KeyValue, Limits, Status,
        StreamsGroupHeartbeatRequest as Request, StreamsGroupHeartbeatResponse as Response,
        TaskIds, TaskOffset, TopicInfo, TopicPartition,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn request() -> Request {
        Request {
            group_id: "streams-g".into(),
            member_id: "streams-m".into(),
            member_epoch: -2,
            endpoint_information_epoch: i32::MAX,
            instance_id: Some(String::new()),
            rack_id: None,
            rebalance_timeout_ms: -1,
            topology: Some(HeartbeatTopology {
                epoch: i32::MIN,
                subtopologies: vec![HeartbeatSubtopology {
                    subtopology_id: "sub".into(),
                    source_topics: vec!["source".into()],
                    source_topic_regex: vec!["events-.*".into()],
                    state_changelog_topics: vec![TopicInfo {
                        name: "changes".into(),
                        partitions: 3,
                        replication_factor: 2,
                        topic_configs: vec![KeyValue {
                            key: "cleanup.policy".into(),
                            value: "compact".into(),
                        }],
                    }],
                    repartition_sink_topics: vec!["sink".into()],
                    repartition_source_topics: vec![TopicInfo {
                        name: "source-internal".into(),
                        partitions: 3,
                        replication_factor: -1,
                        topic_configs: Vec::new(),
                    }],
                    copartition_groups: vec![CopartitionGroup {
                        source_topics: vec![0],
                        source_topic_regex: vec![0],
                        repartition_source_topics: vec![0],
                    }],
                }],
            }),
            active_tasks: Some(vec![TaskIds {
                subtopology_id: "sub".into(),
                partitions: vec![0, i32::MAX],
            }]),
            standby_tasks: Some(Vec::new()),
            warmup_tasks: None,
            process_id: Some("process".into()),
            user_endpoint: Some(Endpoint {
                host: "localhost".into(),
                port: u16::MAX,
            }),
            client_tags: Some(vec![KeyValue {
                key: "zone".into(),
                value: "eu".into(),
            }]),
            task_offsets: Some(vec![TaskOffset {
                subtopology_id: "sub".into(),
                partition: -1,
                offset: i64::MIN,
            }]),
            task_end_offsets: Some(vec![TaskOffset {
                subtopology_id: "sub".into(),
                partition: i32::MAX,
                offset: i64::MAX,
            }]),
            shutdown_application: true,
        }
    }

    #[test]
    fn heartbeat_every_request_field_local_round_trip() -> TestResult {
        let value = request();
        let body = encode_request(&value, 0, Limits::default())?;
        assert_eq!(decode_request(&body, 0, Limits::default())?, value);
        Ok(())
    }

    #[test]
    fn heartbeat_response_preserves_null_empty_status_and_signed_values() -> TestResult {
        let value = Response {
            throttle_time_ms: i32::MAX,
            error_code: -1,
            error_message: Some("message".into()),
            member_id: "member".into(),
            member_epoch: -2,
            heartbeat_interval_ms: 1200,
            acceptable_recovery_lag: i32::MAX,
            task_offset_interval_ms: -1,
            status: Some(vec![Status {
                status_code: i8::MIN,
                status_detail: "詳細".into(),
            }]),
            active_tasks: None,
            standby_tasks: Some(Vec::new()),
            warmup_tasks: Some(vec![TaskIds {
                subtopology_id: "warm".into(),
                partitions: vec![3],
            }]),
            endpoint_information_epoch: i32::MIN,
            partitions_by_user_endpoint: Some(vec![EndpointToPartitions {
                user_endpoint: Endpoint {
                    host: "query-host".into(),
                    port: u16::MAX,
                },
                active_partitions: vec![TopicPartition {
                    topic: "active".into(),
                    partitions: vec![i32::MIN, i32::MAX],
                }],
                standby_partitions: vec![TopicPartition {
                    topic: "standby".into(),
                    partitions: Vec::new(),
                }],
            }]),
        };
        let body = encode_response(&value, 0, Limits::default())?;
        assert_eq!(decode_response(&body, 0, Limits::default())?, value);
        Ok(())
    }

    #[test]
    fn heartbeat_all_truncated_prefixes_and_trailing_bytes_fail() -> TestResult {
        let body = encode_request(&request(), 0, Limits::default())?;
        for end in 0..body.len() {
            assert!(
                decode_request(body.get(..end).ok_or("prefix")?, 0, Limits::default()).is_err()
            );
        }
        let mut trailing = body;
        trailing.push(0);
        assert!(decode_request(&trailing, 0, Limits::default()).is_err());
        Ok(())
    }

    #[test]
    fn heartbeat_nullable_arrays_are_changes_and_not_interchangeable() -> TestResult {
        let null = Request::default();
        let empty = Request {
            active_tasks: Some(Vec::new()),
            standby_tasks: Some(Vec::new()),
            warmup_tasks: Some(Vec::new()),
            client_tags: Some(Vec::new()),
            task_offsets: Some(Vec::new()),
            task_end_offsets: Some(Vec::new()),
            ..Request::default()
        };
        let a = encode_request(&null, 0, Limits::default())?;
        let b = encode_request(&empty, 0, Limits::default())?;
        assert_ne!(a, b);
        assert_eq!(decode_request(&a, 0, Limits::default())?, null);
        assert_eq!(decode_request(&b, 0, Limits::default())?, empty);
        Ok(())
    }

    #[test]
    fn heartbeat_generated_reader_boolean_and_nullable_marker_leniency() -> TestResult {
        // Official v0 default request layout: topology marker16, endpoint marker21,
        // shutdown Boolean25, terminal tag count26. These are independent schema positions.
        let mut body = encode_request(&Request::default(), 0, Limits::default())?;
        assert_eq!(body.len(), 27);
        *body.get_mut(16).ok_or("topology marker")? = 254;
        *body.get_mut(21).ok_or("endpoint marker")? = 128;
        *body.get_mut(25).ok_or("boolean")? = 2;
        let value = decode_request(&body, 0, Limits::default())?;
        assert!(value.topology.is_none());
        assert!(value.user_endpoint.is_none());
        assert!(value.shutdown_application);
        let canonical = encode_request(&value, 0, Limits::default())?;
        assert_eq!(canonical.get(16), Some(&255));
        assert_eq!(canonical.get(21), Some(&255));
        assert_eq!(canonical.get(25), Some(&1));
        Ok(())
    }

    #[test]
    fn heartbeat_zero_nullable_presence_marker_is_present() -> TestResult {
        let value = Request {
            topology: Some(HeartbeatTopology::default()),
            ..Request::default()
        };
        let mut body = encode_request(&value, 0, Limits::default())?;
        *body.get_mut(16).ok_or("topology marker")? = 0;
        assert_eq!(decode_request(&body, 0, Limits::default())?, value);
        Ok(())
    }

    #[test]
    fn heartbeat_unknown_tags_are_discarded_but_duplicates_rejected() -> TestResult {
        let value = Request::default();
        let mut body = encode_request(&value, 0, Limits::default())?;
        assert_eq!(body.pop(), Some(0));
        body.extend_from_slice(&[1, 7, 3, 9, 8, 7]);
        assert_eq!(decode_request(&body, 0, Limits::default())?, value);
        assert!(decode_request(
            &body,
            0,
            Limits {
                tag_bytes: 2,
                ..Limits::default()
            }
        )
        .is_err());
        let mut duplicate = encode_request(&value, 0, Limits::default())?;
        assert_eq!(duplicate.pop(), Some(0));
        duplicate.extend_from_slice(&[2, 7, 0, 7, 0]);
        assert!(decode_request(&duplicate, 0, Limits::default()).is_err());
        Ok(())
    }

    #[test]
    fn heartbeat_aggregate_nested_elements_limit_precedes_output() -> TestResult {
        let value = Request {
            active_tasks: Some(vec![TaskIds {
                subtopology_id: String::new(),
                partitions: vec![0],
            }]),
            standby_tasks: Some(vec![TaskIds {
                subtopology_id: String::new(),
                partitions: vec![1],
            }]),
            ..Request::default()
        };
        let body = encode_request(&value, 0, Limits::default())?;
        let limit = Limits {
            array_elements: 1,
            total_elements: 3,
            ..Limits::default()
        };
        assert!(encode_request(&value, 0, limit).is_err());
        assert!(decode_request(&body, 0, limit).is_err());
        Ok(())
    }

    #[test]
    fn heartbeat_string_output_and_decoded_limits_bound_admission() -> TestResult {
        let value = Request {
            group_id: "abc".into(),
            member_id: "def".into(),
            ..Request::default()
        };
        let body = encode_request(&value, 0, Limits::default())?;
        for limit in [
            Limits {
                string_bytes: 2,
                ..Limits::default()
            },
            Limits {
                total_string_bytes: 5,
                ..Limits::default()
            },
            Limits {
                wire_bytes: body.len() - 1,
                ..Limits::default()
            },
            Limits {
                decoded_bytes: 5,
                ..Limits::default()
            },
        ] {
            assert!(encode_request(&value, 0, limit).is_err());
            assert!(decode_request(&body, 0, limit).is_err());
        }
        assert_eq!(
            decode_request(
                &body,
                0,
                Limits {
                    wire_bytes: body.len(),
                    total_string_bytes: 6,
                    decoded_bytes: 6,
                    ..Limits::default()
                }
            )?,
            value
        );
        Ok(())
    }

    #[test]
    fn heartbeat_invalid_utf8_and_unsupported_versions_rejected() -> TestResult {
        let mut body = encode_request(
            &Request {
                group_id: "x".into(),
                ..Request::default()
            },
            0,
            Limits::default(),
        )?;
        *body.get_mut(1).ok_or("group string")? = 255;
        assert!(decode_request(&body, 0, Limits::default()).is_err());
        for version in [-1, 1, i16::MAX] {
            assert!(encode_request(&Request::default(), version, Limits::default()).is_err());
            assert!(decode_request(&body, version, Limits::default()).is_err());
        }
        Ok(())
    }

    #[test]
    fn heartbeat_nested_and_root_tags_share_one_aggregate_budget() -> TestResult {
        // Empty present topology has a 4-byte epoch, empty subtopology count, tags.
        let value = Request {
            topology: Some(HeartbeatTopology::default()),
            ..Request::default()
        };
        let mut body = encode_request(&value, 0, Limits::default())?;
        // Official v0 schema: topology marker16; topology empty tags22.
        assert_eq!(body.get(22), Some(&0));
        let removed = body.splice(22..23, [1, 7, 2, 8, 9]);
        drop(removed);
        assert_eq!(body.pop(), Some(0));
        body.extend_from_slice(&[1, 9, 1, 10]);
        assert_eq!(decode_request(&body, 0, Limits::default())?, value);
        for limit in [
            Limits {
                total_tagged_fields: 1,
                ..Limits::default()
            },
            Limits {
                tag_bytes: 2,
                ..Limits::default()
            },
        ] {
            assert!(decode_request(&body, 0, limit).is_err());
        }
        Ok(())
    }

    #[test]
    fn heartbeat_struct_array_minimum_wire_prevents_count_allocation() {
        // StreamsGroupHeartbeatResponse has four fixed integers and nullable fields
        // before the endpoint array. Build only its prefix with an impossible count.
        let mut body = vec![0; 6];
        body.extend_from_slice(&[0, 1]); // null error message, empty member id.
        body.extend_from_slice(&[0; 16]);
        body.extend_from_slice(&[0, 0, 0, 0]); // four null task/status arrays.
        body.extend_from_slice(&[0; 4]);
        body.extend_from_slice(&[128, 128, 128, 128, 15]); // enormous endpoint count.
        assert!(decode_response(&body, 0, Limits::default()).is_err());
    }
}

mod describe {
    //! Local bounded-codec checks for API89 only; independent Apache fixtures follow separately.
    use partitionline::protocol::streams::{
        decode_streams_group_describe_request as decode_request,
        decode_streams_group_describe_response as decode_response,
        encode_streams_group_describe_request as encode_request,
        encode_streams_group_describe_response as encode_response, Assignment,
        DescribedStreamsGroup, DescribedSubtopology, DescribedTopology, Endpoint, KeyValue, Limits,
        StreamsGroupDescribeRequest as Request, StreamsGroupDescribeResponse as Response,
        StreamsMember, TaskIds, TaskOffset, TopicInfo,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn response() -> Response {
        Response {
            throttle_time_ms: i32::MAX,
            groups: vec![DescribedStreamsGroup {
                group_id: "streams-group".into(),
                group_state: "Reconciling".into(),
                group_epoch: -1,
                assignment_epoch: i32::MAX,
                authorized_operations: i32::MIN,
                topology: Some(DescribedTopology {
                    epoch: 4,
                    subtopologies: Some(vec![DescribedSubtopology {
                        subtopology_id: "sub".into(),
                        source_topics: vec!["source".into()],
                        repartition_sink_topics: vec!["sink".into()],
                        state_changelog_topics: vec![TopicInfo {
                            name: "changes".into(),
                            partitions: 2,
                            replication_factor: 1,
                            topic_configs: vec![KeyValue {
                                key: "cleanup.policy".into(),
                                value: "compact".into(),
                            }],
                        }],
                        repartition_source_topics: Vec::new(),
                    }]),
                }),
                members: vec![StreamsMember {
                    member_id: "member".into(),
                    member_epoch: i32::MIN,
                    instance_id: Some(String::new()),
                    rack_id: None,
                    client_id: "client".into(),
                    client_host: "host".into(),
                    topology_epoch: 5,
                    process_id: "process".into(),
                    user_endpoint: Some(Endpoint {
                        host: "localhost".into(),
                        port: 65535,
                    }),
                    client_tags: vec![KeyValue {
                        key: "rack".into(),
                        value: "west".into(),
                    }],
                    task_offsets: vec![TaskOffset {
                        subtopology_id: "sub".into(),
                        partition: -1,
                        offset: i64::MIN,
                    }],
                    task_end_offsets: vec![TaskOffset {
                        subtopology_id: "sub".into(),
                        partition: i32::MAX,
                        offset: i64::MAX,
                    }],
                    assignment: Assignment {
                        active_tasks: vec![TaskIds {
                            subtopology_id: "sub".into(),
                            partitions: vec![0],
                        }],
                        standby_tasks: Vec::new(),
                        warmup_tasks: Vec::new(),
                    },
                    target_assignment: Assignment {
                        active_tasks: Vec::new(),
                        standby_tasks: vec![TaskIds {
                            subtopology_id: "sub".into(),
                            partitions: vec![1],
                        }],
                        warmup_tasks: Vec::new(),
                    },
                    is_classic: true,
                }],
                ..DescribedStreamsGroup::default()
            }],
        }
    }

    #[test]
    fn describe_request_and_complete_response_local_round_trip() -> TestResult {
        let value = Request {
            group_ids: vec!["a".into(), "a".into(), "b".into()],
            include_authorized_operations: true,
        };
        let request = encode_request(&value, 0, Limits::default())?;
        assert_eq!(decode_request(&request, 0, Limits::default())?, value);
        let value = response();
        let body = encode_response(&value, 0, Limits::default())?;
        assert_eq!(decode_response(&body, 0, Limits::default())?, value);
        Ok(())
    }

    #[test]
    fn describe_topology_null_and_empty_subtopologies_are_distinct() -> TestResult {
        let values = [
            None,
            Some(DescribedTopology {
                subtopologies: None,
                ..DescribedTopology::default()
            }),
            Some(DescribedTopology {
                subtopologies: Some(Vec::new()),
                ..DescribedTopology::default()
            }),
        ];
        let mut bodies = Vec::new();
        for topology in values {
            let value = Response {
                groups: vec![DescribedStreamsGroup {
                    topology,
                    error_code: 15,
                    error_message: Some(String::new()),
                    ..DescribedStreamsGroup::default()
                }],
                ..Response::default()
            };
            let body = encode_response(&value, 0, Limits::default())?;
            assert_eq!(decode_response(&body, 0, Limits::default())?, value);
            bodies.push(body);
        }
        assert!(bodies.windows(2).all(|pair| pair.first() != pair.last()));
        Ok(())
    }

    #[test]
    fn describe_all_truncated_response_prefixes_and_trailing_bytes_fail() -> TestResult {
        let body = encode_response(&response(), 0, Limits::default())?;
        for end in 0..body.len() {
            assert!(
                decode_response(body.get(..end).ok_or("prefix")?, 0, Limits::default()).is_err()
            );
        }
        let mut body = body;
        body.push(0);
        assert!(decode_response(&body, 0, Limits::default()).is_err());
        Ok(())
    }

    #[test]
    fn describe_hostile_counts_varints_and_nonnullable_null_rejected() {
        for input in [
            &[255, 255, 255, 255, 15, 0, 0][..],
            &[128, 128, 128, 128, 128, 0][..],
            &[0, 0, 0][..],
            &[2, 0, 0, 0][..],
            &[4, 0, 0][..],
        ] {
            assert!(decode_request(input, 0, Limits::default()).is_err());
        }
    }

    #[test]
    fn describe_nonzero_boolean_matches_generated_reader() -> TestResult {
        let body = [1, 255, 0];
        let value = decode_request(&body, 0, Limits::default())?;
        assert!(value.include_authorized_operations);
        assert_eq!(encode_request(&value, 0, Limits::default())?, vec![1, 1, 0]);
        Ok(())
    }

    #[test]
    fn describe_unknown_tag_discard_and_local_tag_order_policy() -> TestResult {
        let input = [1, 0, 1, 7, 1, 255];
        assert_eq!(
            decode_request(&input, 0, Limits::default())?,
            Request::default()
        );
        for tags in [
            &[2, 7, 0, 7, 0][..],
            &[2, 7, 0, 3, 0][..],
            &[1, 7, 2, 0][..],
            &[128, 128, 128, 128, 16][..],
        ] {
            let mut input = vec![1, 0];
            input.extend_from_slice(tags);
            assert!(decode_request(&input, 0, Limits::default()).is_err());
        }
        assert!(decode_request(
            &input,
            0,
            Limits {
                tag_bytes: 1,
                ..Limits::default()
            }
        )
        .is_ok());
        Ok(())
    }

    #[test]
    fn describe_array_string_and_decoded_reservations_are_bounded() -> TestResult {
        let value = Request {
            group_ids: vec!["abc".into(), "def".into()],
            include_authorized_operations: false,
        };
        let body = encode_request(&value, 0, Limits::default())?;
        for limits in [
            Limits {
                array_elements: 1,
                ..Limits::default()
            },
            Limits {
                total_elements: 1,
                ..Limits::default()
            },
            Limits {
                string_bytes: 2,
                ..Limits::default()
            },
            Limits {
                total_string_bytes: 5,
                ..Limits::default()
            },
            Limits {
                decoded_bytes: 1,
                ..Limits::default()
            },
        ] {
            assert!(encode_request(&value, 0, limits).is_err());
            assert!(decode_request(&body, 0, limits).is_err());
        }
        Ok(())
    }

    #[test]
    fn describe_wire_boundary_and_unsupported_versions_rejected() -> TestResult {
        let value = response();
        let body = encode_response(&value, 0, Limits::default())?;
        let limits = Limits {
            wire_bytes: body.len(),
            ..Limits::default()
        };
        assert_eq!(decode_response(&body, 0, limits)?, value);
        assert_eq!(encode_response(&value, 0, limits)?, body);
        assert!(decode_response(
            &body,
            0,
            Limits {
                wire_bytes: body.len() - 1,
                ..limits
            }
        )
        .is_err());
        for version in [-1, 1, i16::MAX] {
            assert!(encode_response(&value, version, Limits::default()).is_err());
            assert!(decode_response(&body, version, Limits::default()).is_err());
        }
        Ok(())
    }

    #[test]
    fn describe_zero_limits_are_rejected_eagerly() {
        let limit = Limits {
            total_elements: 0,
            ..Limits::default()
        };
        assert!(limit.validate().is_err());
        assert!(decode_request(&[], 0, limit).is_err());
        assert!(encode_request(&Request::default(), 0, limit).is_err());
    }
}
