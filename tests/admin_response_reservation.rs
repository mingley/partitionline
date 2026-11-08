//! Admin response decoding preserves wire semantics while growing result vectors.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "integration-test fixtures and assertions"
)]

use bytes::{Buf, BytesMut};
use partitionline::protocol::admin::*;

macro_rules! compare_decoders {
    ($sync:ident, $cooperative:ident, $bytes:expr, $version:expr, $count:expr) => {{
        let bytes = &$bytes[..];
        let mut whole = bytes;
        let expected = $sync(&mut whole, $version).unwrap();
        assert!(whole.is_empty());
        assert_eq!(expected.0.len(), $count);

        let mut whole = bytes;
        let actual = $cooperative(&mut whole, $version).await.unwrap();
        assert!(whole.is_empty());
        assert_eq!(actual, expected);

        // Exercise every short truncation and a fragmented Buf. Both decoders
        // must stop at the same field and return the same error.
        for end in 0..bytes.len().min(256) {
            let prefix = &bytes[..end];
            let mut contiguous = prefix;
            let sync = format!("{:?}", $sync(&mut contiguous, $version));
            let sync_remaining = contiguous.remaining();
            let split = prefix.len() / 2;
            let mut fragmented = prefix[..split].chain(&prefix[split..]);
            let asynchronous = format!("{:?}", $cooperative(&mut fragmented, $version).await);
            assert_eq!(asynchronous, sync, "version {} prefix {end}", $version);
            assert_eq!(fragmented.remaining(), sync_remaining);
        }
    }};
}

#[tokio::test]
async fn admin_response_vectors_preserve_valid_and_truncated_results() {
    let configs = vec![CreatedTopicConfig {
        name: "retention.ms".into(),
        value: Some("123".into()),
        read_only: false,
        config_source: 1,
        is_sensitive: false,
    }];
    let topics: Vec<_> = (0..320)
        .map(|i| TopicResult {
            name: format!("topic-{i}"),
            error_code: 0,
            error_message: None,
            topic_id: [4; 16],
            num_partitions: 2,
            replication_factor: 1,
            configs: configs.clone(),
        })
        .collect();
    for version in [0, 4, 5, 7] {
        let mut bytes = BytesMut::new();
        encode_create_topics_response(&mut bytes, version, &topics).unwrap();
        compare_decoders!(
            decode_create_topics_response,
            decode_create_topics_response_cooperative,
            bytes,
            version,
            topics.len()
        );
    }
    for version in [0, 3, 4, 6] {
        let mut bytes = BytesMut::new();
        encode_delete_topics_response(&mut bytes, version, &topics).unwrap();
        compare_decoders!(
            decode_delete_topics_response,
            decode_delete_topics_response_cooperative,
            bytes,
            version,
            topics.len()
        );
    }
    for version in [0, 1, 2, 3] {
        let mut bytes = BytesMut::new();
        encode_create_partitions_response(&mut bytes, version, &topics).unwrap();
        compare_decoders!(
            decode_create_partitions_response,
            decode_create_partitions_response_cooperative,
            bytes,
            version,
            topics.len()
        );
    }

    let synonyms: Vec<_> = (0..340)
        .map(|i| ConfigSynonym {
            name: format!("synonym-{i}"),
            value: Some("v".into()),
            source: 1,
        })
        .collect();
    let entries = (0..320)
        .map(|i| ConfigEntry {
            name: format!("config-{i}"),
            value: Some("value".into()),
            read_only: false,
            source: 1,
            is_sensitive: false,
            synonyms: if i == 0 { synonyms.clone() } else { Vec::new() },
            config_type: 2,
            documentation: Some("doc".into()),
        })
        .collect();
    let resources = vec![DescribeConfigsResult {
        error_code: 0,
        error_message: None,
        resource_type: 2,
        name: "topic".into(),
        entries,
    }];
    for version in 0..=4 {
        let mut bytes = BytesMut::new();
        encode_describe_configs_response(&mut bytes, version, &resources).unwrap();
        compare_decoders!(
            decode_describe_configs_response,
            decode_describe_configs_response_cooperative,
            bytes,
            version,
            resources.len()
        );
    }

    let altered: Vec<_> = (0..320)
        .map(|i| AlterConfigsResourceResult {
            error_code: 0,
            error_message: None,
            resource_type: 2,
            name: format!("resource-{i}"),
        })
        .collect();
    for version in 0..=2 {
        let mut bytes = BytesMut::new();
        encode_alter_configs_resource_results(&mut bytes, version, &altered).unwrap();
        compare_decoders!(
            decode_alter_configs_resource_results,
            decode_alter_configs_resource_results_cooperative,
            bytes,
            version,
            altered.len()
        );
    }
    for version in 0..=1 {
        let mut bytes = BytesMut::new();
        encode_incremental_alter_configs_resource_results(&mut bytes, version, &altered).unwrap();
        compare_decoders!(
            decode_incremental_alter_configs_resource_results,
            decode_incremental_alter_configs_resource_results_cooperative,
            bytes,
            version,
            altered.len()
        );
    }

    let deleted = vec![DeletedRecordsTopic {
        topic: "topic".into(),
        partitions: (0..400)
            .map(|i| DeletedRecordsPartition {
                partition: i,
                low_watermark: 7,
                error_code: 0,
            })
            .collect(),
    }];
    for version in 0..=2 {
        let mut bytes = BytesMut::new();
        encode_delete_records_topics_response(&mut bytes, version, &deleted).unwrap();
        compare_decoders!(
            decode_delete_records_topics_response,
            decode_delete_records_topics_response_cooperative,
            bytes,
            version,
            deleted.len()
        );
    }
}


#[tokio::test]
async fn describe_topic_partition_vectors_preserve_results_and_errors() {
    use bytes::BufMut;
    use partitionline::protocol::buf::{put_array_len, put_compact_string};

    let mut first = DescribedTopicPartition::new(0);
    first.partition_index = 0;
    first.replica_nodes = (0..5000).collect();
    first.isr_nodes = vec![1, 2];
    first.eligible_leader_replicas = Some(vec![3, 4]);
    first.last_known_elr = Some(Vec::new());
    first.offline_replicas = vec![5];
    let mut topic = DescribedTopicPartitions::new("orders", 0);
    topic.topic_id = [7; 16];
    topic.partitions = (0..400)
        .map(|index| {
            let mut part = if index == 0 {
                first.clone()
            } else {
                DescribedTopicPartition::new(0)
            };
            part.partition_index = index;
            part
        })
        .collect();
    let mut response = DescribeTopicPartitionsResponse::new(vec![topic]);
    response.next_cursor = Some(TopicPartitionCursor::new("orders", 400));
    let mut encoded = BytesMut::new();
    encode_describe_topic_partitions_response(&mut encoded, &response).unwrap();
    let mut sync = &encoded[..];
    let got = decode_describe_topic_partitions_response(&mut sync).unwrap();
    assert!(sync.is_empty());
    assert_eq!(got, response);
    let mut cooperative = &encoded[..];
    assert_eq!(
        decode_describe_topic_partitions_response_cooperative(&mut cooperative)
            .await
            .unwrap(),
        response
    );
    assert!(cooperative.is_empty());

    let mut inputs: Vec<Vec<u8>> = (0..256).map(|end| encoded[..end].to_vec()).collect();
    for variant in 0..3 {
        let mut body = BytesMut::new();
        body.put_i32(0);
        put_array_len(&mut body, true, Some(if variant == 0 { 100_000 } else { 1 })).unwrap();
        body.put_i16(0);
        if variant == 0 {
            partitionline::protocol::buf::put_unsigned_varint(&mut body, u32::MAX);
        } else {
            put_compact_string(&mut body, Some("orders")).unwrap();
            body.extend_from_slice(&[0; 16]);
            body.put_u8(0);
            put_array_len(&mut body, true, Some(if variant == 1 { 100_000 } else { 1 })).unwrap();
            body.put_i16(0);
            body.put_i32(0);
            body.put_i32(1);
            body.put_i32(0);
            if variant == 1 {
                partitionline::protocol::buf::put_unsigned_varint(&mut body, u32::MAX);
            } else {
                put_array_len(&mut body, true, Some(100_000)).unwrap();
            }
        }
        body.resize(body.len() + 100_000, 0);
        inputs.push(body.to_vec());
    }
    for (index, bytes) in inputs.iter().enumerate() {
        let mut sync = bytes.as_slice();
        let expected = format!("{:?}", decode_describe_topic_partitions_response(&mut sync));
        assert!(expected.starts_with("Err("), "input {index}: {expected}");
        let expected_remaining = sync.remaining();
        let middle = bytes.len() / 2;
        let mut fragmented = bytes[..middle].chain(&bytes[middle..]);
        let actual = format!(
            "{:?}",
            decode_describe_topic_partitions_response_cooperative(&mut fragmented).await
        );
        assert_eq!(actual, expected, "input {index}");
        assert_eq!(fragmented.remaining(), expected_remaining, "input {index}");
    }
}
