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
            let asynchronous = format!(
                "{:?}",
                $cooperative(&mut fragmented, $version).await
            );
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
