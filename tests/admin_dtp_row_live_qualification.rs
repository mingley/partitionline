//! Real Kafka pagination smoke for the isolated Admin row checkpoint.

use partitionline::{Admin, AUTHORIZED_OPERATIONS_OMITTED};
use std::time::Duration;

#[tokio::test]
async fn kafka_412_admin_describe_topic_partitions_paginates() {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").expect("KAFKA_BOOTSTRAP must name the test broker");
    let topic_name = std::env::var("PL_DTP_TOPIC").expect("PL_DTP_TOPIC must name the test topic");
    let mut admin = Admin::connect(bootstrap).await.unwrap();
    {
        fn require_send<T: Send>(_: &T) {}
        let names = [topic_name.as_str()];
        let future = admin.describe_topics_with_partition_limit_timeout(
            names.iter(),
            false,
            2,
            Duration::from_secs(10),
        );
        require_send(&future);
    }
    let mut cursor = None;
    let mut pages = 0usize;
    let mut topic_id = None;
    let mut authorized = None;
    let mut raw_parts = Vec::new();

    loop {
        let response = admin
            .describe_topic_partitions_timeout(
                &[topic_name.as_str()],
                2,
                cursor.as_ref(),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        pages += 1;
        assert!(pages <= 8, "pagination did not finish");
        assert_eq!(response.topics.len(), 1);
        let topic = &response.topics[0];
        assert_eq!(topic.name.as_deref(), Some(topic_name.as_str()));
        assert_eq!(topic.error_code, 0);
        assert!(!topic.is_internal);
        assert_ne!(topic.topic_id, [0; 16]);
        if let Some(expected) = topic_id {
            assert_eq!(topic.topic_id, expected);
        } else {
            topic_id = Some(topic.topic_id);
        }
        if let Some(expected) = authorized {
            assert_eq!(topic.topic_authorized_operations, expected);
        } else {
            authorized = Some(topic.topic_authorized_operations);
        }
        for partition in &topic.partitions {
            assert_eq!(partition.error_code, 0);
            assert!(partition.leader_id >= 0);
            assert_eq!(partition.replica_nodes.len(), 1);
            assert_eq!(partition.isr_nodes.len(), 1);
            assert!(partition.offline_replicas.is_empty());
            raw_parts.push((
                partition.partition_index,
                partition.leader_id,
                partition.leader_epoch,
                partition.replica_nodes.clone(),
                partition.isr_nodes.clone(),
                partition.offline_replicas.clone(),
            ));
        }
        let next = response.next_cursor;
        if let Some(next) = next {
            assert_ne!(cursor.as_ref(), Some(&next), "cursor repeated");
            cursor = Some(next);
        } else {
            break;
        }
    }
    assert!(pages >= 4, "limit 2 with seven partitions should use at least four pages");
    assert_eq!(raw_parts.iter().map(|p| p.0).collect::<Vec<_>>(), (0..7).collect::<Vec<_>>());

    for include_authorized in [false, true] {
        let described = admin
            .describe_topics_with_partition_limit_timeout(
                [topic_name.as_str()].iter(),
                include_authorized,
                2,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(described.len(), 1);
        let row = &described[0];
        assert_eq!(row.name, topic_name);
        assert_eq!(Some(row.topic_id), topic_id);
        assert_eq!(row.error_code, 0);
        assert!(!row.is_internal);
        assert_eq!(
            row.authorized_operations,
            if include_authorized {
                authorized.unwrap()
            } else {
                AUTHORIZED_OPERATIONS_OMITTED
            }
        );
        assert!(row.partitions.iter().all(|p| p.topic == topic_name));
        let aggregate_parts: Vec<_> = row
            .partitions
            .iter()
            .map(|partition| (
                partition.partition,
                partition.leader,
                partition.leader_epoch,
                partition.replicas.clone(),
                partition.isr.clone(),
                partition.offline_replicas.clone(),
            ))
            .collect();
        assert_eq!(aggregate_parts, raw_parts);
    }

    admin.close().await.unwrap();
    println!("DTP_LIVE pages={pages} partitions=7 raw_and_aggregate_match=true authorization_modes=2");
}
