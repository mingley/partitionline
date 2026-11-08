//! Isolated live Apache Kafka pagination smoke.
use partitionline::Admin;
use std::time::Duration;

#[tokio::test]
async fn kafka_412_topic_partition_pagination() {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap();
    let topic = std::env::var("KAFKA_TOPIC").unwrap();
    let mut admin = Admin::connect(bootstrap).await.unwrap();
    assert!(admin.versions().contains_key(&75), "broker did not advertise API 75");
    let mut cursor = None;
    let mut indexes = Vec::new();
    let mut pages = 0;
    let mut first_id: Option<[u8; 16]> = None;
    loop {
        let response = admin
            .describe_topic_partitions_timeout(&[topic.as_str()], 1, cursor.as_ref(), Duration::from_secs(20))
            .await
            .unwrap();
        pages += 1;
        assert_eq!(response.topics.len(), 1, "page {pages}: {response:?}");
        let described = &response.topics[0];
        assert_eq!(described.name.as_deref(), Some(topic.as_str()));
        assert_eq!(described.error_code, 0);
        assert_ne!(described.topic_id, [0; 16]);
        if let Some(id) = first_id {
            assert_eq!(described.topic_id, id);
        } else {
            first_id = Some(described.topic_id);
        }
        assert_eq!(described.partitions.len(), 1, "page {pages}: {response:?}");
        indexes.push(described.partitions[0].partition_index);
        cursor = response.next_cursor;
        match cursor.as_ref() {
            None => break,
            Some(next) => {
                assert_eq!(next.topic_name, topic);
                assert_eq!(next.partition_index, pages);
                assert!(pages < 4, "unexpected extra page");
            }
        }
    }
    assert_eq!(pages, 4);
    assert_eq!(indexes, vec![0, 1, 2, 3]);
    let names = [topic.as_str()];
    let result = admin
        .describe_topics_with_partition_limit_timeout(names.iter(), false, 1, Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name(), topic);
    assert_eq!(result[0].error_code(), 0);
    assert_eq!(result[0].topic_id, first_id.unwrap());
    assert_eq!(
        result[0].partitions().iter().map(|part| part.partition).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    println!("Kafka 4.1.2 raw pages={pages}, partitions={indexes:?}; aggregate matched");
}
