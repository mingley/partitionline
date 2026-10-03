use std::time::Duration;
use partitionline::{Admin,ConsumerConfig,Producer,ProducerConfig,ProduceRecord,ShareGroup,ShareRecord,ShareAcquireMode,Error};
use partitionline::admin::{NewTopic,ConfigResourceUpdate,ConfigResource,AlterConfig};
use partitionline::protocol::api_keys::{SHARE_FETCH,SHARE_ACKNOWLEDGE};
use partitionline::error;
fn assert_broker(failure:Error,expected:i16) {assert_eq!(failure.broker_code(),Some(expected));}
async fn live_poll(group: &mut ShareGroup, stage: &str) -> partitionline::ShareRecords {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let records = group.poll().await.unwrap();
            eprintln!("TRACE stage={stage} records={:?} acquired={} metrics={:?}", records.iter().map(|r| (r.offset,r.delivery_count)).collect::<Vec<_>>(),group.acquired_record_count(),group.metrics());
            if !records.is_empty() {
                return records;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("stage={stage} timeout={error:?}"))
}

fn check_live_record(record: &ShareRecord, topic: &str) {
    assert_eq!(record.topic, topic);
    assert_eq!(record.partition, 0);
    assert!((0..4).contains(&record.offset));
    assert_eq!(record.timestamp, 1000 + record.offset);
    assert_eq!(
        record.key.as_deref(),
        Some(format!("key-{}", record.offset).as_bytes())
    );
    assert_eq!(
        record.value.as_deref(),
        Some(format!("value-{}", record.offset).as_bytes())
    );
    assert!(record.delivery_count > 0);
}

#[tokio::main]
async fn main() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap();
        let reference = std::env::var("PL_COMPAT_REFERENCE").unwrap();
        let source = std::env::var("PL_COMPAT_SOURCE_SHA").unwrap();
        let prefix = std::env::var("PL_SHARE_PREFIX").unwrap();
        assert_eq!(reference, "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837");
        assert_eq!(source.len(), 40);
        assert!(source.bytes().all(|b| b.is_ascii_hexdigit()));
        let lock_ms = std::env::var("PL_SHARE_LOCK_MS").map_or(15000, |value| value.parse::<u64>().unwrap());
        assert!((1000..=15000).contains(&lock_ms));
        let topic = format!("{prefix}-records");
        let group_id = format!("{prefix}-group");
        let mut admin = Admin::connect(bootstrap.clone()).await.unwrap();
        let fetch = admin.versions().get(&SHARE_FETCH).unwrap();
        let ack = admin.versions().get(&SHARE_ACKNOWLEDGE).unwrap();
        assert_eq!((fetch.min_version, fetch.max_version, ack.min_version, ack.max_version), (1, 2, 1, 2));
        let features = admin.describe_features().await.unwrap();
        let levels: std::collections::BTreeMap<_, _> = features.finalized_features.iter().map(|f| (f.name.clone(), [f.min_version_level, f.max_version_level])).collect();
        assert!(levels.get("share.version").is_some_and(|v| v[0] >= 1));
        for result in admin.create_topics(&[NewTopic::new(&topic, 1, 1)], 10000, false).await.unwrap() {
            assert_eq!(result.error_code, 0);
        }
        let changes = [ConfigResourceUpdate::new(ConfigResource::group(&group_id), [
            AlterConfig::set("share.auto.offset.reset", "earliest"),
            AlterConfig::set("share.record.lock.duration.ms", lock_ms.to_string()),
        ])];
        for result in admin.incremental_alter_configs_for(&changes, false).await.unwrap() { assert_eq!(result.error_code, 0); }
        let producer = Producer::new(ProducerConfig::bootstrap([bootstrap.clone()]).linger(Duration::from_secs(2))).await.unwrap();
        producer.partitions_for(&topic).await.unwrap();
        // Wait for connection readiness, queue all four, then await their batch
        // acknowledgement. RecordLimit must acquire one record from this batch.
        let produced = producer.send_all((0..4).map(|index| ProduceRecord::to(topic.clone()).partition(0).timestamp(1000 + index).key(format!("key-{index}").into_bytes()).value(format!("value-{index}").into_bytes()))).await.unwrap();
        assert_eq!(produced.iter().map(|record| (record.partition, record.offset)).collect::<Vec<_>>(), vec![(0, 0), (0, 1), (0, 2), (0, 3)]);
        producer.flush().await.unwrap();
        producer.close().await.unwrap();
        let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(100).max_poll_records(1).request_timeout(Duration::from_secs(3));
        let mut owner = ShareGroup::join(cfg.clone(), &group_id, &topic).await.unwrap();
        let mut next = ShareGroup::join(cfg, &group_id, &topic).await.unwrap();
        owner.set_acquire_mode(ShareAcquireMode::RecordLimit);
        next.set_acquire_mode(ShareAcquireMode::RecordLimit);
        let held = live_poll(&mut owner, "initial").await;
        assert_eq!(held.len(), 1);
        check_live_record(&held[0], &topic);
        assert_eq!(owner.acquired_record_count(), 1, "v2 record limit must constrain acquisition across a full batch");
        assert_eq!(held[0].delivery_count, 1);
        assert_eq!(owner.acquisition_lock_timeout_ms(), Some(i32::try_from(lock_ms).unwrap()));
        owner.renew(&held).await.unwrap(); eprintln!("TRACE renew-success held={:?}",held.iter().map(|r| (r.offset,r.delivery_count)).collect::<Vec<_>>());
        assert_eq!(owner.acquired_record_count(), 1);
        assert_eq!(owner.metrics().records_acknowledged, 0);
        let mut accepted = std::collections::BTreeSet::new();
        // Drain available neighbours while proving another member cannot acquire
        // the held offset before its acquisition lock expires.
        loop {
            let records = next.poll().await.unwrap();
            if records.is_empty() { break; }
            assert_eq!(records.len(), 1);
            assert_eq!(next.acquired_record_count(), 1);
            check_live_record(&records[0], &topic);
            assert_ne!(records[0].offset, held[0].offset);
            assert_eq!(records[0].delivery_count, 1);
            assert!(accepted.insert(records[0].offset));
            next.accept(&records).await.unwrap();
        }
        assert_eq!(accepted.len(), 3); eprintln!("TRACE accepted-neighbours={accepted:?}; sleep={lock_ms}ms");
        tokio::time::sleep(Duration::from_millis(lock_ms + 200)).await;
        let expired = live_poll(&mut next, "expiry").await;
        check_live_record(&expired[0], &topic);
        assert_eq!((expired[0].offset, expired[0].delivery_count), (held[0].offset, 2));
        next.release(&expired).await.unwrap(); eprintln!("TRACE release-success expired={:?}",expired.iter().map(|r| (r.offset,r.delivery_count)).collect::<Vec<_>>());
        let released = live_poll(&mut owner, "release").await;
        check_live_record(&released[0], &topic);
        assert_eq!((released[0].offset, released[0].delivery_count), (held[0].offset, 3));
        assert_broker(owner.accept(&held).await.unwrap_err(), error::INVALID_RECORD_STATE);
        assert!(accepted.insert(released[0].offset));
        owner.accept(&released).await.unwrap();
        assert_eq!(accepted, [0, 1, 2, 3].into_iter().collect());
        assert!(next.poll().await.unwrap().is_empty(), "accepted records cannot be reacquired");
        owner.leave().await.unwrap();
        next.leave().await.unwrap();
        eprintln!("SUPPLEMENTAL_TRACE_REPORT {{\"schema_version\":1,\"source_sha\":\"{source}\",\"reference\":\"{reference}\",\"finalized_features\":{levels:?},\"runtime_version\":2,\"acquire_mode\":\"record_limit\",\"lock_ms\":{lock_ms},\"accepted_offsets\":[0,1,2,3],\"release_delivery_count\":3,\"expiry_delivery_count\":2,\"renew\":\"successful\",\"disposition\":\"supported\"}}");
    }).await.unwrap();
}
