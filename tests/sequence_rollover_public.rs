//! Seeded mock history for idempotent producer sequence rollover.
#![cfg(rollover_mock_witness)]

mod common;

use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(20);

#[derive(Clone, Default)]
struct Outcomes {
    acknowledgements: Arc<Mutex<Vec<(i32, i64)>>>,
    errors: Arc<Mutex<Vec<Option<i16>>>>,
}

impl ProducerInterceptor for Outcomes {
    fn on_ack(&self, metadata: &RecordMetadata) {
        self.acknowledgements
            .lock()
            .expect("acknowledgement mutex")
            .push((metadata.partition, metadata.offset));
    }

    fn on_error(&self, error: &Error) {
        self.errors
            .lock()
            .expect("error mutex")
            .push(error.broker_code());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn seeded_max_sequence_two_distinct_records() {
    let arm = std::env::var("ROLLOVER_ARM").expect("ROLLOVER_ARM");
    assert!(matches!(arm.as_str(), "baseline" | "arithmetic"));
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("rollover", 2);
    let outcomes = Outcomes::default();
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(60))
            .max_block(Duration::from_secs(10))
            .interceptor(outcomes.clone()),
    )
    .await
    .expect("connect producer");
    let identity = (producer.__test_producer_id(), producer.__test_producer_epoch());
    assert!(identity.0 >= 0);
    assert!(identity.1 >= 0);

    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("rollover").partition(1).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup delivery");
    assert_eq!((warm.partition, warm.offset), (1, 0));
    assert!(mock.log_values("rollover", 0).is_empty());
    let warm_sequences = mock.produce_sequences();
    assert_eq!(warm_sequences.len(), 1);
    assert_eq!((warm_sequences[0].0, warm_sequences[0].1), identity);
    assert_eq!((warm_sequences[0].2, warm_sequences[0].3), (0, 1));

    // This does not represent a natural broker history. It sets only the
    // still-unused target partition in both peers to the same boundary.
    producer.__test_set_next_sequence("rollover", 0, i32::MAX);
    mock.__test_seed_expected_sequence(identity.0, identity.1, "rollover", 0, i32::MAX);

    let first = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("rollover").partition(0).value(&b"first"[..])),
    )
    .await
    .expect("first delivery deadline")
    .expect("first delivery");
    assert_eq!((first.partition, first.offset), (0, 0));
    assert_eq!(
        mock.log_values("rollover", 0),
        vec![(0, Some(b"first".to_vec()))]
    );

    let second = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("rollover").partition(0).value(&b"second"[..])),
    )
    .await
    .expect("second delivery deadline")
    .expect("second delivery");
    let wire = mock.produce_sequences();
    assert_eq!(wire.len(), 3);
    assert!(wire.iter().all(|entry| (entry.0, entry.1) == identity));
    assert_eq!((wire[1].2, wire[1].3), (i32::MAX, 1));
    assert_eq!((producer.__test_producer_id(), producer.__test_producer_epoch()), identity);

    if arm == "baseline" {
        assert_eq!((wire[2].2, wire[2].3), (i32::MAX, 1));
        assert_eq!((second.partition, second.offset), (0, 0));
        assert_eq!(
            mock.log_values("rollover", 0),
            vec![(0, Some(b"first".to_vec()))]
        );
        assert_eq!(
            *outcomes.acknowledgements.lock().expect("acknowledgements"),
            [(1, 0), (0, 0), (0, 0)]
        );
        println!("ROLLOVER_MOCK baseline second_receipt=0 second_value_absent=true");
    } else {
        assert_eq!((wire[2].2, wire[2].3), (0, 1));
        assert_eq!((second.partition, second.offset), (0, 1));
        assert_eq!(
            mock.log_values("rollover", 0),
            vec![(0, Some(b"first".to_vec())), (1, Some(b"second".to_vec()))]
        );
        assert_eq!(
            *outcomes.acknowledgements.lock().expect("acknowledgements"),
            [(1, 0), (0, 0), (0, 1)]
        );
        println!("ROLLOVER_MOCK arithmetic second_receipt=1 second_value_present=true");
    }
    assert!(outcomes.errors.lock().expect("errors").is_empty());
    let metrics = producer.metrics();
    assert_eq!(metrics.records_queued, 3);
    assert_eq!(metrics.records_acked, 3);
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, 0);
    producer
        .close_timeout(Duration::from_secs(5))
        .await
        .expect("close");
}
