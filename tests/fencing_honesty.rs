//! KL-03 Partial: transactional `PRODUCER_FENCED` fails closed (no local epoch invent).
//!
//! When the broker returns `PRODUCER_FENCED` on Produce, the client must surface
//! that error and must **not** invent a local producer-epoch bump or silent
//! retry that would disguise fencing. Mock-broker fencing classification only —
//! **not** a live three-broker fencing history and not a Suite HOLD lift.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use partitionline::{error, ProduceRecord, Producer, ProducerConfig, RecordBatch};
use std::time::Duration;

#[tokio::test]
async fn producer_fenced_on_produce_fails_closed_without_local_epoch_invent() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-fence-honesty".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();

    // First InitProducerId request carries NO_PRODUCER_EPOCH; broker assigns 0.
    assert_eq!(
        mock.last_init_producer_id_producer_epoch(),
        Some(RecordBatch::NO_PRODUCER_EPOCH)
    );

    mock.set_produce_error_times(error::PRODUCER_FENCED, 1);
    let err = producer
        .send(ProduceRecord::to("t").value(&b"should-not-land"[..]))
        .await
        .expect_err("PRODUCER_FENCED must fail the send");
    assert_eq!(
        err.broker_code(),
        Some(error::PRODUCER_FENCED),
        "surface PRODUCER_FENCED; got {err:?}"
    );
    assert_eq!(
        mock.last_produce_producer_epoch(),
        Some(0),
        "fenced Produce must still have used broker-assigned epoch 0"
    );

    // Inject fencing again. A buggy client that invented epoch 1 would show up here.
    mock.set_produce_error_times(error::PRODUCER_FENCED, 1);
    let err2 = producer
        .send(ProduceRecord::to("t").value(&b"still-fenced"[..]))
        .await
        .expect_err("still fenced without re-init");
    assert_eq!(err2.broker_code(), Some(error::PRODUCER_FENCED));
    assert_eq!(
        mock.last_produce_producer_epoch(),
        Some(0),
        "PRODUCER_FENCED must not invent a local producer epoch"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_epoch(),
        Some(RecordBatch::NO_PRODUCER_EPOCH),
        "no silent InitProducerId / epoch invent after PRODUCER_FENCED"
    );

    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}
