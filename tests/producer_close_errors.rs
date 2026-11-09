//! Close reports worker errors that its initial flush observes.

mod common;

use partitionline::error::TOPIC_AUTHORIZATION_FAILED;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Events {
    acks: AtomicUsize,
    errors: AtomicUsize,
    closes: AtomicUsize,
}

struct Observe(Arc<Events>);

impl ProducerInterceptor for Observe {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.0.acks.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, _error: &Error) {
        let _previous = self.0.errors.fetch_add(1, Ordering::SeqCst);
    }

    fn close(&self) {
        let _previous = self.0.closes.fetch_add(1, Ordering::SeqCst);
    }
}

async fn failed_producer() -> Result<(common::Mock, Producer, Arc<Events>), Error> {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let events = Arc::new(Events::default());
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(3))
            .delivery_timeout(Duration::from_secs(3))
            .interceptor(Observe(Arc::clone(&events))),
    )
    .await?;

    // Warm the route and keep one worker in the producer's worker registry.
    let _warm = producer
        .send(ProduceRecord::to("t").value(&b"warm"[..]))
        .await?;

    mock.set_produce_error_times(TOPIC_AUTHORIZATION_FAILED, 1);
    let result = producer
        .send(ProduceRecord::to("t").value(&b"denied"[..]))
        .await;
    assert_eq!(
        result.as_ref().err().and_then(Error::broker_code),
        Some(TOPIC_AUTHORIZATION_FAILED),
        "the public send must observe the injected broker failure"
    );
    assert_eq!(mock.log_len("t", 0), 1, "the denied record was not appended");
    Ok((mock, producer, events))
}

async fn assert_cleanup(other: &Producer, events: &Events) {
    assert_eq!(events.acks.load(Ordering::SeqCst), 1);
    assert_eq!(events.errors.load(Ordering::SeqCst), 1);
    assert_eq!(events.closes.load(Ordering::SeqCst), 1);
    assert_eq!(other.metrics().bytes_buffered, 0);
    assert!(matches!(
        other.try_send(ProduceRecord::to("t").value(&b"late"[..])),
        Err(Error::Closed)
    ));
    assert!(matches!(
        other.send(ProduceRecord::to("t").value(&b"late"[..])).await,
        Err(Error::Closed)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn flush_reports_recorded_failure_and_consumes_it() -> Result<(), Error> {
    let (_mock, producer, events) = failed_producer().await?;
    let other = producer.clone();
    let flush = producer.flush().await;
    assert_eq!(
        flush.as_ref().err().and_then(Error::broker_code),
        Some(TOPIC_AUTHORIZATION_FAILED),
        "the initial flush must observe the same broker error as send"
    );
    assert!(producer.close().await.is_ok(), "the earlier flush consumed the error");
    assert_cleanup(&other, &events).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn close_returns_recorded_failure_after_shutdown() -> Result<(), Error> {
    let (_mock, producer, events) = failed_producer().await?;
    let other = producer.clone();
    let result = producer.close().await;
    assert_cleanup(&other, &events).await;
    assert_eq!(
        result.as_ref().err().and_then(Error::broker_code),
        Some(TOPIC_AUTHORIZATION_FAILED),
        "close did not report the recorded broker error: {result:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn close_timeout_returns_recorded_failure_after_shutdown() -> Result<(), Error> {
    let (_mock, producer, events) = failed_producer().await?;
    let other = producer.clone();
    let result = producer.close_timeout(Duration::from_secs(3)).await;
    assert_cleanup(&other, &events).await;
    assert_eq!(
        result.as_ref().err().and_then(Error::broker_code),
        Some(TOPIC_AUTHORIZATION_FAILED),
        "close_timeout did not report the recorded broker error: {result:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn successful_close_methods_still_return_ok() -> Result<(), Error> {
    for timed in [false, true] {
        let mock = common::Mock::start().await;
        let producer = Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .linger(Duration::ZERO)
                .request_timeout(Duration::from_secs(3))
                .delivery_timeout(Duration::from_secs(3)),
        )
        .await?;
        let _sent = producer
            .send(ProduceRecord::to("t").value(&b"ok"[..]))
            .await?;
        let other = producer.clone();
        let result = if timed {
            producer.close_timeout(Duration::from_secs(3)).await
        } else {
            producer.close().await
        };
        assert!(result.is_ok(), "successful close returned {result:?}");
        assert!(matches!(
            other.try_send(ProduceRecord::to("t")),
            Err(Error::Closed)
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn closed_worker_is_reported_and_double_close_is_safe() -> Result<(), Error> {
    for timed in [false, true] {
        let mock = common::Mock::start().await;
        let producer = Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .connections(1)
                .linger(Duration::ZERO)
                .request_timeout(Duration::from_secs(3))
                .delivery_timeout(Duration::from_secs(3)),
        )
        .await?;
        let _sent = producer
            .send(ProduceRecord::to("t").value(&b"warm"[..]))
            .await?;
        // This hook forces a dead idle worker; the broker-error controls above
        // exercise the normal public send path without injected task failure.
        let workers = producer.test_worker_tasks();
        assert!(!workers.is_empty(), "warm send did not create a worker");
        for worker in &workers {
            worker.abort();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while workers.iter().any(|worker| !worker.is_finished()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted worker did not stop");

        let other = producer.clone();
        let result = if timed {
            producer.close_timeout(Duration::from_secs(3)).await
        } else {
            producer.close().await
        };
        assert!(
            matches!(&result, Err(Error::Closed)),
            "close did not report worker closure: {result:?}"
        );
        assert!(matches!(
            other.try_send(ProduceRecord::to("t")),
            Err(Error::Closed)
        ));
        assert!(other.close().await.is_ok(), "a second close should be safe");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_close_calls_preserve_closed_admission() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let events = Arc::new(Events::default());
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(3))
            .delivery_timeout(Duration::from_secs(3))
            .interceptor(Observe(Arc::clone(&events))),
    )
    .await?;
    let _sent = producer
        .send(ProduceRecord::to("t").value(&b"warm"[..]))
        .await?;
    let first = producer.clone();
    let second = producer.clone();
    let (one, two) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first.close(), second.close_timeout(Duration::from_secs(3)))
    })
    .await
    .expect("concurrent close calls did not finish");
    assert!(
        matches!(&one, Ok(()) | Err(Error::Closed)),
        "unexpected first close result: {one:?}"
    );
    assert!(
        matches!(&two, Ok(()) | Err(Error::Closed)),
        "unexpected second close result: {two:?}"
    );
    assert_eq!(events.acks.load(Ordering::SeqCst), 1);
    assert_eq!(events.errors.load(Ordering::SeqCst), 0);
    assert_eq!(events.closes.load(Ordering::SeqCst), 2);
    assert!(matches!(
        producer.try_send(ProduceRecord::to("t")),
        Err(Error::Closed)
    ));
    Ok(())
}
