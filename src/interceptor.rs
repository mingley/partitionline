//! Produce and fetch interceptors (Java `ProducerInterceptor` / `ConsumerInterceptor`).

use std::fmt;
use std::sync::Arc;

use crate::consumer::FetchedRecord;
use crate::error::Error;
use crate::producer::{ProduceRecord, RecordMetadata};

/// Mutate a record before it is partitioned and queued, and observe acks.
pub trait ProducerInterceptor: Send + Sync + 'static {
    /// Return the record to send. The default is pass-through.
    fn on_send(&self, rec: ProduceRecord) -> ProduceRecord {
        rec
    }

    /// Broker ack (including `acks=0` local complete).
    fn on_ack(&self, _md: &RecordMetadata) {}

    /// The record failed (broker error, timeout, closed).
    fn on_error(&self, _err: &Error) {}
}

/// Mutate fetch results before they are returned to the caller.
pub trait ConsumerInterceptor: Send + Sync + 'static {
    /// Return the records the caller should see. The default is pass-through.
    fn on_consume(&self, recs: Vec<FetchedRecord>) -> Vec<FetchedRecord> {
        recs
    }
}

/// Chain of [`ProducerInterceptor`]s. Empty is a no-op.
#[derive(Clone, Default)]
pub struct ProducerInterceptors {
    inner: Vec<Arc<dyn ProducerInterceptor>>,
}

impl ProducerInterceptors {
    /// Append one interceptor. They run in insertion order.
    pub fn push(&mut self, i: impl ProducerInterceptor) {
        self.inner.push(Arc::new(i));
    }

    pub(crate) fn on_send(&self, rec: ProduceRecord) -> ProduceRecord {
        let mut rec = rec;
        for i in &self.inner {
            rec = i.on_send(rec);
        }
        rec
    }

    pub(crate) fn on_ack(&self, md: &RecordMetadata) {
        for i in &self.inner {
            i.on_ack(md);
        }
    }

    pub(crate) fn on_error(&self, err: &Error) {
        for i in &self.inner {
            i.on_error(err);
        }
    }
}

impl fmt::Debug for ProducerInterceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProducerInterceptors")
            .field(&self.inner.len())
            .finish()
    }
}

/// Chain of [`ConsumerInterceptor`]s. Empty is a no-op.
#[derive(Clone, Default)]
pub struct ConsumerInterceptors {
    inner: Vec<Arc<dyn ConsumerInterceptor>>,
}

impl ConsumerInterceptors {
    /// Append one interceptor. They run in insertion order.
    pub fn push(&mut self, i: impl ConsumerInterceptor) {
        self.inner.push(Arc::new(i));
    }

    pub(crate) fn on_consume(&self, recs: Vec<FetchedRecord>) -> Vec<FetchedRecord> {
        let mut recs = recs;
        for i in &self.inner {
            recs = i.on_consume(recs);
        }
        recs
    }
}

impl fmt::Debug for ConsumerInterceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ConsumerInterceptors")
            .field(&self.inner.len())
            .finish()
    }
}
