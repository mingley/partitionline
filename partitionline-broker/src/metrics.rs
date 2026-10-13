//! Bounded counters and telemetry for broker operations.
//!
//! Series names and labels are a fixed cardinality budget. Credential-shaped
//! names are refused and are not copied into the error. A disabled registry
//! retains nothing. Each subscriber has a bounded queue; when that queue is
//! full the counter still updates and the caller sees backpressure instead of
//! a larger allocation.
//!
//! Quota and authorization helpers record only an outcome. They do not accept
//! a principal, payload, or secret. This registry is not a Kafka metrics RPC.

use std::collections::BTreeMap;

/// How a counter update was delivered to subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Telemetry is off. No series or queue bytes were retained.
    Disabled,
    /// Every subscriber accepted the event.
    Queued,
    /// At least one subscriber queue was full. The counter still moved.
    Backpressured,
}

/// Why a metric update was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The name or label looks like credential material and was not stored.
    Redacted,
    /// The series or subscriber cap is already full.
    Cardinality,
    /// The name, label, or value is not usable.
    Invalid,
}

/// Registry bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_series: usize,
    max_label_bytes: usize,
    max_subscribers: usize,
    max_queue_events: usize,
}

impl Limits {
    /// Configure positive series, label, subscriber, and queue caps.
    pub fn new(
        max_series: usize,
        max_label_bytes: usize,
        max_subscribers: usize,
        max_queue_events: usize,
    ) -> Result<Self, Error> {
        if !(1..=10_000).contains(&max_series)
            || !(1..=64 * 1024).contains(&max_label_bytes)
            || !(1..=64).contains(&max_subscribers)
            || !(1..=10_000).contains(&max_queue_events)
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            max_series,
            max_label_bytes,
            max_subscribers,
            max_queue_events,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    name: String,
    labels: Vec<(String, String)>,
}

struct Series {
    key: SeriesKey,
    value: u64,
}

struct Subscriber {
    queue: Vec<u32>,
}

/// In-memory counters and a bounded subscriber queue.
pub struct Registry {
    limits: Limits,
    enabled: bool,
    series: Vec<Series>,
    index: BTreeMap<SeriesKey, u32>,
    subscribers: Vec<Subscriber>,
    retained_bytes: usize,
}

/// One exported counter. Labels are the stored pairs, never a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    /// Counter name.
    pub name: String,
    /// Bounded labels.
    pub labels: Vec<(String, String)>,
    /// Current value.
    pub value: u64,
}

impl Registry {
    /// Counters are recorded and subscribers may be attached.
    pub fn open(limits: Limits) -> Self {
        Self {
            limits,
            enabled: true,
            series: Vec::new(),
            index: BTreeMap::new(),
            subscribers: Vec::new(),
            retained_bytes: 0,
        }
    }

    /// Drop telemetry. Later updates retain no bytes and notify no subscriber.
    pub fn disable(&mut self) {
        self.enabled = false;
        self.series.clear();
        self.index.clear();
        self.subscribers.clear();
        self.retained_bytes = 0;
    }

    /// Bytes retained for series labels and queued events.
    pub fn overhead_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Add `value` to a counter. Sensitive names are refused before any copy lands.
    pub fn add(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) -> Result<Delivery, Error> {
        if !self.enabled {
            return Ok(Delivery::Disabled);
        }
        if value == 0 || labels.len() > 8 {
            return Err(Error::Invalid);
        }
        reject_sensitive(name)?;
        for (key, label) in labels {
            reject_sensitive(key)?;
            reject_sensitive(label)?;
        }
        let id = self.series_id(name, labels)?;
        let series = self
            .series
            .get_mut(usize::try_from(id).unwrap_or(usize::MAX))
            .ok_or(Error::Invalid)?;
        series.value = series.value.saturating_add(value);
        let mut dropped = false;
        for subscriber in &mut self.subscribers {
            if subscriber.queue.len() >= self.limits.max_queue_events {
                dropped = true;
                continue;
            }
            subscriber.queue.push(id);
            self.retained_bytes = self.retained_bytes.saturating_add(EVENT_BYTES);
        }
        if dropped {
            Ok(Delivery::Backpressured)
        } else {
            Ok(Delivery::Queued)
        }
    }

    /// Record an authorization outcome without a principal or credential.
    pub fn record_authorization(&mut self, allowed: bool) -> Result<Delivery, Error> {
        let outcome = if allowed { "allowed" } else { "denied" };
        self.add("authorization", &[("outcome", outcome)], 1)
    }

    /// Record a quota decision without a client id or payload size label.
    pub fn record_quota(&mut self, throttled: bool) -> Result<Delivery, Error> {
        let outcome = if throttled { "throttled" } else { "admitted" };
        self.add("quota", &[("outcome", outcome)], 1)
    }

    /// Attach a subscriber. The queue starts empty and stays within the event cap.
    pub fn subscribe(&mut self) -> Result<u32, Error> {
        if !self.enabled || self.subscribers.len() >= self.limits.max_subscribers {
            return Err(Error::Cardinality);
        }
        let id = u32::try_from(self.subscribers.len()).map_err(|_| Error::Invalid)?;
        self.subscribers.push(Subscriber { queue: Vec::new() });
        Ok(id)
    }

    /// Drain one subscriber queue and release its retained event bytes.
    pub fn poll(&mut self, subscriber: u32) -> Result<usize, Error> {
        let queue = self
            .subscribers
            .get_mut(usize::try_from(subscriber).unwrap_or(usize::MAX))
            .ok_or(Error::Invalid)?;
        let count = queue.queue.len();
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(count.saturating_mul(EVENT_BYTES));
        queue.queue.clear();
        Ok(count)
    }

    /// Copy the current counters. The copy is not part of the retained budget.
    pub fn snapshot(&self) -> Vec<Sample> {
        self.series
            .iter()
            .map(|series| Sample {
                name: series.key.name.clone(),
                labels: series.key.labels.clone(),
                value: series.value,
            })
            .collect()
    }

    fn series_id(&mut self, name: &str, labels: &[(&str, &str)]) -> Result<u32, Error> {
        let key = series_key(name, labels)?;
        if let Some(id) = self.index.get(&key).copied() {
            return Ok(id);
        }
        if self.series.len() >= self.limits.max_series {
            return Err(Error::Cardinality);
        }
        let label_bytes = key.name.len()
            + key
                .labels
                .iter()
                .map(|(label_key, value)| label_key.len().saturating_add(value.len()))
                .fold(0usize, usize::saturating_add);
        if label_bytes > self.limits.max_label_bytes
            || self.retained_bytes.saturating_add(label_bytes) > self.limits.max_label_bytes
        {
            return Err(Error::Cardinality);
        }
        let id = u32::try_from(self.series.len()).map_err(|_| Error::Invalid)?;
        self.retained_bytes = self.retained_bytes.saturating_add(label_bytes);
        self.series.push(Series {
            key: key.clone(),
            value: 0,
        });
        self.index.insert(key, id);
        Ok(id)
    }
}

const EVENT_BYTES: usize = 4;

fn series_key(name: &str, labels: &[(&str, &str)]) -> Result<SeriesKey, Error> {
    if !valid_text(name) {
        return Err(Error::Invalid);
    }
    let mut pairs = Vec::new();
    for (key, value) in labels {
        if !valid_text(key) || !valid_text(value) {
            return Err(Error::Invalid);
        }
        pairs.push(((*key).to_owned(), (*value).to_owned()));
    }
    pairs.sort();
    if pairs.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::Invalid);
    }
    Ok(SeriesKey {
        name: name.to_owned(),
        labels: pairs,
    })
}

fn valid_text(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 64
        && text
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn reject_sensitive(text: &str) -> Result<(), Error> {
    let lower = text.to_ascii_lowercase();
    if ["password", "secret", "credential"]
        .iter()
        .any(|word| lower.contains(word))
    {
        Err(Error::Redacted)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits::new(4, 256, 2, 2).unwrap_or_else(|_| panic!("metric limits"))
    }

    #[test]
    fn redacts_secrets_caps_series_and_backpressures_a_stalled_subscriber() {
        let mut disabled = Registry::open(limits());
        disabled.disable();
        for _ in 0..32 {
            assert_eq!(
                disabled
                    .add("produce_bytes", &[], 1)
                    .unwrap_or_else(|error| panic!("{error:?}")),
                Delivery::Disabled
            );
        }
        assert_eq!(disabled.overhead_bytes(), 0);
        assert!(disabled.snapshot().is_empty());

        let mut registry = Registry::open(limits());
        assert!(matches!(
            registry.add("sasl_password", &[], 1),
            Err(Error::Redacted)
        ));
        assert!(matches!(
            registry.add("produce_bytes", &[("secret", "x")], 1),
            Err(Error::Redacted)
        ));
        assert_eq!(registry.overhead_bytes(), 0);
        let error = format!("{:?}", Error::Redacted);
        assert!(!error.to_ascii_lowercase().contains("password"));

        registry
            .record_authorization(false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        registry
            .record_quota(true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let snapshot = registry.snapshot();
        assert!(snapshot.iter().all(|sample| sample.name != "sasl_password"));
        assert_eq!(
            snapshot
                .iter()
                .find(|sample| sample.name == "authorization")
                .map(|sample| sample.value),
            Some(1)
        );

        let subscriber = registry
            .subscribe()
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            registry
                .add("fetch_bytes", &[], 1)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Delivery::Queued
        );
        assert_eq!(
            registry
                .add("fetch_bytes", &[], 1)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Delivery::Queued
        );
        let before = registry.overhead_bytes();
        assert_eq!(
            registry
                .add("fetch_bytes", &[], 4)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Delivery::Backpressured
        );
        assert_eq!(registry.overhead_bytes(), before);
        assert_eq!(
            registry
                .snapshot()
                .iter()
                .find(|sample| sample.name == "fetch_bytes")
                .map(|sample| sample.value),
            Some(6)
        );
        assert_eq!(
            registry
                .poll(subscriber)
                .unwrap_or_else(|error| panic!("{error:?}")),
            2
        );
        assert!(registry.overhead_bytes() < before);
        assert!(matches!(
            registry
                .add("a", &[], 1)
                .and_then(|_| registry.add("b", &[], 1)),
            Err(Error::Cardinality)
        ));
    }
}
