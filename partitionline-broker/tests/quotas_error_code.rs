//! Local quota refusals do not imply a Kafka response error.
#![allow(clippy::unwrap_used)]

use partitionline_broker::quotas::{Error, Kind, Limits, Window, THROTTLING_QUOTA_EXCEEDED};

fn assert_local_refusal(error: Error) {
    assert!(matches!(error, Error::Throttled));
    assert_eq!(error.code(), None);
}

#[test]
fn connection_capacity_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    window.connect("a").unwrap();
    assert_local_refusal(window.connect("b").unwrap_err());
}

#[test]
fn inflight_capacity_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 1, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(Some("a"), Kind::Produce, 1, 0).unwrap();
    assert_local_refusal(window.admit(Some("a"), Kind::Produce, 1, 0).unwrap_err());
    assert!(window.accepted(accepted.work_id));
    assert_eq!(window.complete(accepted.work_id).unwrap(), 1);
}

#[test]
fn produce_rate_refusal_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(Some("a"), Kind::Produce, 20, 0).unwrap();
    assert_eq!(accepted.throttle_ms, 1_000);
    assert_local_refusal(window.admit(Some("a"), Kind::Produce, 1, 0).unwrap_err());
    assert_eq!(window.complete(accepted.work_id).unwrap(), 20);
}

#[test]
fn replication_rate_refusal_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(None, Kind::Replication, 20, 0).unwrap();
    assert_eq!(accepted.throttle_ms, 1_000);
    assert_local_refusal(window.admit(None, Kind::Replication, 1, 0).unwrap_err());
    assert_eq!(window.complete(accepted.work_id).unwrap(), 20);
}

#[test]
fn api_specific_constant_and_other_local_errors_remain_distinct() {
    assert_eq!(THROTTLING_QUOTA_EXCEEDED, 89);
    assert_eq!(Error::InvalidRequest.code(), None);
    assert_eq!(Error::UnknownWork.code(), None);
    assert_eq!(Error::InvalidLimits.code(), None);
}

#[test]
fn per_client_connection_capacity_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 2, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    window.connect("a").unwrap();
    assert_local_refusal(window.connect("a").unwrap_err());
}

#[test]
fn identity_state_capacity_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(Some("a"), Kind::Produce, 20, 0).unwrap();
    assert_eq!(accepted.throttle_ms, 1_000);
    assert_eq!(window.complete(accepted.work_id).unwrap(), 20);
    assert_local_refusal(window.admit(Some("b"), Kind::Produce, 1, 0).unwrap_err());
}

#[test]
fn fetch_rate_refusal_is_not_a_generic_kafka_error() {
    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(Some("a"), Kind::Fetch, 20, 0).unwrap();
    assert_eq!(accepted.throttle_ms, 1_000);
    assert_local_refusal(window.admit(Some("a"), Kind::Fetch, 1, 0).unwrap_err());
    assert_eq!(window.complete(accepted.work_id).unwrap(), 20);
}

#[test]
fn inflight_overflow_is_not_a_generic_kafka_error() {
    let limits = Limits::new(u64::MAX, u64::MAX, u64::MAX, 1, 1, u64::MAX, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(None, Kind::Replication, u64::MAX, 0).unwrap();
    assert_local_refusal(window.admit(None, Kind::Replication, 1, 0).unwrap_err());
    assert!(window.accepted(accepted.work_id));
    assert_eq!(window.complete(accepted.work_id).unwrap(), u64::MAX);
}

#[test]
fn returned_non_throttling_errors_have_no_kafka_code() {
    let error = Limits::new(0, 10, 10, 1, 1, 100, 1_000).unwrap_err();
    assert!(matches!(error, Error::InvalidLimits));
    assert_eq!(error.code(), None);

    let limits = Limits::new(10, 10, 10, 1, 1, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let error = window.admit(Some(""), Kind::Produce, 1, 0).unwrap_err();
    assert!(matches!(error, Error::InvalidRequest));
    assert_eq!(error.code(), None);

    let error = window.complete(123).unwrap_err();
    assert!(matches!(error, Error::UnknownWork));
    assert_eq!(error.code(), None);
}
