//! Public controls for the current quota-window/Produce integration boundary.
#![allow(clippy::unwrap_used)]

use partitionline_broker::protocol::{Limits as ProtocolLimits, RequestHeader};
use partitionline_broker::quotas::{Error, Kind, Limits, Window};

fn header(client: Option<&str>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0i16.to_be_bytes());
    bytes.extend_from_slice(&3i16.to_be_bytes());
    bytes.extend_from_slice(&42i32.to_be_bytes());
    match client {
        Some(client) => {
            bytes.extend_from_slice(&i16::try_from(client.len()).unwrap().to_be_bytes());
            bytes.extend_from_slice(client.as_bytes());
        }
        None => bytes.extend_from_slice(&(-1i16).to_be_bytes()),
    }
    bytes
}

#[test]
fn valid_header_identities_cannot_always_be_window_keys() {
    let long = "a".repeat(250);
    let limits = Limits::new(1, 1, 1, 3, 4, 1, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    for client in [None, Some(""), Some(long.as_str())] {
        let bytes = header(client);
        let (parsed, rest) = RequestHeader::parse(&bytes, 1, ProtocolLimits::default()).unwrap();
        assert_eq!(parsed.client_id, client);
        assert!(rest.is_empty());
        assert!(matches!(
            window.admit(parsed.client_id, Kind::Produce, 1, 0),
            Err(Error::InvalidRequest)
        ));
    }
}

#[test]
fn accumulated_delay_can_refuse_even_when_no_work_is_in_flight() {
    let limits = Limits::new(1, 1, 1, 3, 4, 1, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    for _ in 0..3 {
        window.connect("shared").unwrap();
    }
    let first = window.admit(Some("shared"), Kind::Produce, 1, 0).unwrap();
    assert_eq!(first.throttle_ms, 0);
    assert_eq!(window.complete(first.work_id).unwrap(), 1);
    let second = window.admit(Some("shared"), Kind::Produce, 1, 0).unwrap();
    assert_eq!(second.throttle_ms, 1_000);
    assert_eq!(window.complete(second.work_id).unwrap(), 1);
    assert_eq!(window.inflight_bytes(), 0);
    assert!(matches!(
        window.admit(Some("shared"), Kind::Produce, 1, 0),
        Err(Error::Throttled)
    ));
    let other = window.admit(Some("other"), Kind::Produce, 1, 0).unwrap();
    assert_eq!(other.throttle_ms, 0);
    assert_eq!(window.complete(other.work_id).unwrap(), 1);
}
