//! Socket-level mock tracing and node-specific refusal.
mod common;

use common::WireEventKind;
use partitionline::net::BrokerConn;
use partitionline::protocol::api::negotiate_api_versions;
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH};
use std::time::Duration;

#[tokio::test]
async fn trace_distinguishes_node_refusal_and_successful_socket_negotiation() {
    let mock = common::Mock::start_two_node().await;
    mock.enable_wire_trace(128);
    mock.refuse_node_connections(2, 1);
    mock.set_node_api_max(2, FETCH, 7);
    mock.set_node_api_versions_delay(2, Duration::from_millis(80));
    let node2 = mock.broker_addr(2).unwrap();
    let timeout = Duration::from_secs(1);
    let mut node1 = BrokerConn::connect(&mock.addr, "trace-node1", timeout)
        .await
        .unwrap();
    let _versions = negotiate_api_versions(&mut node1, timeout).await.unwrap();
    let mut refused = BrokerConn::connect(&node2, "trace-refused", timeout)
        .await
        .unwrap();
    assert!(negotiate_api_versions(&mut refused, timeout).await.is_err());
    let mut replacement = BrokerConn::connect(&node2, "trace-node2", timeout)
        .await
        .unwrap();
    let versions = negotiate_api_versions(&mut replacement, timeout)
        .await
        .unwrap();
    assert_eq!(versions.api_version(FETCH).unwrap().max_version, 7);
    common::wait_pred("both ApiVersions responses traced", || {
        mock.wire_trace()
            .iter()
            .filter(|e| matches!(e.kind, WireEventKind::ApiVersionsResponse { .. }))
            .count()
            >= 2
    })
    .await;
    let events = mock.wire_trace();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, WireEventKind::Refused) && e.node == 2)
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|e| matches!(e.kind, WireEventKind::Refused) && e.node == 1));
    for node in [1, 2] {
        let accepted = events
            .iter()
            .find(|e| e.node == node && matches!(e.kind, WireEventKind::Accept))
            .unwrap();
        let request = events
            .iter()
            .find(|e| {
                e.node == node
                    && e.socket == accepted.socket
                    && matches!(
                        e.kind,
                        WireEventKind::Request {
                            api_key: API_VERSIONS,
                            api_version: 4
                        }
                    )
            })
            .unwrap();
        let response = events
            .iter()
            .find(|e| {
                e.node == node
                    && e.socket == accepted.socket
                    && matches!(
                        e.kind,
                        WireEventKind::ApiVersionsResponse { error_code: 0, .. }
                    )
            })
            .unwrap();
        let written = events
            .iter()
            .find(|e| {
                e.node == node
                    && e.socket == accepted.socket
                    && matches!(
                        e.kind,
                        WireEventKind::Response {
                            api_key: API_VERSIONS,
                            api_version: 4,
                        }
                    )
            })
            .unwrap();
        assert!(
            accepted.sequence < request.sequence
                && request.sequence < written.sequence
                && written.sequence < response.sequence
        );
        assert!(accepted.at <= request.at && request.at <= written.at && written.at <= response.at);
        if node == 2 {
            assert!(response.at.duration_since(request.at) >= Duration::from_millis(80));
        }
        if let WireEventKind::ApiVersionsResponse { ranges, .. } = &response.kind {
            let fetch = ranges.iter().find(|v| v.api_key == FETCH).unwrap();
            if node == 2 {
                assert_eq!(fetch.max_version, 7);
            }
        }
    }
    drop(node1);
    drop(refused);
    drop(replacement);
    common::wait_pred("both traced sockets closed", || {
        mock.wire_trace()
            .iter()
            .filter(|e| matches!(e.kind, WireEventKind::Close))
            .count()
            >= 2
    })
    .await;
}

#[tokio::test]
async fn failed_tls_handshake_records_close_for_the_accepted_socket() {
    use tokio::io::AsyncWriteExt;

    let (mock, _tls) = common::Mock::start_tls().await;
    mock.enable_wire_trace(32);
    let mut client = tokio::net::TcpStream::connect(&mock.addr).await.unwrap();
    client.write_all(b"not a TLS handshake").await.unwrap();
    drop(client);
    common::wait_pred("failed TLS socket closed", || {
        mock.wire_trace()
            .iter()
            .any(|e| matches!(e.kind, WireEventKind::Close))
    })
    .await;
    let events = mock.wire_trace();
    let accepted = events
        .iter()
        .find(|e| matches!(e.kind, WireEventKind::Accept))
        .unwrap();
    let closed = events
        .iter()
        .find(|e| matches!(e.kind, WireEventKind::Close))
        .unwrap();
    assert_eq!(
        (accepted.node, accepted.socket),
        (closed.node, closed.socket)
    );
    assert!(accepted.sequence < closed.sequence);
    assert!(!events
        .iter()
        .any(|e| matches!(e.kind, WireEventKind::Request { .. })));
}
