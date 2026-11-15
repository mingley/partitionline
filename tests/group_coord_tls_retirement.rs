//! TLS coordinators must retire an idle socket before replacing it.
mod common;

use common::WireEventKind;
use partitionline::protocol::api_keys::{OFFSET_FETCH, SASL_AUTHENTICATE};
use partitionline::{ConsumerConfig, ConsumerGroup};
use std::time::Duration;

async fn idle_tls_coordinator(require_sasl: bool) {
    let (mock, tls) = common::Mock::start_tls().await;
    mock.enable_wire_trace(4096);
    if require_sasl {
        mock.require_plain_sasl("user", "pass");
    }
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()])
        .max_wait_ms(10)
        .session_timeout(Duration::from_secs(120))
        .heartbeat_interval(Duration::from_secs(60))
        .connections_max_idle(Duration::from_millis(30));
    cfg.tls = Some(tls);
    if require_sasl {
        cfg.sasl_plain = Some(("user".into(), "pass".into()));
    }
    let mut group = ConsumerGroup::join(cfg, "tls-idle-coord", "t")
        .await
        .unwrap();

    let _committed = group.committed().await.unwrap();
    let (first, first_request) = mock
        .wire_trace()
        .iter()
        .rev()
        .find(|event| {
            matches!(
                &event.kind,
                WireEventKind::Request { api_key, .. } if *api_key == OFFSET_FETCH
            )
        })
        .map(|event| (event.socket, event.sequence))
        .expect("first OffsetFetch was not observed");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _committed = group.committed().await.unwrap();
    let trace = mock.wire_trace();
    let (second, second_request) = trace
        .iter()
        .rev()
        .find(|event| {
            matches!(
                &event.kind,
                WireEventKind::Request { api_key, .. } if *api_key == OFFSET_FETCH
            )
        })
        .map(|event| (event.socket, event.sequence))
        .expect("second OffsetFetch was not observed");
    assert_ne!(first, second, "idle coordinator must open a new TLS socket");
    if require_sasl {
        for (socket, offset_request) in [(first, first_request), (second, second_request)] {
            assert!(
                trace.iter().any(|event| {
                    event.socket == socket
                        && event.sequence < offset_request
                        && matches!(
                            &event.kind,
                            WireEventKind::Request { api_key, .. }
                                if *api_key == SASL_AUTHENTICATE
                        )
                }),
                "each TLS coordinator must authenticate before OffsetFetch"
            );
        }
    }

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if mock
                .wire_trace()
                .iter()
                .any(|event| event.socket == first && matches!(&event.kind, WireEventKind::Close))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the old TLS broker socket was not retired");
    group.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn idle_group_coordinator_retires_previous_tls_peer() {
    idle_tls_coordinator(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_group_coordinator_reauthenticates_over_tls() {
    idle_tls_coordinator(true).await;
}
