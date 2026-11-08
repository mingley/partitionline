//! TLS coordinators must retire an idle socket before replacing it.
mod common;

use common::WireEventKind;
use partitionline::protocol::api_keys::OFFSET_FETCH;
use partitionline::{ConsumerConfig, ConsumerGroup};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn idle_group_coordinator_retires_previous_tls_peer() {
    let (mock, tls) = common::Mock::start_tls().await;
    mock.enable_wire_trace(4096);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()])
        .max_wait_ms(10)
        .session_timeout(Duration::from_secs(120))
        .heartbeat_interval(Duration::from_secs(60))
        .connections_max_idle(Duration::from_millis(30));
    cfg.tls = Some(tls);
    let mut group = ConsumerGroup::join(cfg, "tls-idle-coord", "t")
        .await
        .unwrap();

    let _committed = group.committed().await.unwrap();
    let first = mock
        .wire_trace()
        .iter()
        .rev()
        .find(|event| {
            matches!(
                &event.kind,
                WireEventKind::Request { api_key, .. } if *api_key == OFFSET_FETCH
            )
        })
        .expect("first OffsetFetch was not observed")
        .socket;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _committed = group.committed().await.unwrap();
    let second = mock
        .wire_trace()
        .iter()
        .rev()
        .find(|event| {
            matches!(
                &event.kind,
                WireEventKind::Request { api_key, .. } if *api_key == OFFSET_FETCH
            )
        })
        .expect("second OffsetFetch was not observed")
        .socket;
    assert_ne!(first, second, "idle coordinator must open a new TLS socket");

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
