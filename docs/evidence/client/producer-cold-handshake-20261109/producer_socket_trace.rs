//! Qualification-only producer connection trace; no production changes.
mod common;

use common::{Mock, WireEvent, WireEventKind};
use partitionline::protocol::api_keys::{API_VERSIONS, METADATA, PRODUCE, SASL_AUTHENTICATE, SASL_HANDSHAKE};
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::{Duration, Instant};

fn requests(events: &[WireEvent], key: i16) -> Vec<&WireEvent> {
    events.iter().filter(|event| {
        matches!(&event.kind, WireEventKind::Request { api_key, .. } if *api_key == key)
    }).collect()
}

async fn producer(mock: &Mock, sasl: bool) -> Producer {
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.connections = 1;
    cfg.connections_max_idle = Duration::from_secs(30);
    cfg.request_timeout = Duration::from_secs(4);
    cfg.delivery_timeout = Duration::from_secs(4);
    if sasl {
        cfg.sasl_plain = Some(("trace-user".into(), "trace-password".into()));
    }
    Producer::new(cfg).await.unwrap()
}

async fn gated_first_send(sasl: bool) {
    let mock = Mock::start().await;
    mock.set_topic_partitions("t", 1);
    if sasl {
        mock.require_plain_sasl("trace-user", "trace-password");
    }
    mock.enable_wire_trace(512);
    let producer = producer(&mock, sasl).await;
    let before = mock.wire_trace();
    let bootstrap_socket = requests(&before, API_VERSIONS)[0].socket;
    let marker = before.last().map_or(0, |event| event.sequence + 1);
    let gate = mock.hold_api_versions_responses();
    let sender = producer.clone();
    let sent = tokio::spawn(async move {
        sender.send(ProduceRecord::to("t").value(&b"first"[..])).await
    });
    common::wait_pred("new Produce socket ApiVersions request", || {
        requests(&mock.wire_trace(), API_VERSIONS).iter().any(|event| event.sequence >= marker)
    }).await;
    let held = mock.wire_trace();
    let opened = requests(&held, API_VERSIONS).into_iter()
        .find(|event| event.sequence >= marker).unwrap();
    assert_ne!(opened.socket, bootstrap_socket, "the handshake belongs to a new physical socket");
    assert!(!sent.is_finished(), "send completed while the new handshake response was held");
    assert!(requests(&held, PRODUCE).is_empty(), "a Produce frame arrived before negotiation finished");
    let metadata = requests(&held, METADATA);
    assert!(metadata.iter().any(|event| event.socket == bootstrap_socket), "metadata used the bootstrap session");
    let worker_socket = opened.socket;
    gate.close();
    let first = tokio::time::timeout(Duration::from_secs(4), sent).await.unwrap().unwrap().unwrap();
    let second = producer.send(ProduceRecord::to("t").value(&b"second"[..])).await.unwrap();
    assert_eq!((first.partition, first.offset), (0, 0));
    assert_eq!((second.partition, second.offset), (0, 1));
    assert_eq!(mock.log_len("t", 0), 2);
    let after = mock.wire_trace();
    let app = requests(&after, PRODUCE);
    assert_eq!(app.len(), 2);
    assert!(app.iter().all(|event| event.socket == worker_socket));
    let new_versions: Vec<_> = requests(&after, API_VERSIONS).into_iter()
        .filter(|event| event.sequence >= marker).collect();
    assert_eq!(new_versions.len(), 1, "the warm second send should reuse the worker socket");
    if sasl {
        for socket in [bootstrap_socket, worker_socket] {
            assert!(requests(&after, SASL_HANDSHAKE).iter().any(|event| event.socket == socket));
            assert!(requests(&after, SASL_AUTHENTICATE).iter().any(|event| event.socket == socket));
        }
    }
    println!("PL_SOCKET_CONTROL sasl={} bootstrap_socket={} worker_socket={} metadata_requests={} produce_requests={}",
        sasl, bootstrap_socket, worker_socket, metadata.len(), app.len());
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cold_send_waits_for_a_distinct_negotiated_socket() {
    gated_first_send(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cold_send_waits_for_a_distinct_authenticated_socket() {
    gated_first_send(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cold_send_obeys_new_socket_advertisement() {
    let mock = Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.enable_wire_trace(512);
    let producer = producer(&mock, false).await;
    let bootstrap = mock.wire_trace();
    let initial = bootstrap.iter().find_map(|event| match &event.kind {
        WireEventKind::ApiVersionsResponse { error_code: 0, ranges } =>
            ranges.iter().find(|entry| entry.api_key == PRODUCE).map(|entry| entry.max_version),
        _ => None,
    }).expect("bootstrap Produce version");
    assert!(initial > 3, "bootstrap must advertise a later Produce version");
    // Model a peer changing advertised versions between the original session
    // and a fresh session at the same address. This is not a real broker rollout.
    mock.set_api_max(PRODUCE, 3);
    let first = producer.send(ProduceRecord::to("t").value(&b"version"[..])).await.unwrap();
    assert_eq!(first.offset, 0);
    let after = mock.wire_trace();
    let produces = requests(&after, PRODUCE);
    assert_eq!(produces.len(), 1);
    let worker = produces[0].socket;
    let used = match &produces[0].kind {
        WireEventKind::Request { api_version, .. } => *api_version,
        _ => unreachable!(),
    };
    let advertised = after.iter().find_map(|event| {
        if event.socket != worker { return None; }
        match &event.kind {
            WireEventKind::ApiVersionsResponse { error_code: 0, ranges } =>
                ranges.iter().find(|entry| entry.api_key == PRODUCE).map(|entry| entry.max_version),
            _ => None,
        }
    }).expect("worker Produce version");
    assert_eq!(advertised, 3);
    assert_eq!(used, 3);
    println!("PL_SOCKET_VERSION bootstrap_max={} worker_max={} produce_version={}", initial, advertised, used);
    producer.close().await.unwrap();
}

async fn sample(pair: usize, delay_ms: u64) {
    let mock = Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.enable_wire_trace(512);
    let producer = producer(&mock, false).await;
    let before = mock.wire_trace();
    let bootstrap = requests(&before, API_VERSIONS)[0].socket;
    let marker = before.last().map_or(0, |event| event.sequence + 1);
    mock.set_api_versions_delay(Duration::from_millis(delay_ms));
    let start = Instant::now();
    let first = producer.send(ProduceRecord::to("t").value(&b"first"[..])).await.unwrap();
    let first_ns = start.elapsed().as_nanos();
    let start = Instant::now();
    let second = producer.send(ProduceRecord::to("t").value(&b"second"[..])).await.unwrap();
    let warm_ns = start.elapsed().as_nanos();
    assert_eq!((first.offset, second.offset), (0, 1));
    assert_eq!(mock.log_len("t", 0), 2);
    let trace = mock.wire_trace();
    let versions: Vec<_> = requests(&trace, API_VERSIONS).into_iter()
        .filter(|event| event.sequence >= marker).collect();
    let produces = requests(&trace, PRODUCE);
    assert_eq!(versions.len(), 1);
    assert_eq!(produces.len(), 2);
    assert_ne!(versions[0].socket, bootstrap);
    assert!(produces.iter().all(|event| event.socket == versions[0].socket));
    println!("PL_SOCKET_SAMPLE pair={} delay_ms={} first_ns={} warm_ns={} bootstrap_socket={} worker_socket={}",
        pair, delay_ms, first_ns, warm_ns, bootstrap, versions[0].socket);
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "qualification-only injected-delay diagnostic"]
async fn paired_injected_handshake_delay() {
    for pair in 0..12 {
        let order = if pair % 2 == 0 { [0, 50] } else { [50, 0] };
        for delay_ms in order {
            sample(pair, delay_ms).await;
        }
    }
}
