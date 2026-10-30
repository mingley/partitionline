//! Tests for reusing a newly negotiated, authenticated bootstrap lookup socket.
mod common;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use partitionline::protocol::api_keys::{
    API_VERSIONS, FIND_COORDINATOR, OFFSET_COMMIT, SASL_AUTHENTICATE, SASL_HANDSHAKE,
};
use partitionline::protocol::group::decode_offset_commit_request;
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use partitionline::{ConsumerConfig, ConsumerGroup, TopicPartition};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Debug)]
struct Event {
    node: i32,
    socket: u64,
    api: i16,
    version: i16,
    body: Bytes,
}

/// A small SASL/PLAIN terminator. Each client socket owns one upstream socket;
/// non-SASL Kafka frames are forwarded unchanged to the unauthenticated mock.
struct AuthProxy {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for AuthProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn frame(socket: &mut TcpStream) -> Option<Vec<u8>> {
    let len = socket.read_i32().await.ok()?;
    if !(8..=8 * 1024 * 1024).contains(&len) {
        return None;
    }
    let mut frame = vec![0; usize::try_from(len).ok()?];
    let _read = socket.read_exact(&mut frame).await.ok()?;
    Some(frame)
}

async fn write_frame(socket: &mut TcpStream, bytes: &[u8]) -> bool {
    let Ok(len) = i32::try_from(bytes.len()) else {
        return false;
    };
    socket.write_i32(len).await.is_ok() && socket.write_all(bytes).await.is_ok()
}

async fn start_proxy(
    node: i32,
    upstream: String,
    events: Arc<Mutex<Vec<Event>>>,
) -> std::io::Result<AuthProxy> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let next = Arc::new(AtomicU64::new(1));
    let task = tokio::spawn(async move {
        let mut workers = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut client, _)) = accepted else { break; };
                    let upstream = upstream.clone();
                    let events = Arc::clone(&events);
                    let socket = next.fetch_add(1, Ordering::Relaxed);
                    let _worker = workers.spawn(async move {
                        let Ok(mut broker) = TcpStream::connect(upstream).await else { return; };
                        let mut negotiated = false;
                        let mut mechanism = false;
                        let mut authenticated = false;
                        while let Some(raw) = frame(&mut client).await {
                            let mut request = Bytes::copy_from_slice(&raw);
                            let Ok(header) = decode_request_header(&mut request) else { return; };
                            let api = header.api_key();
                            let version = header.api_version();
                            events.lock().push(Event {
                                node, socket, api, version,
                                body: if api == OFFSET_COMMIT { request.clone() } else { Bytes::new() }
                            });
                            if api == API_VERSIONS {
                                if negotiated || authenticated { return; }
                                negotiated = true;
                            } else if api == SASL_HANDSHAKE {
                                if !negotiated || authenticated { return; }
                                let Ok(name) = decode_sasl_handshake_request(&mut request, version) else { return; };
                                if name != "PLAIN" { return; }
                                mechanism = true;
                                let mut response = BytesMut::new();
                                if encode_response_header(&mut response, api, version, header.correlation_id()).is_err()
                                    || encode_sasl_handshake_response(&mut response, version, 0, &["PLAIN"]).is_err()
                                { return; }
                                if !write_frame(&mut client, &response).await { return; }
                                continue;
                            } else if api == SASL_AUTHENTICATE {
                                if !mechanism || authenticated { return; }
                                let Ok(auth) = decode_sasl_authenticate_request(&mut request, version) else { return; };
                                if parse_plain_auth_bytes(&auth) != Some(("user".into(), "pass".into())) { return; }
                                authenticated = true;
                                let mut response = BytesMut::new();
                                if encode_response_header(&mut response, api, version, header.correlation_id()).is_err()
                                    || encode_sasl_authenticate_response(&mut response, version, 0, None, &[], 0).is_err()
                                { return; }
                                if !write_frame(&mut client, &response).await { return; }
                                continue;
                            } else if !authenticated {
                                return;
                            }
                            if !write_frame(&mut broker, &raw).await { return; }
                            let Some(response) = frame(&mut broker).await else { return; };
                            if !write_frame(&mut client, &response).await { return; }
                        }
                    });
                }
                _joined = workers.join_next(), if !workers.is_empty() => {}
            }
        }
    });
    Ok(AuthProxy { addr, task })
}

async fn fixture() -> Result<
    (
        common::Mock,
        AuthProxy,
        AuthProxy,
        Arc<Mutex<Vec<Event>>>,
        ConsumerGroup,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mock = common::Mock::start_two_node().await;
    let real1 = mock.addr.clone();
    let real2 = mock
        .broker_addr(2)
        .ok_or_else(|| std::io::Error::other("mock broker 2 address missing"))?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let proxy1 = start_proxy(1, real1, Arc::clone(&events)).await?;
    let proxy2 = start_proxy(2, real2, Arc::clone(&events)).await?;
    for (node, proxy) in [(1, &proxy1), (2, &proxy2)] {
        let port = proxy.addr.port();
        mock.set_broker_address(node, "127.0.0.1", i32::from(port));
    }
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_node_api_max(1, OFFSET_COMMIT, 9);
    mock.set_node_api_max(2, OFFSET_COMMIT, 9);
    mock.set_coordinator(2);
    let mut cfg = ConsumerConfig::bootstrap([proxy1.addr.to_string()]).max_wait_ms(10);
    cfg.sasl_plain = Some(("user".into(), "pass".into()));
    cfg.heartbeat_interval = Duration::from_secs(5);
    let group = ConsumerGroup::join(cfg, "lookup-auth", "t").await?;
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("lookup-auth") >= 1
    })
    .await;
    Ok((mock, proxy1, proxy2, events, group))
}

fn lookup_socket(events: &[Event], since: usize) -> Option<u64> {
    let recent = events.get(since..)?;
    let find = recent
        .iter()
        .find(|event| event.node == 1 && event.api == FIND_COORDINATOR)?;
    let socket = find.socket;
    let apis: Vec<i16> = recent
        .iter()
        .filter(|event| event.node == 1 && event.socket == socket)
        .map(|event| event.api)
        .collect();
    assert!(
        apis.starts_with(&[
            API_VERSIONS,
            SASL_HANDSHAKE,
            SASL_AUTHENTICATE,
            FIND_COORDINATOR
        ]),
        "lookup socket ordering: {apis:?}"
    );
    Some(socket)
}

#[tokio::test]
async fn returned_bootstrap_lookup_keeps_its_authenticated_socket_for_offset_commit() {
    let (mock, _proxy1, _proxy2, events, mut group) = fixture().await.unwrap();
    group
        .commit_offsets([(TopicPartition::new("t", 0), 1)])
        .await
        .unwrap();
    assert_eq!(mock.last_offset_commit_node(), Some(2));
    let since = events.lock().len();
    mock.set_coordinator(1);
    group
        .commit_offsets([(TopicPartition::new("t", 0), 2)])
        .await
        .unwrap();
    let observed = events.lock().clone();
    let socket = lookup_socket(&observed, since)
        .expect("bootstrap FindCoordinator must be observed after authentication");
    assert!(
        observed[since..]
            .iter()
            .any(|event| event.node == 1 && event.socket == socket && event.api == OFFSET_COMMIT),
        "OffsetCommit must use the actual authenticated lookup socket: {:?}",
        &observed[since..]
    );
    assert_eq!(mock.last_offset_commit_node(), Some(1));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn returned_bootstrap_reencodes_offset_commit_after_broker_downgrade() {
    let (mock, _proxy1, _proxy2, events, mut group) = fixture().await.unwrap();
    group
        .commit_offsets([(TopicPartition::new("t", 0), 1)])
        .await
        .unwrap();
    assert_eq!(mock.last_offset_commit_version(), Some(9));
    let member = mock.last_offset_commit_member().unwrap();
    let generation = mock.last_offset_commit_generation().unwrap();
    let since = events.lock().len();
    mock.set_node_api_max(1, OFFSET_COMMIT, 8);
    mock.set_coordinator(1);
    group
        .commit_offsets([(TopicPartition::new("t", 0), 2)])
        .await
        .unwrap();
    let observed = events.lock().clone();
    let socket = lookup_socket(&observed, since)
        .expect("bootstrap FindCoordinator must be observed after authentication");
    assert!(
        !observed[since..]
            .iter()
            .any(|event| event.node == 1 && event.api == OFFSET_COMMIT && event.version != 8),
        "only the newly negotiated version may reach the downgraded broker: {:?}",
        &observed[since..]
    );
    assert!(observed[since..].iter().any(|event| event.node == 1
        && event.socket == socket
        && event.api == API_VERSIONS
        && event.version == 4));
    let commits: Vec<_> = observed[since..]
        .iter()
        .filter(|event| event.node == 1 && event.api == OFFSET_COMMIT)
        .collect();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].socket, socket);
    let mut body = commits[0].body.clone();
    let (group_id, actual_member, topics, _, instance, actual_generation) =
        decode_offset_commit_request(&mut body, 8).unwrap();
    assert!(body.is_empty());
    assert_eq!(group_id, "lookup-auth");
    assert_eq!(actual_member, member);
    assert_eq!(actual_generation, generation);
    assert_eq!(instance, None);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic, "t");
    assert_eq!(topics[0].partitions.len(), 1);
    assert_eq!(topics[0].partitions[0].partition, 0);
    assert_eq!(topics[0].partitions[0].offset, 2);
    assert_eq!(mock.last_offset_commit_version(), Some(8));
    assert_eq!(mock.last_offset_commit_node(), Some(1));
    group.leave().await.unwrap();
}
