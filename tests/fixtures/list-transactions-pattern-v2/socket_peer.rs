//! Independently scripted, bounded two-broker API66 peer.
//!
//! Targeted bodies are encoded from the pinned Apache schemas, without the
//! partitionline ListTransactions/Metadata encoders. The authored bodies are
//! not relabeled as executed Apache vectors. The Java oracle independently
//! serializes these same cases after an explicitly authorized compilation.
#![allow(
    dead_code,
    reason = "old-main target uses only the existing-API undercount subset"
)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::{Admin, AdminConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};

pub(crate) const BUDGET: Duration = Duration::from_secs(2);
const REQUEST_LIMIT: usize = 256 * 1024;
const CAPTURE_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const RESPONSE_LIMIT: usize = 17 * 1024 * 1024;
const CAPTURE_RESPONSE_BYTES: usize = 24 * 1024 * 1024;
const CAPTURE_ROWS: usize = 192;
const MAX_CONNECTIONS_PER_LISTENER: usize = 16;
const MAX_FRAMES_PER_CONNECTION: usize = 32;
const MAX_QUEUED_REPLIES_PER_BROKER: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Listing {
    pub(crate) id: String,
    pub(crate) pid: i64,
    pub(crate) state: String,
}

pub(crate) fn listing(id: &str, pid: i64, state: &str) -> Listing {
    Listing {
        id: id.into(),
        pid,
        state: state.into(),
    }
}

fn unsigned(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let low = u8::try_from(value & 127).unwrap();
        value >>= 7;
        out.push(low | if value == 0 { 0 } else { 128 });
        if value == 0 {
            break;
        }
    }
}

fn compact(out: &mut Vec<u8>, value: &str) {
    unsigned(out, u32::try_from(value.len() + 1).unwrap());
    out.extend_from_slice(value.as_bytes());
}

fn classic(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&i16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

/// Full API66 v0/v1 response body; both versions have this exact same layout.
pub(crate) fn response(code: i16, listings: &[Listing]) -> Arc<[u8]> {
    let mut out = Vec::new();
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&code.to_be_bytes());
    out.push(1); // compact empty UnknownStateFilters
    unsigned(&mut out, u32::try_from(listings.len() + 1).unwrap());
    for row in listings {
        compact(&mut out, &row.id);
        out.extend_from_slice(&row.pid.to_be_bytes());
        compact(&mut out, &row.state);
        out.push(0); // listing tagged fields
    }
    out.push(0); // root tagged fields
    assert!(out.len() <= RESPONSE_LIMIT);
    out.into()
}

/// Compact count is malicious but the actual body remains eleven bytes.
pub(crate) fn huge_count_response() -> Arc<[u8]> {
    let mut out = vec![0, 0, 0, 0, 0, 0, 1];
    unsigned(&mut out, 100_002);
    out.push(0);
    out.into()
}

pub(crate) fn trailing_response() -> Arc<[u8]> {
    let mut out = response(0, &[]).to_vec();
    out.push(0x7e);
    out.into()
}

pub(crate) fn required_null_responses() -> Vec<Arc<[u8]>> {
    let base = [0u8, 0, 0, 0, 0, 0];
    let mut values = Vec::new();
    // UnknownStateFilters null, TransactionStates null, and a required unknown
    // string null are distinct schema violations with tiny finite frames.
    for tail in [
        &[0, 1, 0][..],
        &[1, 0, 0][..],
        &[2, 0, 1, 0][..],
        &[1, 128][..],
    ] {
        let mut out = base.to_vec();
        out.extend_from_slice(tail);
        values.push(out.into());
    }
    for null_id in [true, false] {
        let mut out = base.to_vec();
        out.extend_from_slice(&[1, 2]);
        if null_id {
            out.push(0);
        } else {
            compact(&mut out, "tx");
        }
        out.extend_from_slice(&41i64.to_be_bytes());
        if null_id {
            compact(&mut out, "Ongoing");
        } else {
            out.push(0);
        }
        out.extend_from_slice(&[0, 0]);
        values.push(out.into());
    }
    values
}

pub(crate) fn oversized_response() -> Arc<[u8]> {
    // Tests the operation's16MiB envelope before decoding, within the existing
    // transport100MiB cap. This one finite fixture is not a heap-peak benchmark.
    vec![0; 16 * 1024 * 1024 + 1].into()
}

pub(crate) fn large_unknown_filters_response() -> Arc<[u8]> {
    let mut out = vec![0; 6];
    unsigned(&mut out, 4097);
    let text = "x".repeat(2300);
    for _ in 0..4096 {
        compact(&mut out, &text);
    }
    out.extend_from_slice(&[1, 0]); // empty states, root tags
    assert!((8 * 1024 * 1024 + 1..10 * 1024 * 1024).contains(&out.len()));
    out.into()
}

#[derive(Clone, Debug)]
pub(crate) enum Reply {
    Body(Arc<[u8]>),
    Delay(Duration, Arc<[u8]>),
    Stall,
    Disconnect,
}

#[derive(Clone, Debug)]
pub(crate) struct Observed {
    pub(crate) listener_slot: usize,
    pub(crate) node: i32,
    pub(crate) api_key: i16,
    pub(crate) api_version: i16,
    pub(crate) correlation_id: i32,
    pub(crate) received_at: Instant,
    pub(crate) request_frame: Arc<[u8]>, // actual four-byte prefix+header+body
    pub(crate) request_body: Arc<[u8]>,
    pub(crate) response_frame: Option<Arc<[u8]>>,
    pub(crate) response_written: bool,
}

pub(crate) struct State {
    pub(crate) sasl: bool,
    pub(crate) observed: Vec<Observed>,
    pub(crate) replies: [VecDeque<Reply>; 2],
    pub(crate) ranges: [Option<(i16, i16)>; 2],
    pub(crate) metadata_delay: Duration,
    pub(crate) metadata_override: Option<Arc<[u8]>>,
    pub(crate) move_second_on_disconnect: bool,
    pub(crate) downgrade_second_on_disconnect: bool,
    moved_second: bool,
    request_bytes: usize,
    response_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct Closed {
    pub(crate) observations: Vec<Observed>,
    pub(crate) listener_tasks_joined: usize,
    pub(crate) connection_tasks_joined: usize,
    pub(crate) shutdown_failures: Vec<String>,
}

pub(crate) struct Peer {
    pub(crate) bootstrap: String,
    pub(crate) state: Arc<Mutex<State>>,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<(usize, Vec<String>)>>,
    addresses: [SocketAddr; 3],
}

fn api_versions(
    ranges: Option<(i16, i16)>,
    allow_old: bool,
    error_code: i16,
    sasl: bool,
) -> Vec<u8> {
    let metadata_version = if allow_old { 4 } else { 1 };
    let mut keys = vec![
        (3i16, metadata_version, metadata_version),
        (18, 0, 0),
        (19, 0, 0),
        (20, 0, 0),
    ];
    if allow_old {
        keys.push((10, 6, 6));
    }
    if sasl {
        keys.extend([(17, 1, 1), (36, 1, 1)]);
    }
    if let Some((min, max)) = ranges {
        keys.push((66, min, max));
    }
    let mut out = Vec::new();
    // KIP-511 unsupported modern requests use this same v0 response schema.
    out.extend_from_slice(&error_code.to_be_bytes());
    out.extend_from_slice(&i32::try_from(keys.len()).unwrap().to_be_bytes());
    for (key, min, max) in keys {
        out.extend_from_slice(&key.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
    }
    out
}

fn metadata(addresses: &[SocketAddr; 3], moved: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&2i32.to_be_bytes());
    for (node, slot) in [(1i32, 0usize), (2, if moved { 2 } else { 1 })] {
        out.extend_from_slice(&node.to_be_bytes());
        classic(&mut out, "127.0.0.1");
        out.extend_from_slice(&i32::from(addresses[slot].port()).to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes()); // nullable rack
    }
    out.extend_from_slice(&1i32.to_be_bytes()); // controller
    out.extend_from_slice(&0i32.to_be_bytes()); // no topic metadata
    out
}

pub(crate) fn invalid_metadata_count(count: i32) -> Arc<[u8]> {
    count.to_be_bytes().to_vec().into()
}

pub(crate) fn hostile_metadata_bodies() -> Vec<Arc<[u8]>> {
    let addresses = [
        "127.0.0.1:19091".parse().unwrap(),
        "127.0.0.1:19092".parse().unwrap(),
        "127.0.0.1:19093".parse().unwrap(),
    ];
    let baseline = metadata(&addresses, false);
    let mut out = Vec::new();
    for count in [-1i32, 1, i32::MAX] {
        let mut body = baseline.clone();
        let at = body.len() - 4;
        body[at..].copy_from_slice(&count.to_be_bytes());
        out.push(body.into());
    }
    // Nearly1MiB with a valid broker prefix and huge unexpected topic count
    // exercises the borrowed preflight, not merely the outer byte ceiling.
    let mut huge_topics = baseline.clone();
    let at = huge_topics.len() - 4;
    huge_topics[at..].copy_from_slice(&i32::MAX.to_be_bytes());
    huge_topics.resize(1024 * 1024 - 1, 0);
    out.push(huge_topics.into());
    out.push(vec![0, 0].into()); // incomplete required broker-array count
    let mut truncated_topics = baseline.clone();
    let _removed_byte = truncated_topics.pop(); // incomplete required topic-array count
    out.push(truncated_topics.into());
    let mut trailing = baseline.clone();
    trailing.push(0x7e);
    out.push(trailing.into());
    let mut duplicate = baseline;
    // first broker count(4)+id(4)+classic host(11)+port(4)+rack(2)
    duplicate[25..29].copy_from_slice(&1i32.to_be_bytes());
    out.push(duplicate.into());
    let mut null_host = 1i32.to_be_bytes().to_vec();
    null_host.extend_from_slice(&1i32.to_be_bytes());
    null_host.extend_from_slice(&(-1i16).to_be_bytes());
    null_host.extend_from_slice(&19091i32.to_be_bytes());
    null_host.extend_from_slice(&(-1i16).to_be_bytes());
    null_host.extend_from_slice(&1i32.to_be_bytes());
    null_host.extend_from_slice(&0i32.to_be_bytes());
    out.push(null_host.into());
    // Tiny valid broker prefix+nonzero enormous topic count must be rejected
    // before the ordinary decoder could reserve a topic Vec.
    // A second distinct case exceeds Metadata's1MiB received-body budget.
    out.push(vec![0; 1024 * 1024 + 1].into());
    out
}

fn old_coordinator(address: SocketAddr) -> Vec<u8> {
    let mut out = vec![0; 4]; // throttle
    out.push(2); // one compact coordinator
    compact(&mut out, "");
    out.extend_from_slice(&1i32.to_be_bytes());
    compact(&mut out, "127.0.0.1");
    out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    out.push(0); // nullable error message
    out.extend_from_slice(&[0, 0]); // coordinator/root tags
    out
}

fn take_uvar(bytes: &mut &[u8]) -> u32 {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        assert!(!bytes.is_empty());
        let byte = bytes[0];
        *bytes = &bytes[1..];
        assert!(shift != 28 || byte < 16, "bounded32bit varint");
        value |= u32::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
    }
    panic!("unterminated varint");
}

fn take_compact(bytes: &mut &[u8]) -> String {
    let encoded = take_uvar(bytes);
    assert!(encoded > 0, "nonnullable compact string");
    let n = usize::try_from(encoded - 1).unwrap();
    assert!(n <= bytes.len());
    let value = std::str::from_utf8(&bytes[..n]).unwrap().to_owned();
    *bytes = &bytes[n..];
    value
}

pub(crate) fn filters(mut bytes: &[u8], version: i16) -> (Vec<String>, Vec<i64>, i64) {
    assert!((0..=2).contains(&version));
    let count = take_uvar(&mut bytes);
    assert!((1..=8193).contains(&count));
    let states = (0..count - 1).map(|_| take_compact(&mut bytes)).collect();
    let count = take_uvar(&mut bytes);
    assert!((1..=8193).contains(&count));
    let pids = (0..count - 1)
        .map(|_| {
            assert!(bytes.len() >= 8);
            let value = i64::from_be_bytes(bytes[..8].try_into().unwrap());
            bytes = &bytes[8..];
            value
        })
        .collect();
    let duration = if version >= 1 {
        assert!(bytes.len() >= 8);
        let value = i64::from_be_bytes(bytes[..8].try_into().unwrap());
        bytes = &bytes[8..];
        value
    } else {
        -1
    };
    if version >= 2 {
        let count = take_uvar(&mut bytes);
        if count > 0 {
            let length = usize::try_from(count - 1).unwrap();
            assert!(bytes.len() >= length);
            bytes = &bytes[length..];
        }
    }
    assert_eq!(bytes, [0]);
    (states, pids, duration)
}

async fn connection(
    mut socket: TcpStream,
    slot: usize,
    addresses: [SocketAddr; 3],
    state: Arc<Mutex<State>>,
    mut stop: watch::Receiver<bool>,
    allow_old: bool,
) {
    let node = if slot == 0 { 1 } else { 2 };
    let mut authenticated = false;
    for _ in 0..MAX_FRAMES_PER_CONNECTION {
        if *stop.borrow() {
            break;
        }
        let length = tokio::select! {
            biased;
            _ = stop.changed() => break,
            n = socket.read_i32() => match n { Ok(n) => n, Err(_) => break },
        };
        assert!((10..=i32::try_from(REQUEST_LIMIT).unwrap()).contains(&length));
        let mut frame = vec![0u8; usize::try_from(length).unwrap() + 4];
        frame[..4].copy_from_slice(&length.to_be_bytes());
        let read = tokio::select! {
            biased;
            _ = stop.changed() => break,
            value = socket.read_exact(&mut frame[4..]) => value,
        };
        if read.is_err() {
            break;
        }
        let api_key = i16::from_be_bytes(frame[4..6].try_into().unwrap());
        let version = i16::from_be_bytes(frame[6..8].try_into().unwrap());
        let correlation = i32::from_be_bytes(frame[8..12].try_into().unwrap());
        let client = i16::from_be_bytes(frame[12..14].try_into().unwrap());
        assert!(client >= -1);
        let mut body_at = 14 + usize::try_from(client.max(0)).unwrap();
        assert!(body_at <= frame.len());
        if matches!(api_key, 10 | 66) || (api_key == 18 && version >= 3) {
            assert_eq!(frame.get(body_at), Some(&0), "request header2 empty tags");
            body_at += 1;
        }
        let body: Arc<[u8]> = frame[body_at..].into();
        let (index, reply, delay) = {
            let mut s = state.lock().await;
            assert!(s.observed.len() < CAPTURE_ROWS);
            s.request_bytes += frame.len();
            assert!(s.request_bytes <= CAPTURE_REQUEST_BYTES);
            let index = s.observed.len();
            s.observed.push(Observed {
                listener_slot: slot,
                node,
                api_key,
                api_version: version,
                correlation_id: correlation,
                received_at: Instant::now(),
                request_frame: frame.into(),
                request_body: Arc::clone(&body),
                response_frame: None,
                response_written: false,
            });
            let reply = match api_key {
                18 => {
                    let error_code = match version {
                        0 => {
                            assert!(body.is_empty(), "legacy ApiVersions0 empty body");
                            0
                        }
                        4 => {
                            // The current Rust/Java clients probe maxv4 using header2.
                            // This independently encoded legacy peer supports onlyv0,
                            // so KIP-511 requires a v0 UNSUPPORTED_VERSION response.
                            let mut request = body.as_ref();
                            assert!(!take_compact(&mut request).is_empty());
                            assert!(!take_compact(&mut request).is_empty());
                            assert_eq!(request, [0], "ApiVersions4 root empty tags");
                            35
                        }
                        other => panic!("unexpected ApiVersions probe version{other}"),
                    };
                    Reply::Body(
                        api_versions(
                            s.ranges[usize::try_from(node - 1).unwrap()],
                            allow_old,
                            error_code,
                            s.sasl,
                        )
                        .into(),
                    )
                }
                17 => {
                    assert!(s.sasl);
                    assert_eq!(version, 1);
                    assert_eq!(body.as_ref(), b"\0\x05PLAIN");
                    let mut out = vec![0, 0, 0, 0, 0, 1];
                    classic(&mut out, "PLAIN");
                    Reply::Body(out.into())
                }
                36 => {
                    assert!(s.sasl);
                    assert_eq!(version, 1);
                    let expected = b"\0qualification-user\0qualification-pass";
                    assert_eq!(
                        i32::from_be_bytes(body[..4].try_into().unwrap()),
                        i32::try_from(expected.len()).unwrap()
                    );
                    assert_eq!(&body[4..], expected);
                    authenticated = true;
                    let mut out = vec![0, 0, 255, 255, 0, 0, 0, 0];
                    out.extend_from_slice(&0i64.to_be_bytes());
                    Reply::Body(out.into())
                }
                3 => {
                    assert!(!s.sasl || authenticated, "Metadata before authentication");
                    if allow_old {
                        assert_eq!(version, 4);
                        assert!(
                            body.as_ref() == [0, 0, 0, 0, 0]
                                || body.as_ref() == [255, 255, 255, 255, 0],
                            "Metadata4 empty/all topic selection with auto-create disabled"
                        );
                    } else {
                        assert_eq!(version, 1);
                        assert_eq!(
                            body.as_ref(),
                            [0, 0, 0, 0],
                            "Metadata1 empty topic selection"
                        );
                    }
                    let mut bytes = metadata(&addresses, s.moved_second);
                    if allow_old {
                        // v4 adds throttle and a nullable cluster ID. Topics
                        // are empty, so no per-topic fields are present.
                        let controller = bytes.len() - 8;
                        drop(bytes.splice(controller..controller, [255, 255]));
                        drop(bytes.splice(0..0, 0i32.to_be_bytes()));
                    }
                    Reply::Body(s.metadata_override.clone().unwrap_or_else(|| bytes.into()))
                }
                66 => {
                    assert!(
                        !s.sasl || authenticated,
                        "ListTransactions before authentication"
                    );
                    let _checked = filters(&body, version);
                    s.replies[usize::try_from(node - 1).unwrap()]
                        .pop_front()
                        .unwrap_or(Reply::Stall)
                }
                10 if allow_old => {
                    assert_eq!(version, 6);
                    assert_eq!(
                        body.as_ref(),
                        [1, 2, 1, 0],
                        "only old transaction-type empty coordinator lookup is allowed"
                    );
                    Reply::Body(old_coordinator(addresses[0]).into())
                }
                _ => panic!("unexpected API{api_key}v{version}; unrelated APIs are omitted"),
            };
            let delay = if api_key == 3 {
                s.metadata_delay
            } else {
                Duration::ZERO
            };
            (index, reply, delay)
        };
        let (wait, response) = match reply {
            Reply::Body(bytes) => (delay, bytes),
            Reply::Delay(wait, bytes) => (wait + delay, bytes),
            Reply::Stall => {
                if !*stop.borrow() {
                    let _stopping = stop.changed().await;
                }
                break;
            }
            Reply::Disconnect => {
                let mut s = state.lock().await;
                if slot == 1 && s.downgrade_second_on_disconnect {
                    s.ranges[1] = Some((1, 1));
                    s.downgrade_second_on_disconnect = false;
                }
                if slot == 1 && s.move_second_on_disconnect {
                    s.moved_second = true;
                }
                break;
            }
        };
        if !wait.is_zero() {
            tokio::select! { biased; _ = stop.changed() => break, () = tokio::time::sleep(wait) => {} }
        }
        assert!(response.len() <= RESPONSE_LIMIT);
        let mut payload = Vec::with_capacity(response.len() + 9);
        let header_tags = usize::from(matches!(api_key, 10 | 66));
        let response_length = i32::try_from(response.len() + 4 + header_tags).unwrap();
        payload.extend_from_slice(&response_length.to_be_bytes());
        payload.extend_from_slice(&correlation.to_be_bytes());
        if header_tags == 1 {
            payload.push(0);
        }
        payload.extend_from_slice(&response);
        let payload: Arc<[u8]> = payload.into();
        {
            let mut s = state.lock().await;
            s.response_bytes += payload.len();
            assert!(s.response_bytes <= CAPTURE_RESPONSE_BYTES);
            s.observed[index].response_frame = Some(Arc::clone(&payload));
        }
        let written = tokio::select! {
            biased;
            _ = stop.changed() => break,
            value = socket.write_all(&payload) => value,
        };
        if written.is_err() {
            break;
        }
        state.lock().await.observed[index].response_written = true;
    }
    let _socket_shutdown = socket.shutdown().await;
}

impl Peer {
    pub(crate) async fn start(ranges: [Option<(i16, i16)>; 2], allow_old: bool) -> Self {
        // Third socket is a replacement endpoint for broker2, not a third broker.
        let listeners = [
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let addresses = std::array::from_fn(|i| listeners[i].local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            sasl: false,
            observed: Vec::new(),
            replies: std::array::from_fn(|_| VecDeque::new()),
            ranges,
            metadata_delay: Duration::ZERO,
            metadata_override: None,
            move_second_on_disconnect: false,
            downgrade_second_on_disconnect: false,
            moved_second: false,
            request_bytes: 0,
            response_bytes: 0,
        }));
        let (stop, receiver) = watch::channel(false);
        let tasks = listeners.into_iter().enumerate().map(|(slot, listener)| {
            let mut receiver = receiver.clone(); let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut connections = JoinSet::new(); let mut accepted = 0;
                let mut joined = 0; let mut errors = Vec::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = receiver.changed() => break,
                        value = connections.join_next(), if !connections.is_empty() => {
                            if let Some(value) = value { joined += 1; if let Err(e) = value { errors.push(e.to_string()); } }
                        }
                        value = listener.accept() => {
                            let (socket, _) = value.unwrap(); accepted += 1;
                            assert!(accepted <= MAX_CONNECTIONS_PER_LISTENER);
                            let _worker = connections.spawn(connection(socket, slot, addresses, Arc::clone(&state), receiver.clone(), allow_old));
                        }
                    }
                }
                while let Some(value) = connections.join_next().await {
                    joined += 1; if let Err(e) = value { errors.push(e.to_string()); }
                }
                (joined, errors)
            })
        }).collect();
        Self {
            bootstrap: addresses[0].to_string(),
            state,
            stop,
            tasks,
            addresses,
        }
    }

    pub(crate) async fn admin(&self) -> partitionline::Result<Admin> {
        let mut config = AdminConfig::bootstrap([self.bootstrap.clone()])
            .connect_timeout(BUDGET)
            .request_timeout(BUDGET)
            .retry_backoff(Duration::from_millis(2))
            .retry_backoff_max(Duration::from_millis(4))
            .reconnect_backoff(Duration::ZERO)
            .reconnect_backoff_max(Duration::ZERO);
        if self.state.lock().await.sasl {
            config.sasl_plain = Some(("qualification-user".into(), "qualification-pass".into()));
        }
        Admin::new(config).await
    }

    pub(crate) async fn script(&self, node: i32, replies: impl IntoIterator<Item = Reply>) {
        assert!((1..=2).contains(&node));
        let mut state = self.state.lock().await;
        for reply in replies {
            assert!(
                state.replies[usize::try_from(node - 1).unwrap()].len()
                    < MAX_QUEUED_REPLIES_PER_BROKER
            );
            state.replies[usize::try_from(node - 1).unwrap()].push_back(reply);
            let queued_bytes: usize = state
                .replies
                .iter()
                .flat_map(|replies| replies.iter())
                .map(|reply| match reply {
                    Reply::Body(bytes) | Reply::Delay(_, bytes) => bytes.len(),
                    _ => 0,
                })
                .sum();
            assert!(
                queued_bytes <= CAPTURE_RESPONSE_BYTES,
                "finite queued reply bytes"
            );
        }
    }

    pub(crate) async fn requests(&self, key: i16) -> Vec<Observed> {
        self.state
            .lock()
            .await
            .observed
            .iter()
            .filter(|r| r.api_key == key)
            .cloned()
            .collect()
    }

    pub(crate) async fn await_requests(&self, key: i16, node: i32, count: usize) {
        tokio::time::timeout(BUDGET, async {
            loop {
                if self
                    .requests(key)
                    .await
                    .iter()
                    .filter(|r| r.node == node)
                    .count()
                    >= count
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    /// Successful tests always call this and inspect joined ownership. Drop's
    /// abort fallback is only failure cleanup, never a qualification verdict.
    pub(crate) async fn shutdown(mut self) -> Closed {
        let _previous = self.stop.send_replace(true);
        let mut closed = Closed {
            observations: Vec::new(),
            listener_tasks_joined: 0,
            connection_tasks_joined: 0,
            shutdown_failures: Vec::new(),
        };
        for mut task in self.tasks.drain(..) {
            match tokio::time::timeout(BUDGET, &mut task).await {
                Ok(Ok((joined, errors))) => {
                    closed.listener_tasks_joined += 1;
                    closed.connection_tasks_joined += joined;
                    closed.shutdown_failures.extend(errors);
                }
                Ok(Err(e)) => closed.shutdown_failures.push(e.to_string()),
                Err(_) => {
                    task.abort();
                    let _joined_after_abort = task.await;
                    closed
                        .shutdown_failures
                        .push("shutdown exceeded joined ownership budget".into());
                }
            }
        }
        closed.observations = self.state.lock().await.observed.clone();
        for address in self.addresses {
            assert!(
                TcpStream::connect(address).await.is_err(),
                "owned listener closed"
            );
            let rebound = TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
        closed
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _previous = self.stop.send_replace(true);
        // A panicking test cannot async-join; such a run is a failure. Every
        // successful case drains/joins through shutdown before its assertions.
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub(crate) async fn retain(closed: &Closed, label: &str) {
    let Some(root) = std::env::var_os("PARTITIONLINE_LIST_TRANSACTIONS_PROOF_DIR") else {
        return;
    };
    assert!(!label.is_empty() && label.len() <= 160);
    assert!(label
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    let directory = Path::new(&root).join(label);
    tokio::fs::create_dir_all(Path::new(&root)).await.unwrap();
    tokio::fs::create_dir(&directory).await.unwrap(); // Never overwrite an earlier attempt.
    let mut manifest = String::from("index\tnode\tlistener_slot\tapi_key\tapi_version\tcorrelation_id\treceived_elapsed_ns\trequest_bytes\trequest_body_offset\tresponse_bytes\tresponse_written\trequest_file\tresponse_file\n");
    let first = closed.observations.first().map(|row| row.received_at);
    for (index, row) in closed.observations.iter().enumerate() {
        let stem = format!(
            "frame-{index:03}-node-{}-key-{}-v{}",
            row.node, row.api_key, row.api_version
        );
        let request_file = format!("{stem}.request.bin");
        tokio::fs::write(directory.join(&request_file), &row.request_frame)
            .await
            .unwrap();
        let response_file = if let Some(bytes) = &row.response_frame {
            let file = format!("{stem}.response.bin");
            tokio::fs::write(directory.join(&file), bytes)
                .await
                .unwrap();
            file
        } else {
            "-".into()
        };
        manifest.push_str(&format!(
            "{index}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{request_file}\t{response_file}\n",
            row.node,
            row.listener_slot,
            row.api_key,
            row.api_version,
            row.correlation_id,
            row.received_at.duration_since(first.unwrap()).as_nanos(),
            row.request_frame.len(),
            row.request_frame.len() - row.request_body.len(),
            row.response_frame.as_ref().map_or(0, |bytes| bytes.len()),
            row.response_written
        ));
    }
    assert!(manifest.len() <= 64 * 1024);
    tokio::fs::write(directory.join("frames.tsv"), manifest)
        .await
        .unwrap();
    let ownership = format!("listener_tasks_joined={}\nconnection_tasks_joined={}\nshutdown_failures={:?}\nretained_after_all_joins=true\n",
        closed.listener_tasks_joined,closed.connection_tasks_joined,closed.shutdown_failures);
    assert!(ownership.len() <= 64 * 1024);
    tokio::fs::write(directory.join("joined-ownership.txt"), ownership)
        .await
        .unwrap();
}

pub(crate) async fn finish(admin: Admin, peer: Peer, label: &str) -> Closed {
    let close = tokio::time::timeout(BUDGET, admin.close()).await;
    let closed = peer.shutdown().await;
    // Capture before any result/ownership assertions, including the unchanged
    // existing-API four-row assertion that must fail against old main.
    tokio::time::timeout(BUDGET, retain(&closed, label))
        .await
        .unwrap();
    assert!(
        matches!(close, Ok(Ok(()))),
        "client close failed or exceeded budget: {close:?}"
    );
    assert!(
        closed.shutdown_failures.is_empty(),
        "peer workers: {:?}",
        closed.shutdown_failures
    );
    assert_eq!(closed.listener_tasks_joined, 3);
    assert!(closed.connection_tasks_joined > 0);
    assert!(closed
        .observations
        .windows(2)
        .all(|p| p[0].received_at <= p[1].received_at));
    for row in &closed.observations {
        let frame = row.request_frame.as_ref();
        assert_eq!(
            usize::try_from(i32::from_be_bytes(frame[..4].try_into().unwrap())).unwrap(),
            frame.len() - 4
        );
        assert_eq!(
            i16::from_be_bytes(frame[4..6].try_into().unwrap()),
            row.api_key
        );
        assert_eq!(
            i16::from_be_bytes(frame[6..8].try_into().unwrap()),
            row.api_version
        );
        assert_eq!(
            i32::from_be_bytes(frame[8..12].try_into().unwrap()),
            row.correlation_id
        );
        assert!(frame.ends_with(&row.request_body));
        assert!(!row.response_written || row.response_frame.is_some());
        if let Some(frame) = &row.response_frame {
            assert_eq!(
                usize::try_from(i32::from_be_bytes(frame[..4].try_into().unwrap())).unwrap(),
                frame.len() - 4
            );
            assert_eq!(
                i32::from_be_bytes(frame[4..8].try_into().unwrap()),
                row.correlation_id
            );
        }
    }
    closed
}
