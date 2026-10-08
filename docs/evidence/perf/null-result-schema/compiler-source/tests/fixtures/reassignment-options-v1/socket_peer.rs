//! Owned two-controller peer using actual Apache reassignment response fixtures.
#![allow(
    dead_code,
    reason = "finite helpers are shared by selected peer profiles"
)]

use partitionline::{Admin, AdminConfig};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};

pub(crate) const BUDGET: Duration = Duration::from_secs(2);
pub(crate) type Assignment = (String, i32, Option<Vec<i32>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Request {
    pub(crate) timeout_ms: i32,
    pub(crate) allow: bool,
    pub(crate) assignments: Vec<Assignment>,
}

fn unsigned(input: &mut &[u8]) -> u32 {
    let mut value = 0;
    for shift in (0..35).step_by(7) {
        let byte = input[0];
        *input = &input[1..];
        assert!(shift < 28 || byte < 16);
        value |= u32::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
    }
    panic!("invalid32bit varint");
}

fn tags(input: &mut &[u8]) {
    let count = unsigned(input);
    assert!(count <= 128);
    for _ in 0..count {
        let _tag = unsigned(input);
        let size = usize::try_from(unsigned(input)).unwrap();
        assert!(size <= input.len());
        *input = &input[size..];
    }
}

fn int(input: &mut &[u8]) -> i32 {
    let value = i32::from_be_bytes(input[..4].try_into().unwrap());
    *input = &input[4..];
    value
}

pub(crate) fn request(mut input: &[u8], version: i16) -> Request {
    assert!((0..=1).contains(&version));
    let timeout_ms = int(&mut input);
    let allow = if version == 0 {
        true
    } else {
        let value = input[0];
        input = &input[1..];
        assert!(value <= 1);
        value != 0
    };
    let count = unsigned(&mut input);
    assert!((1..=10_001).contains(&count));
    let mut assignments = Vec::new();
    for _ in 1..count {
        let size = usize::try_from(unsigned(&mut input)).unwrap();
        assert!(size > 0 && size <= 250);
        let name = std::str::from_utf8(&input[..size - 1]).unwrap().to_owned();
        input = &input[size - 1..];
        let partitions = unsigned(&mut input);
        assert!((1..=10_001).contains(&partitions));
        for _ in 1..partitions {
            let index = int(&mut input);
            let replicas = unsigned(&mut input);
            assert!(replicas <= 100_001);
            let replicas = if replicas == 0 {
                None
            } else {
                Some((1..replicas).map(|_| int(&mut input)).collect())
            };
            tags(&mut input);
            assignments.push((name.clone(), index, replicas));
        }
        tags(&mut input);
    }
    tags(&mut input);
    assert!(input.is_empty());
    Request {
        timeout_ms,
        allow,
        assignments,
    }
}

#[derive(Clone)]
pub(crate) enum Reply {
    Policy,
    Body(Arc<[u8]>),
    Delay(Duration, Arc<[u8]>),
    ControllerMoved(Arc<[u8]>),
    Disconnect(bool),
    Stall,
}

#[derive(Debug, Clone)]
pub(crate) struct Observed {
    pub(crate) node: usize,
    pub(crate) api: i16,
    pub(crate) version: i16,
    pub(crate) body: Vec<u8>,
    pub(crate) request_frame: Vec<u8>,
    pub(crate) response_frame: Option<Vec<u8>>,
}

pub(crate) struct State {
    pub(crate) ranges: [Option<(i16, i16)>; 2],
    pub(crate) controller: usize,
    pub(crate) replies: [VecDeque<Reply>; 2],
    pub(crate) metadata_delay: Duration,
    pub(crate) observed: Vec<Observed>,
}

pub(crate) struct Closed {
    pub(crate) observed: Vec<Observed>,
    pub(crate) listener_tasks_joined: usize,
    pub(crate) connection_tasks_joined: usize,
    pub(crate) ports_closed_and_reusable: bool,
}

pub(crate) struct Peer {
    pub(crate) bootstrap: String,
    pub(crate) state: Arc<Mutex<State>>,
    addresses: [SocketAddr; 2],
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<usize>>,
}

fn classic(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&i16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn versions(range: Option<(i16, i16)>, code: i16) -> Vec<u8> {
    let mut keys = vec![(3i16, 4i16, 4i16), (18, 0, 0), (19, 0, 0), (20, 0, 0)];
    if let Some((low, high)) = range {
        keys.push((45, low, high));
    }
    let mut out = code.to_be_bytes().to_vec();
    out.extend_from_slice(&i32::try_from(keys.len()).unwrap().to_be_bytes());
    for (key, low, high) in keys {
        for value in [key, low, high] {
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
    out
}

fn metadata(addresses: &[SocketAddr; 2], controller: usize) -> Vec<u8> {
    let mut out = 0i32.to_be_bytes().to_vec(); // Metadata4 throttle
    out.extend_from_slice(&2i32.to_be_bytes());
    for (index, address) in addresses.iter().enumerate() {
        out.extend_from_slice(&i32::try_from(index + 1).unwrap().to_be_bytes());
        classic(&mut out, "127.0.0.1");
        out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes());
    }
    out.extend_from_slice(&(-1i16).to_be_bytes()); // nullable cluster ID
    out.extend_from_slice(&i32::try_from(controller).unwrap().to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out
}

pub(crate) fn fixture(version: i16, cell: &str) -> Arc<[u8]> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tests/fixtures/reassignment-options-v1/4.3.1/v{version}-{cell}-response.bin"
        )),
    )
    .unwrap()
    .into()
}

async fn connection(
    mut socket: TcpStream,
    slot: usize,
    addresses: [SocketAddr; 2],
    state: Arc<Mutex<State>>,
    mut stop: watch::Receiver<bool>,
) {
    for _ in 0..64 {
        let length = tokio::select! { biased;
            _ = stop.changed() => break,
            value = socket.read_i32() => match value { Ok(value) => value, Err(_) => break },
        };
        assert!((10..=8 * 1024 * 1024).contains(&length));
        let mut frame = vec![0u8; usize::try_from(length).unwrap() + 4];
        frame[..4].copy_from_slice(&length.to_be_bytes());
        let read = tokio::select! { biased; _ = stop.changed() => break, value = socket.read_exact(&mut frame[4..]) => value };
        if read.is_err() {
            break;
        }
        let api = i16::from_be_bytes(frame[4..6].try_into().unwrap());
        let version = i16::from_be_bytes(frame[6..8].try_into().unwrap());
        let correlation = frame[8..12].to_vec();
        let client_len = i16::from_be_bytes(frame[12..14].try_into().unwrap());
        let mut body_at = 14 + usize::try_from(client_len.max(0)).unwrap();
        if api == 45 || (api == 18 && version >= 3) {
            assert_eq!(frame[body_at], 0);
            body_at += 1;
        }
        let body = frame[body_at..].to_vec();
        let (index, reply) = {
            let mut state = state.lock().await;
            assert!(state.observed.len() < 256);
            assert!(
                state
                    .observed
                    .iter()
                    .map(|row| row.request_frame.len())
                    .sum::<usize>()
                    + frame.len()
                    <= 32 * 1024 * 1024
            );
            let index = state.observed.len();
            state.observed.push(Observed {
                node: slot + 1,
                api,
                version,
                body: body.clone(),
                request_frame: frame,
                response_frame: None,
            });
            let reply = match api {
                18 => Reply::Body(
                    versions(state.ranges[slot], if version == 0 { 0 } else { 35 }).into(),
                ),
                3 => {
                    assert_eq!(version, 4);
                    Reply::Delay(
                        state.metadata_delay,
                        metadata(&addresses, state.controller).into(),
                    )
                }
                45 => {
                    let _parsed = request(&body, version);
                    state.replies[slot].pop_front().unwrap_or(Reply::Policy)
                }
                other => panic!("unexpected API {other}"),
            };
            (index, reply)
        };
        let (delay, response) = match reply {
            Reply::Body(body) => (Duration::ZERO, body),
            Reply::Delay(delay, body) => (delay, body),
            Reply::ControllerMoved(body) => {
                state.lock().await.controller = 2;
                (Duration::ZERO, body)
            }
            Reply::Policy => {
                let parsed = request(&body, version);
                let mut assignments = parsed.assignments;
                assignments.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
                assert_eq!(
                    assignments,
                    vec![
                        ("topic".into(), 0, Some(vec![1, 2, 3])),
                        ("topic".into(), 1, Some(vec![2, 3])),
                        ("topic".into(), 2, None)
                    ]
                );
                (
                    Duration::ZERO,
                    fixture(version, if parsed.allow { "true" } else { "false" }),
                )
            }
            Reply::Disconnect(move_controller) => {
                if move_controller {
                    state.lock().await.controller = 2;
                }
                break;
            }
            Reply::Stall => {
                let _stopped = stop.changed().await;
                break;
            }
        };
        if !delay.is_zero() {
            tokio::select! { biased; _ = stop.changed() => break, () = tokio::time::sleep(delay) => {} }
        }
        let tags = usize::from(api == 45);
        let mut payload = i32::try_from(response.len() + 4 + tags)
            .unwrap()
            .to_be_bytes()
            .to_vec();
        payload.extend_from_slice(&correlation);
        if tags == 1 {
            payload.push(0);
        }
        assert!(response.len() <= 17 * 1024 * 1024);
        payload.extend_from_slice(&response);
        {
            let mut state = state.lock().await;
            assert!(
                state
                    .observed
                    .iter()
                    .filter_map(|row| row.response_frame.as_ref())
                    .map(Vec::len)
                    .sum::<usize>()
                    + payload.len()
                    <= 64 * 1024 * 1024
            );
            state.observed[index].response_frame = Some(payload.clone());
        }
        let written = tokio::select! { biased; _ = stop.changed() => break, value = socket.write_all(&payload) => value };
        if written.is_err() {
            break;
        }
    }
    let _closed = socket.shutdown().await;
}

impl Peer {
    pub(crate) async fn start(ranges: [Option<(i16, i16)>; 2]) -> Self {
        let listeners = [
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let addresses = std::array::from_fn(|index| listeners[index].local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            ranges,
            controller: 1,
            replies: std::array::from_fn(|_| VecDeque::new()),
            metadata_delay: Duration::ZERO,
            observed: Vec::new(),
        }));
        let (stop, receiver) = watch::channel(false);
        let tasks=listeners.into_iter().enumerate().map(|(slot,listener)|{
            let mut receiver=receiver.clone(); let state=Arc::clone(&state);
            tokio::spawn(async move {
                let mut connections=JoinSet::new();let mut accepted=0;let mut joined=0;
                loop {tokio::select!{biased;
                    _ = receiver.changed() => break,
                    finished = connections.join_next(), if !connections.is_empty() => {finished.unwrap().unwrap();joined+=1;},
                    value = listener.accept() => {let (socket,_address)=value.unwrap();accepted+=1;assert!(accepted<=16);
                        let _handle=connections.spawn(connection(socket,slot,addresses,Arc::clone(&state),receiver.clone()));}
                }}
                drop(listener);
                while let Some(finished)=connections.join_next().await {finished.unwrap();joined+=1;}
                assert_eq!(accepted,joined);joined
            })
        }).collect();
        Self {
            bootstrap: addresses[0].to_string(),
            addresses,
            state,
            stop,
            tasks,
        }
    }

    pub(crate) async fn admin(&self) -> partitionline::Result<Admin> {
        let mut config = AdminConfig::bootstrap([self.bootstrap.clone()]);
        config.request_timeout = BUDGET;
        config.connect_timeout = BUDGET;
        config.retry_backoff = Duration::from_millis(5);
        config.retry_backoff_max = Duration::from_millis(10);
        config.reconnect_backoff = Duration::ZERO;
        config.reconnect_backoff_max = Duration::ZERO;
        Admin::new(config).await
    }

    pub(crate) async fn script(&self, node: usize, replies: impl IntoIterator<Item = Reply>) {
        let mut state = self.state.lock().await;
        state.replies[node - 1].extend(replies);
        assert!(state.replies[node - 1].len() <= 16);
    }

    pub(crate) async fn close(mut self, label: &str) -> Closed {
        let _stopped = self.stop.send(true);
        let mut joined = 0;
        let mut connections = 0;
        for task in std::mem::take(&mut self.tasks) {
            connections += tokio::time::timeout(BUDGET, task).await.unwrap().unwrap();
            joined += 1;
        }
        for address in self.addresses {
            assert!(TcpStream::connect(address).await.is_err());
            let _rebound = std::net::TcpListener::bind(address).unwrap();
        }
        let closed = Closed {
            observed: std::mem::take(&mut self.state.lock().await.observed),
            listener_tasks_joined: joined,
            connection_tasks_joined: connections,
            ports_closed_and_reusable: true,
        };
        if let Some(root) = std::env::var_os("PL_REASSIGNMENT_OUTPUT") {
            let out = std::path::PathBuf::from(root).join(label);
            std::fs::create_dir_all(&out).unwrap();
            let mut manifest=format!("listeners_joined={joined}\nconnections_joined={connections}\nports_closed_and_reusable=true\n");
            for (index, row) in closed.observed.iter().enumerate() {
                std::fs::write(
                    out.join(format!("{index:03}-request.bin")),
                    &row.request_frame,
                )
                .unwrap();
                if let Some(frame) = &row.response_frame {
                    std::fs::write(out.join(format!("{index:03}-response.bin")), frame).unwrap();
                }
                manifest.push_str(&format!(
                    "{index}\t{}\t{}\t{}\n",
                    row.node, row.api, row.version
                ));
            }
            std::fs::write(out.join("closure.txt"), manifest).unwrap();
        }
        closed
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _stopped = self.stop.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub(crate) async fn finish(admin: Admin, peer: Peer, label: &str) -> Closed {
    let result = admin.close().await;
    let closed = peer.close(label).await;
    result.unwrap();
    closed
}
