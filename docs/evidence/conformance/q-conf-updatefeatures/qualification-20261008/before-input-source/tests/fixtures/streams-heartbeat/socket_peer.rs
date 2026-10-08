//! Independent bounded GROUP discovery and Streams heartbeat socket peer.
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone)]
pub(crate) enum Reply {
    Body(Vec<u8>),
    Delay(Duration, Vec<u8>),
    Disconnect,
    Stall,
    Correlation(Vec<u8>),
}

#[derive(Clone)]
pub(crate) struct Frame {
    pub(crate) slot: usize,
    pub(crate) api: i16,
    pub(crate) version: i16,
    pub(crate) body: Vec<u8>,
    pub(crate) raw: Vec<u8>,
}

pub(crate) struct State {
    pub(crate) frames: Vec<Frame>,
    pub(crate) replies: [VecDeque<Reply>; 3],
    pub(crate) ranges: [Option<(i16, i16)>; 3],
    pub(crate) routes: VecDeque<usize>,
    pub(crate) find_code: i16,
    pub(crate) find_version: i16,
    pub(crate) active_workers: usize,
    bytes: usize,
}

pub(crate) struct Peer {
    pub(crate) addresses: [SocketAddr; 3],
    pub(crate) state: Arc<Mutex<State>>,
    stop: watch::Sender<bool>,
    listeners: Vec<JoinHandle<()>>,
}

pub(crate) fn compact(out: &mut Vec<u8>, text: &str) {
    let mut n = u32::try_from(text.len() + 1).unwrap();
    loop {
        let b = u8::try_from(n & 127).unwrap();
        n >>= 7;
        out.push(b | if n == 0 { 0 } else { 128 });
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(text.as_bytes());
}

fn api_versions(range: Option<(i16, i16)>, fc: i16, error: i16) -> Vec<u8> {
    let mut out = error.to_be_bytes().to_vec();
    out.extend_from_slice(&(if range.is_some() { 3i32 } else { 2 }).to_be_bytes());
    for (key, min, max) in [(18i16, 0i16, 0i16), (10, fc, fc)] {
        out.extend_from_slice(&key.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
    }
    if let Some((min, max)) = range {
        out.extend_from_slice(&88i16.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
    }
    out
}

fn find_response(address: SocketAddr, key: &[u8], version: i16, code: i16) -> Vec<u8> {
    let mut out = vec![0; 4];
    if version >= 4 {
        assert_eq!(&key[..2], &[0, 2], "GROUP and one compact coordinator key");
        out.push(2);
        out.extend_from_slice(&key[2..key.len() - 1]);
        out.extend_from_slice(&7i32.to_be_bytes());
        compact(&mut out, "127.0.0.1");
        out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        out.extend_from_slice(&code.to_be_bytes());
        out.extend_from_slice(&[0, 0, 0]); // null error, coordinator/root tags
    } else {
        out.extend_from_slice(&code.to_be_bytes());
        if version == 3 {
            out.push(0);
        } else {
            out.extend_from_slice(&(-1i16).to_be_bytes());
        }
        out.extend_from_slice(&7i32.to_be_bytes());
        if version == 3 {
            compact(&mut out, "127.0.0.1");
        } else {
            out.extend_from_slice(&9i16.to_be_bytes());
            out.extend_from_slice(b"127.0.0.1");
        }
        out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        if version == 3 {
            out.push(0);
        }
    }
    out
}

async fn worker(
    mut socket: TcpStream,
    slot: usize,
    addresses: [SocketAddr; 3],
    state: Arc<Mutex<State>>,
    mut stop: watch::Receiver<bool>,
) {
    state.lock().await.active_workers += 1;
    for _ in 0..40 {
        let size = tokio::select! {
            biased;
            _ = stop.changed() => break,
            value = socket.read_i32() => match value { Ok(n) => n, Err(_) => break },
        };
        assert!((10..=1048640).contains(&size));
        let mut raw = size.to_be_bytes().to_vec();
        raw.resize(usize::try_from(size).unwrap() + 4, 0);
        let read = tokio::select! { biased; _ = stop.changed() => break, v = socket.read_exact(&mut raw[4..]) => v };
        if read.is_err() {
            break;
        }
        let api = i16::from_be_bytes(raw[4..6].try_into().unwrap());
        let version = i16::from_be_bytes(raw[6..8].try_into().unwrap());
        let correlation = i32::from_be_bytes(raw[8..12].try_into().unwrap());
        let client = i16::from_be_bytes(raw[12..14].try_into().unwrap());
        let mut at = 14 + usize::try_from(client.max(0)).unwrap();
        if api == 88 || (api == 10 && version >= 3) || (api == 18 && version >= 3) {
            assert_eq!(raw[at], 0);
            at += 1;
        }
        let body = raw[at..].to_vec();
        let reply = {
            let mut s = state.lock().await;
            assert!(s.frames.len() < 128);
            s.bytes += raw.len();
            assert!(s.bytes <= 4 * 1024 * 1024);
            s.frames.push(Frame {
                slot,
                api,
                version,
                body: body.clone(),
                raw,
            });
            match api {
                18 => Reply::Body(api_versions(
                    s.ranges[slot],
                    s.find_version,
                    if version == 0 { 0 } else { 35 },
                )),
                10 => {
                    assert_eq!(slot, 0, "GROUP discovery goes to bootstrap");
                    let route = s.routes.pop_front().unwrap_or(1);
                    Reply::Body(find_response(addresses[route], &body, version, s.find_code))
                }
                88 => {
                    assert_ne!(slot, 0, "heartbeat must go to GROUP coordinator");
                    s.replies[slot].pop_front().unwrap()
                }
                _ => panic!("unrelated API{api}"),
            }
        };
        let (body, wrong) = match reply {
            Reply::Body(body) => (body, false),
            Reply::Correlation(body) => (body, true),
            Reply::Disconnect => break,
            Reply::Stall => {
                tokio::select! {
                    biased;
                    _ = stop.changed() => (),
                    result = socket.read_u8() => assert!(result.is_err(), "cancelled client closes socket"),
                }
                break;
            }
            Reply::Delay(delay, body) => {
                tokio::select! { biased; _ = stop.changed() => break, _ = tokio::time::sleep(delay) => () };
                (body, false)
            }
        };
        assert!(body.len() <= 1024 * 1024);
        let mut response = (if wrong {
            correlation.wrapping_add(1)
        } else {
            correlation
        })
        .to_be_bytes()
        .to_vec();
        if api == 88 || (api == 10 && version >= 3) {
            response.push(0);
        }
        response.extend_from_slice(&body);
        let mut frame = i32::try_from(response.len())
            .unwrap()
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&response);
        let write = tokio::select! { biased; _ = stop.changed() => break, v = socket.write_all(&frame) => v };
        if write.is_err() {
            break;
        }
    }
    state.lock().await.active_workers -= 1;
}

impl Peer {
    pub(crate) async fn start() -> Self {
        let mut sockets = Vec::new();
        let mut addresses = Vec::new();
        for _ in 0..3 {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(socket.local_addr().unwrap());
            sockets.push(socket);
        }
        let addresses: [SocketAddr; 3] = addresses.try_into().unwrap();
        let state = Arc::new(Mutex::new(State {
            frames: Vec::new(),
            replies: std::array::from_fn(|_| VecDeque::new()),
            ranges: [Some((0, 0)); 3],
            routes: VecDeque::new(),
            find_code: 0,
            find_version: 6,
            active_workers: 0,
            bytes: 0,
        }));
        let (stop, rx) = watch::channel(false);
        let mut listeners = Vec::new();
        for (slot, socket) in sockets.into_iter().enumerate() {
            let state = Arc::clone(&state);
            let mut rx = rx.clone();
            listeners.push(tokio::spawn(async move {
                let mut workers = JoinSet::new();let mut accepted = 0;
                loop { tokio::select! { biased;
                    _ = rx.changed() => break,
                    result = workers.join_next(), if !workers.is_empty() => { result.unwrap().unwrap(); },
                    result = socket.accept() => { accepted += 1;assert!(accepted <= 32);let (socket, _) = result.unwrap();let _owned = workers.spawn(worker(socket, slot, addresses, Arc::clone(&state), rx.clone())); }
                } }
                drop(socket);
                while let Some(result) = workers.join_next().await { result.unwrap(); }
            }));
        }
        Self {
            addresses,
            state,
            stop,
            listeners,
        }
    }

    pub(crate) fn config(&self) -> partitionline::StreamsConfig {
        let mut cfg = partitionline::StreamsConfig::bootstrap([self.addresses[0].to_string()]);
        cfg.allow_unstable = true;
        cfg.connection.request_timeout = Duration::from_secs(1);
        cfg.connection.retry_backoff = Duration::from_millis(1);
        cfg.connection.retry_backoff_max = Duration::from_millis(1);
        cfg
    }

    pub(crate) async fn script(&self, slot: usize, replies: impl IntoIterator<Item = Reply>) {
        self.state.lock().await.replies[slot].extend(replies);
    }

    pub(crate) async fn close(mut self) -> Vec<Frame> {
        let _stop_sent = self.stop.send(true);
        for handle in &mut self.listeners {
            match tokio::time::timeout(Duration::from_secs(2), &mut *handle).await {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    handle.abort();
                    let _joined = handle.await;
                    panic!("listener deadline");
                }
            }
        }
        self.listeners.clear();
        for addr in self.addresses {
            assert!(TcpStream::connect(addr).await.is_err());
            let rebound = TcpListener::bind(addr).await.unwrap();
            drop(rebound);
        }
        let frames = self.state.lock().await.frames.clone();
        tokio::task::yield_now().await;
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            0
        );
        frames
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _stop_sent = self.stop.send(true);
        for handle in &self.listeners {
            handle.abort();
        }
    }
}
