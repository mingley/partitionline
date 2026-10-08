//! Independent bounded GROUP discovery and Streams description socket peer.
use std::collections::{HashMap, VecDeque};
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
    pub(crate) group_routes: HashMap<String, usize>,
    pub(crate) group_errors: HashMap<String, i16>,
    pub(crate) groups: HashMap<String, Vec<u8>>,
    pub(crate) downgrade_on_disconnect: bool,
    pub(crate) move_alpha_on_reply: bool,
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
    out.extend_from_slice(&(if range.is_some() { 6i32 } else { 5 }).to_be_bytes());
    for (key, min, max) in [
        (18i16, 0i16, 0i16),
        (10, fc, fc),
        (19, 0, 0),
        (20, 0, 0),
        (3, 4, 4),
    ] {
        out.extend_from_slice(&key.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
    }
    if let Some((min, max)) = range {
        out.extend_from_slice(&89i16.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
    }
    out
}

fn var(input: &mut &[u8]) -> u32 {
    let mut value = 0;
    let mut shift = 0;
    loop {
        let byte = input[0];
        *input = &input[1..];
        value |= u32::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift < 35);
    }
}
fn text(input: &mut &[u8], flexible: bool) -> String {
    let length = if flexible {
        usize::try_from(var(input) - 1).unwrap()
    } else {
        let n = i16::from_be_bytes(input[..2].try_into().unwrap());
        *input = &input[2..];
        usize::try_from(n).unwrap()
    };
    let result = std::str::from_utf8(&input[..length]).unwrap().to_owned();
    *input = &input[length..];
    result
}
fn classic(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&i16::try_from(text.len()).unwrap().to_be_bytes());
    out.extend_from_slice(text.as_bytes());
}
fn metadata(addresses: [SocketAddr; 3]) -> Vec<u8> {
    let mut out = vec![0; 4];
    out.extend_from_slice(&3i32.to_be_bytes());
    for (slot, addr) in addresses.iter().enumerate() {
        out.extend_from_slice(&i32::try_from(slot).unwrap().to_be_bytes());
        classic(&mut out, "127.0.0.1");
        out.extend_from_slice(&i32::from(addr.port()).to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes());
    }
    out.extend_from_slice(&(-1i16).to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out
}
fn find_response(
    addresses: [SocketAddr; 3],
    mut input: &[u8],
    version: i16,
    s: &mut State,
) -> Vec<u8> {
    let mut keys = Vec::new();
    if version >= 4 {
        assert_eq!(input[0], 0);
        input = &input[1..];
        let count = var(&mut input) - 1;
        assert!(count <= 32);
        for _ in 0..count {
            keys.push(text(&mut input, true));
        }
    } else {
        keys.push(text(&mut input, version >= 3));
        assert_eq!(input[0], 0);
        input = &input[1..];
    }
    assert_eq!(input, if version >= 3 { &[0][..] } else { &[][..] });
    let mut out = vec![0; 4];
    if version >= 4 {
        out.push(u8::try_from(keys.len() + 1).unwrap());
    }
    for key in keys {
        let route = s
            .routes
            .pop_front()
            .unwrap_or_else(|| *s.group_routes.get(&key).unwrap_or(&1));
        let address = addresses[route];
        let code = *s.group_errors.get(&key).unwrap_or(&s.find_code);
        if version >= 4 {
            compact(&mut out, &key);
            out.extend_from_slice(&i32::try_from(route).unwrap().to_be_bytes());
            compact(&mut out, "127.0.0.1");
            out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
            out.extend_from_slice(&code.to_be_bytes());
            out.extend_from_slice(&[0, 0]);
        } else {
            out.extend_from_slice(&code.to_be_bytes());
            if version >= 3 {
                out.push(0);
            } else {
                out.extend_from_slice(&(-1i16).to_be_bytes());
            }
            out.extend_from_slice(&i32::try_from(route).unwrap().to_be_bytes());
            if version >= 3 {
                compact(&mut out, "127.0.0.1");
            } else {
                classic(&mut out, "127.0.0.1");
            }
            out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        }
    }
    if version >= 3 {
        out.push(0);
    }
    out
}
fn describe_response(mut input: &[u8], s: &State) -> Vec<u8> {
    let count = var(&mut input) - 1;
    assert!(count <= 32);
    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(text(&mut input, true));
    }
    assert!(input == [0, 0] || input == [1, 0]);
    let mut out = vec![0; 4];
    out.push(u8::try_from(count + 1).unwrap());
    // Reverse order deliberately; clients must map by ID, not zip by position.
    for key in keys.iter().rev() {
        let raw = s.groups.get(key).unwrap();
        assert_eq!(raw[4], 2);
        assert_eq!(raw.last(), Some(&0));
        out.extend_from_slice(&raw[5..raw.len() - 1]);
    }
    out.push(0);
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
        if api == 89 || (api == 10 && version >= 3) || (api == 18 && version >= 3) {
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
                10 => Reply::Body(find_response(addresses, &body, version, &mut s)),
                89 => {
                    assert_ne!(slot, 0, "description must go to GROUP coordinator");
                    let reply = s.replies[slot]
                        .pop_front()
                        .unwrap_or_else(|| Reply::Body(describe_response(&body, &s)));
                    if slot == 1 && s.move_alpha_on_reply {
                        let _previous = s.group_routes.insert("alpha".into(), 2);
                        s.move_alpha_on_reply = false;
                    }
                    reply
                }
                3 => Reply::Body(metadata(addresses)),
                _ => panic!("unrelated API{api}"),
            }
        };
        let (body, wrong) = match reply {
            Reply::Body(body) => (body, false),
            Reply::Correlation(body) => (body, true),
            Reply::Disconnect => {
                let mut s = state.lock().await;
                if s.downgrade_on_disconnect {
                    s.ranges[slot] = Some((1, 1));
                }
                break;
            }
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
        if api == 89 || (api == 10 && version >= 3) {
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
            group_routes: HashMap::new(),
            group_errors: HashMap::new(),
            groups: HashMap::new(),
            downgrade_on_disconnect: false,
            move_alpha_on_reply: false,
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

    pub(crate) fn config(&self) -> partitionline::AdminConfig {
        let mut cfg = partitionline::AdminConfig::bootstrap([self.addresses[0].to_string()]);
        cfg.request_timeout = Duration::from_secs(1);
        cfg.retry_backoff = Duration::from_millis(1);
        cfg.retry_backoff_max = Duration::from_millis(1);
        cfg.reconnect_backoff = Duration::from_millis(1);
        cfg.reconnect_backoff_max = Duration::from_millis(1);
        cfg
    }
    pub(crate) async fn admin(&self) -> partitionline::Admin {
        partitionline::Admin::new(self.config()).await.unwrap()
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
