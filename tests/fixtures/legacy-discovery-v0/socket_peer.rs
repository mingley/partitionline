//! Owned, bounded discovery peer. Wire bodies follow the Apache schemas.
#![expect(clippy::unwrap_used, reason = "owned finite fixture assertions")]
use std::{collections::VecDeque, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Mutex},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone)]
pub(crate) enum Reply {
    Body(Vec<u8>),
    Delay(Duration, Vec<u8>),
    Disconnect,
}
#[derive(Clone)]
pub(crate) struct Frame {
    pub slot: usize,
    pub api: i16,
    pub version: i16,
    pub body: Vec<u8>,
    pub raw: Vec<u8>,
}
pub(crate) struct State {
    pub frames: Vec<Frame>,
    pub responses: Vec<Frame>,
    pub metadata: Option<(i16, i16)>,
    pub find: Option<(i16, i16)>,
    pub routes: VecDeque<usize>,
    pub replies: VecDeque<Reply>,
    pub offset_replies: VecDeque<Reply>,
    pub startup: VecDeque<Reply>,
    pub metadata_replies: VecDeque<Reply>,
    pub workers: usize,
    bytes: usize,
}
pub(crate) struct Peer {
    pub addresses: [SocketAddr; 3],
    pub state: Arc<Mutex<State>>,
    stop: watch::Sender<bool>,
    listeners: Vec<JoinHandle<()>>,
}
fn string(out: &mut Vec<u8>, text: &str, flexible: bool) {
    if flexible {
        out.push(u8::try_from(text.len() + 1).unwrap());
    } else {
        out.extend_from_slice(&i16::try_from(text.len()).unwrap().to_be_bytes());
    }
    out.extend_from_slice(text.as_bytes());
}
fn count(out: &mut Vec<u8>, n: usize, flexible: bool) {
    if flexible {
        out.push(u8::try_from(n + 1).unwrap());
    } else {
        out.extend_from_slice(&i32::try_from(n).unwrap().to_be_bytes());
    }
}
pub(crate) fn metadata(address: SocketAddr, version: i16) -> Vec<u8> {
    let f = version >= 9;
    let mut out = Vec::new();
    if version >= 3 {
        out.extend_from_slice(&0i32.to_be_bytes());
    }
    count(&mut out, 1, f);
    out.extend_from_slice(&7i32.to_be_bytes());
    string(&mut out, "::1", f);
    out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
    if version >= 1 {
        if f {
            out.push(0);
        } else {
            out.extend_from_slice(&(-1i16).to_be_bytes());
        }
    }
    if f {
        out.push(0);
    }
    if version >= 2 {
        string(&mut out, "cluster", f);
    }
    if version >= 1 {
        out.extend_from_slice(&7i32.to_be_bytes());
    }
    count(&mut out, 1, f);
    out.extend_from_slice(&0i16.to_be_bytes());
    string(&mut out, "topic", f);
    if version >= 10 {
        out.extend_from_slice(&[1; 16]);
    }
    if version >= 1 {
        out.push(0);
    }
    count(&mut out, 1, f);
    out.extend_from_slice(&0i16.to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&7i32.to_be_bytes());
    if version >= 7 {
        out.extend_from_slice(&4i32.to_be_bytes());
    }
    for _ in 0..2 {
        count(&mut out, 1, f);
        out.extend_from_slice(&7i32.to_be_bytes());
    }
    if version >= 5 {
        count(&mut out, 0, f);
    }
    if f {
        out.push(0);
    }
    if version >= 8 {
        out.extend_from_slice(&i32::MIN.to_be_bytes());
    }
    if f {
        out.push(0);
    }
    if (8..=10).contains(&version) {
        out.extend_from_slice(&i32::MIN.to_be_bytes());
    }
    if version >= 13 {
        out.extend_from_slice(&0i16.to_be_bytes());
    }
    if f {
        out.push(0);
    }
    out
}
fn find(address: SocketAddr, version: i16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if version >= 1 {
        out.extend_from_slice(&0i32.to_be_bytes());
    }
    if version >= 4 {
        assert_eq!(*body.first().unwrap(), 0);
        assert_eq!(*body.get(1).unwrap(), 2);
        out.push(2);
        let len = usize::from(*body.get(2).unwrap());
        out.extend_from_slice(body.get(2..2 + len).unwrap());
        out.extend_from_slice(&7i32.to_be_bytes());
        string(&mut out, "::1", true);
        out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        out.extend_from_slice(&[0, 0, 0, 0, 0]);
    } else {
        out.extend_from_slice(&0i16.to_be_bytes());
        if version >= 1 {
            if version >= 3 {
                out.push(0);
            } else {
                out.extend_from_slice(&(-1i16).to_be_bytes());
            }
        }
        out.extend_from_slice(&7i32.to_be_bytes());
        string(&mut out, "::1", version >= 3);
        out.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        if version >= 3 {
            out.push(0);
        }
    }
    out
}
pub(crate) fn offsets() -> Vec<u8> {
    let mut out = vec![0; 4];
    out.push(2);
    string(&mut out, "group", true);
    out.push(2);
    string(&mut out, "topic", true);
    out.push(2);
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&123i64.to_be_bytes());
    out.extend_from_slice(&4i32.to_be_bytes());
    out.push(1);
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    out
}
pub(crate) fn versions(
    metadata: Option<(i16, i16)>,
    find: Option<(i16, i16)>,
    error: i16,
) -> Vec<u8> {
    let mut keys = vec![
        (18i16, 0i16, 0i16),
        (19, 0, 0),
        (20, 0, 0),
        (0, 3, 3),
        (1, 4, 4),
        (9, 9, 9),
        (11, 2, 2),
    ];
    if let Some((min, max)) = metadata {
        keys.push((3, min, max));
    }
    if let Some((min, max)) = find {
        keys.push((10, min, max));
    }
    let mut out = error.to_be_bytes().to_vec();
    count(&mut out, keys.len(), false);
    for (api, min, max) in keys {
        out.extend_from_slice(&api.to_be_bytes());
        out.extend_from_slice(&min.to_be_bytes());
        out.extend_from_slice(&max.to_be_bytes());
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
    state.lock().await.workers += 1;
    for _ in 0..40 {
        let size = tokio::select! {biased; _=stop.changed()=>break, result=socket.read_i32()=>match result {Ok(n)=>usize::try_from(n).unwrap(),Err(_)=>break}};
        assert!((10..=1024 * 1024).contains(&size));
        let mut raw = vec![0; size];
        let read = tokio::select! {biased;_=stop.changed()=>break,result=socket.read_exact(&mut raw)=>result};
        if read.is_err() {
            break;
        }
        let api = i16::from_be_bytes(raw.get(0..2).unwrap().try_into().unwrap());
        assert!([18, 3, 10, 9, 1, 11].contains(&api), "unrelated API{api}");
        let version = i16::from_be_bytes(raw.get(2..4).unwrap().try_into().unwrap());
        let correlation = raw.get(4..8).unwrap();
        let client = usize::try_from(i16::from_be_bytes(
            raw.get(8..10).unwrap().try_into().unwrap(),
        ))
        .unwrap();
        let mut at = 10 + client;
        let flex = api == 18 && version >= 3
            || api == 3 && version >= 9
            || api == 10 && version >= 3
            || api == 9 && version >= 6;
        if flex {
            assert_eq!(*raw.get(at).unwrap(), 0);
            at += 1;
        }
        let body = raw.get(at..).unwrap().to_vec();
        let reply = {
            let mut s = state.lock().await;
            s.bytes += raw.len();
            assert!(s.frames.len() < 128 && s.bytes <= 4 * 1024 * 1024);
            s.frames.push(Frame {
                slot,
                api,
                version,
                body: body.clone(),
                raw: raw.clone(),
            });
            match api {
                18 => s.startup.pop_front().unwrap_or_else(|| {
                    Reply::Body(versions(
                        s.metadata,
                        s.find,
                        if version == 0 { 0 } else { 35 },
                    ))
                }),
                3 => s
                    .metadata_replies
                    .pop_front()
                    .unwrap_or_else(|| Reply::Body(metadata(addresses[1], version))),
                10 => s.replies.pop_front().unwrap_or_else(|| {
                    let target = s.routes.pop_front().unwrap_or(1);
                    Reply::Body(find(*addresses.get(target).unwrap(), version, &body))
                }),
                9 => s
                    .offset_replies
                    .pop_front()
                    .unwrap_or_else(|| Reply::Body(offsets())),
                1 => Reply::Body(vec![0; 8]),
                11 => {
                    let mut out = vec![0; 4];
                    out.extend_from_slice(&30i16.to_be_bytes());
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                    out.extend_from_slice(&[0; 10]);
                    Reply::Body(out)
                }
                _ => Reply::Disconnect,
            }
        };
        let response = match reply {
            Reply::Body(v) => v,
            Reply::Disconnect => break,
            Reply::Delay(delay, v) => {
                tokio::select! {biased;_=stop.changed()=>break,_=tokio::time::sleep(delay)=>()};
                v
            }
        };
        let mut frame = correlation.to_vec();
        if flex && api != 18 {
            frame.push(0);
        }
        frame.extend_from_slice(&response);
        assert!(frame.len() <= 1024 * 1024);
        {
            let mut s = state.lock().await;
            s.bytes += frame.len();
            assert!(s.responses.len() < 128 && s.bytes <= 4 * 1024 * 1024);
            s.responses.push(Frame {
                slot,
                api,
                version,
                body: response,
                raw: frame.clone(),
            });
        }
        let mut output = i32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
        output.extend_from_slice(&frame);
        let write = tokio::select! {biased;_=stop.changed()=>break,result=socket.write_all(&output)=>result};
        if write.is_err() {
            break;
        }
    }
    state.lock().await.workers -= 1;
}
impl Peer {
    pub(crate) async fn start() -> Self {
        let mut sockets = Vec::new();
        let mut addresses = Vec::new();
        for _ in 0..3 {
            let socket = TcpListener::bind("[::1]:0").await.unwrap();
            addresses.push(socket.local_addr().unwrap());
            sockets.push(socket);
        }
        let addresses: [SocketAddr; 3] = addresses.try_into().unwrap();
        let state = Arc::new(Mutex::new(State {
            frames: Vec::new(),
            responses: Vec::new(),
            metadata: Some((0, 0)),
            find: Some((0, 0)),
            routes: VecDeque::new(),
            replies: VecDeque::new(),
            offset_replies: VecDeque::new(),
            startup: VecDeque::new(),
            metadata_replies: VecDeque::new(),
            workers: 0,
            bytes: 0,
        }));
        let (stop, rx) = watch::channel(false);
        let mut listeners = Vec::new();
        for (slot, socket) in sockets.into_iter().enumerate() {
            let state = Arc::clone(&state);
            let mut rx = rx.clone();
            listeners.push(tokio::spawn(async move {let mut workers=JoinSet::new();let mut accepted=0;loop {tokio::select!{biased;_=rx.changed()=>break,result=workers.join_next(),if !workers.is_empty()=>{result.unwrap().unwrap();},result=socket.accept()=>{accepted+=1;assert!(accepted<=32);let(socket,_)=result.unwrap();let _owned=workers.spawn(worker(socket,slot,addresses,Arc::clone(&state),rx.clone()));}}}drop(socket);while let Some(result)=workers.join_next().await{result.unwrap();}}));
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
        cfg.request_timeout = Duration::from_millis(800);
        cfg.retry_backoff = Duration::from_millis(1);
        cfg.retry_backoff_max = Duration::from_millis(1);
        cfg
    }
    pub(crate) async fn close(mut self) -> Vec<Frame> {
        let _sent = self.stop.send(true);
        for handle in &mut self.listeners {
            tokio::time::timeout(Duration::from_secs(2), &mut *handle)
                .await
                .unwrap()
                .unwrap();
        }
        self.listeners.clear();
        assert_eq!(self.state.lock().await.workers, 0);
        for addr in self.addresses {
            assert!(TcpStream::connect(addr).await.is_err());
            drop(TcpListener::bind(addr).await.unwrap());
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
        let _sent = self.stop.send(true);
        for handle in &self.listeners {
            handle.abort();
        }
    }
}
