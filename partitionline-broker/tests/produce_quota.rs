//! Public controls for the opt-in router-wide Produce bandwidth policy.
#![allow(clippy::unwrap_used)]

use partitionline_broker::{
    catalog::{Catalog, TopicId},
    fetch, metadata::{self, ProduceQuota, QuotaRouter},
    produce,
    transport::{self, ExchangeControl, Handler, Peer, Transport},
};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::{Duration, Instant},
};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Notify};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch() -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("partitionline-quota-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    fs::create_dir(&root)?;
    let config = common();
    let (mut catalog, _) = Catalog::open(root.join("catalog"), config.catalog_limits)?;
    let mut id = [0;16]; id[15] = 2;
    catalog.create("alpha", TopicId::new(id)?, 2)?;
    drop(catalog);
    Ok(root)
}
fn common() -> metadata::Config {
    metadata::Config::new(0, "127.0.0.1".into(), 19095, "quota-test".into())
}
fn fixture(name: &str, suffix: &str) -> Result<Vec<u8>> {
    Ok(fs::read(Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/produce/4.3.1")
        .join(format!("{name}.{suffix}.bin")))?)
}
async fn open(root: &Path, millis: u32) -> Result<QuotaRouter> {
    Ok(QuotaRouter::open_with_read_store(
        root.join("catalog"), common(), produce::Config::new(root.join("parts")),
        fetch::Limits::default(), ProduceQuota::new(1, 1, millis)?,
    ).await?.0)
}
async fn send(socket: &mut TcpStream, bytes: &[u8]) -> Result {
    socket.write_all(&i32::try_from(bytes.len())?.to_be_bytes()).await?;
    socket.write_all(bytes).await?;
    Ok(())
}
async fn receive(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let size = socket.read_i32().await?;
    let mut bytes = vec![0; usize::try_from(size)?];
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}
#[derive(Debug, PartialEq, Eq)]
enum SocketObservation {
    NoData,
    Byte(u8),
    Eof,
}

async fn observe_socket(socket: &TcpStream, wait: Duration) -> Result<SocketObservation> {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(wait, socket.peek(&mut byte)).await {
        Err(_) => Ok(SocketObservation::NoData),
        Ok(Ok(0)) => Ok(SocketObservation::Eof),
        Ok(Ok(_)) => Ok(SocketObservation::Byte(byte[0])),
        Ok(Err(error)) => Err(error.into()),
    }
}

#[tokio::test]
async fn socket_probe_distinguishes_empty_data_and_eof() -> Result {
    let listener = TcpListener::bind(([127, 0, 0, 1], 0)).await?;
    let mut client = TcpStream::connect(listener.local_addr()?).await?;
    let (mut server, _) = listener.accept().await?;
    server.write_all(&[7]).await?;
    client.readable().await?;
    let mut byte = [0u8; 1];
    assert_eq!(client.try_read(&mut byte)?, 1);
    assert_eq!(byte[0], 7);
    assert_eq!(observe_socket(&client, Duration::from_millis(100)).await?, SocketObservation::NoData);
    server.write_all(&[9]).await?;
    assert_eq!(observe_socket(&client, Duration::from_secs(1)).await?, SocketObservation::Byte(9));
    client.read_exact(&mut byte).await?;
    server.shutdown().await?;
    assert_eq!(observe_socket(&client, Duration::from_secs(1)).await?, SocketObservation::Eof);
    Ok(())
}

fn metadata_request() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&3i16.to_be_bytes());
    bytes.extend_from_slice(&1i16.to_be_bytes());
    bytes.extend_from_slice(&77i32.to_be_bytes());
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_produce_goldens_get_only_the_writer_throttle_field() -> Result {
    for version in [3, 5, 6, 8, 9, 13] {
        let root = scratch()?;
        let router = open(&root, 500).await?;
        let name = format!("produce-v{version}-acks1");
        let request = fixture(&name, "request")?;
        let mut expected = fixture(&name, "response")?;
        let start = Instant::now();
        let (outcome, deadline) = router.dispatch_controlled(request).await?.into_parts();
        let response = outcome.response.ok_or("missing response")?;
        let field = expected.len().checked_sub(if version >= 9 { 5 } else { 4 }).ok_or("short response")?;
        expected[field..field + 4].copy_from_slice(&500i32.to_be_bytes());
        assert_eq!(response, expected, "version {version}");
        assert!(deadline.is_some_and(|until| until >= start + Duration::from_millis(500)));
        router.shutdown().await?;
        fs::remove_dir_all(root)?;
    }
    Ok(())
}

struct Observe {
    inner: Arc<QuotaRouter>,
    done: Arc<Notify>,
}
impl Handler for Observe {
    type Error = metadata::QuotaRouterError;
    async fn handle(&self, _request: Vec<u8>) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
        Err(metadata::QuotaRouterError::ControlledExchangeRequired)
    }
    async fn exchange_with_control(
        &self, peer: &Peer, request: Vec<u8>, spare: Vec<u8>,
    ) -> std::result::Result<ExchangeControl, Self::Error> {
        let outcome = Handler::exchange_with_control(self.inner.as_ref(), peer, request, spare).await?;
        self.done.notify_one();
        Ok(outcome)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_zero_mutes_a_pipelined_request_then_resumes() -> Result {
    let root = scratch()?;
    let router = Arc::new(open(&root, 800).await?);
    let done = Arc::new(Notify::new());
    let handler = Arc::new(Observe { inner: Arc::clone(&router), done: Arc::clone(&done) });
    let mut transport = Transport::bind(([127,0,0,1], 0).into(), transport::Config::default(), handler).await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture("produce-v3-acks0", "request")?).await?;
    tokio::time::timeout(Duration::from_secs(3), done.notified()).await?;
    send(&mut socket, &metadata_request()).await?;
    assert!(tokio::time::timeout(Duration::from_millis(100), socket.readable()).await.is_err(), "a muted connection read the next request early");
    let response = tokio::time::timeout(Duration::from_secs(3), receive(&mut socket)).await??;
    assert_eq!(&response[..4], &77i32.to_be_bytes());
    transport.shutdown().await?;
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttle_response_is_sent_before_mute_and_shutdown_interrupts_it() -> Result {
    let root = scratch()?;
    let router = Arc::new(open(&root, 2_000).await?);
    let done = Arc::new(Notify::new());
    let handler = Arc::new(Observe { inner: Arc::clone(&router), done: Arc::clone(&done) });
    let mut transport = Transport::bind(([127,0,0,1], 0).into(), transport::Config::default(), handler).await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture("produce-v6-acks1", "request")?).await?;
    tokio::time::timeout(Duration::from_secs(3), done.notified()).await?;
    let response = tokio::time::timeout(Duration::from_millis(500), receive(&mut socket)).await??;
    assert_eq!(i32::from_be_bytes(response[response.len()-4..].try_into()?), 2_000);
    send(&mut socket, &metadata_request()).await?;
    if let Ok(readiness) = tokio::time::timeout(Duration::from_millis(100), socket.readable()).await {
        readiness?;
        let actual = observe_socket(&socket, Duration::from_millis(100)).await?;
        panic!("readiness returned during mute; byte-level observation: {actual:?}");
    }
    tokio::time::timeout(Duration::from_secs(1), transport.shutdown()).await??;
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}
