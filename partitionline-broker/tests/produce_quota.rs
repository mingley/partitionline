//! Socket controls for plaintext Produce pacing.

use partitionline_broker::{
    catalog::{Catalog, TopicId},
    metadata, produce,
    quotas::ProduceQuota,
    transport::{self, Transport},
};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
static NEXT: AtomicU64 = AtomicU64::new(0);
const METADATA: [u8; 14] = [0, 3, 0, 1, 0, 0, 0, 77, 255, 255, 255, 255, 255, 255];

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "partitionline-wire-quota-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn listen(root: &Path, quota: ProduceQuota) -> Result<(Arc<metadata::Router>, Transport)> {
    let router_config = metadata::Config::new(0, "127.0.0.1".into(), 19095, "quota-fixture".into());
    let path = root.join("catalog");
    let (mut catalog, _) = Catalog::open(&path, router_config.catalog_limits)?;
    let mut id = [0; 16];
    id[15] = 2;
    catalog.create("alpha", TopicId::new(id)?, 2)?;
    drop(catalog);
    let (router, _) = metadata::Router::open_with_store(
        path,
        router_config,
        produce::Config::new(root.join("parts")),
    )
    .await?;
    let router = Arc::new(router);
    let transport = Transport::bind_with_produce_quota(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::new(
            4,
            1,
            1024 * 1024,
            1024 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )?,
        Arc::clone(&router),
        quota,
    )
    .await?;
    Ok((router, transport))
}

fn fixture(version: i16, acks: i16, suffix: &str) -> Result<Vec<u8>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/produce/4.3.1")
        .join(format!("produce-v{version}-acks{acks}.{suffix}.bin"));
    Ok(fs::read(path)?)
}

fn with_label(mut request: Vec<u8>, label: &[u8; 14]) -> Vec<u8> {
    assert_eq!(&request[8..10], &14i16.to_be_bytes());
    request[10..24].copy_from_slice(label);
    request
}

async fn send(socket: &mut TcpStream, bytes: &[u8]) -> Result {
    socket.write_i32(i32::try_from(bytes.len())?).await?;
    socket.write_all(bytes).await?;
    Ok(())
}

async fn receive(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let read = async {
        let size = usize::try_from(socket.read_i32().await?)?;
        if !(4..=1024 * 1024).contains(&size) {
            return Err("unexpected response length".into());
        }
        let mut bytes = vec![0; size];
        socket.read_exact(&mut bytes).await?;
        Ok(bytes)
    };
    tokio::time::timeout(Duration::from_secs(10), read).await?
}

fn clear_throttle(version: i16, response: &mut [u8]) -> Result<i32> {
    let start = response
        .len()
        .checked_sub(if version >= 9 { 5 } else { 4 })
        .ok_or("short Produce response")?;
    let bytes = &mut response[start..start + 4];
    let millis = i32::from_be_bytes((&*bytes).try_into()?);
    bytes.fill(0);
    Ok(millis)
}

async fn stop(mut transport: Transport, router: Arc<metadata::Router>) -> Result {
    let report = transport.shutdown().await?;
    router.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert_eq!(report.worker_failures, 0);
    assert_eq!(report.handler_errors, 0);
    Ok(())
}

#[tokio::test]
async fn every_version_changes_only_the_throttle_field() -> Result {
    for version in 3..=13 {
        for acks in [-1, 1] {
            let root = Scratch::new()?;
            let quota = ProduceQuota::new(1, 1, 60_000, &[])?;
            let (router, transport) = listen(&root.0, quota).await?;
            let mut socket = TcpStream::connect(transport.local_addr()).await?;
            send(&mut socket, &fixture(version, acks, "request")?).await?;
            let mut response = receive(&mut socket).await?;
            let millis = clear_throttle(version, &mut response)?;
            assert!(
                (50_000..=60_000).contains(&millis),
                "Produce v{version} acks={acks} throttle was {millis}"
            );
            assert_eq!(
                response,
                fixture(version, acks, "response")?,
                "Produce v{version} acks={acks}"
            );
            stop(transport, router).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn acks_zero_paces_the_next_request_and_appends_once() -> Result {
    let root = Scratch::new()?;
    let quota = ProduceQuota::new(1, 1, 400, &[])?;
    let (router, transport) = listen(&root.0, quota).await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture(9, 0, "request")?).await?;
    send(&mut socket, &METADATA).await?;
    let mut probe = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.peek(&mut probe))
            .await
            .is_err(),
        "a response or EOF arrived before the acks=0 pause"
    );
    let response = receive(&mut socket).await?;
    assert_eq!(&response[..4], &77i32.to_be_bytes());

    send(&mut socket, &fixture(3, 1, "request")?).await?;
    let mut second = receive(&mut socket).await?;
    assert!(clear_throttle(3, &mut second)? > 0);
    let mut expected = fixture(3, 1, "response")?;
    expected[25..33].copy_from_slice(&1i64.to_be_bytes());
    assert_eq!(second, expected);
    stop(transport, router).await
}

#[tokio::test]
async fn configured_labels_share_debt_and_release_the_handler_slot() -> Result {
    let root = Scratch::new()?;
    let alpha = with_label(fixture(3, 1, "request")?, b"quota-alpha-v3");
    let bravo = with_label(fixture(3, 1, "request")?, b"quota-bravo-v3");
    let quota = ProduceQuota::new(
        20,
        u64::try_from(alpha.len())?,
        8_000,
        &["quota-alpha-v3", "quota-bravo-v3"],
    )?;
    let (router, transport) = listen(&root.0, quota).await?;
    let addr = transport.local_addr();
    let mut first = TcpStream::connect(addr).await?;
    let mut second = TcpStream::connect(addr).await?;
    let mut independent = TcpStream::connect(addr).await?;
    send(&mut first, &alpha).await?;
    assert_eq!(clear_throttle(3, &mut receive(&mut first).await?)?, 0);
    send(&mut second, &alpha).await?;
    let mut response = receive(&mut second).await?;
    let millis = clear_throttle(3, &mut response)?;
    assert!(millis >= 1_000, "another alpha connection did not share debt");
    let mut expected = fixture(3, 1, "response")?;
    expected[25..33].copy_from_slice(&1i64.to_be_bytes());
    assert_eq!(response, expected);

    send(&mut second, &METADATA).await?;
    send(&mut independent, &bravo).await?;
    let mut response = receive(&mut independent).await?;
    assert_eq!(clear_throttle(3, &mut response)?, 0);
    expected[25..33].copy_from_slice(&2i64.to_be_bytes());
    assert_eq!(response, expected);
    // The only handler slot already served Bravo while Alpha stayed paused.
    let mut probe = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), second.peek(&mut probe))
            .await
            .is_err(),
        "Alpha's queued Metadata response arrived during the pause"
    );
    stop(transport, router).await
}
