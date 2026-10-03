//! Independent Apache read goldens and durable bounded ordinary snapshots.
#![allow(clippy::unwrap_used)]
use partitionline_broker::{
    catalog::{Catalog, TopicId},
    fetch,
    metadata::{self, Router},
    partition, produce,
    transport::{self, Transport},
};
use std::{
    error::Error as StdError,
    fs::{self, File},
    io::{Read, Write},
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
static NEXT: AtomicU64 = AtomicU64::new(0);
fn scratch(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "partitionline-fetch-{}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn common() -> metadata::Config {
    metadata::Config::new(0, "127.0.0.1".into(), 19095, "partitionline-fixture".into())
}
fn read(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(128 * 1024).read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    File::create(path)?.write_all(bytes)
}
fn base() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fetch")
}
async fn seed(root: PathBuf, log: bool, multi: bool) -> Result<(), Box<dyn StdError>> {
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn StdError + Send + Sync>> {
        fs::create_dir(&root)?;
        let (mut catalog, _) =
            Catalog::open(root.join("catalog.journal"), common().catalog_limits)?;
        let mut id = [0; 16];
        id[15] = 2;
        catalog.create("alpha", TopicId::new(id)?, 2)?;
        id[15] = 3;
        catalog.create("__consumer_offsets", TopicId::new(id)?, 1)?;
        if log {
            fs::create_dir(root.join("partitions"))?;
            let config = produce::Config::new(root.join("partitions"));
            let (mut part, _) = partition::Partition::open(
                root.join("partitions/00000000000000000000000000000002-0.journal"),
                0,
                config.journal_limits,
                config.record_limits,
            )?;
            let first = read(&base().join("4.3.1/log-batch-0.bin"))?;
            let second = read(&base().join("4.3.1/log-batch-3.bin"))?;
            if multi {
                let mut both = first;
                both.extend_from_slice(&second);
                part.append(&both)?;
            } else {
                part.append(&first)?;
                part.append(&second)?;
            }
        }
        Ok(())
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    Ok(())
}
async fn clean(root: PathBuf) -> Result<(), Box<dyn StdError>> {
    tokio::task::spawn_blocking(move || fs::remove_dir_all(root)).await??;
    Ok(())
}
async fn open(
    root: &Path,
    config: metadata::Config,
    limits: fetch::Limits,
) -> Result<Router, Box<dyn StdError>> {
    Ok(Router::open_with_read_store(
        root.join("catalog.journal"),
        config,
        produce::Config::new(root.join("partitions")),
        limits,
    )
    .await?
    .0)
}
async fn fixture(name: &str) -> Result<Vec<u8>, Box<dyn StdError>> {
    let path = base().join("4.3.1").join(format!("{name}.request.bin"));
    Ok(tokio::task::spawn_blocking(move || read(&path)).await??)
}
fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(D[usize::from(b >> 4)]));
        out.push(char::from(D[usize::from(b & 15)]));
    }
    out
}
fn header(key: i16, version: i16) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&key.to_be_bytes());
    b.extend_from_slice(&version.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b
}
fn string(b: &mut Vec<u8>, name: &str) {
    b.extend_from_slice(&(name.len() as i16).to_be_bytes());
    b.extend_from_slice(name.as_bytes());
}
fn fetch_request(
    version: i16,
    offset: i64,
    wait: i32,
    min: i32,
    max: i32,
    parts: &[(i32, i32)],
) -> Vec<u8> {
    let mut b = header(1, version);
    for x in [-1, wait, min, max] {
        b.extend_from_slice(&x.to_be_bytes());
    }
    b.push(0);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&(parts.len() as i32).to_be_bytes());
    for &(index, limit) in parts {
        b.extend_from_slice(&index.to_be_bytes());
        b.extend_from_slice(&offset.to_be_bytes());
        if version >= 5 {
            b.extend_from_slice(&0i64.to_be_bytes());
        }
        b.extend_from_slice(&limit.to_be_bytes());
    }
    b
}
fn metadata_request() -> Vec<u8> {
    let mut b = header(3, 1);
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b
}
fn delete() -> Vec<u8> {
    let mut b = header(20, 1);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&60000i32.to_be_bytes());
    b
}
fn create() -> Vec<u8> {
    let mut b = header(19, 4);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&2i32.to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&60000i32.to_be_bytes());
    b.push(0);
    b
}
fn produce(bytes: &[u8], index: i32) -> Vec<u8> {
    let mut b = header(0, 3);
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&60000i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&index.to_be_bytes());
    b.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    b.extend_from_slice(bytes);
    b
}
fn fetch_error(b: &[u8]) -> i16 {
    i16::from_be_bytes(b[27..29].try_into().unwrap())
}
fn watermark(b: &[u8]) -> i64 {
    i64::from_be_bytes(b[29..37].try_into().unwrap())
}
fn records(b: &[u8], v: i16) -> &[u8] {
    let at = if v >= 5 { 61 } else { 53 };
    let n = i32::from_be_bytes(b[at - 4..at].try_into().unwrap()) as usize;
    &b[at..at + n]
}
async fn poll_pending<F: std::future::Future>(
    f: std::pin::Pin<&mut F>,
) -> Result<(), Box<dyn StdError>> {
    let mut f = f;
    std::future::poll_fn(|cx| match f.as_mut().poll(cx) {
        std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(Err("waiter completed prematurely")),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn independent_apache_read_goldens() -> Result<(), Box<dyn StdError>> {
    let mut observations = Vec::new();
    let mut api_versions_cases = Vec::new();
    let mut count = 0;
    let capture = std::env::var_os("PARTITIONLINE_FETCH_RESPONSE_DIR").map(PathBuf::from);
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let dir = base().join(release);
        let lines = tokio::task::spawn_blocking({
            let dir = dir.clone();
            move || read(&dir.join("cases.tsv"))
        })
        .await??;
        let mut versions = Vec::new();
        for line in std::str::from_utf8(&lines)?
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let c: Vec<_> = line.split('\t').collect();
            assert_eq!(c.len(), 5);
            assert_eq!(c[3], "log");
            versions.push((c[1].parse::<i16>()?, c[2].parse::<i16>()?));
            let name = c[0];
            let root = scratch(name);
            seed(root.clone(), true, false).await?;
            let router = open(&root, common(), fetch::Limits::default()).await?;
            let (input, expected) = tokio::task::spawn_blocking({
                let dir = dir.clone();
                let name = name.to_owned();
                move || {
                    let path = dir.join(format!("{name}.response.bin"));
                    Ok::<_, std::io::Error>((
                        read(&dir.join(format!("{name}.request.bin")))?,
                        if path.exists() {
                            Some(read(&path)?)
                        } else {
                            None
                        },
                    ))
                }
            })
            .await??;
            let result = router.dispatch(input).await;
            let response_hex = match (c[4], result, expected) {
                ("response", Ok(Some(actual)), Some(expected)) => {
                    assert_eq!(actual, expected, "{release}/{name}");
                    if let Some(capture) = &capture {
                        let dir = capture.join(release);
                        let name = name.to_owned();
                        let bytes = actual.clone();
                        tokio::task::spawn_blocking(move || {
                            fs::create_dir_all(&dir)?;
                            write(&dir.join(format!("{name}.actual-response.bin")), &bytes)
                        })
                        .await??;
                    }
                    format!("\"{}\"", hex(&actual))
                }
                (
                    "structural_reject",
                    Err(metadata::Error::Fetch(
                        fetch::Error::Protocol(_) | fetch::Error::RequestCount,
                    )),
                    None,
                ) => "null".into(),
                (outcome, actual, expected) => {
                    return Err(format!(
                        "{release}/{name}: {outcome} actual{actual:?} expected{expected:?}"
                    )
                    .into())
                }
            };
            observations.push(format!("{{\"release\":\"{release}\",\"case\":\"{name}\",\"outcome\":\"{}\",\"response_hex\":{response_hex}}}",c[4]));
            router.shutdown().await?;
            clean(root).await?;
            count += 1;
        }
        let root = scratch("api-versions");
        seed(root.clone(), false, false).await?;
        let router = open(&root, common(), fetch::Limits::default()).await?;
        for version in 0..=4 {
            let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/metadata")
                .join(release)
                .join(format!("api-versions-v{version}.request.bin"));
            let input = tokio::task::spawn_blocking(move || read(&file)).await??;
            let correlation = i32::from_be_bytes(input[4..8].try_into().unwrap());
            let response = router.respond(input).await?;
            assert_eq!(
                advertisement(&response, version),
                vec![
                    (0, 3, 13),
                    (1, 4, 6),
                    (2, 1, 3),
                    (3, 0, 13),
                    (18, 0, 4),
                    (19, 2, 4),
                    (20, 1, 6)
                ]
            );
            api_versions_cases.push(format!("{{\"release\":\"{release}\",\"version\":{version},\"correlation_id\":{correlation},\"header_version\":0,\"response_hex\":\"{}\"}}",hex(&response)));
        }
        router.shutdown().await?;
        clean(root).await?;
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(
            versions,
            vec![(1, 4), (1, 5), (1, 6), (2, 1), (2, 2), (2, 3)]
        );
    }
    assert_eq!(count, 366);
    if let Some(path) = std::env::var_os("PARTITIONLINE_FETCH_REPORT") {
        let list = fetch::DATA_API_VERSIONS
            .iter()
            .map(|a| {
                format!(
                    "{{\"api_key\":{},\"min_version\":{},\"max_version\":{}}}",
                    a.api_key, a.min_version, a.max_version
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let data = format!(
            "{{\"schema_version\":1,\"read_write_api_versions\":[{list}],\"case_results\":[{}],\"api_versions_cases\":[{}]}}\n",
            observations.join(","),api_versions_cases.join(",")
        );
        tokio::task::spawn_blocking(move || write(Path::new(&path), data.as_bytes())).await??;
    }
    Ok(())
}

#[tokio::test]
async fn restart_and_containing_multi_batch_entry_keep_exact_records(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("restart-multi");
    seed(root.clone(), true, true).await?;
    for _ in 0..2 {
        let router = open(&root, common(), fetch::Limits::default()).await?;
        for offset in 0..=4 {
            let name = format!("fetch-v6-offset{offset}");
            let result = router.respond(fixture(&name).await?).await?;
            let path = base().join("4.3.1").join(format!("{name}.response.bin"));
            assert_eq!(
                result,
                tokio::task::spawn_blocking(move || read(&path)).await??
            );
        }
        let response = router
            .respond(fixture("list-offsets-v3-timestamp1003").await?)
            .await?;
        assert_eq!(
            i64::from_be_bytes(response[29..37].try_into().unwrap()),
            1007
        );
        assert_eq!(i64::from_be_bytes(response[37..45].try_into().unwrap()), 1);
        router.shutdown().await?;
    }
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn lazy_empty_reads_and_delete_recreate_resolve_current_identity(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("identity");
    seed(root.clone(), true, false).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(1, 10000)]))
        .await?;
    assert_eq!(watermark(&response), 0);
    assert!(records(&response, 6).is_empty());
    let dir = root.join("partitions");
    let count =
        tokio::task::spawn_blocking(move || fs::read_dir(dir).map(|files| files.count())).await??;
    assert_eq!(count, 1);
    router.respond(delete()).await?;
    router.respond(create()).await?;
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&response), 0);
    assert!(records(&response, 6).is_empty());
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-3.bin"))).await??;
    let ack = router.respond(produce(&batch, 0)).await?;
    assert_eq!(&ack[25..33], &0i64.to_be_bytes());
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&response), 1);
    assert_eq!(&records(&response, 6)[16..], &batch[16..]);
    assert_eq!(&records(&response, 6)[12..16], &0i32.to_be_bytes());
    router.shutdown().await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&response), 1);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn scan_and_hard_output_limits_fail_without_false_timestamp_miss(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("limits");
    seed(root.clone(), true, false).await?;
    for limits in [
        fetch::Limits::new(103, 100, 100)?,
        fetch::Limits::new(1000, 1, 100)?,
    ] {
        let router = open(&root, common(), limits).await?;
        assert!(matches!(
            router
                .respond(fixture("list-offsets-v3-timestamp1011").await?)
                .await,
            Err(metadata::Error::Fetch(fetch::Error::ScanLimit))
        ));
        let response = router
            .respond(fixture("list-offsets-v3-latest").await?)
            .await?;
        assert_eq!(i64::from_be_bytes(response[37..45].try_into().unwrap()), 4);
        router.shutdown().await?;
    }
    let mut cfg = common();
    cfg.max_response_bytes = 164;
    let router = open(&root, cfg, fetch::Limits::default()).await?;
    assert!(matches!(
        router
            .respond(fetch_request(6, 0, 0, 0, 1, &[(0, 1)]))
            .await,
        Err(metadata::Error::Fetch(fetch::Error::ResponseLimit))
    ));
    router.shutdown().await?;
    let mut cfg = common();
    cfg.max_response_bytes = 165;
    let router = open(&root, cfg, fetch::Limits::default()).await?;
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 1, &[(0, 1)]))
        .await?;
    assert_eq!(response.len(), 165);
    assert_eq!(records(&response, 6).len(), 104);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn wake_between_snapshot_and_receipt_is_not_lost() -> Result<(), Box<dyn StdError>> {
    let root = scratch("wake-race");
    seed(root.clone(), false, false).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let mut waiter = Box::pin(router.dispatch(fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    // FIFO barrier proves the empty snapshot has completed, while its receiver is
    // deliberately unpolled. A subsequent committed append must remain observable.
    barrier(&router).await?;
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-0.bin"))).await??;
    router.respond(produce(&batch, 0)).await?;
    let response = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await??
        .unwrap();
    assert_eq!(watermark(&response), 3);
    let mut expected = batch.clone();
    expected[12..16].copy_from_slice(&0i32.to_be_bytes());
    assert_eq!(records(&response, 6), expected);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn wait_keeps_admission_cancel_releases_and_shutdown_wakes() -> Result<(), Box<dyn StdError>>
{
    let root = scratch("wait-admission");
    seed(root.clone(), false, false).await?;
    let mut cfg = common();
    cfg.max_queued_requests = 1;
    let router = open(&root, cfg, fetch::Limits::default()).await?;
    let input = fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)]);
    let mut waiter = Box::pin(router.dispatch(input.clone()));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    poll_pending(waiter.as_mut()).await?;
    assert!(matches!(
        router.dispatch(input.clone()).await,
        Err(metadata::Error::QueueFull)
    ));
    drop(waiter);
    let mut waiter = Box::pin(router.dispatch(input));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    poll_pending(waiter.as_mut()).await?;
    router.shutdown().await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiter).await?,
        Err(metadata::Error::Stopped)
    ));
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn waiting_read_does_not_block_append_and_delete_wakes() -> Result<(), Box<dyn StdError>> {
    let root = scratch("wait-write");
    seed(root.clone(), false, false).await?;
    let mut cfg = common();
    cfg.max_queued_requests = 2;
    let router = open(&root, cfg, fetch::Limits::default()).await?;
    let mut waiter = Box::pin(router.dispatch(fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    poll_pending(waiter.as_mut()).await?;
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-0.bin"))).await??;
    let ack =
        tokio::time::timeout(Duration::from_secs(2), router.respond(produce(&batch, 0))).await??;
    assert_eq!(&ack[25..33], &0i64.to_be_bytes());
    let response = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await??
        .unwrap();
    let mut expected = batch.clone();
    expected[12..16].copy_from_slice(&0i32.to_be_bytes());
    assert_eq!(records(&response, 6), expected);
    let mut waiter = Box::pin(router.dispatch(fetch_request(6, 3, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    poll_pending(waiter.as_mut()).await?;
    router.respond(delete()).await?;
    let response = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await??
        .unwrap();
    assert_eq!(fetch_error(&response), 3);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn absolute_wait_cap_and_minimum_clamping() -> Result<(), Box<dyn StdError>> {
    let root = scratch("deadline");
    seed(root.clone(), false, false).await?;
    let router = open(&root, common(), fetch::Limits::new(1024, 10, 30)?).await?;
    let start = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        router.respond(fetch_request(6, 0, 60000, 100, 100, &[(0, 100)])),
    )
    .await??;
    assert!(start.elapsed() >= Duration::from_millis(25));
    assert!(records(&response, 6).is_empty());
    // The minimum is clamped to the request maximum, so max0 never waits.
    let response = tokio::time::timeout(
        Duration::from_millis(20),
        router.respond(fetch_request(6, 0, 60000, 100, 0, &[(0, 0)])),
    )
    .await??;
    assert!(records(&response, 6).is_empty());
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

async fn send(socket: &mut TcpStream, input: &[u8]) -> std::io::Result<()> {
    socket
        .write_all(&(input.len() as i32).to_be_bytes())
        .await?;
    socket.write_all(input).await
}
async fn receive(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let n = socket.read_i32().await?;
    if !(0..=128 * 1024).contains(&n) {
        return Err(std::io::Error::other("response size"));
    }
    let mut bytes = vec![0; n as usize];
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}
#[tokio::test]
async fn real_tcp_supported_goldens_and_unsupported_version_eof() -> Result<(), Box<dyn StdError>> {
    let root = scratch("tcp");
    seed(root.clone(), true, false).await?;
    let router = Arc::new(open(&root, common(), fetch::Limits::default()).await?);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    for name in [
        "fetch-v4-offset1",
        "fetch-v5-oversized-first-batch",
        "fetch-v6-read-committed-ordinary",
        "list-offsets-v1-timestamp1003",
        "list-offsets-v2-latest",
        "list-offsets-v3-read-committed-latest",
    ] {
        send(&mut socket, &fixture(name).await?).await?;
        let actual = receive(&mut socket).await?;
        let path = base().join("4.3.1").join(format!("{name}.response.bin"));
        assert_eq!(
            actual,
            tokio::task::spawn_blocking(move || read(&path)).await??
        );
    }
    for (key, version) in [(1, 3), (1, 7), (2, 0), (2, 4)] {
        let mut socket = TcpStream::connect(transport.local_addr()).await?;
        send(&mut socket, &header(key, version)).await?;
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await??,
            0
        );
    }
    transport.shutdown().await?;
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

/// Independent actual Apache/native peer harness; normal test runs are inert.
#[tokio::test]
async fn serve_live_probe() -> Result<(), Box<dyn StdError>> {
    let Some(port) = std::env::var_os("PARTITIONLINE_FETCH_LIVE_PORT") else {
        return Ok(());
    };
    let port: u16 = port.to_str().ok_or("port")?.parse()?;
    let root = PathBuf::from(std::env::var_os("PARTITIONLINE_FETCH_LIVE_DIR").ok_or("directory")?);
    let mut cfg = common();
    cfg.advertised_port = port;
    let store = match std::env::var_os("PARTITIONLINE_FETCH_LIVE_SEGMENTS") {
        None => produce::Config::new(root.join("partitions")),
        Some(value) if value == "1" => rolling_store(&root)?,
        Some(_) => return Err("PARTITIONLINE_FETCH_LIVE_SEGMENTS must be1 when set".into()),
    };
    let router = Arc::new(
        Router::open_with_read_store(
            root.join("catalog.journal"),
            cfg,
            store,
            fetch::Limits::default(),
        )
        .await?
        .0,
    );
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], port).into(),
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await?;
    tokio::task::spawn_blocking({
        let root = root.clone();
        move || write(&root.join("ready"), b"ready")
    })
    .await??;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let stop = tokio::task::spawn_blocking({
            let root = root.clone();
            move || root.join("stop").exists()
        })
        .await?;
        if stop || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    transport.shutdown().await?;
    router.shutdown().await?;
    Ok(())
}

async fn barrier(router: &Router) -> Result<(), Box<dyn StdError>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match router.respond(metadata_request()).await {
                Ok(_) => return Ok::<_, metadata::Error>(()),
                Err(metadata::Error::QueueFull) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn unpolled_snapshot_holds_combined_admission_and_cancel_releases(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("unpolled");
    seed(root.clone(), true, false).await?;
    let mut cfg = common();
    cfg.max_queued_requests = 1;
    let ceiling = cfg.protocol_limits.max_request_bytes() + cfg.max_response_bytes;
    assert!(matches!(
        Router::open_with_read_store(
            root.join("catalog.journal"),
            cfg.clone(),
            produce::Config::new(root.join("partitions")),
            fetch::Limits::default().with_retained_bytes(ceiling - 1)?
        )
        .await,
        Err(metadata::Error::Fetch(fetch::Error::InvalidLimits))
    ));
    let router = open(
        &root,
        cfg,
        fetch::Limits::default().with_retained_bytes(ceiling)?,
    )
    .await?;
    let input = fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]);
    let mut unpolled = Box::pin(router.dispatch(input.clone()));
    poll_pending(unpolled.as_mut()).await?;
    barrier(&router).await?;
    assert!(matches!(
        router.dispatch(input.clone()).await,
        Err(metadata::Error::QueueFull)
    ));
    drop(unpolled);
    let result = router.respond(input).await?;
    assert_eq!(watermark(&result), 4);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn registered_handle_corruption_poison_wakes_waiters_and_maps_legacy_errors(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("read-poison");
    seed(root.clone(), true, false).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let mut waiter = Box::pin(router.dispatch(fetch_request(6, 4, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    poll_pending(waiter.as_mut()).await?;
    let path = root.join("partitions/00000000000000000000000000000002-0.journal");
    tokio::task::spawn_blocking(move || {
        let mut bytes = read(&path)?;
        let end = bytes.len() - 1;
        bytes[end] ^= 1;
        write(&path, &bytes)
    })
    .await??;
    let response = router
        .respond(fetch_request(4, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(fetch_error(&response), 6);
    assert!(records(&response, 4).is_empty());
    let response = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await??
        .unwrap();
    assert_eq!(fetch_error(&response), 56);
    let response = router
        .respond(fetch_request(5, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(fetch_error(&response), 6);
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(1, 10000)]))
        .await?;
    assert_eq!(fetch_error(&response), 0);
    assert_eq!(watermark(&response), 0);
    router.shutdown().await?;
    assert!(matches!(
        Router::open_with_read_store(
            root.join("catalog.journal"),
            common(),
            produce::Config::new(root.join("partitions")),
            fetch::Limits::default()
        )
        .await,
        Err(metadata::Error::Produce(produce::Error::Partition(_)))
    ));
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn global_first_batch_exception_and_request_order_preserve_capped_reads(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("global-first");
    seed(root.clone(), true, true).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-3.bin"))).await??;
    router.respond(produce(&batch, 1)).await?;
    // One global exception, not one oversized exception per partition. Later
    // partitions keep their watermarks but have empty bytes until next fetch.
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 1, &[(1, 1), (0, 1)]))
        .await?;
    assert_eq!(records(&response, 6).len(), 78);
    let second = 61 + 78;
    assert_eq!(&response[second..second + 4], &0i32.to_be_bytes());
    assert_eq!(&response[second + 6..second + 14], &4i64.to_be_bytes());
    assert_eq!(&response[second + 34..second + 38], &0i32.to_be_bytes());
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 104, &[(0, 104)]))
        .await?;
    assert_eq!(records(&response, 6).len(), 104);
    let response = router
        .respond(fetch_request(6, 3, 0, 0, 78, &[(0, 78)]))
        .await?;
    assert_eq!(records(&response, 6), batch);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn transactional_control_and_idempotent_writes_cannot_change_committed_visibility(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("ordinary-committed");
    seed(root.clone(), true, false).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/produce/4.3.1");
    let rows = tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || read(&dir.join("cases.tsv"))
    })
    .await??;
    let mut checked = 0;
    for line in std::str::from_utf8(&rows)?.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 5 {
            continue;
        }
        if fields[0].contains("transaction")
            || fields[0].contains("control")
            || fields[0].contains("idempotent")
        {
            let file = dir.join(format!("{}.request.bin", fields[0]));
            let request = tokio::task::spawn_blocking(move || read(&file)).await??;
            let response = router.respond(request).await?;
            let at = if fields[2].parse::<i16>()? >= 9 {
                27
            } else {
                23
            };
            assert_ne!(
                i16::from_be_bytes(response[at..at + 2].try_into().unwrap()),
                0
            );
            checked += 1;
        }
    }
    assert!(checked >= 3);
    let response = router
        .respond(fixture("fetch-v6-read-committed-ordinary").await?)
        .await?;
    assert_eq!(&response[29..37], &4i64.to_be_bytes());
    assert_eq!(&response[37..45], &4i64.to_be_bytes());
    assert_eq!(&response[53..57], &0i32.to_be_bytes());
    let expected = base().join("4.3.1/fetch-v6-read-committed-ordinary.response.bin");
    assert_eq!(
        response,
        tokio::task::spawn_blocking(move || read(&expected)).await??
    );
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn transport_deadline_cancels_wait_and_releases_read_admission(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("tcp-cancel");
    seed(root.clone(), false, false).await?;
    let mut cfg = common();
    cfg.max_queued_requests = 1;
    let router = Arc::new(open(&root, cfg, fetch::Limits::default()).await?);
    let transport_config = transport::Config::new(
        4,
        4,
        128 * 1024,
        128 * 1024,
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(2),
    )?;
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport_config,
        Arc::clone(&router),
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(
        &mut socket,
        &fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)]),
    )
    .await?;
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await??,
        0
    );
    // Deadline drops the handler future and its held data permit. A fresh socket
    // can append immediately and retrieve the same exact protected record bytes.
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-0.bin"))).await??;
    send(&mut socket, &produce(&batch, 0)).await?;
    let ack = receive(&mut socket).await?;
    assert_eq!(&ack[25..33], &0i64.to_be_bytes());
    send(
        &mut socket,
        &fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]),
    )
    .await?;
    let response = receive(&mut socket).await?;
    assert_eq!(&records(&response, 6)[16..], &batch[16..]);
    let report = transport.shutdown().await?;
    assert_eq!(report.handler_deadlines, 1);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn transport_joined_shutdown_cancels_live_long_poll_without_stopping_store(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("tcp-stop");
    seed(root.clone(), false, false).await?;
    let mut cfg = common();
    cfg.max_queued_requests = 1;
    let router = Arc::new(open(&root, cfg, fetch::Limits::default()).await?);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(
        &mut socket,
        &fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)]),
    )
    .await?;
    // A second data operation observes saturation only after the first socket's
    // handler has actually obtained admission. No scheduler delay is guessed.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                router
                    .respond(fetch_request(6, 0, 0, 0, 0, &[(0, 0)]))
                    .await,
                Err(metadata::Error::QueueFull)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let report = tokio::time::timeout(Duration::from_secs(2), transport.shutdown()).await??;
    assert_eq!(report.worker_failures, 0);
    let mut byte = [0];
    assert_eq!(socket.read(&mut byte).await?, 0);
    let result = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&result), 0);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

fn advertisement(response: &[u8], version: i16) -> Vec<(i16, i16, i16)> {
    assert_eq!(&response[4..6], &0i16.to_be_bytes());
    let (count, mut at) = if version >= 3 {
        (usize::from(response[6]) - 1, 7)
    } else {
        (
            i32::from_be_bytes(response[6..10].try_into().unwrap()) as usize,
            10,
        )
    };
    let mut result = Vec::new();
    for _ in 0..count {
        result.push((
            i16::from_be_bytes(response[at..at + 2].try_into().unwrap()),
            i16::from_be_bytes(response[at + 2..at + 4].try_into().unwrap()),
            i16::from_be_bytes(response[at + 4..at + 6].try_into().unwrap()),
        ));
        at += 6;
        if version >= 3 {
            assert_eq!(response[at], 0);
            at += 1;
        }
    }
    if version >= 1 {
        assert_eq!(&response[at..at + 4], &0i32.to_be_bytes());
        at += 4;
    }
    if version >= 3 {
        assert_eq!(response[at], 0);
        at += 1;
    }
    assert_eq!(at, response.len());
    result
}

#[tokio::test]
async fn explicit_read_profile_preserves_metadata_and_produce_advertisements(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("profiles");
    seed(root.clone(), false, false).await?;
    let metadata = Router::open(root.join("catalog.journal"), common())
        .await?
        .0;
    for version in 0..=4 {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/metadata/4.3.1");
        let (request, expected) = tokio::task::spawn_blocking(move || {
            Ok::<_, std::io::Error>((
                read(&dir.join(format!("api-versions-v{version}.request.bin")))?,
                read(&dir.join(format!("api-versions-v{version}.response.bin")))?,
            ))
        })
        .await??;
        assert_eq!(metadata.respond(request).await?, expected);
    }
    for key in [0, 1, 2] {
        assert!(
            matches!(metadata.respond(header(key,3)).await,Err(metadata::Error::Protocol(partitionline_broker::protocol::Error::UnimplementedApi(k))) if k==key)
        );
    }
    metadata.shutdown().await?;
    let producer = Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        produce::Config::new(root.join("partitions")),
    )
    .await?
    .0;
    for version in 0..=4 {
        let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/metadata/4.3.1")
            .join(format!("api-versions-v{version}.request.bin"));
        let request = tokio::task::spawn_blocking(move || read(&file)).await??;
        assert_eq!(
            advertisement(&producer.respond(request).await?, version),
            vec![(0, 3, 13), (3, 0, 13), (18, 0, 4), (19, 2, 4), (20, 1, 6)]
        );
    }
    for key in [1, 2] {
        assert!(
            matches!(producer.respond(header(key,4)).await,Err(metadata::Error::Protocol(partitionline_broker::protocol::Error::UnimplementedApi(k))) if k==key)
        );
    }
    producer.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[tokio::test]
async fn malformed_counts_prefixes_duplicates_and_semantic_bounds_never_mutate(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("malformed");
    seed(root.clone(), true, false).await?;
    let router = open(&root, common(), fetch::Limits::default()).await?;
    let canonical = fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]);
    for n in 4..canonical.len() {
        assert!(
            router.respond(canonical[..n].to_vec()).await.is_err(),
            "prefix{n}"
        );
    }
    for count in [-1, i32::MAX] {
        let mut input = canonical.clone();
        input[27..31].copy_from_slice(&count.to_be_bytes());
        assert!(router.respond(input).await.is_err());
    }
    for at in [10, 14, 18, 22] {
        let mut input = canonical.clone();
        input[at..at + 4].copy_from_slice(&(-2i32).to_be_bytes());
        let response = router.respond(input).await?;
        assert_eq!(fetch_error(&response), 42);
    }
    let mut input = canonical.clone();
    input[26] = 2;
    assert_eq!(fetch_error(&router.respond(input).await?), 42);
    let response = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000), (0, 10000)]))
        .await?;
    assert_eq!(fetch_error(&response), 42);
    assert!(records(&response, 6).is_empty());
    let second = 61;
    assert_eq!(&response[second + 4..second + 6], &42i16.to_be_bytes());
    let response = router.respond(canonical).await?;
    assert_eq!(watermark(&response), 4);
    let expected = base().join("4.3.1/log-batch-0.bin");
    assert_eq!(
        &records(&response, 6)[..104],
        tokio::task::spawn_blocking(move || read(&expected)).await??
    );
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

#[test]
fn invalid_local_read_limits_are_rejected_before_io() {
    for bounds in [
        (0, 1, 1),
        (64 * 1024 * 1024 + 1, 1, 1),
        (1, 0, 1),
        (1, 65537, 1),
        (1, 1, 0),
        (1, 1, 600001),
    ] {
        assert!(matches!(
            fetch::Limits::new(bounds.0, bounds.1, bounds.2),
            Err(fetch::Error::InvalidLimits)
        ));
    }
    for bytes in [0, 1024 * 1024 * 1024 + 1] {
        assert!(matches!(
            fetch::Limits::default().with_retained_bytes(bytes),
            Err(fetch::Error::InvalidLimits)
        ));
    }
}

fn rolling_store(root: &Path) -> Result<produce::Config, Box<dyn StdError>> {
    let mut config = produce::Config::new(root.join("partitions"));
    config.segment_limits = Some(partitionline_broker::segments::Limits::new(
        150,
        64,
        4096,
        16,
        1 << 30,
        2 * 1024 * 1024,
        64 * 1024 * 1024,
    )?);
    Ok(config)
}
async fn open_rolling(root: &Path, limits: fetch::Limits) -> Result<Router, Box<dyn StdError>> {
    Ok(Router::open_with_read_store(
        root.join("catalog.journal"),
        common(),
        rolling_store(root)?,
        limits,
    )
    .await?
    .0)
}
async fn seed_rolling(root: PathBuf) -> Result<(), Box<dyn StdError>> {
    seed(root.clone(), false, false).await?;
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn StdError + Send + Sync>> {
        fs::create_dir(root.join("partitions"))?;
        let config = produce::Config::new(root.join("partitions"));
        let limits = partitionline_broker::segments::Limits::new(
            150,
            64,
            4096,
            16,
            1 << 30,
            2 * 1024 * 1024,
            64 * 1024 * 1024,
        )?;
        let (mut part, _) = partition::Partition::open_segmented(
            root.join("partitions/00000000000000000000000000000002-0.segments"),
            0,
            config.journal_limits,
            config.record_limits,
            limits,
        )?;
        part.append(&read(&base().join("4.3.1/log-batch-0.bin"))?)?;
        part.append(&read(&base().join("4.3.1/log-batch-3.bin"))?)?;
        assert_eq!(part.segment_count(), 2);
        Ok(())
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    Ok(())
}
#[tokio::test]
async fn rolling_real_router_replays_all_independent_read_goldens() -> Result<(), Box<dyn StdError>>
{
    let mut observations = Vec::new();
    let capture = std::env::var_os("PARTITIONLINE_SEGMENTS_RESPONSE_DIR").map(PathBuf::from);
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base().join(release);
        let lines = tokio::task::spawn_blocking({
            let d = directory.clone();
            move || read(&d.join("cases.tsv"))
        })
        .await??;
        for line in std::str::from_utf8(&lines)?
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let c: Vec<_> = line.split('\t').collect();
            assert_eq!(c.len(), 5);
            let name = c[0];
            let root = scratch(name);
            seed_rolling(root.clone()).await?;
            let router = open_rolling(&root, fetch::Limits::default()).await?;
            let (input, expected) = tokio::task::spawn_blocking({
                let d = directory.clone();
                let n = name.to_owned();
                move || {
                    Ok::<_, std::io::Error>((
                        read(&d.join(format!("{n}.request.bin")))?,
                        if d.join(format!("{n}.response.bin")).exists() {
                            Some(read(&d.join(format!("{n}.response.bin")))?)
                        } else {
                            None
                        },
                    ))
                }
            })
            .await??;
            let result = router.dispatch(input).await;
            let response_hex = match (c[4], result, expected) {
                ("response", Ok(Some(actual)), Some(expected)) => {
                    assert_eq!(actual, expected, "rolling {release}/{name}");
                    if let Some(capture) = &capture {
                        let d = capture.join(release);
                        let n = name.to_owned();
                        let b = actual.clone();
                        tokio::task::spawn_blocking(move || {
                            fs::create_dir_all(&d)?;
                            write(&d.join(format!("{n}.actual-response.bin")), &b)
                        })
                        .await??;
                    }
                    format!("\"{}\"", hex(&actual))
                }
                (
                    "structural_reject",
                    Err(metadata::Error::Fetch(
                        fetch::Error::Protocol(_) | fetch::Error::RequestCount,
                    )),
                    None,
                ) => "null".into(),
                (outcome, actual, expected) => {
                    return Err(format!(
                        "rolling {release}/{name} {outcome}: actual{actual:?} expected{expected:?}"
                    )
                    .into())
                }
            };
            observations.push(format!("{{\"release\":\"{release}\",\"case\":\"{name}\",\"outcome\":\"{}\",\"response_hex\":{response_hex}}}",c[4]));
            router.shutdown().await?;
            clean(root).await?;
        }
    }
    assert_eq!(observations.len(), 366);
    if let Some(out) = std::env::var_os("PARTITIONLINE_SEGMENTS_REPORT") {
        let bytes=format!("{{\"schema\":1,\"scope\":\"actual rolling Router7; data/index recovery certified before dispatch; original366 Apache read fixtures\",\"roll_bytes\":150,\"case_count\":366,\"case_results\":[{}]}}\n",observations.join(",")).into_bytes();
        let out = PathBuf::from(out);
        tokio::task::spawn_blocking(move || write(&out, &bytes)).await??;
    }
    Ok(())
}
#[tokio::test]
async fn rolling_actual_tcp_restart_keeps_whole_batch_offsets_and_oversized_rules(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("rolling-tcp");
    seed_rolling(root.clone()).await?;
    for _ in 0..2 {
        let router = Arc::new(open_rolling(&root, fetch::Limits::default()).await?);
        let mut transport = Transport::bind(
            ([127, 0, 0, 1], 0).into(),
            transport::Config::default(),
            Arc::clone(&router),
        )
        .await?;
        let mut socket = TcpStream::connect(transport.local_addr()).await?;
        for name in [
            "fetch-v4-offset1",
            "fetch-v5-oversized-first-batch",
            "fetch-v6-offset3",
            "list-offsets-v1-timestamp1003",
            "list-offsets-v2-latest",
            "list-offsets-v3-timestamp1011",
        ] {
            send(&mut socket, &fixture(name).await?).await?;
            let actual = receive(&mut socket).await?;
            let p = base().join("4.3.1").join(format!("{name}.response.bin"));
            assert_eq!(
                actual,
                tokio::task::spawn_blocking(move || read(&p)).await??
            );
        }
        transport.shutdown().await?;
        router.shutdown().await?;
    }
    clean(root).await?;
    Ok(())
}
#[tokio::test]
async fn rolling_lost_append_receipt_and_delete_wake_without_blocking_actor(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("rolling-lost-receipt");
    seed_rolling(root.clone()).await?;
    let router = open_rolling(&root, fetch::Limits::default()).await?;
    let mut waiter = Box::pin(router.respond(fetch_request(6, 4, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    let batch =
        tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-3.bin"))).await??;
    let mut append = Box::pin(router.respond(produce(&batch, 0)));
    poll_pending(append.as_mut()).await?;
    barrier(&router).await?;
    drop(append);
    let result = tokio::time::timeout(Duration::from_secs(2), waiter).await??;
    assert_eq!(watermark(&result), 5);
    let mut expected = batch;
    expected[..8].copy_from_slice(&4i64.to_be_bytes());
    expected[12..16].copy_from_slice(&0i32.to_be_bytes());
    assert_eq!(records(&result, 6), expected);
    let mut waiter = Box::pin(router.respond(fetch_request(6, 5, 30000, 1, 10000, &[(0, 10000)])));
    poll_pending(waiter.as_mut()).await?;
    barrier(&router).await?;
    router.respond(delete()).await?;
    assert_eq!(
        fetch_error(&tokio::time::timeout(Duration::from_secs(2), waiter).await??),
        3
    );
    router.respond(create()).await?;
    let basic = include_bytes!("fixtures/records/valid-basic.bin");
    router.respond(produce(basic, 0)).await?;
    let fresh = router
        .respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&fresh), 1);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}
#[tokio::test]
async fn rolling_hidden_checkpoint_work_exhausts_request_budget_without_false_miss(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("rolling-scan-budget");
    seed(root.clone(), false, false).await?;
    tokio::task::spawn_blocking({
        let root = root.clone();
        move || -> Result<(), Box<dyn StdError + Send + Sync>> {
            fs::create_dir(root.join("partitions"))?;
            let cfg = produce::Config::new(root.join("partitions"));
            let limits = partitionline_broker::segments::Limits::new(
                10000,
                16,
                3,
                3,
                1 << 30,
                2 * 1024 * 1024,
                64 * 1024 * 1024,
            )?;
            let (mut p, _) = partition::Partition::open_segmented(
                root.join("partitions/00000000000000000000000000000002-0.segments"),
                0,
                cfg.journal_limits,
                cfg.record_limits,
                limits,
            )?;
            for _ in 0..4 {
                p.append(include_bytes!("fixtures/records/valid-basic.bin"))?;
            }
            Ok(())
        }
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    let mut store = rolling_store(&root)?;
    store.segment_limits = Some(partitionline_broker::segments::Limits::new(
        10000,
        16,
        3,
        3,
        1 << 30,
        2 * 1024 * 1024,
        64 * 1024 * 1024,
    )?);
    let router = Router::open_with_read_store(
        root.join("catalog.journal"),
        common(),
        store,
        fetch::Limits::new(10000, 1, 1000)?,
    )
    .await?
    .0;
    assert!(matches!(
        router
            .respond(fetch_request(6, 2, 0, 0, 10000, &[(0, 10000)]))
            .await,
        Err(metadata::Error::Fetch(fetch::Error::ScanLimit))
    ));
    let response = router
        .respond(fetch_request(6, 3, 0, 0, 10000, &[(0, 10000)]))
        .await?;
    assert_eq!(watermark(&response), 4);
    assert_eq!(records(&response, 6).len(), 74);
    router.shutdown().await?;
    clean(root).await?;
    Ok(())
}

// Exact actual Apache component payloads and applicable from-offset-zero
// timestamp outcomes, independently generated by SegmentIndexProbe.java.
// Source pin afaf4ea94eb958616445708d2d2f6846250a5e50; actual 4.3.1-run-1.json
// SHA256 a792fd632681ab4620c0b623b96819769caf02c71952817f206261d0cd375458. All three pinned releases and reopens agree.
// Negative component searches and explicit from_offset are not Kafka wire
// ListOffsets cases. Their scope stays with the separate Apache receipt.
const APACHE_ROLLED_BATCHES: [&str; 6] = [
    "000000000000000000000053ffffffff02ba243d3100000000000100000000000003e800000000000003efffffffffffffffffffffffffffff0000000220000000086b6579300c76616c7565300020000e02086b6579310c76616c75653100",
    "000000000000000200000053ffffffff02547b604d00000000000100000000000003eb00000000000003efffffffffffffffffffffffffffff0000000220000000086b6579320c76616c7565320020000802086b6579330c76616c75653300",
    "000000000000000400000053ffffffff020e4ca88200000000000100000000000003e300000000000003f2ffffffffffffffffffffffffffff0000000220000000086b6579340c76616c7565340020001e02086b6579350c76616c75653500",
    "000000000000000600000053ffffffff02984f2aaa00000000000100000000000003f000000000000003f2ffffffffffffffffffffffffffff0000000220000000086b6579360c76616c7565360020000402086b6579370c76616c75653700",
    "000000000000000800000053ffffffff02f8bcae0700000000000100000000000003ee00000000000003f4ffffffffffffffffffffffffffff0000000220000000086b6579380c76616c7565380020000c02086b6579390c76616c75653900",
    "000000000000000a00000057ffffffff026185160000000000000100000000000003f400000000000003f4ffffffffffffffffffffffffffff00000002240000000a6b657931300e76616c7565313000240017020a6b657931310e76616c7565313100",
];
const APACHE_ROLLED_TIMESTAMPS: [(i64, i64, i64); 15] = [
    (0, 1000, 0),
    (994, 1000, 0),
    (995, 1000, 0),
    (999, 1000, 0),
    (1000, 1000, 0),
    (1001, 1007, 1),
    (1003, 1007, 1),
    (1006, 1007, 1),
    (1007, 1007, 1),
    (1008, 1010, 5),
    (1010, 1010, 5),
    (1011, 1012, 9),
    (1012, 1012, 9),
    (1013, -1, -1),
    (9223372036854775807, -1, -1),
];
fn apache_rolled_batches() -> Result<Vec<Vec<u8>>, Box<dyn StdError>> {
    APACHE_ROLLED_BATCHES
        .iter()
        .map(|hex| {
            hex.as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let digit = |x: u8| match x {
                        b'0'..=b'9' => Ok(x - b'0'),
                        b'a'..=b'f' => Ok(x - b'a' + 10),
                        _ => Err("invalid pinned hex"),
                    };
                    Ok((digit(pair[0])? << 4) | digit(pair[1])?)
                })
                .collect::<Result<Vec<u8>, &str>>()
                .map_err(|e| e.into())
        })
        .collect()
}
fn oracle_rolling_store(root: &Path) -> Result<produce::Config, Box<dyn StdError>> {
    let mut config = produce::Config::new(root.join("partitions"));
    config.journal_limits = partitionline_broker::journal::Limits::new(1024, 8192, 16, 4096)?;
    config.segment_limits = Some(partitionline_broker::segments::Limits::new(
        400,
        8,
        2,
        1,
        1024 * 1024,
        64 * 1024,
        4096,
    )?);
    Ok(config)
}
#[tokio::test]
async fn authentic_apache_rolled_records_match_real_tcp_timestamp_and_whole_batch_seeks(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("rolling-apache-component-reference");
    seed(root.clone(), false, false).await?;
    let batches = apache_rolled_batches()?;
    let mut assigned = batches.clone();
    for batch in &mut assigned {
        // Produce assigns the known single-node leader epoch outside the CRC.
        batch[12..16].copy_from_slice(&0i32.to_be_bytes());
    }
    let mut observations = Vec::new();
    for round in 0..2 {
        let router = Arc::new(
            Router::open_with_read_store(
                root.join("catalog.journal"),
                common(),
                oracle_rolling_store(&root)?,
                fetch::Limits::default(),
            )
            .await?
            .0,
        );
        let mut transport = Transport::bind(
            ([127, 0, 0, 1], 0).into(),
            transport::Config::default(),
            Arc::clone(&router),
        )
        .await?;
        let mut socket = TcpStream::connect(transport.local_addr()).await?;
        if round == 0 {
            for batch in &batches {
                send(&mut socket, &produce(batch, 0)).await?;
                let response = receive(&mut socket).await?;
                assert_eq!(i16::from_be_bytes(response[23..25].try_into()?), 0);
                let expected = i64::from_be_bytes(batch[..8].try_into()?);
                assert_eq!(i64::from_be_bytes(response[25..33].try_into()?), expected);
            }
        }
        for offset in 0..=12 {
            // A nonzero byte cap below one batch still returns the first whole
            // batch, including the batch containing an interior offset.
            let request = fetch_request(6, offset, 0, 0, 1, &[(0, 1)]);
            send(&mut socket, &request).await?;
            let actual = receive(&mut socket).await?;
            assert_eq!(fetch_error(&actual), 0);
            assert_eq!(watermark(&actual), 12);
            let expected = if offset == 12 {
                &[][..]
            } else {
                &assigned[offset as usize / 2][..]
            };
            assert_eq!(records(&actual, 6), expected, "round{round}/offset{offset}");
            observations.push(format!("{{\"kind\":\"fetch\",\"round\":{round},\"offset\":{offset},\"request_hex\":\"{}\",\"response_hex\":\"{}\"}}", hex(&request), hex(&actual)));
        }
        for &(query, timestamp, offset) in &APACHE_ROLLED_TIMESTAMPS {
            let mut request = fixture("list-offsets-v3-timestamp1003").await?;
            let end = request.len();
            request[end - 8..].copy_from_slice(&query.to_be_bytes());
            send(&mut socket, &request).await?;
            let actual = receive(&mut socket).await?;
            let mut expected = tokio::task::spawn_blocking(|| {
                read(&base().join("4.3.1/list-offsets-v3-timestamp1003.response.bin"))
            })
            .await??;
            let end = expected.len();
            expected[end - 16..end - 8].copy_from_slice(&timestamp.to_be_bytes());
            expected[end - 8..].copy_from_slice(&offset.to_be_bytes());
            assert_eq!(actual, expected, "round{round}/timestamp{query}");
            observations.push(format!("{{\"kind\":\"timestamp\",\"round\":{round},\"query\":{query},\"expected_timestamp\":{timestamp},\"expected_offset\":{offset},\"request_hex\":\"{}\",\"response_hex\":\"{}\"}}", hex(&request), hex(&actual)));
        }
        transport.shutdown().await?;
        router.shutdown().await?;
        if round == 0 {
            let root_copy = root.clone();
            tokio::task::spawn_blocking(move || -> Result<(), Box<dyn StdError + Send + Sync>> {
                let config = oracle_rolling_store(&root_copy)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let (mut partition, recovery) = partition::Partition::open_segmented(
                    root_copy.join("partitions/00000000000000000000000000000002-0.segments"),
                    0,
                    config.journal_limits,
                    config.record_limits,
                    config.segment_limits.unwrap(),
                )?;
                assert_eq!(partition.segment_count(), 3);
                assert_eq!(recovery.next_offset, 12);
                // Explicit owner maintenance, between stopped serving sessions.
                // This is identical generation replacement, not compaction.
                partition.replace_sealed(0)?;
                Ok(())
            })
            .await?
            .map_err(|e| e as Box<dyn StdError>)?;
        }
    }
    if let Some(out) = std::env::var_os("PARTITIONLINE_SEGMENTS_ORACLE_REPORT") {
        let out = PathBuf::from(out);
        let bytes = format!("{{\"schema\":1,\"oracle_source\":\"afaf4ea94eb958616445708d2d2f6846250a5e50\",\"oracle_json_sha256\":\"a792fd632681ab4620c0b623b96819769caf02c71952817f206261d0cd375458\",\"scope\":\"actual TCP ordinary Produce/Fetch/ListOffsets with three rolling segments, identical sealed replacement and restart;15 applicable normal from0 timestamp queries per round;whole containing batch min-one;component negative/from-offset/partial-slice cases excluded\",\"timestamp_cases\":30,\"fetch_cases\":26,\"case_results\":[{}]}}\n", observations.join(",")).into_bytes();
        tokio::task::spawn_blocking(move || write(&out, &bytes)).await??;
    }
    clean(root).await?;
    Ok(())
}
