//! Independent Apache Produce goldens, durable identities, bounds and wire outcomes.
#![allow(clippy::unwrap_used)]
use partitionline_broker::{
    catalog::{Catalog, TopicId},
    journal,
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
        "partitionline-produce-{}-{}-{label}",
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
async fn setup(root: PathBuf) -> Result<(), Box<dyn StdError>> {
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn StdError + Send + Sync>> {
        fs::create_dir(&root)?;
        let (mut catalog, _) =
            Catalog::open(root.join("catalog.journal"), common().catalog_limits)?;
        let mut id = [0; 16];
        id[15] = 2;
        catalog.create("alpha", TopicId::new(id)?, 2)?;
        Ok(())
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    Ok(())
}
async fn cleanup(root: PathBuf) -> Result<(), Box<dyn StdError>> {
    tokio::task::spawn_blocking(move || fs::remove_dir_all(root)).await??;
    Ok(())
}
fn settings(root: &Path, seed: &str) -> Result<produce::Config, Box<dyn StdError>> {
    let mut config = produce::Config::new(root.join("partitions"));
    if seed == "fixture_small_entry" {
        config.journal_limits = journal::Limits::new(80, 4096, 16, 4096)?;
        config.max_stores = 2;
        config.max_disk_bytes = 8192;
        config.max_index_bytes = 2048;
    } else {
        assert_eq!(seed, "fixture");
    }
    Ok(config)
}
async fn fixture(release: &str, name: &str) -> Result<Vec<u8>, Box<dyn StdError>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/produce")
        .join(release)
        .join(format!("{name}.request.bin"));
    Ok(tokio::task::spawn_blocking(move || read(&path)).await??)
}
fn header(key: i16, version: i16) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&key.to_be_bytes());
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&77i32.to_be_bytes());
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    bytes
}
fn string(bytes: &mut Vec<u8>, name: &str) {
    bytes.extend_from_slice(&(name.len() as i16).to_be_bytes());
    bytes.extend_from_slice(name.as_bytes());
}
fn delete() -> Vec<u8> {
    let mut bytes = header(20, 1);
    bytes.extend_from_slice(&1i32.to_be_bytes());
    string(&mut bytes, "alpha");
    bytes.extend_from_slice(&60000i32.to_be_bytes());
    bytes
}
fn create() -> Vec<u8> {
    let mut bytes = header(19, 4);
    bytes.extend_from_slice(&1i32.to_be_bytes());
    string(&mut bytes, "alpha");
    bytes.extend_from_slice(&2i32.to_be_bytes());
    bytes.extend_from_slice(&1i16.to_be_bytes());
    bytes.extend_from_slice(&0i32.to_be_bytes());
    bytes.extend_from_slice(&0i32.to_be_bytes());
    bytes.extend_from_slice(&60000i32.to_be_bytes());
    bytes.push(0);
    bytes
}
fn error(response: &[u8]) -> i16 {
    i16::from_be_bytes(response[23..25].try_into().unwrap())
}
fn offset(response: &[u8]) -> i64 {
    i64::from_be_bytes(response[25..33].try_into().unwrap())
}
fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        text.push(char::from(D[(b >> 4) as usize]));
        text.push(char::from(D[(b & 15) as usize]));
    }
    text
}

#[tokio::test]
async fn independent_apache_produce_goldens() -> Result<(), Box<dyn StdError>> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/produce");
    let mut observations = Vec::new();
    let mut cases = 0;
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base.join(release);
        let lines = tokio::task::spawn_blocking({
            let directory = directory.clone();
            move || read(&directory.join("cases.tsv"))
        })
        .await??;
        let mut coverage = Vec::new();
        for line in std::str::from_utf8(&lines)?
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 5);
            let name = columns[0];
            assert_eq!(columns[1], "0");
            let version = columns[2].parse::<i16>()?;
            coverage.push(version);
            let root = scratch(name);
            setup(root.clone()).await?;
            let (router, _) = Router::open_with_store(
                root.join("catalog.journal"),
                common(),
                settings(&root, columns[3])?,
            )
            .await?;
            let (request, expected) = tokio::task::spawn_blocking({
                let directory = directory.clone();
                let name = name.to_owned();
                move || {
                    let response = directory.join(format!("{name}.response.bin"));
                    Ok::<_, std::io::Error>((
                        read(&directory.join(format!("{name}.request.bin")))?,
                        if response.exists() {
                            Some(read(&response)?)
                        } else {
                            None
                        },
                    ))
                }
            })
            .await??;
            let result = router.dispatch(request).await;
            let response_hex = match (columns[4], result, expected) {
                ("response", Ok(Some(actual)), Some(expected)) => {
                    assert_eq!(actual, expected, "{release}/{name}");
                    format!("\"{}\"", hex(&actual))
                }
                ("no_response_keep_open", Ok(None), None) => "null".into(),
                ("close", Err(metadata::Error::Produce(produce::Error::AcksZeroError)), None) => {
                    "null".into()
                }
                (
                    "structural_reject",
                    Err(metadata::Error::Produce(
                        produce::Error::Protocol(_) | produce::Error::RequestCount,
                    )),
                    None,
                ) => "null".into(),
                (outcome, actual, expected) => {
                    return Err(format!(
                        "{release}/{name}: {outcome} observed{actual:?} expected{expected:?}"
                    )
                    .into())
                }
            };
            observations.push(format!("{{\"release\":\"{release}\",\"case\":\"{name}\",\"outcome\":\"{}\",\"response_hex\":{response_hex}}}",columns[4]));
            router.shutdown().await?;
            cleanup(root).await?;
            cases += 1;
        }
        coverage.sort_unstable();
        coverage.dedup();
        assert_eq!(coverage, (3..=13).collect::<Vec<_>>());
    }
    assert!(cases >= 444);
    if let Some(path) = std::env::var_os("PARTITIONLINE_PRODUCE_REPORT") {
        let list = produce::DATA_API_VERSIONS
            .iter()
            .map(|api| {
                format!(
                    "{{\"api_key\":{},\"min_version\":{},\"max_version\":{}}}",
                    api.api_key, api.min_version, api.max_version
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let data = format!(
            "{{\"schema_version\":1,\"data_api_versions\":[{list}],\"case_results\":[{}]}}\n",
            observations.join(",")
        );
        tokio::task::spawn_blocking(move || write(Path::new(&path), data.as_bytes())).await??;
    }
    Ok(())
}

#[tokio::test]
async fn offsets_replay_and_deleted_identity_never_reuses_data() -> Result<(), Box<dyn StdError>> {
    let root = scratch("replay");
    setup(root.clone()).await?;
    let input = fixture("4.3.1", "produce-v3-acks1").await?;
    let (router, _) = Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        settings(&root, "fixture")?,
    )
    .await?;
    for base in 0..2 {
        let result = router.dispatch(input.clone()).await?.unwrap();
        assert_eq!(error(&result), 0);
        assert_eq!(offset(&result), base);
    }
    router.shutdown().await?;
    let (router, _) = Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        settings(&root, "fixture")?,
    )
    .await?;
    let result = router.dispatch(input.clone()).await?.unwrap();
    assert_eq!(offset(&result), 2);
    router.respond(delete()).await?;
    router.respond(create()).await?;
    let old = fixture("4.3.1", "produce-v13-acks1").await?;
    let result = router.dispatch(old).await?.unwrap();
    assert_eq!(i16::from_be_bytes(result[27..29].try_into().unwrap()), 100);
    let result = router.dispatch(input).await?.unwrap();
    assert_eq!(offset(&result), 0);
    router.shutdown().await?;
    let record_count = tokio::task::spawn_blocking({
        let root = root.clone();
        move || -> Result<(i64, usize), Box<dyn StdError + Send + Sync>> {
            let path = root.join("partitions/00000000000000000000000000000002-0.journal");
            let config = produce::Config::new(root.join("partitions"));
            let (mut log, _) =
                partition::Partition::open(path, 0, config.journal_limits, config.record_limits)?;
            let entries = log.fetch(0, 16, 4096)?;
            assert_eq!(entries.len(), 3);
            for entry in &entries {
                assert_eq!(&entry.payload[12..16], &0i32.to_be_bytes());
            }
            Ok((
                log.next_offset(),
                fs::read_dir(root.join("partitions"))?.count(),
            ))
        }
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    assert_eq!(record_count, (3, 2));
    cleanup(root).await?;
    Ok(())
}

#[tokio::test]
async fn response_and_normalized_budgets_fail_before_append() -> Result<(), Box<dyn StdError>> {
    let root = scratch("bounds");
    setup(root.clone()).await?;
    let input = fixture("4.3.1", "produce-v3-rich-records").await?;
    let mut limits = settings(&root, "fixture")?;
    limits.max_normalized_bytes = 1;
    let (router, _) =
        Router::open_with_store(root.join("catalog.journal"), common(), limits).await?;
    assert!(matches!(
        router.dispatch(input).await,
        Err(metadata::Error::Produce(produce::Error::ResourceLimit))
    ));
    router.shutdown().await?;
    let count = tokio::task::spawn_blocking({
        let root = root.clone();
        move || fs::read_dir(root.join("partitions")).map(|r| r.count())
    })
    .await??;
    assert_eq!(count, 0);
    let mut limits = settings(&root, "fixture")?;
    limits.max_stores = 4096;
    assert!(matches!(
        Router::open_with_store(root.join("catalog.journal"), common(), limits).await,
        Err(metadata::Error::Produce(produce::Error::InvalidConfig))
    ));
    cleanup(root).await?;
    Ok(())
}

#[tokio::test]
async fn historical_store_and_index_budgets_do_not_advance_offsets() -> Result<(), Box<dyn StdError>>
{
    let root = scratch("history-bound");
    setup(root.clone()).await?;
    let mut limits = settings(&root, "fixture")?;
    limits.max_stores = 1;
    limits.max_disk_bytes = 4096;
    limits.max_index_bytes = 64;
    limits.journal_limits = journal::Limits::new(1024, 4096, 1, 4096)?;
    let (router, _) =
        Router::open_with_store(root.join("catalog.journal"), common(), limits.clone()).await?;
    let input = fixture("4.3.1", "produce-v3-acks1").await?;
    let first = router.dispatch(input.clone()).await?.unwrap();
    assert_eq!(error(&first), 0);
    assert_eq!(offset(&first), 0);
    assert_eq!(error(&router.dispatch(input.clone()).await?.unwrap()), 56);
    let mut other = input.clone();
    let at = other.windows(5).position(|b| b == b"alpha").unwrap() + 9;
    other[at..at + 4].copy_from_slice(&1i32.to_be_bytes());
    assert_eq!(error(&router.dispatch(other).await?.unwrap()), 56);
    router.respond(delete()).await?;
    router.respond(create()).await?;
    assert_eq!(error(&router.dispatch(input).await?.unwrap()), 56);
    router.shutdown().await?;
    let (router, _) =
        Router::open_with_store(root.join("catalog.journal"), common(), limits).await?;
    router.shutdown().await?;
    let (next, count) = tokio::task::spawn_blocking({
        let root = root.clone();
        move || -> Result<(i64, usize), Box<dyn StdError + Send + Sync>> {
            let (log, _) = partition::Partition::open(
                root.join("partitions/00000000000000000000000000000002-0.journal"),
                0,
                journal::Limits::new(1024, 4096, 1, 4096)?,
                produce::Config::new(root.join("partitions")).record_limits,
            )?;
            Ok((
                log.next_offset(),
                fs::read_dir(root.join("partitions"))?.count(),
            ))
        }
    })
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    assert_eq!((next, count), (1, 1));
    cleanup(root).await?;
    Ok(())
}

#[tokio::test]
async fn malformed_entire_body_never_appends() -> Result<(), Box<dyn StdError>> {
    let root = scratch("malformed");
    setup(root.clone()).await?;
    let (router, _) = Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        settings(&root, "fixture")?,
    )
    .await?;
    let canonical = fixture("4.3.1", "produce-v3-acks1").await?;
    for length in 4..canonical.len() {
        assert!(
            router.dispatch(canonical[..length].to_vec()).await.is_err(),
            "prefix{length}"
        );
    }
    let mut tail = canonical.clone();
    tail.push(0);
    assert!(router.dispatch(tail).await.is_err());
    let mut traversal = canonical.clone();
    let at = traversal
        .windows(5)
        .position(|bytes| bytes == b"alpha")
        .unwrap();
    let name = b"../../outside-topic";
    traversal[at - 2..at].copy_from_slice(&(name.len() as i16).to_be_bytes());
    traversal.splice(at..at + 5, name.iter().copied());
    let result = router.dispatch(traversal).await?.unwrap();
    let code_at = 18 + name.len();
    assert_eq!(
        i16::from_be_bytes(result[code_at..code_at + 2].try_into().unwrap()),
        3
    );
    let result = router.dispatch(canonical).await?.unwrap();
    assert_eq!(offset(&result), 0);
    router.shutdown().await?;
    cleanup(root).await?;
    Ok(())
}

#[tokio::test]
async fn corrupt_or_unknown_store_fails_recovery_without_partial_router(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("corrupt");
    setup(root.clone()).await?;
    let config = settings(&root, "fixture")?;
    let (router, _) =
        Router::open_with_store(root.join("catalog.journal"), common(), config.clone()).await?;
    router
        .dispatch(fixture("4.3.1", "produce-v3-acks1").await?)
        .await?;
    router.shutdown().await?;
    let path = root.join("partitions/00000000000000000000000000000002-0.journal");
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || {
            let mut bytes = read(&path)?;
            let end = bytes.len() - 1;
            bytes[end] ^= 1;
            write(&path, &bytes)
        }
    })
    .await??;
    assert!(matches!(
        Router::open_with_store(root.join("catalog.journal"), common(), config.clone()).await,
        Err(metadata::Error::Produce(produce::Error::Partition(_)))
    ));
    tokio::task::spawn_blocking({
        let root = root.clone();
        move || {
            fs::remove_file(path)?;
            write(&root.join("partitions/unknown.journal"), b"unknown")
        }
    })
    .await??;
    assert!(matches!(
        Router::open_with_store(root.join("catalog.journal"), common(), config).await,
        Err(metadata::Error::Produce(produce::Error::InvalidStore))
    ));
    cleanup(root).await?;
    Ok(())
}

async fn send(socket: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    socket
        .write_all(&(bytes.len() as i32).to_be_bytes())
        .await?;
    socket.write_all(bytes).await
}
async fn receive(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let size = socket.read_i32().await?;
    if !(0..=128 * 1024).contains(&size) {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let mut result = vec![0; size as usize];
    socket.read_exact(&mut result).await?;
    Ok(result)
}
#[tokio::test]
async fn acks_zero_sends_no_frame_keeps_channel_and_errors_close() -> Result<(), Box<dyn StdError>>
{
    let root = scratch("acks0");
    setup(root.clone()).await?;
    let (router, _) = Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        settings(&root, "fixture")?,
    )
    .await?;
    let router = Arc::new(router);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture("4.3.1", "produce-v3-acks0").await?).await?;
    let mut metadata = header(3, 1);
    metadata.extend_from_slice(&(-1i32).to_be_bytes());
    send(&mut socket, &metadata).await?;
    let response = tokio::time::timeout(Duration::from_secs(2), receive(&mut socket)).await??;
    assert_eq!(&response[..4], &77i32.to_be_bytes());
    send(
        &mut socket,
        &fixture("4.3.1", "produce-v3-acks0-error").await?,
    )
    .await?;
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await??,
        0
    );
    let report = transport.shutdown().await?;
    assert_eq!(report.handler_errors, 1);
    router.shutdown().await?;
    cleanup(root).await?;
    Ok(())
}

/// Independent forced-version live peer harness; ordinary runs are inert.
#[tokio::test]
async fn serve_live_probe() -> Result<(), Box<dyn StdError>> {
    let Some(port) = std::env::var_os("PARTITIONLINE_PRODUCE_LIVE_PORT") else {
        return Ok(());
    };
    let port: u16 = port.to_str().ok_or("port")?.parse()?;
    let root =
        PathBuf::from(std::env::var_os("PARTITIONLINE_PRODUCE_LIVE_DIR").ok_or("directory")?);
    let mut config = common();
    config.advertised_port = port;
    let (router, _) = Router::open_with_store(
        root.join("catalog.journal"),
        config,
        produce::Config::new(root.join("partitions")),
    )
    .await?;
    let router = Arc::new(router);
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

#[tokio::test]
async fn rolling_store_preserves_offsets_historical_budget_and_rejects_wrong_layout(
) -> Result<(), Box<dyn StdError>> {
    let root = scratch("rolling");
    setup(root.clone()).await?;
    let mut store = produce::Config::new(root.join("partitions"));
    store.segment_limits = Some(partitionline_broker::segments::Limits::new(
        150,
        2,
        4,
        2,
        8 * 1024 * 1024,
        65536,
        2 * 1024 * 1024,
    )?);
    let input = fixture("4.3.1", "produce-v3-acks1").await?;
    for round in 0..2 {
        let router = Router::open_with_store(root.join("catalog.journal"), common(), store.clone())
            .await?
            .0;
        if round == 0 {
            for n in 0..2 {
                let response = router.respond(input.clone()).await?;
                assert_eq!(error(&response), 0);
                assert_eq!(offset(&response), n);
            }
        }
        let response = router.respond(input.clone()).await?;
        assert_eq!(error(&response), 56);
        router.shutdown().await?;
    }
    // No silent second empty log is opened for an existing rolling identity.
    assert!(Router::open_with_store(
        root.join("catalog.journal"),
        common(),
        produce::Config::new(root.join("partitions"))
    )
    .await
    .is_err());
    // Both persistent data and sidecars/staging count in the configured envelope.
    store.max_index_bytes = store.max_stores * store.segment_limits.unwrap().max_index_bytes() - 1;
    assert!(matches!(
        Router::open_with_store(root.join("catalog.journal"), common(), store).await,
        Err(metadata::Error::Produce(produce::Error::InvalidConfig))
    ));
    cleanup(root).await?;
    Ok(())
}
