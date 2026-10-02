//! Persistent router goldens, hostile requests, lifecycle and live peer harness.
#![allow(clippy::unwrap_used)]
use partitionline_broker::{
    catalog::{Catalog, TopicId},
    metadata::{Config, Error, Router},
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
static NEXT: AtomicU64 = AtomicU64::new(0);
fn scratch(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "partitionline-metadata-{}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn config() -> Config {
    Config::new(0, "127.0.0.1".into(), 19095, "partitionline-fixture".into())
}
fn read(path: &Path) -> std::io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(128 * 1024).read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)
}
async fn setup(path: PathBuf, seed: bool) -> Result<(), Box<dyn StdError>> {
    tokio::task::spawn_blocking(
        move || -> Result<(), partitionline_broker::catalog::Error> {
            let (mut catalog, _) = Catalog::open(path, config().catalog_limits)?;
            if seed {
                let mut id = [0; 16];
                id[15] = 2;
                catalog.create("alpha", TopicId::new(id)?, 2)?;
                id[15] = 3;
                catalog.create("__consumer_offsets", TopicId::new(id)?, 1)?;
            }
            Ok(())
        },
    )
    .await??;
    Ok(())
}
fn i16(bytes: &mut Vec<u8>, v: i16) {
    bytes.extend_from_slice(&v.to_be_bytes());
}
fn i32(bytes: &mut Vec<u8>, v: i32) {
    bytes.extend_from_slice(&v.to_be_bytes());
}
fn string(bytes: &mut Vec<u8>, v: &str) {
    i16(bytes, v.len() as i16);
    bytes.extend_from_slice(v.as_bytes());
}
fn header(key: i16, version: i16) -> Vec<u8> {
    let mut bytes = Vec::new();
    i16(&mut bytes, key);
    i16(&mut bytes, version);
    i32(&mut bytes, 91);
    i16(&mut bytes, -1);
    if (key == 3 && version >= 9) || (key == 20 && version >= 4) || (key == 18 && version >= 3) {
        bytes.push(0);
    }
    bytes
}
fn create_request(name: &str, parts: i32, rf: i16, validate: bool) -> Vec<u8> {
    let mut bytes = header(19, 4);
    i32(&mut bytes, 1);
    string(&mut bytes, name);
    i32(&mut bytes, parts);
    i16(&mut bytes, rf);
    i32(&mut bytes, 0);
    i32(&mut bytes, 0);
    i32(&mut bytes, 60_000);
    bytes.push(u8::from(validate));
    bytes
}
fn metadata_all() -> Vec<u8> {
    let mut bytes = header(3, 13);
    bytes.push(0);
    bytes.push(0);
    bytes.push(0);
    bytes.push(0);
    bytes
}
fn delete_request(name: &str) -> Vec<u8> {
    let mut bytes = header(20, 1);
    i32(&mut bytes, 1);
    string(&mut bytes, name);
    i32(&mut bytes, 60_000);
    bytes
}
fn create_error(bytes: &[u8]) -> i16 {
    let len = u16::from_be_bytes(bytes[12..14].try_into().unwrap()) as usize;
    i16::from_be_bytes(bytes[14 + len..16 + len].try_into().unwrap())
}
async fn state(path: PathBuf) -> Result<(usize, usize, u64), Box<dyn StdError>> {
    Ok(tokio::task::spawn_blocking(move || {
        Catalog::open(path, config().catalog_limits).map(|(cat, _)| {
            (
                cat.topic_count(),
                cat.identity_count(),
                cat.total_partitions(),
            )
        })
    })
    .await??)
}

#[tokio::test]
async fn persistent_create_validate_delete_and_fresh_identity() -> Result<(), Box<dyn StdError>> {
    let path = scratch("restart");
    let (router, _) = Router::open(path.clone(), config()).await?;
    assert_eq!(
        create_error(&router.respond(create_request("new", 2, 1, true)).await?),
        0
    );
    router.shutdown().await?;
    assert_eq!(state(path.clone()).await?, (0, 0, 0));
    let (router, _) = Router::open(path.clone(), config()).await?;
    assert_eq!(
        create_error(&router.respond(create_request("new", 2, 1, false)).await?),
        0
    );
    let before = router.respond(metadata_all()).await?;
    assert_eq!(
        create_error(&router.respond(create_request("new", 2, 1, true)).await?),
        36
    );
    router.shutdown().await?;
    let first = tokio::task::spawn_blocking({
        let path = path.clone();
        move || {
            let (cat, _) = Catalog::open(path, config().catalog_limits)?;
            Ok::<_, partitionline_broker::catalog::Error>(cat.by_name("new").unwrap().id())
        }
    })
    .await??;
    assert_eq!(first.bytes()[6] >> 4, 4);
    assert_eq!(first.bytes()[8] >> 6, 2);
    assert_ne!(first.bytes()[0] >> 2, 62);
    let (router, _) = Router::open(path.clone(), config()).await?;
    assert_eq!(router.respond(metadata_all()).await?, before);
    router.respond(delete_request("new")).await?;
    assert_eq!(
        create_error(&router.respond(create_request("new", 1, 1, false)).await?),
        0
    );
    router.shutdown().await?;
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || -> Result<(), partitionline_broker::catalog::Error> {
            let (cat, _) = Catalog::open(path, config().catalog_limits)?;
            assert!(cat.is_tombstoned(first));
            assert_ne!(cat.by_name("new").unwrap().id(), first);
            assert_eq!(cat.operation_count(), 3);
            Ok(())
        }
    })
    .await??;
    fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn validation_errors_timeout_and_no_peer_paths() -> Result<(), Box<dyn StdError>> {
    let path = scratch("invalid");
    let (router, _) = Router::open(path.clone(), config()).await?;
    for (name, parts, rf, expected) in [
        ("../escape", 1, 1, 17),
        ("..", 1, 1, 17),
        ("", 1, 1, 17),
        ("__cluster_metadata", 1, 1, 42),
        ("badparts", 0, 1, 37),
        ("badrf", 1, 2, 38),
    ] {
        assert_eq!(
            create_error(
                &router
                    .respond(create_request(name, parts, rf, false))
                    .await?
            ),
            expected,
            "{name}"
        );
    }
    let mut timeout = create_request("timeout", 1, 1, false);
    let len = timeout.len();
    timeout[len - 5..len - 1].copy_from_slice(&0i32.to_be_bytes());
    assert_eq!(create_error(&router.respond(timeout).await?), 7);
    let mut trailing = create_request("no-partial", 1, 1, false);
    trailing.push(0);
    assert!(matches!(
        router.respond(trailing).await,
        Err(Error::Protocol(_))
    ));
    let mut multi = create_request("first", 1, 1, false);
    multi[10..14].copy_from_slice(&2i32.to_be_bytes());
    multi.truncate(multi.len() - 5);
    i16(&mut multi, 1);
    multi.push(255);
    i32(&mut multi, 1);
    i16(&mut multi, 1);
    i32(&mut multi, 0);
    i32(&mut multi, 0);
    i32(&mut multi, 60000);
    multi.push(0);
    assert!(matches!(
        router.respond(multi).await,
        Err(Error::Protocol(_))
    ));
    let mut hostile = header(19, 4);
    i32(&mut hostile, i32::MAX);
    assert!(matches!(
        router.respond(hostile).await,
        Err(Error::RequestCount)
    ));
    let mut invalid_bool = create_request("no-boolean", 1, 1, false);
    *invalid_bool.last_mut().unwrap() = 2;
    assert!(matches!(
        router.respond(invalid_bool).await,
        Err(Error::InvalidBoolean)
    ));
    for key in [0, 1, 17, 22] {
        assert!(router.respond(header(key, 0)).await.is_err());
    }
    for (key, version) in [(3, 14), (19, 1), (19, 5), (20, 0), (20, 7)] {
        assert!(matches!(
            router.respond(header(key, version)).await,
            Err(Error::UnsupportedVersion { .. })
        ));
    }
    router.shutdown().await?;
    assert_eq!(state(path.clone()).await?, (0, 0, 0));
    fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn resource_bounds_and_response_preflight_are_non_mutating() -> Result<(), Box<dyn StdError>>
{
    let path = scratch("bounds");
    let mut settings = config();
    settings.max_response_bytes = 64;
    let (router, _) = Router::open(path.clone(), settings).await?;
    assert!(matches!(
        router
            .respond(create_request("would-fit-catalog", 1, 1, false))
            .await,
        Err(Error::ResponseLimit)
    ));
    router.shutdown().await?;
    assert_eq!(state(path.clone()).await?, (0, 0, 0));
    fs::remove_file(path)?;
    let path = scratch("topics");
    let mut settings = config();
    settings.catalog_limits = partitionline_broker::catalog::Limits::new(
        1,
        2,
        2,
        2,
        4,
        2048,
        partitionline_broker::journal::Limits::default(),
    )?;
    let (router, _) = Router::open(path.clone(), settings).await?;
    assert_eq!(
        create_error(&router.respond(create_request("one", 2, 1, false)).await?),
        0
    );
    assert_eq!(
        create_error(&router.respond(create_request("two", 1, 1, true)).await?),
        89
    );
    router.shutdown().await?;
    assert_eq!(state(path.clone()).await?, (1, 1, 2));
    fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn independent_apache_metadata_admin_goldens() -> Result<(), Box<dyn StdError>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/metadata");
    let mut count = 0;
    let mut observations = Vec::new();
    let required: Vec<_> = partitionline_broker::protocol::IMPLEMENTED_API_VERSIONS
        .iter()
        .flat_map(|api| {
            (api.min_version..=api.max_version).map(move |version| (api.api_key, version))
        })
        .collect();
    for version in ["4.1.2", "4.2.1", "4.3.1"] {
        let base = root.join(version);
        let mut coverage = Vec::new();
        let cases = tokio::task::spawn_blocking({
            let base = base.clone();
            move || read(&base.join("cases.tsv"))
        })
        .await??;
        for line in std::str::from_utf8(&cases)?
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 4);
            let name = columns[0];
            let pair = (columns[1].parse::<i16>()?, columns[2].parse::<i16>()?);
            assert!(required.contains(&pair));
            coverage.push(pair);
            let seed = columns[3] == "fixture";
            let path = scratch(name);
            setup(path.clone(), seed).await?;
            let (router, _) = Router::open(path.clone(), config()).await?;
            let (request, expected) = tokio::task::spawn_blocking({
                let base = base.clone();
                let name = name.to_owned();
                move || {
                    Ok::<_, std::io::Error>((
                        read(&base.join(format!("{name}.request.bin")))?,
                        if base.join(format!("{name}.response.bin")).exists() {
                            Some(read(&base.join(format!("{name}.response.bin")))?)
                        } else {
                            None
                        },
                    ))
                }
            })
            .await??;
            let response_hex = match (router.respond(request).await, expected) {
                (Ok(actual), Some(expected)) => {
                    assert_eq!(actual, expected, "{version}/{name}");
                    format!("\"{}\"", hex(&actual))
                }
                (Err(Error::InvalidTarget), None) => "null".to_owned(),
                (actual, expected) => {
                    return Err(
                        format!("{version}/{name}: {actual:?} expected {expected:?}").into(),
                    )
                }
            };
            observations.push(format!(
                "{{\"release\":\"{version}\",\"case\":\"{name}\",\"response_hex\":{response_hex}}}"
            ));
            router.shutdown().await?;
            fs::remove_file(path)?;
            count += 1;
        }
        coverage.sort_unstable();
        coverage.dedup();
        assert_eq!(coverage, required, "{version}: advertised pair coverage");
    }
    assert!(
        count >= 84,
        "all implemented versions must have independent fixtures"
    );
    if let Some(path) = std::env::var_os("PARTITIONLINE_METADATA_REPORT") {
        let implemented = partitionline_broker::protocol::IMPLEMENTED_API_VERSIONS
            .iter()
            .map(|api| {
                format!(
                    "{{\"api_key\":{},\"min_version\":{},\"max_version\":{}}}",
                    api.api_key, api.min_version, api.max_version
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let registry = include_str!("../../tests/conformance/broker/implemented-api-versions.json");
        let source_hash = |key: &str| {
            registry
                .split_once(&format!("\"{key}\""))
                .unwrap()
                .1
                .split_once(':')
                .unwrap()
                .1
                .split('"')
                .nth(1)
                .unwrap()
        };
        let data = format!("{{\"schema_version\":1,\"metadata_source_sha256\":\"{}\",\"metadata_test_source_sha256\":\"{}\",\"implemented_api_versions\":[{}],\"case_results\":[{}]}}\n", source_hash("metadata_source_sha256"), source_hash("metadata_test_source_sha256"), implemented, observations.join(","));
        tokio::task::spawn_blocking(move || write(Path::new(&path), data.as_bytes())).await??;
    }
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn shutdown_is_joined_idempotent_and_stops_admission() -> Result<(), Box<dyn StdError>> {
    let path = scratch("shutdown");
    let (router, _) = Router::open(path.clone(), config()).await?;
    router.shutdown().await?;
    router.shutdown().await?;
    assert!(matches!(
        router.respond(metadata_all()).await,
        Err(Error::Stopped)
    ));
    let (reopened, _) = Router::open(path.clone(), config()).await?;
    reopened.shutdown().await?;
    fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn corrupt_complete_catalog_fails_startup_without_partial_metadata(
) -> Result<(), Box<dyn StdError>> {
    let path = scratch("corrupt");
    setup(path.clone(), true).await?;
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || -> std::io::Result<()> {
            let mut bytes = read(&path)?;
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            write(&path, &bytes)
        }
    })
    .await??;
    assert!(matches!(
        Router::open(path.clone(), config()).await,
        Err(Error::Catalog(_))
    ));
    fs::remove_file(path)?;
    Ok(())
}

/// Opt-in wire server used by independent Java/C client probes; default is inert.
#[tokio::test]
async fn serve_live_probe() -> Result<(), Box<dyn StdError>> {
    let Some(port) = std::env::var_os("PARTITIONLINE_METADATA_LIVE_PORT") else {
        return Ok(());
    };
    let port: u16 = port.to_str().ok_or("invalid port")?.parse()?;
    let dir = PathBuf::from(
        std::env::var_os("PARTITIONLINE_METADATA_LIVE_DIR").ok_or("missing live directory")?,
    );
    let mut settings = config();
    settings.advertised_port = port;
    let (router, _) = Router::open(dir.join("catalog.journal"), settings).await?;
    let router = Arc::new(router);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], port).into(),
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await?;
    tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || write(&dir.join("ready"), b"ready\n")
    })
    .await??;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let stopped = tokio::task::spawn_blocking({
            let dir = dir.clone();
            move || dir.join("stop").exists()
        })
        .await?;
        if stopped {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("live probe stop deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    transport.shutdown().await?;
    router.shutdown().await?;
    Ok(())
}
