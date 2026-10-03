//! Durable logical floors, bounded DeleteRecords parsing and ordinary read semantics.
#![allow(clippy::unwrap_used)]

use partitionline_broker::{
    catalog::{Catalog, TopicId},
    fetch, journal,
    metadata::{self, Router},
    partition, produce, protocol, retention, segments,
    transport::{self, Transport},
};
use std::{
    error::Error as StdError,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

type Result<T = ()> = std::result::Result<T, Box<dyn StdError>>;
const FIRST: &[u8] = include_bytes!("fixtures/fetch/4.3.1/log-batch-0.bin");
const SECOND: &[u8] = include_bytes!("fixtures/fetch/4.3.1/log-batch-3.bin");
fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "partitionline-retention-{}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn common() -> metadata::Config {
    metadata::Config::new(0, "127.0.0.1".into(), 19095, "partitionline-fixture".into())
}
fn settings(root: &Path) -> Result<produce::Config> {
    let mut config = produce::Config::new(root.join("partitions"));
    config.journal_limits = journal::Limits::new(1024, 8192, 16, 4096)?;
    config.segment_limits = Some(segments::Limits::new(
        150,
        8,
        4,
        2,
        1024 * 1024,
        64 * 1024,
        4096,
    )?);
    config.max_stores = 8;
    Ok(config)
}
async fn seed(root: PathBuf, log: bool) -> Result {
    tokio::task::spawn_blocking(
        move || -> std::result::Result<(), Box<dyn StdError + Send + Sync>> {
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
                let config = settings(&root).map_err(|e| e.to_string())?;
                let (mut part, _) = partition::Partition::open_segmented(
                    root.join("partitions/00000000000000000000000000000002-0.segments"),
                    0,
                    config.journal_limits,
                    config.record_limits,
                    config.segment_limits.ok_or("rolling")?,
                )?;
                part.append(FIRST)?;
                part.append(SECOND)?;
            }
            Ok(())
        },
    )
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    Ok(())
}
async fn clean(root: PathBuf) -> Result {
    tokio::task::spawn_blocking(move || fs::remove_dir_all(root)).await??;
    Ok(())
}
async fn open(root: &Path, cfg: retention::Config) -> Result<Router> {
    Ok(Router::open_with_retention_store(
        root.join("catalog.journal"),
        common(),
        settings(root)?,
        fetch::Limits::default(),
        cfg,
    )
    .await?
    .0)
}
fn header(key: i16, version: i16) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&key.to_be_bytes());
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&77i32.to_be_bytes());
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    if key == 21 && version >= 2 {
        bytes.push(0);
    }
    bytes
}
fn var(bytes: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        bytes.push(if value == 0 { byte } else { byte | 128 });
        if value == 0 {
            break;
        }
    }
}
fn count(bytes: &mut Vec<u8>, value: usize, flex: bool) {
    if flex {
        var(bytes, value + 1);
    } else {
        bytes.extend_from_slice(&(value as i32).to_be_bytes());
    }
}
fn string(bytes: &mut Vec<u8>, value: &str, flex: bool) {
    if flex {
        var(bytes, value.len() + 1);
    } else {
        bytes.extend_from_slice(&(value.len() as i16).to_be_bytes());
    }
    bytes.extend_from_slice(value.as_bytes());
}
fn delete(version: i16, topics: &[(&str, &[(i32, i64)])]) -> Vec<u8> {
    let flex = version == 2;
    let mut bytes = header(21, version);
    count(&mut bytes, topics.len(), flex);
    for (name, parts) in topics {
        string(&mut bytes, name, flex);
        count(&mut bytes, parts.len(), flex);
        for (index, offset) in *parts {
            bytes.extend_from_slice(&index.to_be_bytes());
            bytes.extend_from_slice(&offset.to_be_bytes());
            if flex {
                bytes.push(0);
            }
        }
        if flex {
            bytes.push(0);
        }
    }
    bytes.extend_from_slice(&60000i32.to_be_bytes());
    if flex {
        bytes.push(0);
    }
    bytes
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let (value, rest) = self.0.split_at(n);
        self.0 = rest;
        value
    }
    fn i16(&mut self) -> i16 {
        i16::from_be_bytes(self.take(2).try_into().unwrap())
    }
    fn i32(&mut self) -> i32 {
        i32::from_be_bytes(self.take(4).try_into().unwrap())
    }
    fn i64(&mut self) -> i64 {
        i64::from_be_bytes(self.take(8).try_into().unwrap())
    }
    fn var(&mut self) -> usize {
        let mut value = 0;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.take(1)[0];
            value |= usize::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return value;
            }
        }
        unreachable!()
    }
    fn count(&mut self, flex: bool) -> usize {
        if flex {
            self.var() - 1
        } else {
            self.i32() as usize
        }
    }
}
fn results(bytes: &[u8], version: i16) -> Vec<(String, i32, i64, i16)> {
    let flex = version == 2;
    let mut r = Reader(bytes);
    assert_eq!(r.i32(), 77);
    if flex {
        assert_eq!(r.take(1), &[0]);
    }
    assert_eq!(r.i32(), 0);
    let mut result = Vec::new();
    for _ in 0..r.count(flex) {
        let n = if flex {
            r.count(true)
        } else {
            r.i16() as usize
        };
        let name = std::str::from_utf8(r.take(n)).unwrap().to_owned();
        for _ in 0..r.count(flex) {
            result.push((name.clone(), r.i32(), r.i64(), r.i16()));
            if flex {
                assert_eq!(r.take(1), &[0]);
            }
        }
        if flex {
            assert_eq!(r.take(1), &[0]);
        }
    }
    if flex {
        assert_eq!(r.take(1), &[0]);
    }
    assert!(r.0.is_empty());
    result
}
fn fetch_request(version: i16, offset: i64, wait: i32, minimum: i32) -> Vec<u8> {
    let mut bytes = header(1, version);
    for value in [-1i32, wait, minimum, 4096] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes.push(0);
    count(&mut bytes, 1, false);
    string(&mut bytes, "alpha", false);
    count(&mut bytes, 1, false);
    bytes.extend_from_slice(&0i32.to_be_bytes());
    bytes.extend_from_slice(&offset.to_be_bytes());
    if version >= 5 {
        bytes.extend_from_slice(&0i64.to_be_bytes());
    }
    bytes.extend_from_slice(&4096i32.to_be_bytes());
    bytes
}
fn list_request(version: i16, timestamp: i64) -> Vec<u8> {
    let mut bytes = header(2, version);
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    if version >= 2 {
        bytes.push(0);
    }
    count(&mut bytes, 1, false);
    string(&mut bytes, "alpha", false);
    count(&mut bytes, 1, false);
    bytes.extend_from_slice(&0i32.to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes
}
fn fetch_error(bytes: &[u8]) -> i16 {
    i16::from_be_bytes(bytes[27..29].try_into().unwrap())
}
fn list_offset(bytes: &[u8], version: i16) -> i64 {
    let at = if version >= 2 { 37 } else { 33 };
    i64::from_be_bytes(bytes[at..at + 8].try_into().unwrap())
}

#[tokio::test]
async fn middle_of_batch_floor_is_durable_and_keeps_containing_bytes() -> Result {
    let root = scratch("middle");
    seed(root.clone(), true).await?;
    for round in 0..2 {
        let router = open(&root, retention::Config::default()).await?;
        if round == 0 {
            for version in 0..=2 {
                let response = router
                    .respond(delete(version, &[("alpha", &[(0, 2)])]))
                    .await?;
                assert_eq!(results(&response, version), vec![("alpha".into(), 0, 2, 0)]);
            }
        }
        for version in 4..=6 {
            assert_eq!(
                fetch_error(&router.respond(fetch_request(version, 1, 0, 0)).await?),
                1
            );
            let response = router.respond(fetch_request(version, 2, 0, 0)).await?;
            assert_eq!(fetch_error(&response), 0);
            assert_eq!(&response[29..37], &4i64.to_be_bytes());
            if version >= 5 {
                assert_eq!(&response[45..53], &2i64.to_be_bytes());
            }
            let at = if version >= 5 { 61 } else { 53 };
            // Returning a whole containing batch preserves its protected CRC and offsets.
            assert_eq!(&response[at..at + FIRST.len()], FIRST);
        }
        for version in 1..=3 {
            // Complete bounded retained-record scan is a local policy. Apache
            // can stop at an earlier qualifying segment and report a miss when
            // its only matching timestamp precedes the logical floor.
            for (wanted, expected) in [(-2, 2), (-1, 4), (0, 2), (1000, 2), (1004, 3), (1011, -1)] {
                assert_eq!(
                    list_offset(
                        &router.respond(list_request(version, wanted)).await?,
                        version
                    ),
                    expected
                );
            }
        }
        router.shutdown().await?;
    }
    clean(root).await
}

#[tokio::test]
async fn high_watermark_deletion_preserves_append_end_and_empty_partition() -> Result {
    let root = scratch("hw");
    seed(root.clone(), true).await?;
    let router = open(&root, retention::Config::default()).await?;
    let response = router
        .respond(delete(2, &[("alpha", &[(0, -1), (1, -1)])]))
        .await?;
    assert_eq!(
        results(&response, 2),
        vec![("alpha".into(), 0, 4, 0), ("alpha".into(), 1, 0, 0)]
    );
    assert_eq!(
        fetch_error(&router.respond(fetch_request(6, 3, 0, 0)).await?),
        1
    );
    let response = router.respond(fetch_request(6, 4, 0, 0)).await?;
    assert_eq!(fetch_error(&response), 0);
    assert_eq!(&response[45..53], &4i64.to_be_bytes());
    assert_eq!(&response[57..61], &0i32.to_be_bytes());
    router.shutdown().await?;
    let root_copy = root.clone();
    tokio::task::spawn_blocking(
        move || -> std::result::Result<(), Box<dyn StdError + Send + Sync>> {
            let cfg = settings(&root_copy).map_err(|e| e.to_string())?;
            let (mut part, _) = partition::Partition::open_segmented(
                root_copy.join("partitions/00000000000000000000000000000002-0.segments"),
                0,
                cfg.journal_limits,
                cfg.record_limits,
                cfg.segment_limits.ok_or("rolling")?,
            )?;
            assert_eq!(part.log_start_offset(), 4);
            assert_eq!(part.next_offset(), 4);
            assert_eq!(part.append(SECOND)?.base_offset, 4);
            Ok(())
        },
    )
    .await?
    .map_err(|e| e as Box<dyn StdError>)?;
    let router = open(&root, retention::Config::default()).await?;
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        4
    );
    assert_eq!(
        list_offset(&router.respond(list_request(3, -1)).await?, 3),
        5
    );
    let produced = router
        .respond(include_bytes!("fixtures/produce/4.3.1/produce-v5-acks1.request.bin").to_vec())
        .await?;
    assert_eq!(&produced[23..25], &0i16.to_be_bytes());
    assert_eq!(&produced[25..33], &5i64.to_be_bytes());
    assert_eq!(&produced[41..49], &4i64.to_be_bytes());
    assert_eq!(
        list_offset(&router.respond(list_request(3, -1)).await?, 3),
        6
    );
    router.shutdown().await?;
    let old_profile = Router::open_with_read_store(
        root.join("catalog.journal"),
        common(),
        settings(&root)?,
        fetch::Limits::default(),
    )
    .await?
    .0;
    assert_eq!(
        list_offset(&old_profile.respond(list_request(3, -2)).await?, 3),
        4
    );
    assert_eq!(
        fetch_error(&old_profile.respond(fetch_request(6, 3, 0, 0)).await?),
        1
    );
    old_profile.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn invalid_offsets_unknown_internal_and_last_duplicate_are_partition_local() -> Result {
    let root = scratch("errors");
    seed(root.clone(), true).await?;
    let router = open(&root, retention::Config::default()).await?;
    for version in 0..=2 {
        for invalid in [-2, 5, i64::MAX] {
            let response = router
                .respond(delete(version, &[("alpha", &[(0, invalid)])]))
                .await?;
            assert_eq!(
                results(&response, version),
                vec![("alpha".into(), 0, -1, 1)]
            );
        }
        let response = router
            .respond(delete(
                version,
                &[
                    ("missing", &[(0, 0)]),
                    ("__consumer_offsets", &[(0, 0)]),
                    ("alpha", &[(-1, 0), (2, 0), (0, i64::MAX)]),
                    ("alpha", &[(0, 1)]),
                    ("empty", &[]),
                ],
            ))
            .await?;
        assert_eq!(
            results(&response, version),
            vec![
                ("__consumer_offsets".into(), 0, -1, 17),
                ("alpha".into(), -1, -1, 3),
                ("alpha".into(), 0, 1, 0),
                ("alpha".into(), 2, -1, 3),
                ("missing".into(), 0, -1, 3),
            ]
        );
    }
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        1
    );
    router.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn complete_parse_and_response_preflight_precede_every_mutation() -> Result {
    let root = scratch("preflight");
    seed(root.clone(), true).await?;
    let router = open(&root, retention::Config::default()).await?;
    for version in 0..=2 {
        let valid = delete(version, &[("alpha", &[(0, 4)])]);
        for length in 0..valid.len() {
            assert!(
                router.respond(valid[..length].to_vec()).await.is_err(),
                "v{version}/prefix{length}"
            );
        }
        let mut trailing = valid;
        trailing.push(0);
        assert!(router.respond(trailing).await.is_err());
    }
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        0
    );
    router.shutdown().await?;
    let mut common = common();
    common.max_response_bytes = 64;
    let router = Router::open_with_retention_store(
        root.join("catalog.journal"),
        common,
        settings(&root)?,
        fetch::Limits::default(),
        retention::Config::default(),
    )
    .await?
    .0;
    assert!(router
        .respond(delete(
            0,
            &[("alpha", &[(0, 4), (1, 0), (2, 0), (3, 0), (4, 0)])]
        ))
        .await
        .is_err());
    router.shutdown().await?;
    let router = open(&root, retention::Config::default()).await?;
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        0
    );
    router.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn hostile_counts_utf8_varints_and_tag_order_fail_before_deletion() -> Result {
    let root = scratch("hostile");
    seed(root.clone(), true).await?;
    let router = open(&root, retention::Config::default()).await?;
    let classic = delete(0, &[("alpha", &[(0, 4)])]);
    for at in [10, 21] {
        for count in [i32::MAX, -1, -2] {
            let mut bad = classic.clone();
            bad[at..at + 4].copy_from_slice(&count.to_be_bytes());
            assert!(router.respond(bad).await.is_err());
        }
    }
    let mut utf8 = classic;
    utf8[16] = 255;
    assert!(matches!(
        router.respond(utf8).await,
        Err(metadata::Error::Retention(retention::Error::Protocol(
            protocol::Error::InvalidUtf8
        )))
    ));
    let flexible = delete(2, &[("alpha", &[(0, 4)])]);
    let mut overflow = flexible.clone();
    drop(overflow.splice(11..12, [255, 255, 255, 255, 255]));
    assert!(matches!(
        router.respond(overflow).await,
        Err(metadata::Error::Retention(retention::Error::Protocol(
            protocol::Error::InvalidVarint
        )))
    ));
    let mut tags = flexible;
    drop(tags.splice(31..32, [2, 1, 0, 1, 0]));
    assert!(matches!(
        router.respond(tags).await,
        Err(metadata::Error::Retention(retention::Error::Protocol(
            protocol::Error::InvalidTagOrder
        )))
    ));
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        0
    );
    router.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn floor_advance_wakes_waiting_fetch_without_new_append() -> Result {
    let root = scratch("wakeup");
    seed(root.clone(), true).await?;
    let router = std::sync::Arc::new(open(&root, retention::Config::default()).await?);
    let waiting = tokio::spawn({
        let router = std::sync::Arc::clone(&router);
        async move { router.respond(fetch_request(6, 0, 5000, 4096)).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    assert!(!waiting.is_finished());
    router.respond(delete(2, &[("alpha", &[(0, 2)])])).await?;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), waiting).await???;
    assert_eq!(fetch_error(&response), 1);
    router.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn old_profiles_and_nonrolling_config_do_not_advertise_retention() -> Result {
    let root = scratch("profiles");
    seed(root.clone(), false).await?;
    let router = Router::open_with_read_store(
        root.join("catalog.journal"),
        common(),
        settings(&root)?,
        fetch::Limits::default(),
    )
    .await?
    .0;
    assert!(matches!(
        router.respond(delete(2, &[("alpha", &[(0, 0)])])).await,
        Err(metadata::Error::Protocol(
            protocol::Error::UnimplementedApi(21)
        ))
    ));
    router.shutdown().await?;
    assert!(Router::open_with_retention_store(
        root.join("catalog.journal"),
        common(),
        produce::Config::new(root.join("partitions")),
        fetch::Limits::default(),
        retention::Config::default()
    )
    .await
    .is_err());
    clean(root).await
}

#[test]
fn explicit_policy_work_and_thresholds_are_checked() {
    assert!(retention::Config::default().validate().is_ok());
    for config in [
        retention::Config {
            max_sweep_stores: 0,
            ..retention::Config::default()
        },
        retention::Config {
            max_segments_per_store: 0,
            ..retention::Config::default()
        },
        retention::Config {
            retention_ms: Some(u64::MAX),
            ..retention::Config::default()
        },
        retention::Config {
            retention_bytes: Some(u64::MAX),
            ..retention::Config::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
    assert!(retention::Config {
        retention_ms: Some(0),
        retention_bytes: Some(0),
        ..retention::Config::default()
    }
    .validate()
    .is_ok());
}

#[tokio::test]
async fn age_sweep_uses_strict_max_timestamp_boundary_and_restarts() -> Result {
    let root = scratch("age");
    seed(root.clone(), true).await?;
    let policy = retention::Config {
        retention_ms: Some(100),
        max_segments_per_store: 1,
        max_sweep_stores: 1,
        ..retention::Config::default()
    };
    let router = open(&root, policy).await?;
    let equal = router.sweep_retention(1107).await?;
    assert_eq!(equal.stores_visited, 1);
    assert_eq!(equal.stores_changed, 0);
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        0
    );
    let expired = router.sweep_retention(1108).await?;
    assert_eq!(expired.stores_changed, 1);
    assert_eq!(expired.reclaimed_files, 2);
    assert!(expired.reclaimed_bytes > 0);
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        3
    );
    assert_eq!(
        fetch_error(&router.respond(fetch_request(6, 2, 0, 0)).await?),
        1
    );
    assert_eq!(
        fetch_error(&router.respond(fetch_request(6, 3, 0, 0)).await?),
        0
    );
    router.shutdown().await?;
    let router = open(&root, policy).await?;
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        3
    );
    router.shutdown().await?;
    clean(root).await
}

#[tokio::test]
async fn size_sweep_counts_payload_bytes_and_keeps_a_segment_larger_than_excess() -> Result {
    let root = scratch("size");
    seed(root.clone(), true).await?;
    let router = open(
        &root,
        retention::Config {
            retention_bytes: Some((SECOND.len() + 1) as u64),
            ..retention::Config::default()
        },
    )
    .await?;
    assert_eq!(router.sweep_retention(0).await?.stores_changed, 0);
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        0
    );
    router.shutdown().await?;
    let router = open(
        &root,
        retention::Config {
            retention_bytes: Some(SECOND.len() as u64),
            ..retention::Config::default()
        },
    )
    .await?;
    assert_eq!(router.sweep_retention(0).await?.stores_changed, 1);
    assert_eq!(
        list_offset(&router.respond(list_request(3, -2)).await?, 3),
        3
    );
    let response = router.respond(fetch_request(6, 3, 0, 0)).await?;
    assert_eq!(&response[61..61 + SECOND.len()], SECOND);
    router.shutdown().await?;
    clean(root).await
}

fn read_bounded(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(128 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 128 * 1024 {
        return Err(std::io::Error::other("fixture exceeds 128KiB"));
    }
    Ok(bytes)
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out
}
fn advertisement(response: &[u8], version: i16, correlation: i32) -> Vec<(i16, i16, i16)> {
    let mut reader = Reader(response);
    assert_eq!(reader.i32(), correlation);
    assert_eq!(reader.i16(), 0);
    let mut result = Vec::new();
    for _ in 0..reader.count(version >= 3) {
        result.push((reader.i16(), reader.i16(), reader.i16()));
        if version >= 3 {
            assert_eq!(reader.take(1), &[0]);
        }
    }
    if version >= 1 {
        assert_eq!(reader.i32(), 0);
    }
    if version >= 3 {
        assert_eq!(reader.take(1), &[0]);
    }
    assert!(reader.0.is_empty());
    result
}

#[tokio::test]
async fn independent_apache_delete_records_goldens() -> Result {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/retention");
    let mut observations = Vec::new();
    let mut profiles = Vec::new();
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base.join(release);
        let index = tokio::task::spawn_blocking({
            let directory = directory.clone();
            move || read_bounded(&directory.join("cases.tsv"))
        })
        .await??;
        let mut versions = Vec::new();
        let mut count = 0;
        for line in std::str::from_utf8(&index)?.lines() {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 5);
            assert_eq!(columns[1], "21");
            let version: i16 = columns[2].parse()?;
            assert!((0..=2).contains(&version));
            versions.push(version);
            let name = columns[0];
            assert!(name.len() <= 128);
            assert!(name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'));
            let root = scratch(name);
            match columns[3] {
                "fixture" | "fixture_floor_2" => seed(root.clone(), true).await?,
                "fixture_empty" => seed(root.clone(), false).await?,
                other => return Err(format!("unknown fixture seed: {other}").into()),
            }
            let router = open(&root, retention::Config::default()).await?;
            if columns[3] == "fixture_floor_2" {
                assert_eq!(
                    results(
                        &router.respond(delete(2, &[("alpha", &[(0, 2)])])).await?,
                        2
                    ),
                    vec![("alpha".into(), 0, 2, 0)]
                );
            }
            let (input, expected) = tokio::task::spawn_blocking({
                let directory = directory.clone();
                let name = name.to_owned();
                move || {
                    let response = directory.join(format!("{name}.response.bin"));
                    Ok::<_, std::io::Error>((
                        read_bounded(&directory.join(format!("{name}.request.bin")))?,
                        if response.exists() {
                            Some(read_bounded(&response)?)
                        } else {
                            None
                        },
                    ))
                }
            })
            .await??;
            let outcome = router.respond(input).await;
            let response_hex = match (columns[4], outcome, expected) {
                ("response", Ok(actual), Some(expected)) => {
                    assert_eq!(actual, expected, "{release}/{name}");
                    format!("\"{}\"", hex(&actual))
                }
                (
                    "reject" | "structural_reject",
                    Err(metadata::Error::Retention(
                        retention::Error::Protocol(_) | retention::Error::RequestCount,
                    )),
                    None,
                ) => "null".into(),
                (declared, actual, expected) => {
                    return Err(format!(
                        "{release}/{name}: {declared} actual {actual:?}, expected {expected:?}"
                    )
                    .into())
                }
            };
            observations.push(format!("{{\"release\":\"{release}\",\"case\":\"{name}\",\"outcome\":\"{}\",\"response_hex\":{response_hex}}}", columns[4]));
            router.shutdown().await?;
            clean(root).await?;
            count += 1;
            assert!(count <= 256);
        }
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(versions, vec![0, 1, 2]);
        assert_eq!(count, 121);

        let root = scratch("independent-api-versions");
        seed(root.clone(), false).await?;
        let router = open(&root, retention::Config::default()).await?;
        for version in 0..=4 {
            let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/metadata")
                .join(release)
                .join(format!("api-versions-v{version}.request.bin"));
            let input = tokio::task::spawn_blocking(move || read_bounded(&file)).await??;
            let correlation = i32::from_be_bytes(input[4..8].try_into()?);
            let request_hex = hex(&input);
            let actual = router.respond(input).await?;
            assert_eq!(
                advertisement(&actual, version, correlation),
                vec![
                    (0, 3, 13),
                    (1, 4, 6),
                    (2, 1, 3),
                    (3, 0, 13),
                    (18, 0, 4),
                    (19, 2, 4),
                    (20, 1, 6),
                    (21, 0, 2),
                ]
            );
            profiles.push(format!("{{\"release\":\"{release}\",\"api_version\":{version},\"correlation_id\":{correlation},\"request_hex\":\"{request_hex}\",\"response_hex\":\"{}\"}}", hex(&actual)));
        }
        router.shutdown().await?;
        clean(root).await?;
    }
    if let Some(output) = std::env::var_os("PARTITIONLINE_RETENTION_REPORT") {
        let ranges = retention::DATA_API_VERSIONS
            .iter()
            .map(|range| {
                format!(
                    "{{\"api_key\":{},\"min_version\":{},\"max_version\":{}}}",
                    range.api_key, range.min_version, range.max_version
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let report = format!("{{\"schema_version\":1,\"retention_api_versions\":[{ranges}],\"case_results\":[{}],\"api_versions_cases\":[{}]}}\n", observations.join(","), profiles.join(","));
        tokio::task::spawn_blocking(move || File::create(output)?.write_all(report.as_bytes()))
            .await??;
    }
    Ok(())
}

/// Explicit live peer harness; ordinary test runs start no network server.
#[tokio::test]
async fn serve_live_probe() -> Result {
    let Some(port) = std::env::var_os("PARTITIONLINE_RETENTION_LIVE_PORT") else {
        return Ok(());
    };
    let port: u16 = port.to_str().ok_or("port")?.parse()?;
    let root =
        PathBuf::from(std::env::var_os("PARTITIONLINE_RETENTION_LIVE_DIR").ok_or("directory")?);
    let mut common = common();
    common.advertised_port = port;
    let router = std::sync::Arc::new(
        Router::open_with_retention_store(
            root.join("catalog.journal"),
            common,
            settings(&root)?,
            fetch::Limits::default(),
            retention::Config::default(),
        )
        .await?
        .0,
    );
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], port).into(),
        transport::Config::default(),
        std::sync::Arc::clone(&router),
    )
    .await?;
    tokio::task::spawn_blocking({
        let root = root.clone();
        move || File::create(root.join("ready"))?.write_all(b"ready")
    })
    .await??;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
    loop {
        let stop = tokio::task::spawn_blocking({
            let root = root.clone();
            move || root.join("stop").exists()
        })
        .await?;
        if stop || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    transport.shutdown().await?;
    router.shutdown().await?;
    Ok(())
}
