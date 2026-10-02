//! Durable credential actor guarantees, independent of network sessions.
#![cfg(feature = "sasl")]
use partitionline_broker::security::{
    credentials::{Change, Error, Limits, Store},
    sasl::{Algorithm, Error as SaslError, Secret},
};
use pbkdf2::pbkdf2_hmac;
use sha2::{Sha256, Sha512};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
// These wrappers are executed only by explicitly joined blocking jobs.
#[allow(clippy::disallowed_methods)]
fn read_bytes(path: PathBuf) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}
#[allow(clippy::disallowed_methods)]
fn write_bytes(path: PathBuf, bytes: Vec<u8>) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Path(PathBuf);
impl Path {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "partitionline-sasl-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Path {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn upsert(algorithm: Algorithm, password: &str) -> Change {
    let salt = b"public-synthetic-salt-for-test".to_vec();
    let mut salted = vec![
        0;
        if algorithm == Algorithm::Sha256 {
            32
        } else {
            64
        }
    ];
    match algorithm {
        Algorithm::Sha256 => pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 4096, &mut salted),
        Algorithm::Sha512 => pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, 4096, &mut salted),
    }
    Change::Upsert {
        algorithm,
        salt,
        iterations: 4096,
        salted_password: Secret::new(salted),
    }
}
fn plain(name: &str, password: &str) -> Secret {
    Secret::new(format!("\0{name}\0{password}").into_bytes())
}
#[tokio::test]
async fn two_algorithms_restart_rotation_delete_and_metadata_only_description() -> Result {
    let path = Path::new();
    let (store, recovery) = Store::open(&path.0, Limits::default()).await?;
    assert!(recovery.initialized);
    store
        .mutate(
            "user".into(),
            vec![
                upsert(Algorithm::Sha256, "pencil"),
                upsert(Algorithm::Sha512, "pencil"),
            ],
        )
        .await?;
    let identity = store.plain(plain("user", "pencil")).await?;
    assert_eq!(identity.name(), "user");
    let rows = store.describe(None).await?;
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|r| r.user() == "user" && r.iterations() == 4096));
    assert!(!format!("{rows:?} {store:?} {identity:?}").contains("pencil"));
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(recovery.recovered_entries, 1);
    store.plain(plain("user", "pencil")).await?;
    store
        .mutate(
            "user".into(),
            vec![
                upsert(Algorithm::Sha256, "rotated"),
                Change::Delete(Algorithm::Sha512),
            ],
        )
        .await?;
    assert_eq!(
        store.plain(plain("user", "pencil")).await.err(),
        Some(SaslError::AuthenticationFailed)
    );
    store.plain(plain("user", "rotated")).await?;
    assert_eq!(store.describe(None).await?.len(), 1);
    store
        .mutate("user".into(), vec![Change::Delete(Algorithm::Sha256)])
        .await?;
    assert!(store.describe(None).await?.is_empty());
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(recovery.recovered_entries, 3);
    assert_eq!(
        store.plain(plain("user", "rotated")).await.err(),
        Some(SaslError::AuthenticationFailed)
    );
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn persisted_bytes_contain_only_verifier_form() -> Result {
    let path = Path::new();
    let change = upsert(Algorithm::Sha512, "synthetic-secret-not-persisted");
    // Independently derive the transient admin input for a byte-level absence check.
    let mut salted = [0; 64];
    pbkdf2_hmac::<Sha512>(
        b"synthetic-secret-not-persisted",
        b"public-synthetic-salt-for-test",
        4096,
        &mut salted,
    );
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    store.mutate("user".into(), vec![change]).await?;
    store.shutdown().await?;
    let bytes = tokio::task::spawn_blocking({
        let path = path.0.clone();
        move || read_bytes(path)
    })
    .await??;
    assert!(!bytes.windows(salted.len()).any(|v| v == salted));
    assert!(!bytes
        .windows(b"synthetic-secret-not-persisted".len())
        .any(|v| v == b"synthetic-secret-not-persisted"));
    assert!(bytes.windows(8).any(|v| v == b"PLSASL01"));
    Ok(())
}
#[tokio::test]
async fn invalid_and_missing_changes_never_partially_commit() -> Result {
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(
        store
            .mutate(
                "user".into(),
                vec![
                    upsert(Algorithm::Sha256, "pencil"),
                    Change::Delete(Algorithm::Sha256)
                ]
            )
            .await,
        Err(Error::Duplicate)
    );
    assert_eq!(
        store
            .mutate(
                "user".into(),
                vec![
                    upsert(Algorithm::Sha256, "pencil"),
                    Change::Delete(Algorithm::Sha512)
                ]
            )
            .await,
        Err(Error::NotFound)
    );
    assert_eq!(
        store
            .mutate("".into(), vec![upsert(Algorithm::Sha256, "pencil")])
            .await,
        Err(Error::Invalid)
    );
    assert!(store.describe(None).await?.is_empty());
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(recovery.recovered_entries, 0);
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn retained_operation_budget_preserves_last_confirmed_generation() -> Result {
    let path = Path::new();
    let (store, _) = Store::open(
        &path.0,
        Limits {
            operations: 1,
            ..Limits::default()
        },
    )
    .await?;
    let generation = store
        .mutate("user".into(), vec![upsert(Algorithm::Sha256, "pencil")])
        .await?;
    assert_eq!(
        store
            .mutate("user".into(), vec![upsert(Algorithm::Sha256, "rotated")])
            .await,
        Err(Error::Budget)
    );
    assert!(store.is_healthy());
    assert_eq!(
        store.plain(plain("user", "pencil")).await?.generation(),
        generation
    );
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn raw_utf8_plain_identity_and_password_survive_restart() -> Result {
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    store
        .mutate("用户".into(), vec![upsert(Algorithm::Sha256, "päss💫")])
        .await?;
    store.shutdown().await?;
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(store.plain(plain("用户", "päss💫")).await?.name(), "用户");
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn incomplete_tail_repair_is_reported_but_complete_corruption_fails_closed() -> Result {
    use std::io::Write;
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    store
        .mutate("user".into(), vec![upsert(Algorithm::Sha256, "pencil")])
        .await?;
    store.shutdown().await?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path.0)?
        .write_all(b"PLEN")?;
    let (store, recovery) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(recovery.truncated_bytes, 4);
    store.plain(plain("user", "pencil")).await?;
    store.shutdown().await?;
    let mut bytes = tokio::task::spawn_blocking({
        let path = path.0.clone();
        move || read_bytes(path)
    })
    .await??;
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    tokio::task::spawn_blocking({
        let path = path.0.clone();
        move || write_bytes(path, bytes)
    })
    .await??;
    assert_eq!(
        Store::open(&path.0, Limits::default()).await.err(),
        Some(Error::Corrupt)
    );
    Ok(())
}
#[cfg(unix)]
#[tokio::test]
async fn file_permissions_and_symlinks_are_enforced_without_diagnostics_leaks() -> Result {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    assert_eq!(
        std::fs::metadata(&path.0)?.permissions().mode() & 0o777,
        0o600
    );
    store.shutdown().await?;
    std::fs::set_permissions(&path.0, std::fs::Permissions::from_mode(0o644))?;
    assert_eq!(
        Store::open(&path.0, Limits::default()).await.err(),
        Some(Error::InsecureFile)
    );
    let alias = Path::new();
    symlink(&path.0, &alias.0)?;
    assert_eq!(
        Store::open(&alias.0, Limits::default()).await.err(),
        Some(Error::InsecureFile)
    );
    for error in [Error::InsecureFile, Error::Corrupt, Error::Unavailable] {
        assert!(!format!("{error:?} {error}").contains(&path.0.display().to_string()));
    }
    Ok(())
}
#[tokio::test]
async fn shutdown_disables_new_authentication_and_admin() -> Result {
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    store.shutdown().await?;
    assert!(!store.is_healthy());
    assert_eq!(
        store.plain(plain("user", "pencil")).await.err(),
        Some(SaslError::Unavailable)
    );
    assert_eq!(
        store
            .mutate("user".into(), vec![upsert(Algorithm::Sha256, "pencil")])
            .await,
        Err(Error::Unavailable)
    );
    assert_eq!(store.describe(None).await.err(), Some(Error::Unavailable));
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn negotiated_plain_keeps_its_generation_while_new_sessions_observe_rotation() -> Result {
    let path = Path::new();
    let (store, _) = Store::open(&path.0, Limits::default()).await?;
    let old_generation = store
        .mutate("user".into(), vec![upsert(Algorithm::Sha256, "old")])
        .await?;
    let captured = store.begin_plain()?;
    store
        .mutate("user".into(), vec![upsert(Algorithm::Sha256, "new")])
        .await?;
    assert_eq!(
        captured.finish(plain("user", "old")).await?.generation(),
        old_generation
    );
    assert_eq!(
        store.plain(plain("user", "old")).await.err(),
        Some(SaslError::AuthenticationFailed)
    );
    assert_eq!(
        store.plain(plain("user", "new")).await?.generation(),
        old_generation + 1
    );
    store.shutdown().await?;
    Ok(())
}
