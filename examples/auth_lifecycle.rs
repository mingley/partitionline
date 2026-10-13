//! One named secure-broker lifecycle case (KL06-06).
//!
//! `AUTH_LIFECYCLE_CASE` selects the case. Broker on `KAFKA_BOOTSTRAP`.
//! SASL and TLS come from the same environment variables as `examples/sasl.rs`.
//! Prints one JSON object and exits 0 only when the expected outcome holds.

use std::time::{Duration, Instant};

use partitionline::{
    error::{COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR},
    Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup, Error, OidcConfig, Sasl,
    TlsConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let case = std::env::var("AUTH_LIFECYCLE_CASE").unwrap_or_else(|_| "admin_reauth".into());
    let sleep_ms: u64 = std::env::var("AUTH_SLEEP_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_500);
    match case.as_str() {
        "admin_reauth" => run_admin_reauth(sleep_ms).await,
        "consumer_reauth" => run_consumer_reauth(sleep_ms).await,
        "group_reauth" => run_group_reauth(sleep_ms).await,
        "admin_bad_password" => run_admin_bad_password().await,
        "admin_idle_reconnect" => run_admin_idle_reconnect().await,
        other => {
            emit(
                other,
                false,
                &format!("unknown AUTH_LIFECYCLE_CASE {other}"),
            );
            Err(format!("unknown AUTH_LIFECYCLE_CASE {other}").into())
        }
    }
}

fn emit(case: &str, ok: bool, detail: &str) {
    let detail = detail.replace('\\', "\\\\").replace('"', "\\\"");
    println!(r#"{{"case":"{case}","ok":{ok},"detail":"{detail}"}}"#);
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn sasl_from_env() -> partitionline::Result<Sasl> {
    if let (Ok(url), Ok(id), Ok(secret)) = (
        std::env::var("OIDC_TOKEN_URL"),
        std::env::var("OIDC_CLIENT_ID"),
        std::env::var("OIDC_CLIENT_SECRET"),
    ) {
        return Ok(Sasl::oidc(OidcConfig::new(url, id, secret)));
    }
    if let Ok(principal) = std::env::var("SASL_OAUTH_PRINCIPAL") {
        return Ok(Sasl::oauthbearer(principal));
    }
    let username = env("KAFKA_USERNAME", "alice");
    let password = env("KAFKA_PASSWORD", "secret");
    Ok(match env("SASL_MECHANISM", "PLAIN").as_str() {
        "PLAIN" => Sasl::plain(username, password),
        "SCRAM-SHA-256" => Sasl::scram_sha256(username, password),
        "SCRAM-SHA-512" => Sasl::scram_sha512(username, password),
        other => {
            return Err(Error::protocol(format!(
                "unsupported SASL_MECHANISM {other}"
            )))
        }
    })
}

async fn tls_from_env() -> partitionline::Result<Option<TlsConfig>> {
    let Ok(ca_path) = std::env::var("TLS_CA_PEM") else {
        return Ok(None);
    };
    let mut tls = TlsConfig::default().ca_pem(
        tokio::fs::read(&ca_path)
            .await
            .map_err(|e| Error::protocol(format!("read TLS_CA_PEM {ca_path}: {e}")))?,
    );
    if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
        if !name.is_empty() {
            tls = tls.server_name(name);
        }
    }
    Ok(Some(tls))
}

async fn admin_cfg() -> partitionline::Result<AdminConfig> {
    let bootstrap = env("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let mut cfg = AdminConfig::bootstrap([bootstrap]).sasl(sasl_from_env()?);
    if let Some(tls) = tls_from_env().await? {
        cfg = cfg.tls(tls);
    }
    Ok(cfg)
}

async fn consumer_cfg() -> partitionline::Result<ConsumerConfig> {
    let bootstrap = env("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let mut cfg = ConsumerConfig::bootstrap([bootstrap]).sasl(sasl_from_env()?);
    cfg.max_wait_ms = 50;
    cfg.heartbeat_interval = Duration::from_millis(80);
    if let Some(tls) = tls_from_env().await? {
        cfg = cfg.tls(tls);
    }
    Ok(cfg)
}

fn fail_closed(err: &Error) -> bool {
    matches!(
        err,
        Error::Broker { .. } | Error::Closed | Error::Protocol(_) | Error::Io(_) | Error::Timeout
    )
}

async fn run_admin_reauth(sleep_ms: u64) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut admin = Admin::new(admin_cfg().await?).await?;
    let first = admin.describe_cluster().await?;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    let second = admin.describe_cluster().await?;
    admin.close().await?;
    let ok = !first.brokers().is_empty() && !second.brokers().is_empty();
    emit(
        "admin_reauth",
        ok,
        &format!(
            "brokers={} then {} after {}ms",
            first.brokers().len(),
            second.brokers().len(),
            started.elapsed().as_millis()
        ),
    );
    if ok {
        Ok(())
    } else {
        Err("admin_reauth lost cluster brokers after the session window".into())
    }
}

async fn run_consumer_reauth(sleep_ms: u64) -> Result<(), Box<dyn std::error::Error>> {
    let topic = env("KAFKA_TOPIC", "partitionline");
    let mut consumer = Consumer::new(consumer_cfg().await?).await?;
    let first = consumer.partitions_for(&topic).await?;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    let second = consumer.partitions_for(&topic).await?;
    consumer.close().await?;
    let ok = !first.is_empty() && first.len() == second.len();
    emit(
        "consumer_reauth",
        ok,
        &format!("partitions={} then {}", first.len(), second.len()),
    );
    if ok {
        Ok(())
    } else {
        Err("consumer_reauth lost partitions after the session window".into())
    }
}

async fn run_group_reauth(sleep_ms: u64) -> Result<(), Box<dyn std::error::Error>> {
    let topic = env("KAFKA_TOPIC", "partitionline");
    let group_id = env("KAFKA_GROUP", "pl-auth-lifecycle");
    let cfg = consumer_cfg().await?;
    let mut last = None;
    let mut group = None;
    for attempt in 0..8_u32 {
        match ConsumerGroup::join(cfg.clone(), group_id.clone(), topic.clone()).await {
            Ok(joined) => {
                group = Some(joined);
                break;
            }
            Err(err)
                if err.broker_code().is_some_and(|code| {
                    matches!(
                        code,
                        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
                    )
                }) =>
            {
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt + 1))).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
    let mut group = match group {
        Some(g) => g,
        None => {
            return Err(last
                .map(Into::into)
                .unwrap_or_else(|| "group_reauth: join retry exhausted".into()))
        }
    };
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    let recs = group.poll().await?;
    group.leave().await?;
    emit(
        "group_reauth",
        true,
        &format!("poll_records={} after {sleep_ms}ms", recs.len()),
    );
    Ok(())
}

async fn run_admin_bad_password() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = env("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let username = env("KAFKA_USERNAME", "alice");
    let mut cfg =
        AdminConfig::bootstrap([bootstrap]).sasl(Sasl::plain(username, "definitely-wrong"));
    if let Some(tls) = tls_from_env().await? {
        cfg = cfg.tls(tls);
    }
    match Admin::new(cfg).await {
        Ok(_admin) => {
            emit(
                "admin_bad_password",
                false,
                "connect succeeded with bad password",
            );
            Err("admin_bad_password unexpectedly connected".into())
        }
        Err(err) if fail_closed(&err) => {
            emit("admin_bad_password", true, &format!("rejected: {err}"));
            Ok(())
        }
        Err(err) => {
            emit(
                "admin_bad_password",
                false,
                &format!("unexpected error class: {err}"),
            );
            Err(err.into())
        }
    }
}

async fn run_admin_idle_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = admin_cfg().await?;
    cfg.connections_max_idle = Duration::from_millis(400);
    let mut admin = Admin::new(cfg).await?;
    let first = admin.describe_cluster().await?;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let second = admin.describe_cluster().await?;
    admin.close().await?;
    let ok = !first.brokers().is_empty() && !second.brokers().is_empty();
    emit(
        "admin_idle_reconnect",
        ok,
        &format!(
            "idle reconnect brokers={} then {}",
            first.brokers().len(),
            second.brokers().len()
        ),
    );
    if ok {
        Ok(())
    } else {
        Err("admin_idle_reconnect failed after idle close".into())
    }
}
