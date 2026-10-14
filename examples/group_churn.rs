//! One group-protocol churn cell (KL03-21).
//!
//! `GROUP_CHURN_PHASE` is `seed`, `produce`, `member`, or `verify`. Profile and churn
//! come from `GROUP_CHURN_PROFILE` / `GROUP_CHURN_CHURN`. Session, heartbeat,
//! max-poll and share-lock budgets are separate env values.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use partitionline::{
    error, Admin, AdminConfig, AutoOffsetReset, Consumer, ConsumerConfig, ConsumerGroup, Error,
    IsolationLevel, NewTopic, ProduceRecord, Producer, ProducerConfig, Sasl, ShareGroup,
    TopicPartition,
};

const CRASH_EXIT: i32 = 77;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let phase = env::var("GROUP_CHURN_PHASE").unwrap_or_else(|_| "member".into());
    match phase.as_str() {
        "seed" => seed().await?,
        "produce" => produce_records(&Names::from_env()).await?,
        "member" => member().await?,
        "verify" => verify().await?,
        other => return Err(Error::protocol(format!("unknown GROUP_CHURN_PHASE {other}")).into()),
    }
    Ok(())
}

#[derive(Clone)]
struct Names {
    bootstrap: String,
    topic: String,
    group: String,
    profile: String,
    churn: String,
    member: String,
    rf: i16,
}

impl Names {
    fn from_env() -> Self {
        Self {
            bootstrap: env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:19092".into()),
            topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-grp".into()),
            group: env::var("KAFKA_GROUP").unwrap_or_else(|_| "pl-grp".into()),
            profile: env::var("GROUP_CHURN_PROFILE").unwrap_or_else(|_| "classic".into()),
            churn: env::var("GROUP_CHURN_CHURN").unwrap_or_else(|_| "member_loss".into()),
            member: env::var("GROUP_CHURN_MEMBER").unwrap_or_else(|_| "a".into()),
            rf: i16::try_from(env_u64("GROUP_CHURN_RF", 3).unwrap_or(3)).unwrap_or(3),
        }
    }

    fn servers(&self) -> impl Iterator<Item = &str> {
        self.bootstrap.split(',').map(str::trim)
    }
}

fn env_u64(name: &str, default: u64) -> partitionline::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| Error::protocol(format!("invalid {name}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(Error::protocol(format!("invalid {name}"))),
    }
}

fn sasl_from_env() -> Option<Sasl> {
    let user = env::var("KAFKA_USERNAME").ok()?;
    let pass = env::var("KAFKA_PASSWORD").ok()?;
    Some(Sasl::plain(user, pass))
}

fn coordinator_starting(error: &Error) -> bool {
    matches!(
        error.broker_code(),
        Some(
            error::COORDINATOR_NOT_AVAILABLE
                | error::COORDINATOR_LOAD_IN_PROGRESS
                | error::NOT_COORDINATOR
                | error::INVALID_REPLICATION_FACTOR
                | error::LEADER_NOT_AVAILABLE
                | error::NOT_LEADER_OR_FOLLOWER
                | error::NOT_ENOUGH_REPLICAS
                | error::ILLEGAL_GENERATION
                | error::STALE_MEMBER_EPOCH
                | error::UNKNOWN_MEMBER_ID
                | error::REBALANCE_IN_PROGRESS
        )
    )
}

async fn retry<T, F, Fut>(timeout: Duration, mut body: F) -> partitionline::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = partitionline::Result<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match body().await {
            Ok(value) => return Ok(value),
            Err(error) if coordinator_starting(&error) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn consumer_config(names: &Names) -> partitionline::Result<ConsumerConfig> {
    let session_ms = env_u64("GROUP_CHURN_SESSION_MS", 10_000)?;
    let hb_ms = env_u64("GROUP_CHURN_HEARTBEAT_MS", 150)?;
    let max_poll_ms = env_u64("GROUP_CHURN_MAX_POLL_MS", 4_000)?;
    let mut cfg = ConsumerConfig::bootstrap(names.servers())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .auto_commit(false)
        .isolation(IsolationLevel::ReadUncommitted)
        .max_wait_ms(250)
        .max_poll_records(8)
        .session_timeout(Duration::from_millis(session_ms))
        .heartbeat_interval(Duration::from_millis(hb_ms))
        .max_poll_interval(Duration::from_millis(max_poll_ms))
        .request_timeout(Duration::from_secs(8));
    if let Some(sasl) = sasl_from_env() {
        cfg = cfg.sasl(sasl);
    }
    Ok(cfg)
}

async fn ensure_topic(names: &Names) -> partitionline::Result<()> {
    retry(Duration::from_secs(30), || {
        let names = names.clone();
        async move {
            let mut acfg = AdminConfig::bootstrap(names.servers());
            if let Some(sasl) = sasl_from_env() {
                acfg = acfg.sasl(sasl);
            }
            let mut admin = Admin::new(acfg).await?;
            let min_isr = if names.rf >= 3 { "2" } else { "1" };
            let created = admin
                .create_topics(
                    &[NewTopic::new(names.topic.clone(), 3, names.rf)
                        .config("min.insync.replicas", min_isr)],
                    30_000,
                    false,
                )
                .await?;
            for result in created {
                if result.error_code != 0 && result.error_code != 36 {
                    return Err(Error::broker(result.error_code, result.name));
                }
            }
            admin.close().await?;
            Ok(())
        }
    })
    .await
}

async fn seed() -> partitionline::Result<()> {
    let names = Names::from_env();
    ensure_topic(&names).await?;
    if env_flag("GROUP_CHURN_SKIP_PRODUCE") {
        return Ok(());
    }
    produce_records(&names).await
}

fn env_flag(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref(),
        Some("1" | "true" | "yes" | "TRUE" | "YES")
    )
}

async fn produce_records(names: &Names) -> partitionline::Result<()> {
    let mut pcfg = ProducerConfig::bootstrap(names.servers())
        .acks(partitionline::Acks::All)
        .idempotent(true)
        .linger(Duration::ZERO)
        .request_timeout(Duration::from_secs(8))
        .delivery_timeout(Duration::from_secs(12));
    if let Some(sasl) = sasl_from_env() {
        pcfg = pcfg.sasl(sasl);
    }
    let producer = retry(Duration::from_secs(20), || {
        let cfg = pcfg.clone();
        async move { Producer::new(cfg).await }
    })
    .await?;
    let count = env_u64("GROUP_CHURN_RECORDS", 6)?;
    for i in 0..count {
        let id = format!("g{i:02}");
        let part = i32::try_from(i % 3).unwrap_or(0);
        drop(
            producer
                .send(
                    ProduceRecord::to(names.topic.clone())
                        .partition(part)
                        .key(id.as_bytes().to_vec())
                        .value(id.as_bytes().to_vec()),
                )
                .await?,
        );
    }
    producer.close().await?;
    Ok(())
}

fn rec_text(bytes: Option<&[u8]>) -> String {
    bytes
        .and_then(|v| std::str::from_utf8(v).ok())
        .unwrap_or("missing")
        .to_string()
}

async fn write_path(path: &PathBuf, body: &str) -> partitionline::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::protocol(e.to_string()))?;
    }
    tokio::fs::write(path, body)
        .await
        .map_err(|e| Error::protocol(e.to_string()))
}

async fn wait_file(path: &PathBuf, timeout: Duration) -> partitionline::Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::fs::try_exists(path)
            .await
            .map_err(|e| Error::protocol(e.to_string()))?
        {
            return tokio::fs::read_to_string(path)
                .await
                .map_err(|e| Error::protocol(e.to_string()));
        }
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn go_action(body: &str) -> String {
    if body.contains("die") {
        "die".into()
    } else if body.contains("hold") {
        "hold".into()
    } else {
        "continue".into()
    }
}

#[derive(Clone)]
struct Seen {
    id: String,
    partition: i32,
    offset: i64,
    delivery_count: i16,
}

fn push_seen(out: &mut Vec<Seen>, id: String, partition: i32, offset: i64, delivery_count: i16) {
    if out.iter().any(|s| s.id == id) {
        return;
    }
    out.push(Seen {
        id,
        partition,
        offset,
        delivery_count,
    });
}

async fn member() -> partitionline::Result<()> {
    let names = Names::from_env();
    if names.profile == "share" {
        return share_member(&names).await;
    }
    partition_member(&names).await
}

async fn join_group(names: &Names) -> partitionline::Result<ConsumerGroup> {
    let cfg = consumer_config(names)?;
    retry(Duration::from_secs(20), || {
        let cfg = cfg.clone();
        let group_id = names.group.clone();
        let topic = names.topic.clone();
        let profile = names.profile.clone();
        async move {
            match profile.as_str() {
                "cooperative" => {
                    ConsumerGroup::join_cooperative_sticky_topics(cfg, group_id, [topic]).await
                }
                "kip848" => ConsumerGroup::join_consumer_topics(cfg, group_id, [topic]).await,
                _ => ConsumerGroup::join_topics(cfg, group_id, [topic]).await,
            }
        }
    })
    .await
}

async fn partition_member(names: &Names) -> partitionline::Result<()> {
    let mut group = join_group(names).await?;
    let ready = PathBuf::from(env::var("GROUP_CHURN_READY").unwrap_or_else(|_| "ready".into()));
    let go = PathBuf::from(env::var("GROUP_CHURN_GO").unwrap_or_else(|_| "go".into()));
    let history = PathBuf::from(
        env::var("GROUP_CHURN_HISTORY").unwrap_or_else(|_| "group-churn-member.json".into()),
    );
    let mut seen = Vec::new();
    arm_partition_poll(&mut group, &mut seen, Duration::from_secs(12)).await?;
    write_path(&ready, &render_ready(names, &group.assignment())).await?;
    let action = go_action(&wait_file(&go, Duration::from_secs(40)).await?);
    if action == "die" {
        drop(group.leave().await);
        std::process::exit(CRASH_EXIT);
    }
    let mut events: Vec<&str> = vec!["joined"];
    if action == "hold" {
        let hold_ms = env_u64("GROUP_CHURN_MAX_POLL_MS", 4_000)?.saturating_add(800);
        tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        match group.poll_timeout(Duration::from_secs(2)).await {
            Err(Error::MaxPollInterval) => events.push("max_poll_left"),
            Err(error) if group.is_left_max_poll() => {
                drop(error);
                events.push("max_poll_left");
            }
            Err(error) => return Err(error),
            Ok(_) => return Err(Error::protocol("member was not kicked after max poll")),
        }
        write_path(&history, &render_member(names, &seen, &events, true)).await?;
        return Ok(());
    }
    if names.churn == "pause" {
        collect_partition(&mut group, &mut seen, 2, Duration::from_secs(12)).await?;
        let assigned = group.assignment();
        group.pause(assigned);
        events.push("paused");
        let paused =
            PathBuf::from(env::var("GROUP_CHURN_PAUSED").unwrap_or_else(|_| "paused".into()));
        write_path(&paused, "paused\n").await?;
        let resume =
            PathBuf::from(env::var("GROUP_CHURN_RESUME").unwrap_or_else(|_| "resume".into()));
        drop(wait_file(&resume, Duration::from_secs(20)).await?);
        let assigned = group.assignment();
        group.resume(assigned);
        events.push("resumed");
    }
    collect_partition(&mut group, &mut seen, 6, Duration::from_secs(25)).await?;
    if names.churn != "max_poll_expiry" {
        match group.commit().await {
            Ok(()) => events.push("committed"),
            Err(error)
                if matches!(
                    error.broker_code(),
                    Some(
                        error::ILLEGAL_GENERATION
                            | error::STALE_MEMBER_EPOCH
                            | error::UNKNOWN_MEMBER_ID
                            | error::REBALANCE_IN_PROGRESS
                    )
                ) =>
            {
                events.push("commit_retried");
            }
            Err(error) => return Err(error),
        }
    }
    drop(group.leave().await);
    write_path(&history, &render_member(names, &seen, &events, false)).await?;
    Ok(())
}

async fn arm_partition_poll(
    group: &mut ConsumerGroup,
    seen: &mut Vec<Seen>,
    timeout: Duration,
) -> partitionline::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match group.poll_timeout(Duration::from_millis(800)).await {
            Ok(recs) => {
                for rec in recs.iter() {
                    push_seen(
                        seen,
                        rec_text(rec.value.as_deref()),
                        rec.partition,
                        rec.offset,
                        1,
                    );
                }
                return Ok(());
            }
            Err(Error::Timeout) if Instant::now() < deadline => {}
            Err(error)
                if (error.is_retriable() || coordinator_starting(&error))
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(Error::Timeout) => return Ok(()),
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
    }
}

async fn collect_partition(
    group: &mut ConsumerGroup,
    seen: &mut Vec<Seen>,
    need: usize,
    timeout: Duration,
) -> partitionline::Result<()> {
    let deadline = Instant::now() + timeout;
    while seen.len() < need && Instant::now() < deadline {
        match group.poll_timeout(Duration::from_millis(800)).await {
            Ok(recs) => {
                for rec in recs.iter() {
                    push_seen(
                        seen,
                        rec_text(rec.value.as_deref()),
                        rec.partition,
                        rec.offset,
                        1,
                    );
                }
                if !recs.is_empty() {
                    drop(group.commit_with_metadata(recs.next_offsets()).await);
                }
            }
            Err(Error::Timeout) => {}
            Err(error) if error.is_retriable() || coordinator_starting(&error) => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(Error::MaxPollInterval) => return Err(Error::MaxPollInterval),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn join_share(names: &Names) -> partitionline::Result<ShareGroup> {
    let cfg = consumer_config(names)?;
    retry(Duration::from_secs(20), || {
        let cfg = cfg.clone();
        let group_id = names.group.clone();
        let topic = names.topic.clone();
        async move { ShareGroup::join(cfg, group_id, topic).await }
    })
    .await
}

async fn recover_share_after_loss(
    mut group: ShareGroup,
    names: &Names,
    seen: &mut Vec<Seen>,
    events: &mut Vec<&str>,
) -> partitionline::Result<ShareGroup> {
    let deadline = Instant::now() + Duration::from_secs(8);
    while group.assignment().len() < 3 && Instant::now() < deadline {
        match group.poll_timeout(Duration::from_millis(400)).await {
            Ok(recs) => {
                if recs.is_empty() {
                    continue;
                }
                for rec in recs.iter() {
                    push_seen(
                        seen,
                        rec_text(rec.value.as_deref()),
                        rec.partition,
                        rec.offset,
                        rec.delivery_count,
                    );
                }
                group.accept(recs.as_ref()).await?;
            }
            Err(Error::Timeout) => {}
            Err(error) if error.is_retriable() || coordinator_starting(&error) => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(error) => return Err(error),
        }
    }
    if group.assignment().len() >= 3 {
        events.push("reassigned");
        return Ok(group);
    }
    events.push("rejoined");
    drop(group.leave().await);
    let mut group = join_share(names).await?;
    arm_share_poll(&mut group, Duration::from_secs(12)).await?;
    Ok(group)
}

async fn share_member(names: &Names) -> partitionline::Result<()> {
    let mut group = join_share(names).await?;
    let ready = PathBuf::from(env::var("GROUP_CHURN_READY").unwrap_or_else(|_| "ready".into()));
    let go = PathBuf::from(env::var("GROUP_CHURN_GO").unwrap_or_else(|_| "go".into()));
    let history = PathBuf::from(
        env::var("GROUP_CHURN_HISTORY").unwrap_or_else(|_| "group-churn-member.json".into()),
    );
    arm_share_poll(&mut group, Duration::from_secs(12)).await?;
    write_path(&ready, &render_ready(names, &group.assignment())).await?;
    let action = go_action(&wait_file(&go, Duration::from_secs(40)).await?);
    if action == "die" {
        drop(group.leave().await);
        std::process::exit(CRASH_EXIT);
    }
    let mut seen = Vec::new();
    let mut events: Vec<&str> = vec!["joined"];
    if action == "hold" {
        let hold_ms = env_u64("GROUP_CHURN_SHARE_LOCK_MS", 2_000)?;
        let recs = group.poll_timeout(Duration::from_secs(8)).await?;
        for rec in recs.iter() {
            push_seen(
                seen.as_mut(),
                rec_text(rec.value.as_deref()),
                rec.partition,
                rec.offset,
                rec.delivery_count,
            );
        }
        events.push("lock_held");
        tokio::time::sleep(Duration::from_millis(hold_ms.saturating_add(400))).await;
        if names.member == "a" {
            drop(group.release(recs.as_ref()).await);
            events.push("released_for_redelivery");
            write_path(&history, &render_member(names, &seen, &events, true)).await?;
            drop(group.leave().await);
            return Ok(());
        }
    }
    if names.churn == "pause" {
        let recs = group.poll_timeout(Duration::from_secs(8)).await?;
        for rec in recs.iter() {
            push_seen(
                seen.as_mut(),
                rec_text(rec.value.as_deref()),
                rec.partition,
                rec.offset,
                rec.delivery_count,
            );
        }
        events.push("paused_hold");
        let paused =
            PathBuf::from(env::var("GROUP_CHURN_PAUSED").unwrap_or_else(|_| "paused".into()));
        write_path(&paused, "paused\n").await?;
        let resume =
            PathBuf::from(env::var("GROUP_CHURN_RESUME").unwrap_or_else(|_| "resume".into()));
        drop(wait_file(&resume, Duration::from_secs(20)).await?);
        group.accept(recs.as_ref()).await?;
        events.push("accepted");
    }
    if names.churn == "member_loss" {
        group = recover_share_after_loss(group, names, &mut seen, &mut events).await?;
    }
    collect_share(&mut group, &mut seen, 6, Duration::from_secs(25)).await?;
    events.push("accepted");
    drop(group.leave().await);
    write_path(&history, &render_member(names, &seen, &events, false)).await?;
    Ok(())
}

async fn arm_share_poll(group: &mut ShareGroup, timeout: Duration) -> partitionline::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match group.poll_timeout(Duration::from_millis(800)).await {
            Ok(recs) => {
                if !recs.is_empty() {
                    drop(group.release(recs.as_ref()).await);
                }
                return Ok(());
            }
            Err(Error::Timeout) if Instant::now() < deadline => {}
            Err(error)
                if (error.is_retriable() || coordinator_starting(&error))
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(Error::Timeout) => return Ok(()),
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
    }
}

async fn collect_share(
    group: &mut ShareGroup,
    seen: &mut Vec<Seen>,
    need: usize,
    timeout: Duration,
) -> partitionline::Result<()> {
    let deadline = Instant::now() + timeout;
    while seen.len() < need && Instant::now() < deadline {
        match group.poll_timeout(Duration::from_millis(800)).await {
            Ok(recs) => {
                if recs.is_empty() {
                    continue;
                }
                for rec in recs.iter() {
                    push_seen(
                        seen,
                        rec_text(rec.value.as_deref()),
                        rec.partition,
                        rec.offset,
                        rec.delivery_count,
                    );
                }
                group.accept(recs.as_ref()).await?;
            }
            Err(Error::Timeout) => {}
            Err(error) if error.is_retriable() || coordinator_starting(&error) => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn verify() -> partitionline::Result<()> {
    let names = Names::from_env();
    let mut ccfg = ConsumerConfig::bootstrap(names.servers())
        .isolation(IsolationLevel::ReadUncommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .request_timeout(Duration::from_secs(8));
    ccfg.max_wait_ms = 400;
    if let Some(sasl) = sasl_from_env() {
        ccfg = ccfg.sasl(sasl);
    }
    let mut consumer = Consumer::new(ccfg).await?;
    consumer
        .assign_partitions((0..3).map(|p| TopicPartition::new(names.topic.clone(), p)))
        .await?;
    let wait = Duration::from_millis(env_u64("GROUP_CHURN_VERIFY_MS", 4_000)?);
    let deadline = Instant::now() + wait;
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        match consumer.fetch_timeout(Duration::from_millis(400)).await {
            Ok(recs) => {
                for rec in recs.iter() {
                    push_seen(
                        &mut seen,
                        rec_text(rec.value.as_deref()),
                        rec.partition,
                        rec.offset,
                        1,
                    );
                }
            }
            Err(Error::Timeout) => {}
            Err(error) if error.is_retriable() => {}
            Err(error) => return Err(error),
        }
    }
    let history = PathBuf::from(
        env::var("GROUP_CHURN_HISTORY").unwrap_or_else(|_| "group-churn-verify.json".into()),
    );
    write_path(&history, &render_member(&names, &seen, &["verify"], false)).await?;
    Ok(())
}

fn json_escape(s: &str) -> String {
    let mut out = String::new();
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn render_ready(names: &Names, assignment: &[TopicPartition]) -> String {
    let mut parts = String::new();
    for (i, tp) in assignment.iter().enumerate() {
        if i > 0 {
            parts.push(',');
        }
        parts.push_str(&json_escape(&format!("{}-{}", tp.topic, tp.partition)));
    }
    format!(
        "{{\"member\":{},\"profile\":{},\"churn\":{},\"assignment\":[{}]}}\n",
        json_escape(&names.member),
        json_escape(&names.profile),
        json_escape(&names.churn),
        parts
    )
}

fn render_member(names: &Names, seen: &[Seen], events: &[&str], uncommitted: bool) -> String {
    let mut recs = String::new();
    for (i, rec) in seen.iter().enumerate() {
        if i > 0 {
            recs.push(',');
        }
        recs.push_str("{\"id\":");
        recs.push_str(&json_escape(&rec.id));
        recs.push_str(",\"topic\":");
        recs.push_str(&json_escape(&names.topic));
        recs.push_str(",\"partition\":");
        recs.push_str(&rec.partition.to_string());
        recs.push_str(",\"offset\":");
        recs.push_str(&rec.offset.to_string());
        recs.push_str(",\"key\":");
        recs.push_str(&json_escape(&rec.id));
        recs.push_str(",\"payload\":");
        recs.push_str(&json_escape(&rec.id));
        recs.push_str(",\"status\":\"acked\"");
        if names.profile == "share" {
            recs.push_str(",\"ack_type\":\"accept\",\"delivery_attempt\":");
            recs.push_str(&rec.delivery_count.to_string());
        }
        recs.push('}');
    }
    let mut ev = String::new();
    for (i, event) in events.iter().enumerate() {
        if i > 0 {
            ev.push(',');
        }
        ev.push_str(&json_escape(event));
    }
    format!(
        "{{\"id\":{},\"member\":{},\"profile\":{},\"churn\":{},\"uncommitted\":{},\"events\":[{}],\"consumed\":[{}]}}\n",
        json_escape(&format!("{}/{}", names.profile, names.churn)),
        json_escape(&names.member),
        json_escape(&names.profile),
        json_escape(&names.churn),
        if uncommitted { "true" } else { "false" },
        ev,
        recs
    )
}
