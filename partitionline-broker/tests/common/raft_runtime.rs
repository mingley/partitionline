use partitionline_broker::raft::{
    membership::{Bootstrap, Endpoint, Key, Voter, Voters},
    protocol,
    runtime::{Config, Limits, Paths, Peer, Runtime, StorageLimits},
    snapshot,
};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn hex(bytes: &[u8]) -> TestResult<String> {
    use std::fmt::Write;
    let mut out = String::new();
    out.try_reserve_exact(bytes.len() * 2)?;
    for b in bytes {
        write!(out, "{b:02x}")?;
    }
    Ok(out)
}
fn unhex(s: &str) -> TestResult<Vec<u8>> {
    if s.len() > 128 * 1024 || !s.len().is_multiple_of(2) {
        return Err("bounded hex".into());
    }
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| Ok(u8::from_str_radix(std::str::from_utf8(b)?, 16)?))
        .collect()
}
fn read_text(path: &Path, limit: u64) -> TestResult<String> {
    let mut out = String::new();
    File::open(path)?.take(limit + 1).read_to_string(&mut out)?;
    if out.len() as u64 > limit {
        return Err("text bound".into());
    }
    Ok(out)
}
// Build bounded raw-text context only when a response fails validation.
fn diagnostic_snippet(text: &str) -> String {
    const MAX_BYTES: usize = 256;
    let mut end = text.len().min(MAX_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == text.len() {
        format!("{text:?}")
    } else {
        format!(
            "{:?} [truncated; original_bytes={}]",
            &text[..end],
            text.len()
        )
    }
}
fn response_fields<'a, const N: usize>(answer: &'a str, kind: &str) -> TestResult<[&'a str; N]> {
    let mut fields = answer.split_whitespace();
    if fields.next() != Some(kind) {
        return Err(format!(
            "expected {kind} response, got {}",
            diagnostic_snippet(answer)
        )
        .into());
    }
    let mut result = [""; N];
    for field in &mut result {
        *field = fields
            .next()
            .ok_or_else(|| format!("missing {kind} field in {}", diagnostic_snippet(answer)))?;
    }
    if fields.next().is_some() {
        return Err(format!("extra {kind} fields in {}", diagnostic_snippet(answer)).into());
    }
    Ok(result)
}
fn response_value<T: std::str::FromStr>(
    value: &str,
    context: impl FnOnce() -> String,
) -> TestResult<T>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|error| {
        format!(
            "invalid {} value {}: {error}",
            context(),
            diagnostic_snippet(value)
        )
        .into()
    })
}
fn proposal_position_response(answer: &str) -> TestResult<(u64, u64)> {
    let [term, index] = response_fields(answer, "position")?;
    let term = response_value(term, || {
        format!("proposal position response {}", diagnostic_snippet(answer))
    })?;
    let index = response_value(index, || {
        format!("proposal position response {}", diagnostic_snippet(answer))
    })?;
    if term == 0 || index == 0 {
        return Err(format!(
            "invalid proposal position response {}",
            diagnostic_snippet(answer)
        )
        .into());
    }
    Ok((term, index))
}
fn image_response(answer: &str) -> TestResult<(u64, u64, u64)> {
    let [term, index, bytes] = response_fields(answer, "image")?;
    Ok((
        response_value(term, || {
            format!("checkpoint response {}", diagnostic_snippet(answer))
        })?,
        response_value(index, || {
            format!("checkpoint response {}", diagnostic_snippet(answer))
        })?,
        response_value(bytes, || {
            format!("checkpoint response {}", diagnostic_snippet(answer))
        })?,
    ))
}
/// Verify every record through the selected image base. Expected proposals
/// must match exactly; every other entry must be an empty election barrier.
fn check_checkpoint_prefix(
    prefix: &str,
    expected: &[(u64, u64, String)],
    image_term: u64,
    image_base: u64,
) -> TestResult {
    if image_base == 0 {
        return Err("checkpoint base must be nonzero".into());
    }
    let mut prior = 0;
    for (term, index, _) in expected {
        if *term == 0 || *index <= prior || *index > image_base {
            return Err("expected checkpoint positions are invalid".into());
        }
        prior = *index;
    }
    let mut entries = prefix.split(';');
    let mut next_data = 0;
    let mut last_term = 0;
    for position in 1..=image_base {
        let entry = entries.next().ok_or("checkpoint prefix missing entry")?;
        let mut fields = entry.splitn(4, ':');
        let term: u64 = fields.next().ok_or("missing term")?.parse()?;
        let index: u64 = fields.next().ok_or("missing index")?.parse()?;
        let kind = fields.next().ok_or("missing kind")?;
        let payload = fields.next().ok_or("missing payload")?;
        if term == 0 || index != position {
            return Err("checkpoint prefix position mismatch".into());
        }
        if let Some((expected_term, expected_index, expected_payload)) = expected.get(next_data) {
            if index == *expected_index {
                if term != *expected_term || kind != "0" || payload != expected_payload {
                    return Err("checkpoint prefix changed expected data".into());
                }
                next_data += 1;
            } else if kind != "1" || !payload.is_empty() {
                return Err("checkpoint prefix has an unexpected entry".into());
            }
        } else if kind != "1" || !payload.is_empty() {
            return Err("checkpoint prefix has an unexpected entry".into());
        }
        last_term = term;
    }
    if next_data != expected.len() || last_term != image_term {
        return Err("checkpoint base or expected data mismatch".into());
    }
    Ok(())
}

#[test]
fn checkpoint_prefix_requires_exact_data_and_only_barriers() {
    let good = "1:1:1:;1:2:0:6162;2:3:1:;2:4:0:6364;3:5:1:";
    let expected = vec![(1, 2, "6162".to_string()), (2, 4, "6364".to_string())];
    assert!(check_checkpoint_prefix(good, &expected, 3, 5).is_ok());
    for (text, term, base) in [
        // Unexpected data before the first expected record.
        ("1:1:0:65;1:2:0:6162;2:3:1:;2:4:0:6364;3:5:1:", 3, 5),
        // Unexpected data or configuration between expected records.
        ("1:1:1:;1:2:0:6162;2:3:0:65;2:4:0:6364;3:5:1:", 3, 5),
        ("1:1:1:;1:2:0:6162;2:3:2:65;2:4:0:6364;3:5:1:", 3, 5),
        // Unexpected data after the last expected record.
        ("1:1:1:;1:2:0:6162;2:3:1:;2:4:0:6364;3:5:0:65", 3, 5),
        // A barrier cannot carry data, and the prefix must be complete.
        ("1:1:1:;1:2:0:6162;2:3:1:65;2:4:0:6364;3:5:1:", 3, 5),
        ("1:1:1:;1:2:0:6162;2:3:1:;2:4:0:6364", 3, 5),
        // The data and image base term must match.
        ("1:1:1:;1:2:0:61;2:3:1:;2:4:0:6364;3:5:1:", 3, 5),
        (good, 2, 5),
        (good, 2, 3),
    ] {
        assert!(check_checkpoint_prefix(text, &expected, term, base).is_err());
    }
    assert!(check_checkpoint_prefix(good, &[(1, 0, "61".into())], 3, 5).is_err());
    assert!(check_checkpoint_prefix(good, &[(1, 2, "6162".into()), (1, 2, "6162".into())], 3, 5).is_err());
}

fn change_response(answer: &str) -> TestResult<(u64, u64, bool)> {
    let [epoch, index, committed] = response_fields(answer, "change")?;
    Ok((
        response_value(epoch, || {
            format!("voter change response {}", diagnostic_snippet(answer))
        })?,
        response_value(index, || {
            format!("voter change response {}", diagnostic_snippet(answer))
        })?,
        response_value(committed, || {
            format!("voter change response {}", diagnostic_snippet(answer))
        })?,
    ))
}

#[test]
fn response_diagnostics_bound_multibyte_and_control_text() {
    for suffix in ["🦀".repeat(512), "\u{1b}".repeat(1024)] {
        for answer in [format!("error {suffix}"), format!("position 1 {suffix}")] {
            let error = proposal_position_response(&answer).unwrap_err().to_string();
            assert!(error.contains("truncated; original_bytes="));
            assert!(
                error.len() < 4096,
                "diagnostic grew unexpectedly: {}",
                error.len()
            );
        }
    }
}

#[test]
fn numeric_response_context_is_built_only_for_errors() {
    let called = std::cell::Cell::new(false);
    let value: u64 = response_value("17", || {
        called.set(true);
        "unneeded context".to_owned()
    })
    .unwrap();
    assert_eq!(value, 17);
    assert!(!called.get());
    let error = response_value::<u64>("x", || {
        called.set(true);
        "sample context".to_owned()
    })
    .unwrap_err()
    .to_string();
    assert!(called.get());
    assert!(error.contains("sample context"));
}

#[test]
fn proposal_responses_require_a_complete_position_and_retain_failures() {
    assert_eq!(
        proposal_position_response("position 2 17").unwrap(),
        (2, 17)
    );
    for answer in [
        "error private peer runtime: Node(NotLeader)",
        "error private peer runtime: Deadline",
        "position 1 x",
        "position x 2",
        "position 1 2 extra",
        "position 0 1",
        "position 1 0",
        "position 1",
    ] {
        let error = proposal_position_response(answer).unwrap_err().to_string();
        assert!(error.contains(answer), "the actual reply was lost: {error}");
    }
}

#[test]
fn checkpoint_and_voter_responses_retain_the_actual_failure() {
    assert_eq!(image_response("image 2 17 8192").unwrap(), (2, 17, 8192));
    assert_eq!(change_response("change 3 18 true").unwrap(), (3, 18, true));
    for answer in [
        "error private peer runtime: Node(NotLeader)",
        "image 2 17 x",
        "image 2 x 8192",
        "image x 17 8192",
        "image 2 17",
        "image 2 17 8192 extra",
    ] {
        let error = image_response(answer).unwrap_err().to_string();
        assert!(error.contains(answer), "the actual reply was lost: {error}");
    }
    for answer in [
        "error private peer runtime: Deadline",
        "change x 18 true",
        "change 3 x true",
        "change 3 18 x",
        "change 3 18",
        "change 3 18 true extra",
    ] {
        let error = change_response(answer).unwrap_err().to_string();
        assert!(error.contains(answer), "the actual reply was lost: {error}");
    }
}

fn write_text(path: &Path, s: &str) -> TestResult {
    let tmp = path.with_extension("partial");
    let mut file = File::create(&tmp)?;
    file.write_all(s.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(tmp, path)?;
    File::open(path.parent().ok_or("parent")?)?.sync_all()?;
    Ok(())
}
fn temporary(name: &str) -> TestResult<PathBuf> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let p = std::env::temp_dir().join(format!(
        "partitionline76-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p)?;
    Ok(p)
}
fn voter(id: u32, address: SocketAddr) -> TestResult<Voter> {
    Ok(Voter::new(
        Key::new(id, [(id + 1) as u8; 16])?,
        vec![Endpoint::new(
            "CONTROLLER".into(),
            address.ip().to_string(),
            address.port(),
        )?],
        0,
        1,
    )?)
}
fn config(local: usize, routes: &[SocketAddr], active: usize) -> TestResult<Config> {
    config_timing(local, routes, active, false)
}

fn config_timing(local: usize, routes: &[SocketAddr], active: usize, long_election: bool) -> TestResult<Config> {
    let voters = routes
        .iter()
        .enumerate()
        .map(|(i, a)| voter(i as u32, *a))
        .collect::<TestResult<Vec<_>>>()?;
    let genesis = Voters::new(0, Default::default(), 1, voters[..active].to_vec())?;
    let bootstrap = Bootstrap::new(voters[local].clone(), genesis, "CONTROLLER".into())?;
    let peers = voters
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != local)
        .map(|(i, v)| Ok(Peer::new(v.clone(), routes[i])?))
        .collect::<TestResult<Vec<_>>>()?;
    let mut controller =
        protocol::Config::new(local as u32, vec![local as u32], "private-76-test".into())?;
    controller.timer_seed = local as u64 * 37 + 1;
    let storage = StorageLimits {
        record_bytes: 64 * 1024,
        chunk_bytes: 256 * 1024,
        live_entries: 64,
        live_bytes: 1024 * 1024,
        operations: 4096,
        wal_bytes: 8 * 1024 * 1024,
        fetch_bytes: 256 * 1024,
        images: snapshot::Limits::new(
            2 * 1024 * 1024,
            64,
            1024 * 1024,
            64 * 1024,
            32 * 1024,
            8,
            32,
        )?,
    };
    let limits = Limits {
        incoming: routes.len().saturating_sub(1).max(1),
        client_slots: 4,
        frame_bytes: 512 * 1024,
        election_min_ms: if long_election { 3000 } else { 80 },
        election_max_ms: if long_election { 4500 } else { 160 },
        quorum_ms: 1500,
        tick_ms: 10,
        heartbeat_ms: 40,
        connect_ms: 250,
        rpc_ms: 500,
        transfer_ms: 1000,
        command_ms: 3000,
    };
    Ok(Config::new(controller, bootstrap, peers, storage, limits)?)
}
fn paths(root: &Path) -> TestResult<Paths> {
    Ok(Paths::new(
        root.join("metadata.wal"),
        root.join("election.wal"),
        root.join("images"),
    )?)
}

#[test]
fn runtime_child_process() -> TestResult {
    let Some(root) = std::env::var_os("PL_RUNTIME_CHILD_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let local: usize = std::env::var("PL_RUNTIME_CHILD_ID")?.parse()?;
    let active: usize = std::env::var("PL_RUNTIME_CHILD_ACTIVE")?.parse()?;
    let text = std::env::var("PL_RUNTIME_CHILD_ROUTES")?;
    if text.len() > 8192 {
        return Err("routes bound".into());
    }
    let routes = text
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<Vec<SocketAddr>, _>>()?;
    if routes.is_empty() || routes.len() > 64 || local >= routes.len() || active > routes.len() {
        return Err("route count".into());
    }
    let backend: SocketAddr = std::env::var("PL_RUNTIME_CHILD_BACKEND")?.parse()?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?
        .block_on(async {
            let listener = TcpListener::bind(backend).await?;
            write_text(&root.join("listen-address"), &listener.local_addr()?.to_string())?;
            let long_election = std::env::var("PL_RUNTIME_CHILD_LONG_ELECTION").is_ok_and(|v| v == "1");
            let cfg = if long_election {
                config_timing(local, &routes, active, true)?
            } else {
                config(local, &routes, active)?
            };
            let configured_task_bound = cfg.task_bound();
            let configured_managed_bytes = cfg.managed_bytes();
            let configured_client_slots = 4;
            let configured_socket_bound = 2 * routes.len().saturating_sub(1).max(1);
            let mut runtime = Runtime::start(listener, cfg, paths(&root)?)?;
            runtime.wait_ready().await?;
            let handle = runtime.handle();
            write_text(&root.join("ready"), "ready\n")?;
            let mut next = 0;
            let mut sampled_peaks = [0usize; 4];
            loop {
                let observed = handle.diagnostics();
                assert!(observed.network_tasks <= configured_task_bound);
                assert!(observed.sockets <= configured_socket_bound);
                assert!(observed.transport_bytes <= configured_managed_bytes);
                assert!(observed.client_slots <= configured_client_slots);
                for (peak, value) in sampled_peaks.iter_mut().zip([
                    observed.network_tasks, observed.sockets, observed.transport_bytes, observed.client_slots
                ]) { *peak = (*peak).max(value); }
                let state = handle.state().await?;
                let records = handle.fetch_committed(1, 64, 256 * 1024).await?;
                let prefix = records
                    .iter()
                    .map(|r| {
                        Ok(format!(
                            "{}:{}:{}:{}",
                            r.term,
                            r.index,
                            match r.kind {
                                partitionline_broker::raft::replication::RecordKind::Data => 0,
                                partitionline_broker::raft::replication::RecordKind::Barrier => 1,
                                partitionline_broker::raft::replication::RecordKind::Voters => 2,
                            },
                            hex(&r.payload)?
                        ))
                    })
                    .collect::<TestResult<Vec<_>>>()?
                    .join(";");
                write_text(
                    &root.join("state"),
                    &format!(
                        "{} {:?} {} {} {}\n{prefix}\nimage {} {}\n",
                        state.election.persistent.term,
                        state.election.role,
                        state.last_position.index,
                        state.committed_end,
                        state.ready,
                        state.base_position.term,
                        state.base_position.index
                    ),
                )?;
                let inbox = root.join(format!("inbox-{next}"));
                if inbox.exists() {
                    let command = read_text(&inbox, 150 * 1024)?;
                    let answer = if let Some(value) = command.strip_prefix("propose ") {
                        match handle.propose(unhex(value.trim())?).await {
                            Ok(p) => format!("position {} {}", p.term, p.index),
                            Err(e) => format!("error {e}"),
                        }
                    } else if let Some(value) = command.strip_prefix("checkpoint ") {
                        let value: u8 = value.trim().parse()?;
                        match handle.checkpoint([value; 16]).await {
                            Ok(d) => format!("image {} {} {}", d.base.term, d.base.index, d.bytes),
                            Err(e) => format!("error {e}"),
                        }
                    } else if let Some(value) = command.strip_prefix("add ") {
                        let value: usize = value.trim().parse()?;
                        if value >= routes.len() { return Err("add route bound".into()); }
                        match handle.add_voter(voter(value as u32, routes[value])?.key()).await {
                            Ok(r) => format!("change {} {} {}", r.epoch, r.position.index, r.committed),
                            Err(e) => format!("error {e}"),
                        }
                    } else if let Some(value) = command.strip_prefix("remove ") {
                        let value: usize = value.trim().parse()?;
                        match handle
                            .remove_voter(voter(value as u32, routes[value])?.key())
                            .await
                        {
                            Ok(r) => {
                                format!("change {} {} {}", r.epoch, r.position.index, r.committed)
                            }
                            Err(e) => format!("error {e}"),
                        }
                    } else if command.trim() == "observe" {
                        let current = handle.state().await?;
                        format!(
                            "observe {} {} {} {}",
                            current.last_position.term,
                            current.last_position.index,
                            current.committed_end,
                            current.wal_durable_ops
                        )
                    } else if command.trim() == "exit" {
                        write_text(
                            &root.join("exit-receipt"),
                            "actual process exit88 after confirmed state publication\n",
                        )?;
                        std::process::exit(88);
                    } else if command.trim() == "shutdown" {
                        runtime.shutdown().await?;
                        assert_eq!(handle.diagnostics().network_tasks, 0);
                        assert_eq!(handle.diagnostics().sockets, 0);
                        assert_eq!(handle.diagnostics().transport_bytes, 0);
                        assert_eq!(handle.diagnostics().client_slots, 0);
                        let final_diagnostics = handle.diagnostics();
                        let receipt = format!("{{\"schema_version\":1,\"source_sha\":\"{}\",\"pid\":{},\"local_id\":{local},\"directory\":\"{}\",\"genesis_voter_count\":{active},\"configured_route_count\":{},\"configured_task_bound\":{configured_task_bound},\"configured_managed_bytes\":{configured_managed_bytes},\"supervisor_shutdown_returned\":true,\"sampled_peak_network_tasks\":{},\"sampled_peak_sockets\":{},\"sampled_peak_transport_bytes\":{},\"sampled_peak_client_slots\":{},\"sampled_peak_limit\":\"20ms observer samples are lower bounds; lib owner metrics retain logical lifetime high-water counters\",\"final_network_tasks\":{},\"final_sockets\":{},\"final_transport_bytes\":{},\"final_client_slots\":{},\"stopping\":{}}}\n", std::env::var("PL_PEER_RUNTIME_SOURCE_SHA").unwrap_or_else(|_| "unqualified-test".into()), std::process::id(), hex(&[(local+1) as u8;16])?, routes.len(), sampled_peaks[0],sampled_peaks[1],sampled_peaks[2],sampled_peaks[3],final_diagnostics.network_tasks,final_diagnostics.sockets,final_diagnostics.transport_bytes,final_diagnostics.client_slots,final_diagnostics.stopping);
                        write_text(&root.join("joined-receipt.json"), &receipt)?;
                        write_text(&root.join(format!("outbox-{next}")), "joined")?;
                        return Ok(());
                    } else {
                        return Err("unknown control command".into());
                    };
                    write_text(&root.join(format!("outbox-{next}")), &answer)?;
                    next += 1;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
}

struct Gate {
    count: usize,
    allowed: Vec<AtomicBool>,
    delay: Vec<AtomicU64>,
    connections: Vec<AtomicU64>,
    delayed_connection: Vec<AtomicU64>,
    delayed_prior_connection: Vec<AtomicU64>,
    delayed_rpc: Vec<AtomicU64>,
    delayed_ms: Vec<AtomicU64>,
    delayed_forward: Vec<AtomicU64>,
    delayed_request_at: Vec<AtomicU64>,
    delayed_selected_at: Vec<AtomicU64>,
    delayed_closed_at: Vec<AtomicU64>,
    delayed_forward_at: Vec<AtomicU64>,
    capture: Option<PathBuf>,
    clock: std::time::Instant,
    captured_bytes: AtomicU64,
    capture_failed: AtomicBool,
}
impl Gate {
    fn new(count: usize, capture: Option<PathBuf>) -> Self {
        Self {
            count,
            allowed: (0..count * count).map(|_| AtomicBool::new(true)).collect(),
            delay: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            connections: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_connection: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_prior_connection: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_rpc: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_ms: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_forward: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_request_at: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_selected_at: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_closed_at: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            delayed_forward_at: (0..count * count).map(|_| AtomicU64::new(0)).collect(),
            capture,
            clock: std::time::Instant::now(),
            captured_bytes: AtomicU64::new(0),
            capture_failed: AtomicBool::new(false),
        }
    }
    fn capture_file(&self, name: &str, bytes: &[u8]) -> TestResult {
        let result = (|| -> TestResult {
            let Some(root) = &self.capture else {
                return Ok(());
            };
            let size = u64::try_from(bytes.len())?;
            // Preserve fetch_update's capped, checked-add reservation on MSRV.
            // fetch_max would not charge cumulative bytes or enforce the cap.
            let mut current = self.captured_bytes.load(Ordering::Acquire);
            loop {
                let next = current
                    .checked_add(size)
                    .filter(|next| *next <= 16 * 1024 * 1024)
                    .ok_or("finite per-case proxy capture bytes")?;
                match self.captured_bytes.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(name))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.capture_failed.store(true, Ordering::Release);
        }
        result
    }
    fn set(&self, a: usize, b: usize, value: bool) {
        self.allowed[a * self.count + b].store(value, Ordering::Release);
    }
    fn permits(&self, a: usize, b: usize) -> bool {
        a < self.count
            && b < self.count
            && self.allowed[a * self.count + b].load(Ordering::Acquire)
            && self.allowed[b * self.count + a].load(Ordering::Acquire)
    }
}
async fn packet<R: AsyncRead + Unpin>(r: &mut R) -> TestResult<Vec<u8>> {
    let n = r.read_u32().await? as usize;
    if !(28..=512 * 1024).contains(&n) {
        return Err("frame bound".into());
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(n + 4)?;
    bytes.extend_from_slice(&(n as u32).to_be_bytes());
    bytes.resize(n + 4, 0);
    r.read_exact(&mut bytes[4..]).await?;
    Ok(bytes)
}
#[derive(Default)]
struct ConnectionObservation {
    append_rpc: AtomicU64,
    request_at: AtomicU64,
    request_closed_at: AtomicU64,
}

struct ForwardReceipt {
    source: usize,
    target: usize,
    connection: u64,
    reply: bool,
    ordinal: u64,
    received_ms: u128,
    delay_ms: u64,
    forward_started_ms: Option<u128>,
    forward_finished_ms: Option<u128>,
    write_ok: Option<bool>,
    disposition: &'static str,
}
impl Gate {
    fn forward_receipt(&self, name: &str, bytes: &[u8], r: ForwardReceipt) -> TestResult {
        let rpc = u64::from_be_bytes(bytes[16..24].try_into()?);
        let time = |v: Option<u128>| v.map_or_else(|| "null".into(), |n| n.to_string());
        let ok = r.write_ok.map_or_else(|| "null".into(), |b| b.to_string());
        let metadata = format!("{{\"schema_version\":2,\"packet_file\":\"{name}\",\"proxy_pid\":{},\"clock_basis\":\"parent proxy process monotonic Instant epoch; not owner process clock\",\"source\":{},\"target\":{},\"connection\":{},\"reply\":{},\"ordinal\":{},\"kind\":{},\"rpc\":{rpc},\"received_ms\":{},\"selected_delay_ms\":{},\"forward_started_ms\":{},\"forward_finished_ms\":{},\"forward_write_ok\":{ok},\"disposition\":\"{}\"}}\n", std::process::id(), r.source, r.target, r.connection, r.reply, r.ordinal, bytes[14], r.received_ms, r.delay_ms, time(r.forward_started_ms), time(r.forward_finished_ms), r.disposition);
        self.capture_file(&format!("{name}.forward.json"), metadata.as_bytes())
    }
}
#[expect(
    clippy::too_many_arguments,
    reason = "test proxy tracks each stream's gate, direction, identity and stop receiver"
)]
async fn pipe<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
    gate: Arc<Gate>,
    source: usize,
    target: usize,
    reply: bool,
    connection: u64,
    observed: Arc<ConnectionObservation>,
    mut stop: watch::Receiver<bool>,
) -> TestResult {
    let slot = source * gate.count + target;
    let mut ordinal = 0;
    loop {
        let result = tokio::select! {_ = stop.changed()=>return Ok(()),p=packet(&mut r)=>p};
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                // Source EOF while holding a matched response is a separate
                // observation from the proxy forwarding that response.
                if !reply && error.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(e.kind(), std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe)
                }) {
                    let closed_at = u64::try_from(gate.clock.elapsed().as_millis())? + 1;
                    observed.request_closed_at.store(closed_at, Ordering::Release);
                    if gate.delayed_connection[slot].load(Ordering::Acquire) == connection + 1 {
                        gate.delayed_closed_at[slot].store(closed_at, Ordering::Release);
                    }
                }
                return Err(error);
            }
        };
        let received_ms = gate.clock.elapsed().as_millis();
        let name = format!(
            "wire-{source}-{target}-{connection}-{}-{ordinal}.bin",
            if reply { "response" } else { "request" }
        );
        // Capture actual read bytes before waiting, including a response whose
        // source times out while the proxy is holding it. Forwarding is a later
        // fact; only the owner's ACK command proves response consumption.
        gate.capture_file(&name, &bytes)?;
        let rpc = u64::from_be_bytes(bytes[16..24].try_into()?);
        if !reply {
            observed.request_at.store(u64::try_from(received_ms)? + 1, Ordering::Relaxed);
            observed.append_rpc.store(if bytes[14] == 20 { rpc } else { 0 }, Ordering::Release);
        }
        // Match the held AppendReply to an actual Append on this connection.
        // Never select an already-observed closed connection.
        let delay = if reply && bytes[14] == 21
            && observed.append_rpc.load(Ordering::Acquire) == rpc
            && observed.request_closed_at.load(Ordering::Acquire) == 0
        {
            gate.delay[slot].swap(0, Ordering::AcqRel)
        } else {
            0
        };
        let delayed_start = if delay > 0 {
            gate.delayed_rpc[slot].store(rpc, Ordering::Relaxed);
            gate.delayed_request_at[slot].store(observed.request_at.load(Ordering::Relaxed), Ordering::Relaxed);
            gate.delayed_connection[slot].store(connection + 1, Ordering::Release);
            gate.delayed_selected_at[slot].store(u64::try_from(received_ms)? + 1, Ordering::Relaxed);
            gate.delayed_prior_connection[slot]
                .store(gate.connections[slot].load(Ordering::Acquire), Ordering::Relaxed);
            Some(Instant::now())
        } else {
            None
        };
        let mut receipt = ForwardReceipt {
            source,
            target,
            connection,
            reply,
            ordinal,
            received_ms,
            delay_ms: delay,
            forward_started_ms: None,
            forward_finished_ms: None,
            write_ok: None,
            disposition: "partition-drop",
        };
        if !gate.permits(source, target) {
            gate.forward_receipt(&name, &bytes, receipt)?;
            return Ok(());
        }
        if delay > 0 {
            tokio::select! {
                _ = stop.changed()=>{
                    receipt.disposition="shutdown-before-forward";
                    gate.forward_receipt(&name, &bytes, receipt)?;
                    return Ok(());
                },
                _ = tokio::time::sleep(Duration::from_millis(delay))=>{}
            }
        }
        if !gate.permits(source, target) {
            gate.forward_receipt(&name, &bytes, receipt)?;
            return Ok(());
        }
        let held = if let Some(start) = delayed_start {
            let held_ms = u64::try_from(start.elapsed().as_millis())?;
            Some(held_ms)
        } else {
            None
        };
        receipt.forward_started_ms = Some(gate.clock.elapsed().as_millis());
        let result = w.write_all(&bytes).await;
        receipt.forward_finished_ms = Some(gate.clock.elapsed().as_millis());
        receipt.write_ok = Some(result.is_ok());
        receipt.disposition = if result.is_ok() {
            "forward-write-ok"
        } else {
            "forward-write-error"
        };
        if let Some(held_ms) = held {
            gate.delayed_forward[slot]
                .store(if result.is_ok() { 1 } else { 2 }, Ordering::Relaxed);
            gate.delayed_forward_at[slot].store(u64::try_from(receipt.forward_started_ms.ok_or("missing forward start")?)? + 1, Ordering::Relaxed);
            gate.delayed_ms[slot].store(held_ms, Ordering::Release);
        }
        gate.forward_receipt(&name, &bytes, receipt)?;
        result?;
        ordinal += 1;
        if ordinal > 10000 {
            return Err("finite capture count".into());
        }
    }
}
async fn proxy(
    listener: TcpListener,
    backend: PathBuf,
    target: usize,
    gate: Arc<Gate>,
    mut stop: watch::Receiver<bool>,
) -> TestResult {
    let permits = Arc::new(Semaphore::new(64));
    let mut tasks = JoinSet::new();
    let mut connection = 0u64;
    loop {
        tokio::select! {
            _ = stop.changed()=>break,
            result=listener.accept()=>{
                let (mut client,_)=result?;let Ok(permit)=permits.clone().try_acquire_owned() else{continue;};
                let gate=gate.clone();let mut stop=stop.clone();let backend=backend.clone();let id=connection;connection+=1;
                tasks.spawn(async move{
                    let _permit=permit;
                    client.set_nodelay(true)?;
                    let first=tokio::time::timeout(Duration::from_secs(2),packet(&mut client)).await??;
                    if first.len()<32 || first[14]!=1{return Err::<(),Box<dyn std::error::Error+Send+Sync>>("Hello required".into());}
                    let source=u32::from_be_bytes(first[28..32].try_into()?) as usize;
                    if !gate.permits(source,target){return Ok(());}
                    gate.connections[source*gate.count+target].fetch_max(id+1,Ordering::AcqRel);
                    let deadline=Instant::now()+Duration::from_secs(2);
                    let address=loop {
                        if backend.exists(){break read_text(&backend,128)?.trim().parse::<SocketAddr>()?;}
                        if Instant::now()>=deadline{return Err("backend listener publication deadline".into());}
                        tokio::select!{_ = stop.changed()=>return Ok(()),_ = tokio::time::sleep(Duration::from_millis(10))=>{}}
                    };
                    let mut server=TcpStream::connect(address).await?;server.set_nodelay(true)?;server.write_all(&first).await?;
                    gate.capture_file(&format!("hello-{source}-{target}-{id}.bin"), &first)?;
                    let (cr,cw)=client.into_split();let (sr,sw)=server.into_split();
                    // EOF in one direction must not cancel the other future
                    // while it holds an already-read delayed response. Dropping
                    // its write half shuts down that direction; stop also wakes
                    // both pipes, so proxy shutdown still joins every task.
                    let observed=Arc::new(ConnectionObservation::default());
                    let (request,response)=tokio::join!(pipe(cr,sw,gate.clone(),source,target,false,id,observed.clone(),stop.clone()),pipe(sr,cw,gate,source,target,true,id,observed,stop));
                    request.and(response)
                });
            }
            _ = tasks.join_next(),if !tasks.is_empty()=>{}
        }
    }
    drop(listener);
    while tasks.join_next().await.is_some() {}
    Ok(())
}

struct Process {
    child: Child,
    root: PathBuf,
    next: u64,
}
struct Cluster {
    root: PathBuf,
    processes: Vec<Option<Process>>,
    routes: Vec<SocketAddr>,
    backends: Vec<SocketAddr>,
    active: usize,
    long_election: bool,
    gate: Arc<Gate>,
    stop: watch::Sender<bool>,
    proxies: Vec<JoinHandle<TestResult>>,
}
impl Cluster {
    async fn start(count: usize) -> TestResult<Self> {
        Self::start_profile(count, count).await
    }
    async fn start_profile(count: usize, active: usize) -> TestResult<Self> {
        Self::start_timing(count, active, false).await
    }
    async fn start_long_election(count: usize) -> TestResult<Self> {
        Self::start_timing(count, count, true).await
    }
    async fn start_timing(count: usize, active: usize, long_election: bool) -> TestResult<Self> {
        if count < 2 || active == 0 || active > count {
            return Err("profile bounds".into());
        }
        let root = temporary(&format!("tcp-{count}-genesis-{active}"))?;
        let mut proxy_listeners = Vec::new();
        let mut routes = Vec::new();
        let mut backends = Vec::new();
        for _ in 0..count {
            // The child binds its own port and publishes the live address.
            // Reserving then dropping a parent listener lets an outbound
            // ephemeral connection take that port before the child can bind.
            backends.push("127.0.0.1:0".parse()?);
            let p = TcpListener::bind("127.0.0.1:0").await?;
            routes.push(p.local_addr()?);
            proxy_listeners.push(p);
        }
        let case = root.file_name().ok_or("capture case")?;
        let capture = std::env::var_os("PL_PEER_RUNTIME_CAPTURE_DIR")
            .map(|p| PathBuf::from(p).join(case).join("wire"));
        if let Some(p) = &capture {
            fs::create_dir_all(p)?;
        }
        let gate = Arc::new(Gate::new(count, capture));
        let (stop, rx) = watch::channel(false);
        let mut proxies = Vec::new();
        for (target, listener) in proxy_listeners.into_iter().enumerate() {
            let g = gate.clone();
            let s = rx.clone();
            let b = root.join(format!("node-{target}/listen-address"));
            proxies.push(tokio::spawn(proxy(listener, b, target, g, s)));
        }
        let mut cluster = Self {
            root,
            processes: (0..count).map(|_| None).collect(),
            routes,
            backends,
            active,
            long_election,
            gate,
            stop,
            proxies,
        };
        for id in 0..count {
            cluster.spawn(id)?;
        }
        cluster.ready().await?;
        Ok(cluster)
    }
    fn spawn(&mut self, id: usize) -> TestResult {
        let root = self.root.join(format!("node-{id}"));
        fs::create_dir_all(&root)?;
        for name in ["ready", "state", "listen-address"] {
            let p = root.join(name);
            if p.exists() {
                fs::remove_file(p)?;
            }
        }
        let next = (0..1000)
            .find(|n| !root.join(format!("inbox-{n}")).exists())
            .ok_or("control count")?;
        // New process control sequence begins at0; old controls are moved into
        // a retained epoch directory rather than replayed against recovered state.
        if next > 0 {
            let old = root.join(format!("controls-before-{next}"));
            fs::create_dir(&old)?;
            for n in 0..next {
                for kind in ["inbox", "outbox"] {
                    let p = root.join(format!("{kind}-{n}"));
                    if p.exists() {
                        fs::rename(p, old.join(format!("{kind}-{n}")))?;
                    }
                }
            }
        }
        let log = File::create(root.join(format!("process-{next}.log")))?;
        let child = Command::new(std::env::current_exe()?)
            .arg("runtime_child_process")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env("PL_RUNTIME_CHILD_ROOT", &root)
            .env("PL_RUNTIME_CHILD_ID", id.to_string())
            .env("PL_RUNTIME_CHILD_ACTIVE", self.active.to_string())
            .env("PL_RUNTIME_CHILD_LONG_ELECTION", if self.long_election { "1" } else { "0" })
            .env(
                "PL_RUNTIME_CHILD_ROUTES",
                self.routes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .env("PL_RUNTIME_CHILD_BACKEND", self.backends[id].to_string())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        self.processes[id] = Some(Process {
            child,
            root,
            next: 0,
        });
        Ok(())
    }
    async fn ready(&mut self) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut ready = true;
            for p in self.processes.iter_mut().flatten() {
                if let Some(code) = p.child.try_wait()? {
                    return Err(format!(
                        "child startup {code}; retained logs at {}",
                        p.root.display()
                    )
                    .into());
                }
                ready &= p.root.join("ready").exists();
            }
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn state(&self, id: usize) -> TestResult<(u64, String, u64, u64, String)> {
        let p = self.processes[id].as_ref().ok_or("node closed")?;
        let text = read_text(&p.root.join("state"), 1024 * 1024)?;
        let mut lines = text.lines();
        let first = lines.next().ok_or("state header")?;
        let fields = first.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 5 {
            return Err(format!(
                "node{id} state fields at {}: {}",
                p.root.display(),
                diagnostic_snippet(first)
            )
            .into());
        }
        Ok((
            response_value(fields[0], || {
                format!(
                    "node{id} state at {}: {}",
                    p.root.display(),
                    diagnostic_snippet(first)
                )
            })?,
            fields[1].into(),
            response_value(fields[2], || {
                format!(
                    "node{id} state at {}: {}",
                    p.root.display(),
                    diagnostic_snippet(first)
                )
            })?,
            response_value(fields[3], || {
                format!(
                    "node{id} state at {}: {}",
                    p.root.display(),
                    diagnostic_snippet(first)
                )
            })?,
            lines.next().unwrap_or("").into(),
        ))
    }
    fn image_base(&self, id: usize) -> TestResult<(u64, u64)> {
        let p = self.processes[id].as_ref().ok_or("node closed")?;
        let text = read_text(&p.root.join("state"), 1024 * 1024)?;
        let line = text.lines().nth(2).ok_or("public base-position line")?;
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 || fields[0] != "image" {
            return Err(format!(
                "node{id} base-position fields at {}: {}",
                p.root.display(),
                diagnostic_snippet(line)
            )
            .into());
        }
        Ok((
            response_value(fields[1], || {
                format!(
                    "node{id} base position at {}: {}",
                    p.root.display(),
                    diagnostic_snippet(line)
                )
            })?,
            response_value(fields[2], || {
                format!(
                    "node{id} base position at {}: {}",
                    p.root.display(),
                    diagnostic_snippet(line)
                )
            })?,
        ))
    }
    fn ready_to_write(&self, id: usize) -> TestResult<bool> {
        let process = self.processes[id].as_ref().ok_or("node closed")?;
        let text = read_text(&process.root.join("state"), 1024 * 1024)?;
        let header = text.lines().next().ok_or("state header")?;
        response_value(
            header.split_whitespace().nth(4).ok_or("state readiness")?,
            || {
                format!(
                    "node{id} readiness at {}: {}",
                    process.root.display(),
                    diagnostic_snippet(header)
                )
            },
        )
    }
    fn require_completed_image_frames(&self, source: usize, target: usize) -> TestResult {
        let Some(root) = &self.gate.capture else {
            return Ok(());
        };
        let prefix = format!("wire-{source}-{target}-");
        let mut chunks = 0;
        let mut finish = 0;
        let mut finished = 0;
        for item in fs::read_dir(root)? {
            let item = item?;
            let name = item.file_name();
            let name = name.to_str().ok_or("capture filename UTF8")?;
            if !name.starts_with(&prefix) || !name.ends_with(".bin") {
                continue;
            }
            let mut bytes = Vec::new();
            File::open(item.path())?
                .take(512 * 1024 + 5)
                .read_to_end(&mut bytes)?;
            if bytes.len() < 32 || bytes.len() > 512 * 1024 + 4 {
                return Err("captured frame bound".into());
            }
            match bytes[14] {
                32 => chunks += 1,
                34 => finish += 1,
                35 => finished += 1,
                _ => {}
            }
        }
        assert!(
            chunks >= 3 && finish >= 1 && finished >= 1,
            "actual complete multichunk private image stream required"
        );
        Ok(())
    }
    async fn wait_held_append(&self, source: usize, target: usize) -> TestResult {
        let slot = source * self.gate.count + target;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let held_ms = self.gate.delayed_ms[slot].load(Ordering::Acquire);
            let rpc = self.gate.delayed_rpc[slot].load(Ordering::Relaxed);
            let connection = self.gate.delayed_connection[slot].load(Ordering::Acquire);
            let request_at = self.gate.delayed_request_at[slot].load(Ordering::Relaxed);
            let selected_at = self.gate.delayed_selected_at[slot].load(Ordering::Relaxed);
            let closed_at = self.gate.delayed_closed_at[slot].load(Ordering::Acquire);
            let forward_at = self.gate.delayed_forward_at[slot].load(Ordering::Relaxed);
            if held_ms >= 500 && rpc > 0 && connection > 0 && request_at > 0
                && request_at <= selected_at && selected_at < closed_at && closed_at <= forward_at
            {
                println!("PL_DELAYED_APPEND_HELD source={source} target={target} rpc={rpc} connection={connection} request_at={request_at} selected_at={selected_at} source_eof_at={closed_at} forward_at={forward_at} held_ms={held_ms} owner_consumption=unobserved");
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("delayed AppendReply or source EOF not observed: source={source} target={target} rpc={rpc} connection={connection} request_at={request_at} selected_at={selected_at} closed_at={closed_at} forward_at={forward_at} held_ms={held_ms}").into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn require_delayed_reconnect(&self, source: usize, target: usize) -> TestResult {
        let slot = source * self.gate.count + target;
        let held_ms = self.gate.delayed_ms[slot].load(Ordering::Acquire);
        let previous = self.gate.delayed_prior_connection[slot].load(Ordering::Relaxed);
        let latest = self.gate.connections[slot].load(Ordering::Acquire);
        let rpc = self.gate.delayed_rpc[slot].load(Ordering::Relaxed);
        let connection = self.gate.delayed_connection[slot].load(Ordering::Acquire);
        let request_at = self.gate.delayed_request_at[slot].load(Ordering::Relaxed);
        let selected_at = self.gate.delayed_selected_at[slot].load(Ordering::Relaxed);
        let closed_at = self.gate.delayed_closed_at[slot].load(Ordering::Acquire);
        let forward_at = self.gate.delayed_forward_at[slot].load(Ordering::Relaxed);
        let write = match self.gate.delayed_forward[slot].load(Ordering::Relaxed) {
            1 => "ok",
            2 => "error",
            _ => return Err("missing delayed proxy write result".into()),
        };
        if latest <= previous {
            return Err(format!("delayed AppendReply or reconnect not observed: source={source} target={target} rpc={rpc} connection={connection} prior_latest={previous} latest={latest} held_ms={held_ms}").into());
        }
        println!("PL_DELAYED_APPEND source={source} target={target} rpc={rpc} connection={connection} prior_latest={previous} latest={latest} request_at={request_at} selected_at={selected_at} source_eof_at={closed_at} forward_at={forward_at} held_ms={held_ms} proxy_write={write} owner_consumption=unobserved");
        Ok(())
    }
    async fn leader(&mut self, excluded: &[usize]) -> TestResult<usize> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let candidates = (0..self.processes.len())
                .filter(|i| self.processes[*i].is_some() && !excluded.contains(i))
                .filter(|i| {
                    self.state(*i).is_ok_and(|s| {
                        s.1 == "Leader"
                            && s.3 > 0
                            && s.4.split(';').next_back().is_some_and(|record| {
                                let mut fields = record.split(':');
                                fields.next().and_then(|term| term.parse::<u64>().ok()) == Some(s.0)
                                    && fields.next().and_then(|index| index.parse::<u64>().ok())
                                        == Some(s.3)
                            })
                    }) && self.ready_to_write(*i).unwrap_or(false)
                })
                .collect::<Vec<_>>();
            if candidates.len() == 1 {
                return Ok(candidates[0]);
            }
            if Instant::now() >= deadline {
                let ids = (0..self.processes.len())
                    .filter(|id| !excluded.contains(id))
                    .collect::<Vec<_>>();
                return Err(format!(
                    "no unique committed leader; {}",
                    self.diagnostics("unique-committed-leader", 0, &ids)
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn command(&mut self, id: usize, command: &str) -> TestResult<String> {
        let p = self.processes[id].as_mut().ok_or("node closed")?;
        let n = p.next;
        p.next += 1;
        write_text(&p.root.join(format!("inbox-{n}")), command)?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let out = p.root.join(format!("outbox-{n}"));
            if out.exists() {
                return read_text(&out, 4096);
            }
            if p.child.try_wait()?.is_some() {
                return Err("child exited before response".into());
            }
            if Instant::now() >= deadline {
                return Err("command deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn proposal_position(&mut self, id: usize, phase: &str, answer: &str) -> TestResult<u64> {
        self.response(id, phase, proposal_position_response(answer))
            .map(|(_, index)| index)
    }
    fn response<T>(&mut self, id: usize, phase: &str, result: TestResult<T>) -> TestResult<T> {
        result.map_err(|error| {
            let ids = (0..self.processes.len()).collect::<Vec<_>>();
            format!(
                "{phase} on node{id}: {error}; {}",
                self.diagnostics(phase, 0, &ids)
            )
            .into()
        })
    }
    fn diagnostics(&mut self, phase: &str, index: u64, ids: &[usize]) -> String {
        let mut text = format!("runtime diagnostics phase={phase} index={index} ids={ids:?};");
        for (id, process) in self.processes.iter_mut().enumerate() {
            let Some(process) = process else {
                text.push_str(&format!(" node{id}=closed;"));
                continue;
            };
            let status = match process.child.try_wait() {
                Ok(None) => format!("running(pid={})", process.child.id()),
                Ok(Some(code)) => format!("exited({code})"),
                Err(error) => format!("status-error({error})"),
            };
            let mut bytes = Vec::new();
            let header = match File::open(process.root.join("state"))
                .and_then(|file| file.take(2048).read_to_end(&mut bytes))
            {
                Ok(count) => format!(
                    "read{count}B firstline={:?}",
                    String::from_utf8_lossy(&bytes).lines().next().unwrap_or("")
                ),
                Err(error) => format!("state-read-error({error})"),
            };
            text.push_str(&format!(
                " node{id}={status} control_next={} {header};",
                process.next
            ));
            // Error diagnostics are test evidence, not a full state image. A
            // fixed ceiling also covers larger permitted route tables.
            if text.len() > 16 * 1024 {
                let mut end = 16 * 1024 - " [diagnostic truncated]".len();
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                text.truncate(end);
                text.push_str(" [diagnostic truncated]");
                break;
            }
        }
        text
    }
    async fn commit(&mut self, phase: &str, index: u64, ids: &[usize]) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if ids
                .iter()
                .all(|i| self.state(*i).is_ok_and(|s| s.3 >= index))
            {
                let prefixes = ids
                    .iter()
                    .map(|i| self.state(*i).map(|s| s.4))
                    .collect::<TestResult<Vec<_>>>()?;
                let prefix = prefixes[0]
                    .split(';')
                    .take(index as usize)
                    .collect::<Vec<_>>();
                for p in &prefixes[1..] {
                    assert_eq!(
                        prefix,
                        p.split(';').take(index as usize).collect::<Vec<_>>()
                    );
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "commit deadline budget_ms=15000; {}",
                    self.diagnostics(phase, index, ids)
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn crash(&mut self, id: usize) -> TestResult {
        let p = self.processes[id].as_mut().ok_or("node closed")?;
        let n = p.next;
        p.next += 1;
        write_text(&p.root.join(format!("inbox-{n}")), "exit")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(code) = p.child.try_wait()? {
                assert_eq!(code.code(), Some(88));
                if let Some(wire) = &self.gate.capture {
                    let target = wire
                        .parent()
                        .ok_or("case capture parent")?
                        .join(format!("crashed-node-{id}-pid-{}.json", p.child.id()));
                    write_text(&target, &format!(
                        "{{\"node_id\":{id},\"pid\":{},\"exit_code\":88,\"parent_waited\":true,\"runtime_joined\":false,\"fault\":\"controlled process exit before restart\"}}\n",
                        p.child.id()
                    ))?;
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err("process exit deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.processes[id] = None;
        Ok(())
    }
    async fn shutdown(&mut self) -> TestResult {
        for id in 0..self.processes.len() {
            if self.processes[id].is_some() {
                assert_eq!(self.command(id, "shutdown").await?, "joined");
                let p = self.processes[id].as_mut().ok_or("node")?;
                let code = p.child.wait()?;
                assert!(code.success());
                if let Some(wire) = &self.gate.capture {
                    let target = wire
                        .parent()
                        .ok_or("case capture parent")?
                        .join(format!("joined-node-{id}-pid-{}.json", p.child.id()));
                    let receipt = read_text(&p.root.join("joined-receipt.json"), 8192)?;
                    write_text(&target, &receipt)?;
                }
                self.processes[id] = None;
            }
        }
        self.stop.send(true)?;
        while let Some(p) = self.proxies.pop() {
            p.await??;
        }
        assert!(
            !self.gate.capture_failed.load(Ordering::Acquire),
            "every actual packet/forward receipt must fit and persist"
        );
        Ok(())
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for p in self.processes.iter_mut().flatten() {
            let _ = p.child.kill();
            let _ = p.child.wait();
        }
        let _ = self.stop.send(true);
        for p in &self.proxies {
            p.abort();
        }
    }
}

async fn observer_image_history(total: usize) -> TestResult {
    let genesis = total - 1;
    let observer = genesis;
    let mut cluster = Cluster::start_profile(total, genesis).await?;
    let leader = cluster.leader(&[observer]).await?;
    let voters = (0..genesis).collect::<Vec<_>>();
    // The observer route is configured but outside genesis. No ordinary peer
    // preparation is eligible before feature negotiation starts admission.
    assert_eq!(cluster.state(observer)?.3, 0);
    let mut expected_data = Vec::new();
    for byte in [71u8, 72u8] {
        let payload = hex(&vec![byte; 40 * 1024])?;
        let answer = cluster
            .command(leader, &format!("propose {payload}"))
            .await?;
        let (term, index) = cluster.response(
            leader,
            "observer-prefix-proposal",
            proposal_position_response(&answer),
        )?;
        expected_data.push((term, index, payload));
        cluster
            .commit("observer-prefix-on-genesis", index, &voters)
            .await?;
    }
    let checkpoint = cluster.command(leader, "checkpoint 62").await?;
    let (base_term, base, image_bytes) = cluster.response(
        leader,
        "observer-prefix-checkpoint",
        image_response(&checkpoint),
    )?;
    assert!(image_bytes > 64 * 1024, "real transfer must exceed 64KiB");
    cluster.commit("observer-checkpoint-base", base, &voters).await?;
    let prefix = cluster.state(leader)?.4;
    check_checkpoint_prefix(&prefix, &expected_data, base_term, base)?;
    assert_eq!(cluster.image_base(observer)?.1, 0);
    let added = cluster.command(leader, &format!("add {observer}")).await?;
    let (_, addition, _) = cluster
        .response(leader, "observer-addition", change_response(&added))
        .map_err(|error| format!("observer target {observer}: {error}"))?;
    let all = (0..total).collect::<Vec<_>>();
    cluster
        .commit("observer-addition-all", addition, &all)
        .await?;
    assert_eq!(
        cluster.image_base(observer)?.1,
        base,
        "catch-up must select a durable image, not merely append its prefix"
    );
    cluster.require_completed_image_frames(leader, observer)?;
    let prefix = cluster.state(observer)?.4;
    cluster.crash(observer).await?;
    cluster.spawn(observer)?;
    cluster.ready().await?;
    cluster
        .commit("observer-addition-reopen-all", addition, &all)
        .await?;
    assert_eq!(
        cluster.image_base(observer)?.1,
        base,
        "reopen must recover the selected generation"
    );
    assert_eq!(
        cluster
            .state(observer)?
            .4
            .split(';')
            .take(addition as usize)
            .collect::<Vec<_>>(),
        prefix
            .split(';')
            .take(addition as usize)
            .collect::<Vec<_>>()
    );
    let current = cluster.leader(&[]).await?;
    let answer = cluster
        .command(
            current,
            &format!(
                "propose {}",
                hex(b"suffix after installed observer restart")?
            ),
        )
        .await?;
    let suffix = cluster.proposal_position(current, "observer-post-reopen-proposal", &answer)?;
    cluster
        .commit("observer-post-reopen-suffix", suffix, &all)
        .await?;
    cluster.shutdown().await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_election_can_extend_checkpoint_past_last_data() -> TestResult {
    let mut cluster = Cluster::start_profile(5, 4).await?;
    let observer = 4;
    let voters = (0..4).collect::<Vec<_>>();
    let old = cluster.leader(&[observer]).await?;
    let mut last_data = 0;
    let mut last_term = 0;
    let mut expected = Vec::new();
    for byte in [71u8, 72u8] {
        let payload = vec![byte; 40 * 1024];
        let answer = cluster.command(old, &format!("propose {}", hex(&payload)?)).await?;
        (last_term, last_data) = proposal_position_response(&answer)?;
        expected.push((last_term, last_data, hex(&payload)?));
        cluster.commit("forced-checkpoint-data", last_data, &voters).await?;
    }
    for id in 0..5 {
        if id != old {
            cluster.gate.set(old, id, false);
            cluster.gate.set(id, old, false);
        }
    }
    let current = cluster.leader(&[old, observer]).await?;
    let state = cluster.state(current)?;
    assert!(state.0 > last_term, "new election must advance the term");
    assert!(state.3 > last_data, "the current-term barrier must commit after data");
    let barrier = state.3;
    for id in 0..5 {
        cluster.gate.set(old, id, true);
        cluster.gate.set(id, old, true);
    }
    cluster.commit("forced-checkpoint-barrier", barrier, &voters).await?;
    let current = cluster.leader(&[observer]).await?;
    let response = cluster.command(current, "checkpoint 67").await?;
    let (term, base, bytes) = image_response(&response)?;
    assert!(bytes > 64 * 1024);
    assert!(base > last_data, "checkpoint must include the later barrier");
    cluster.commit("forced-checkpoint-base", base, &voters).await?;
    let prefix = cluster.state(current)?.4;
    check_checkpoint_prefix(&prefix, &expected, term, base)?;
    println!("PL_CHECKPOINT_LATER_BARRIER data={last_data} base={base} term={term}");
    cluster.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_demoted_leader_requires_fresh_selection() -> TestResult {
    let count = 3;
    let mut cluster = Cluster::start(count).await?;
    let old = cluster.leader(&[]).await?;
    let answer = cluster.command(old, &format!("propose {}", hex(b"before-handover")?)).await?;
    let prior = cluster.proposal_position(old, "forced-handover-prior", &answer)?;
    let all = (0..count).collect::<Vec<_>>();
    cluster.commit("forced-handover-prior-all", prior, &all).await?;
    for id in 0..count {
        if id != old {
            cluster.gate.set(old, id, false);
            cluster.gate.set(id, old, false);
        }
    }
    let _new = cluster.leader(&[old]).await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while cluster.state(old)?.1 == "Leader" {
        if Instant::now() >= deadline {
            return Err("old leader did not lose its quorum lease".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = cluster.command(old, "observe").await?;
    let stale = cluster.command(old, &format!("propose {}", hex(b"must-not-append")?)).await?;
    assert_eq!(stale, "error private peer runtime: Node(NotLeader)");
    let after = cluster.command(old, "observe").await?;
    assert_eq!(before, after, "rejected stale-leader proposal changed the log");
    let selected = cluster.leader(&[]).await?;
    assert_ne!(selected, old);
    let peer = (0..count).find(|id| *id != selected && *id != old).ok_or("surviving peer")?;
    assert_ne!(selected, peer);
    let answer = cluster.command(selected, &format!("propose {}", hex(b"after-handover")?)).await?;
    let position = cluster.proposal_position(selected, "forced-handover-current", &answer)?;
    let majority = all.iter().copied().filter(|id| *id != old).collect::<Vec<_>>();
    cluster.commit("forced-handover-majority", position, &majority).await?;
    for id in 0..count {
        cluster.gate.set(old, id, true);
        cluster.gate.set(id, old, true);
    }
    cluster.commit("forced-handover-healed", position, &all).await?;
    println!("PL_STALE_LEADER_REJECTED old={old} current={selected} peer={peer}");
    cluster.shutdown().await
}

// This control uses a longer election interval so that the follower cannot
// campaign while its reply is deliberately held beyond the 500 ms RPC budget.
// The short-election fault histories above and below keep their original timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matched_late_append_reconnects_after_rpc_deadline() -> TestResult {
    let count = 3;
    let mut cluster = Cluster::start_long_election(count).await?;
    let leader = cluster.leader(&[]).await?;
    let follower = (leader + 1) % count;
    let all = (0..count).collect::<Vec<_>>();
    cluster.gate.delay[leader * count + follower].store(800, Ordering::Release);
    let first = hex(b"delayed-reconnect-first")?;
    let answer = cluster.command(leader, &format!("propose {first}")).await?;
    let (first_term, first_index) = cluster.response(leader, "reconnect-first", proposal_position_response(&answer))?;
    cluster.commit("reconnect-first-all", first_index, &all).await?;
    cluster.wait_held_append(leader, follower).await?;
    // This is a new operation, not a retry of the already committed first one.
    let second = hex(b"delayed-reconnect-progress")?;
    let answer = cluster.command(leader, &format!("propose {second}")).await?;
    let (second_term, second_index) = cluster.response(leader, "reconnect-progress", proposal_position_response(&answer))?;
    assert!(second_index > first_index);
    cluster.commit("reconnect-progress-all", second_index, &all).await?;
    let prefix = cluster.state(leader)?.4;
    for (term, index, payload) in [(first_term, first_index, first), (second_term, second_index, second)] {
        let entry = format!("{term}:{index}:0:{payload}");
        assert_eq!(prefix.split(';').nth(usize::try_from(index - 1)?), Some(entry.as_str()));
    }
    cluster.require_delayed_reconnect(leader, follower)?;
    cluster.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_three_peer_tcp_observer_multichunk_image_install_reopen_and_join() -> TestResult {
    observer_image_history(3).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join() -> TestResult {
    observer_image_history(5).await
}

async fn fault_history(count: usize) -> TestResult {
    let mut cluster = Cluster::start(count).await?;
    let leader = cluster.leader(&[]).await?;
    let answer = cluster
        .command(
            leader,
            &format!("propose {}", hex(b"durable-before-minority")?),
        )
        .await?;
    let index = cluster.proposal_position(leader, "fault-initial-proposal", &answer)?;
    let all = (0..count).collect::<Vec<_>>();
    cluster.commit("fault-initial-all", index, &all).await?;
    let follower = (leader + 1) % count;
    cluster.crash(follower).await?;
    let answer = cluster
        .command(
            leader,
            &format!("propose {}", hex(b"survives-minority-process-exit")?),
        )
        .await?;
    let after = cluster.proposal_position(leader, "fault-after-process-exit-proposal", &answer)?;
    let majority = all
        .iter()
        .copied()
        .filter(|i| *i != follower)
        .collect::<Vec<_>>();
    cluster
        .commit("fault-after-process-exit-majority", after, &majority)
        .await?;
    let checkpoint = cluster.command(leader, "checkpoint 21").await?;
    cluster.response(
        leader,
        "fault-prefix-checkpoint",
        image_response(&checkpoint),
    )?;
    cluster.spawn(follower)?;
    cluster.ready().await?;
    cluster
        .commit("fault-restarted-follower-all", after, &all)
        .await?;
    // Reacquire the leader after restart: the earlier leader may have been
    // demoted during recovery. Pick a distinct live peer before arming the gate.
    // A possibly appended proposal is never retried.
    let leader = cluster.leader(&[]).await?;
    let follower = if follower == leader {
        (leader + 1) % count
    } else {
        follower
    };
    // An observed AppendReply must cross the configured RPC deadline. The
    // proxy's write result alone does not prove that an owner consumed it.
    cluster.gate.delay[leader * count + follower].store(800, Ordering::Release);
    let answer = cluster
        .command(leader, &format!("propose {}", hex(b"reordered-late-ack")?))
        .await?;
    let reordered =
        cluster.proposal_position(leader, "fault-delayed-old-reply-proposal", &answer)?;
    cluster
        .commit("fault-delayed-old-reply-all", reordered, &all)
        .await?;
    cluster.wait_held_append(leader, follower).await?;
    // An election may have changed leadership while the reply was held.
    // Drive a distinct proposal through the current leader exactly once.
    let leader = cluster.leader(&[]).await?;
    let payload = hex(b"after-held-reply-fault-progress")?;
    let answer = cluster.command(leader, &format!("propose {payload}")).await?;
    let (term, progress) = cluster.response(leader, "fault-delayed-progress", proposal_position_response(&answer))?;
    assert!(progress > reordered);
    cluster.commit("fault-delayed-progress-all", progress, &all).await?;
    let entry = format!("{term}:{progress}:0:{payload}");
    assert_eq!(cluster.state(leader)?.4.split(';').nth(usize::try_from(progress - 1)?), Some(entry.as_str()));
    // Isolate the prior leader bidirectionally. Majority elects autonomously;
    // the isolated writer cannot gain a committed divergent prefix.
    for id in 0..count {
        if id != leader {
            cluster.gate.set(leader, id, false);
            cluster.gate.set(id, leader, false);
        }
    }
    // While its previous quorum contact lease is still valid the isolated old
    // leader may synchronize an uncommitted suffix. It cannot commit it; later
    // known-leader catch-up must repair that suffix without changing the prefix.
    let isolated = cluster
        .command(
            leader,
            &format!("propose {}", hex(b"isolated-uncommitted-suffix")?),
        )
        .await?;
    if isolated.starts_with("position ") {
        let diverged = cluster.proposal_position(leader, "fault-isolated-proposal", &isolated)?;
        assert!(
            cluster.state(leader)?.3 < diverged,
            "isolated local synchronization is never a majority receipt"
        );
    } else {
        assert!(isolated.starts_with("error "));
    }
    let survivor = cluster.leader(&[leader]).await?;
    let answer = cluster
        .command(
            survivor,
            &format!("propose {}", hex(b"majority-after-partition")?),
        )
        .await?;
    let committed =
        cluster.proposal_position(survivor, "fault-partition-majority-proposal", &answer)?;
    let surviving = all
        .iter()
        .copied()
        .filter(|i| *i != leader)
        .collect::<Vec<_>>();
    cluster
        .commit("fault-partition-new-majority", committed, &surviving)
        .await?;
    let lease_deadline = Instant::now() + Duration::from_secs(5);
    while cluster.state(leader)?.1 == "Leader" {
        if Instant::now() >= lease_deadline {
            return Err("isolated quorum lease failed to revoke".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let isolated = cluster
        .command(
            leader,
            &format!("propose {}", hex(b"writer-after-quorum-expiry")?),
        )
        .await?;
    assert!(isolated.starts_with("error "));
    for id in 0..count {
        cluster.gate.set(leader, id, true);
        cluster.gate.set(id, leader, true);
    }
    cluster
        .commit("fault-healed-partition-all", committed, &all)
        .await?;
    // A committed removed leader cannot contribute a match in the new set.
    let current = cluster.leader(&[]).await?;
    let removed = cluster
        .command(current, &format!("remove {current}"))
        .await?;
    let (_, removal, _) = cluster.response(
        current,
        "fault-current-leader-removal",
        change_response(&removed),
    )?;
    let remaining = all
        .iter()
        .copied()
        .filter(|i| *i != current)
        .collect::<Vec<_>>();
    cluster
        .commit("fault-removed-leader-new-set", removal, &remaining)
        .await?;
    let next = cluster.leader(&[current]).await?;
    assert_ne!(current, next);
    cluster.shutdown().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_three_node_tcp_minority_partition_late_ack_image_restart_and_removal() -> TestResult
{
    fault_history(3).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_five_node_tcp_minority_partition_late_ack_image_restart_and_removal() -> TestResult
{
    fault_history(5).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_ready_and_shutdown_keep_owner_until_successful_join_and_port_reuse() -> TestResult
{
    let root = temporary("ownership")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut runtime = Runtime::start(listener, config(0, &[address], 1)?, paths(&root)?)?;
    // Canceling an observation is not dropping the runtime's retained owner.
    {
        let ready = runtime.wait_ready();
        tokio::pin!(ready);
        let _ = tokio::time::timeout(Duration::from_nanos(1), &mut ready).await;
    }
    runtime.wait_ready().await?;
    let handle = runtime.handle();
    let _ = tokio::time::timeout(Duration::from_nanos(1), runtime.shutdown()).await;
    runtime.shutdown().await?;
    assert_eq!(handle.diagnostics().network_tasks, 0);
    assert_eq!(handle.diagnostics().sockets, 0);
    assert_eq!(handle.diagnostics().transport_bytes, 0);
    assert!(handle.state().await.is_err());
    let rebound = TcpListener::bind(address).await?;
    drop(rebound);
    fs::remove_dir_all(root)?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_oversized_hello_saturation_closes_all_tasks_on_join() -> TestResult {
    let root = temporary("framing")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let cfg = config(0, &[address], 1)?;
    let bound = cfg.task_bound();
    let managed = cfg.managed_bytes();
    assert!(managed <= 512 * 1024 * 1024);
    let mut runtime = Runtime::start(listener, cfg, paths(&root)?)?;
    runtime.wait_ready().await?;
    let handle = runtime.handle();
    let mut clients = Vec::new();
    for _ in 0..16 {
        if let Ok(s) = TcpStream::connect(address).await {
            clients.push(s);
        }
    }
    for client in &mut clients {
        let _ = client.write_u32(u32::MAX).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.diagnostics().network_tasks <= bound);
    assert!(handle.diagnostics().transport_bytes <= managed);
    runtime.shutdown().await?;
    assert_eq!(handle.diagnostics().network_tasks, 0);
    assert_eq!(handle.diagnostics().sockets, 0);
    drop(clients);
    fs::remove_dir_all(root)?;
    Ok(())
}
