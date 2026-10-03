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
    if s.len() > 128 * 1024 || s.len() % 2 != 0 {
        return Err("bounded hex".into());
    }
    s.as_bytes()
        .chunks_exact(2)
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
        election_min_ms: 80,
        election_max_ms: 160,
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
            let cfg = config(local, &routes, active)?;
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
async fn pipe<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
    gate: Arc<Gate>,
    source: usize,
    target: usize,
    reply: bool,
    connection: u64,
    mut stop: watch::Receiver<bool>,
) -> TestResult {
    let mut ordinal = 0;
    loop {
        let bytes = tokio::select! {_ = stop.changed()=>return Ok(()),p=packet(&mut r)=>p?};
        let received_ms = gate.clock.elapsed().as_millis();
        let name = format!(
            "wire-{source}-{target}-{connection}-{}-{ordinal}.bin",
            if reply { "response" } else { "request" }
        );
        // Capture actual read bytes before waiting, including a response whose
        // source times out while the proxy is holding it. Forwarding is a later
        // fact; only the owner's ACK command proves response consumption.
        gate.capture_file(&name, &bytes)?;
        let delay = if reply {
            gate.delay[source * gate.count + target].swap(0, Ordering::AcqRel)
        } else {
            0
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
        receipt.forward_started_ms = Some(gate.clock.elapsed().as_millis());
        let result = w.write_all(&bytes).await;
        receipt.forward_finished_ms = Some(gate.clock.elapsed().as_millis());
        receipt.write_ok = Some(result.is_ok());
        receipt.disposition = if result.is_ok() {
            "forward-write-ok"
        } else {
            "forward-write-error"
        };
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
    backend: SocketAddr,
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
                let gate=gate.clone();let stop=stop.clone();let id=connection;connection+=1;
                tasks.spawn(async move{
                    let _permit=permit;
                    let first=tokio::time::timeout(Duration::from_secs(2),packet(&mut client)).await??;
                    if first.len()<32 || first[14]!=1{return Err::<(),Box<dyn std::error::Error+Send+Sync>>("Hello required".into());}
                    let source=u32::from_be_bytes(first[28..32].try_into()?) as usize;
                    if !gate.permits(source,target){return Ok(());}
                    let mut server=TcpStream::connect(backend).await?;server.write_all(&first).await?;
                    gate.capture_file(&format!("hello-{source}-{target}-{id}.bin"), &first)?;
                    let (cr,cw)=client.into_split();let (sr,sw)=server.into_split();
                    // EOF in one direction must not cancel the other future
                    // while it holds an already-read delayed response. Dropping
                    // its write half shuts down that direction; stop also wakes
                    // both pipes, so proxy shutdown still joins every task.
                    let (request,response)=tokio::join!(pipe(cr,sw,gate.clone(),source,target,false,id,stop.clone()),pipe(sr,cw,gate,source,target,true,id,stop));
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
    gate: Arc<Gate>,
    stop: watch::Sender<bool>,
    proxies: Vec<JoinHandle<TestResult>>,
}
impl Cluster {
    async fn start(count: usize) -> TestResult<Self> {
        Self::start_profile(count, count).await
    }
    async fn start_profile(count: usize, active: usize) -> TestResult<Self> {
        if count < 2 || active == 0 || active > count {
            return Err("profile bounds".into());
        }
        let root = temporary(&format!("tcp-{count}-genesis-{active}"))?;
        let mut backend_listeners = Vec::new();
        let mut proxy_listeners = Vec::new();
        let mut routes = Vec::new();
        let mut backends = Vec::new();
        for _ in 0..count {
            let b = TcpListener::bind("127.0.0.1:0").await?;
            backends.push(b.local_addr()?);
            backend_listeners.push(b);
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
            let b = backends[target];
            proxies.push(tokio::spawn(proxy(listener, b, target, g, s)));
        }
        drop(backend_listeners);
        let mut cluster = Self {
            root,
            processes: (0..count).map(|_| None).collect(),
            routes,
            backends,
            active,
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
        for name in ["ready", "state"] {
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
                    return Err(format!("child startup {code}").into());
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
            return Err("state fields".into());
        }
        Ok((
            fields[0].parse()?,
            fields[1].into(),
            fields[2].parse()?,
            fields[3].parse()?,
            lines.next().unwrap_or("").into(),
        ))
    }
    fn image_base(&self, id: usize) -> TestResult<(u64, u64)> {
        let p = self.processes[id].as_ref().ok_or("node closed")?;
        let text = read_text(&p.root.join("state"), 1024 * 1024)?;
        let line = text.lines().nth(2).ok_or("public base-position line")?;
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 || fields[0] != "image" {
            return Err("base-position fields".into());
        }
        Ok((fields[1].parse()?, fields[2].parse()?))
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
    async fn leader(&self, excluded: &[usize]) -> TestResult<usize> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let candidates = (0..self.processes.len())
                .filter(|i| self.processes[*i].is_some() && !excluded.contains(i))
                .filter(|i| self.state(*i).is_ok_and(|s| s.1 == "Leader" && s.3 > 0))
                .collect::<Vec<_>>();
            if candidates.len() == 1 {
                return Ok(candidates[0]);
            }
            if Instant::now() >= deadline {
                return Err("no unique committed leader".into());
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
    fn commit_diagnostics(&mut self, phase: &str, index: u64, ids: &[usize]) -> String {
        let mut text =
            format!("commit deadline phase={phase} index={index} ids={ids:?} budget_ms=15000;");
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
                return Err(self.commit_diagnostics(phase, index, ids).into());
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
    let mut committed = 0;
    for byte in [71u8, 72u8] {
        let answer = cluster
            .command(leader, &format!("propose {}", hex(&vec![byte; 40 * 1024])?))
            .await?;
        committed = answer
            .split_whitespace()
            .nth(2)
            .ok_or("proposal response")?
            .parse()?;
        cluster
            .commit("observer-prefix-on-genesis", committed, &voters)
            .await?;
    }
    let checkpoint = cluster.command(leader, "checkpoint 62").await?;
    assert!(checkpoint.starts_with("image "));
    let image_bytes: u64 = checkpoint
        .split_whitespace()
        .nth(3)
        .ok_or("image byte count")?
        .parse()?;
    assert!(image_bytes > 64 * 1024, "real transfer must exceed 64KiB");
    let base: u64 = checkpoint
        .split_whitespace()
        .nth(2)
        .ok_or("image base")?
        .parse()?;
    assert_eq!(base, committed);
    assert_eq!(cluster.image_base(observer)?.1, 0);
    let added = cluster.command(leader, &format!("add {observer}")).await?;
    assert!(
        added.starts_with("change "),
        "actual public observer addition: {added}"
    );
    let addition: u64 = added
        .split_whitespace()
        .nth(2)
        .ok_or("addition position")?
        .parse()?;
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
    let suffix: u64 = answer
        .split_whitespace()
        .nth(2)
        .ok_or("suffix position")?
        .parse()?;
    cluster
        .commit("observer-post-reopen-suffix", suffix, &all)
        .await?;
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
    let index: u64 = answer
        .split_whitespace()
        .nth(2)
        .ok_or("proposal response")?
        .parse()?;
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
    let after: u64 = answer
        .split_whitespace()
        .nth(2)
        .ok_or("proposal response")?
        .parse()?;
    let majority = all
        .iter()
        .copied()
        .filter(|i| *i != follower)
        .collect::<Vec<_>>();
    cluster
        .commit("fault-after-process-exit-majority", after, &majority)
        .await?;
    assert!(cluster
        .command(leader, "checkpoint 21")
        .await?
        .starts_with("image "));
    cluster.spawn(follower)?;
    cluster.ready().await?;
    cluster
        .commit("fault-restarted-follower-all", after, &all)
        .await?;
    // One delayed old response crosses its source RPC deadline and a reconnect.
    // Actual proxy packets and owner ACKs distinguish forward attempts from receipts.
    cluster.gate.delay[leader * count + follower].store(800, Ordering::Release);
    let answer = cluster
        .command(leader, &format!("propose {}", hex(b"reordered-late-ack")?))
        .await?;
    let reordered: u64 = answer
        .split_whitespace()
        .nth(2)
        .ok_or("proposal response")?
        .parse()?;
    cluster
        .commit("fault-delayed-old-reply-all", reordered, &all)
        .await?;
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
        let diverged: u64 = isolated
            .split_whitespace()
            .nth(2)
            .ok_or("isolated position")?
            .parse()?;
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
    let committed: u64 = answer
        .split_whitespace()
        .nth(2)
        .ok_or("proposal response")?
        .parse()?;
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
    assert!(removed.starts_with("change "));
    let removal: u64 = removed
        .split_whitespace()
        .nth(2)
        .ok_or("removal response")?
        .parse()?;
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
