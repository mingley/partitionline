//! Proposed actual TCP metadata application integration; no native Kafka KRaft wire claim.

use partitionline_broker::{
    catalog,
    metadata::{Config as RouterConfig, Router},
    metadata_quorum,
    raft::{
        election::Role,
        membership::{Bootstrap, Endpoint, Key, Voter, Voters},
        protocol,
        runtime::{Config, Limits, Paths, Peer, Runtime, StorageLimits},
        snapshot,
    },
};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{net::TcpListener, time::Instant};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn ensure(value: bool, message: &'static str) -> Result {
    if value {
        Ok(())
    } else {
        Err(message.into())
    }
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "partitionline14-tcp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p)?;
        Ok(Self(p))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}
fn config(local: usize, routes: &[SocketAddr]) -> Result<Config> {
    let voters = routes
        .iter()
        .enumerate()
        .map(|(id, address)| {
            Ok(Voter::new(
                Key::new(id as u32, [(id + 1) as u8; 16])?,
                vec![Endpoint::new(
                    "CONTROLLER".into(),
                    address.ip().to_string(),
                    address.port(),
                )?],
                0,
                1,
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    let genesis = Voters::new(0, Default::default(), 1, voters.clone())?;
    let bootstrap = Bootstrap::new(voters[local].clone(), genesis, "CONTROLLER".into())?;
    let peers = voters
        .iter()
        .enumerate()
        .filter(|(id, _)| *id != local)
        .map(|(id, v)| Ok(Peer::new(v.clone(), routes[id])?))
        .collect::<Result<Vec<_>>>()?;
    let mut controller = protocol::Config::new(
        local as u32,
        vec![local as u32],
        "metadata-quorum-tcp".into(),
    )?;
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
        incoming: routes.len() - 1,
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
fn app_limits() -> metadata_quorum::Limits {
    metadata_quorum::Limits {
        catalog: catalog::Limits::default(),
        positions: 64,
        record_bytes: 64 * 1024,
        source_bytes: 1024 * 1024,
        journal_bytes: 2 * 1024 * 1024,
    }
}
fn paths(root: &Path) -> Result<Paths> {
    Ok(Paths::new(
        root.join("content.wal"),
        root.join("election.wal"),
        root.join("images"),
    )?)
}
struct Cluster {
    temp: Temp,
    routes: Vec<SocketAddr>,
    nodes: Vec<Option<Runtime>>,
    routers: Vec<Option<Router>>,
}
impl Cluster {
    async fn start(count: usize) -> Result<Self> {
        let temp = Temp::new()?;
        let mut listeners = Vec::new();
        let mut routes = Vec::new();
        for _ in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            routes.push(listener.local_addr()?);
            listeners.push(listener);
        }
        // Own every started runtime before the first fallible launch so an
        // initialization failure cannot detach an earlier node's joined drain.
        let mut group = Self {
            temp,
            routes,
            nodes: (0..count).map(|_| None).collect(),
            routers: (0..count).map(|_| None).collect(),
        };
        let initialized: Result = async {
            for (id, listener) in listeners.into_iter().enumerate() {
                let root = group.temp.0.join(id.to_string());
                fs::create_dir(&root)?;
                group.nodes[id] = Some(Runtime::start(
                    listener,
                    config(id, &group.routes)?,
                    paths(&root)?,
                )?);
            }
            for id in 0..count {
                group.nodes[id]
                    .as_ref()
                    .ok_or("missing node")?
                    .wait_ready()
                    .await?;
            }
            for id in 0..count {
                group.open_router(id).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = initialized {
            // Preserve the startup error while attempting every owned join.
            if let Err(cleanup) = group.close().await {
                eprintln!("metadata fixture startup cleanup also failed: {cleanup}");
            }
            return Err(error);
        }
        Ok(group)
    }
    async fn open_router(&mut self, id: usize) -> Result {
        let source = self.nodes[id].as_ref().ok_or("missing node")?.handle();
        let config = RouterConfig::new(100, "127.0.0.1".into(), 9092, "metadata-quorum-tcp".into());
        let (router, _) = Router::open_quorum(
            self.temp.0.join(id.to_string()).join("application.wal"),
            config,
            source,
            app_limits(),
            Duration::from_secs(3),
        )
        .await?;
        self.routers[id] = Some(router);
        Ok(())
    }
    async fn leader(&self) -> Result<usize> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            for (id, node) in self.nodes.iter().enumerate() {
                if let Some(node) = node {
                    let state = node.handle().state().await?;
                    if state.election.role == Role::Leader
                        && state.active_term == Some(state.election.persistent.term)
                    {
                        return Ok(id);
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err("metadata application election deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn stop(&mut self, id: usize) -> Result {
        if let Some(router) = self.routers[id].as_ref() {
            router.shutdown().await?;
        }
        self.routers[id] = None;
        if let Some(node) = self.nodes[id].as_mut() {
            node.shutdown().await?;
        }
        self.nodes[id] = None;
        Ok(())
    }
    async fn restart(&mut self, id: usize) -> Result {
        let listener = TcpListener::bind(self.routes[id]).await?;
        let node = Runtime::start(
            listener,
            config(id, &self.routes)?,
            paths(&self.temp.0.join(id.to_string()))?,
        )?;
        self.nodes[id] = Some(node);
        self.nodes[id]
            .as_ref()
            .ok_or("missing restart owner")?
            .wait_ready()
            .await?;
        self.open_router(id).await
    }
    async fn close(&mut self) -> Result {
        // Attempt every joined shutdown even if an earlier owner reports error.
        let mut first = None;
        for router in self.routers.iter_mut() {
            if let Some(owner) = router.as_ref() {
                if let Err(e) = owner.shutdown().await {
                    if first.is_none() {
                        first = Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
            }
            *router = None;
        }
        for node in self.nodes.iter_mut() {
            if let Some(owner) = node.as_mut() {
                let handle = owner.handle();
                if let Err(e) = owner.shutdown().await {
                    if first.is_none() {
                        first = Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                    }
                } else {
                    let d = handle.diagnostics();
                    if d.network_tasks != 0
                        || d.sockets != 0
                        || d.transport_bytes != 0
                        || d.client_slots != 0
                    {
                        if first.is_none() {
                            first = Some("postjoin runtime owners remain".into());
                        }
                    }
                }
            }
            *node = None;
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
fn string(bytes: &mut Vec<u8>, name: &str) -> Result {
    let len = i16::try_from(name.len())?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(name.as_bytes());
    Ok(())
}
fn header(api: i16, version: i16) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&api.to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&77i32.to_be_bytes());
    out.extend_from_slice(&(-1i16).to_be_bytes());
    out
}
fn create(name: &str) -> Result<Vec<u8>> {
    let mut out = header(19, 3);
    out.extend_from_slice(&1i32.to_be_bytes());
    string(&mut out, name)?;
    out.extend_from_slice(&1i32.to_be_bytes());
    out.extend_from_slice(&1i16.to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&3000i32.to_be_bytes());
    out.push(0);
    Ok(out)
}
fn metadata(name: &str) -> Result<Vec<u8>> {
    let mut out = header(3, 0);
    out.extend_from_slice(&1i32.to_be_bytes());
    string(&mut out, name)?;
    Ok(out)
}
struct Read<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Read<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or("overflow")?;
        let out = self.bytes.get(self.at..end).ok_or("truncated response")?;
        self.at = end;
        Ok(out)
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into()?))
    }
    fn string(&mut self) -> Result<Option<&'a str>> {
        let n = self.i16()?;
        if n == -1 {
            return Ok(None);
        }
        let n = usize::try_from(n)?;
        Ok(Some(std::str::from_utf8(self.take(n)?)?))
    }
    fn end(&self) -> Result {
        ensure(self.at == self.bytes.len(), "response tail")
    }
}
fn create_error(bytes: &[u8], name: &str) -> Result<i16> {
    let mut r = Read { bytes, at: 0 };
    ensure(r.i32()? == 77, "correlation")?;
    r.i32()?;
    ensure(r.i32()? == 1, "one result")?;
    ensure(r.string()? == Some(name), "topic name")?;
    let error = r.i16()?;
    r.string()?;
    r.end()?;
    Ok(error)
}
fn metadata_error(bytes: &[u8], name: &str) -> Result<i16> {
    let mut r = Read { bytes, at: 0 };
    ensure(r.i32()? == 77, "correlation")?;
    let brokers = usize::try_from(r.i32()?)?;
    ensure(brokers <= 16, "broker bound")?;
    for _ in 0..brokers {
        r.i32()?;
        r.string()?;
        r.i32()?;
    }
    ensure(r.i32()? == 1, "one topic")?;
    let error = r.i16()?;
    ensure(r.string()? == Some(name), "topic name")?;
    let partitions = usize::try_from(r.i32()?)?;
    ensure(partitions <= 16, "partition bound")?;
    for _ in 0..partitions {
        r.i16()?;
        r.i32()?;
        r.i32()?;
        for _ in 0..2 {
            let count = usize::try_from(r.i32()?)?;
            ensure(count <= 16, "replica bound")?;
            for _ in 0..count {
                r.i32()?;
            }
        }
    }
    r.end()?;
    Ok(error)
}
async fn observed(group: &Cluster, id: usize, name: &str) -> Result {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = group.routers[id]
            .as_ref()
            .ok_or("missing router")?
            .respond(metadata(name)?)
            .await?;
        if metadata_error(&response, name)? == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("metadata follower application deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn quorum_application_case(count: usize) -> Result {
    let mut group = Cluster::start(count).await?;
    let result: Result = async {
        let leader = group.leader().await?;
        let response = group.routers[leader]
            .as_ref()
            .ok_or("missing router")?
            .respond(create("committed-before-minority")?)
            .await?;
        ensure(
            create_error(&response, "committed-before-minority")? == 0,
            "first create quorum outcome",
        )?;
        for id in 0..count {
            observed(&group, id, "committed-before-minority").await?;
        }
        let stopped = (0..count)
            .filter(|id| *id != leader)
            .take(count / 2)
            .collect::<Vec<_>>();
        for id in &stopped {
            group.stop(*id).await?;
        }
        let response = group.routers[leader]
            .as_ref()
            .ok_or("missing router")?
            .respond(create("committed-with-majority")?)
            .await?;
        ensure(
            create_error(&response, "committed-with-majority")? == 0,
            "remaining majority create",
        )?;
        for (id, node) in group.nodes.iter().enumerate() {
            if node.is_some() {
                observed(&group, id, "committed-with-majority").await?;
            }
        }
        for id in &stopped {
            group.restart(*id).await?;
            observed(&group, *id, "committed-before-minority").await?;
            observed(&group, *id, "committed-with-majority").await?;
        }
        // A follower command cannot publish a locally synced false success.
        let current_leader = group.leader().await?;
        let follower = (0..count)
            .find(|id| *id != current_leader)
            .ok_or("follower")?;
        let response = group.routers[follower]
            .as_ref()
            .ok_or("missing router")?
            .respond(create("follower-must-not-publish")?)
            .await?;
        ensure(
            create_error(&response, "follower-must-not-publish")? == 41,
            "follower must return NotController",
        )?;
        let response = group.routers[current_leader]
            .as_ref()
            .ok_or("missing router")?
            .respond(metadata("follower-must-not-publish")?)
            .await?;
        ensure(
            metadata_error(&response, "follower-must-not-publish")? == 3,
            "failed follower create must remain invisible",
        )?;
        Ok(())
    }
    .await;
    let closed = group.close().await;
    result?;
    closed
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_node_tcp_router_waits_for_source_commit_and_replays_after_minority_restart() -> Result
{
    quorum_application_case(3).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_tcp_metadata_application_keeps_committed_views_through_two_restarts() -> Result {
    quorum_application_case(5).await
}
