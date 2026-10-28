//! Loopback-only, metadata-only private quorum. No data-plane replication.
use partitionline_broker::{
    catalog, metadata, metadata_quorum,
    raft::{
        membership::{Bootstrap, Endpoint, Key, Voter, Voters},
        protocol,
        runtime::{Config as RaftConfig, Limits, Paths, Peer, Runtime, StorageLimits},
        snapshot,
    },
    transport,
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

#[derive(Clone)]
struct Member {
    id: u32,
    directory: [u8; 16],
    private: SocketAddr,
    client: SocketAddr,
}
struct Config {
    data: PathBuf,
    cluster: String,
    local: u32,
    members: Vec<Member>,
}

fn directory(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 || !value.is_ascii() {
        return Err("directory ID must be 32 hexadecimal characters".into());
    }
    let mut result = [0; 16];
    for (index, output) in result.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid directory ID")?;
    }
    if result == [0; 16] {
        return Err("directory ID must be nonzero".into());
    }
    Ok(result)
}
fn parse(args: &[String]) -> Result<Config, String> {
    let mut options = HashMap::new();
    for pair in args[1..].chunks(2) {
        if pair.len() != 2 {
            return Err("serve-quorum options must be name/value pairs".into());
        }
        let name = pair[0].as_str();
        if !matches!(name, "--data" | "--cluster" | "--id" | "--members") {
            return Err(format!("unknown serve-quorum option {name}"));
        }
        if options.insert(name, pair[1].as_str()).is_some() {
            return Err(format!("duplicate serve-quorum option {name}"));
        }
    }
    let required = |name| {
        options
            .get(name)
            .copied()
            .ok_or_else(|| format!("missing {name}"))
    };
    let data = PathBuf::from(required("--data")?);
    if data.as_os_str().is_empty() {
        return Err("empty data directory".into());
    }
    let cluster = required("--cluster")?.to_owned();
    if cluster.is_empty() || cluster.len() > 255 || !cluster.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("cluster ID must be 1–255 printable ASCII bytes".into());
    }
    let local: u32 = required("--id")?
        .parse()
        .map_err(|_| "invalid controller ID")?;
    if local > i32::MAX as u32 {
        return Err("controller ID exceeds Kafka range".into());
    }
    let raw = required("--members")?;
    if raw.len() > 8192 {
        return Err("member configuration too long".into());
    }
    let mut members = Vec::new();
    for part in raw.split(',') {
        let fields: Vec<_> = part.split('@').collect();
        if fields.len() != 4 {
            return Err("each member must be ID@DIRECTORY_HEX@PRIVATE_ADDR@CLIENT_ADDR".into());
        }
        let id: u32 = fields[0].parse().map_err(|_| "invalid member ID")?;
        if id > i32::MAX as u32 {
            return Err("member ID exceeds Kafka range".into());
        }
        let private: SocketAddr = fields[2].parse().map_err(|_| "invalid private address")?;
        let client: SocketAddr = fields[3].parse().map_err(|_| "invalid client address")?;
        if !private.ip().is_loopback()
            || !client.ip().is_loopback()
            || private.port() == 0
            || client.port() == 0
        {
            return Err("metadata-only profile requires nonzero loopback addresses".into());
        }
        members.push(Member {
            id,
            directory: directory(fields[1])?,
            private,
            client,
        });
    }
    if members.len() != 3 {
        return Err("metadata-only profile requires exactly three members".into());
    }
    members.sort_unstable_by_key(|member| member.id);
    for (index, member) in members.iter().enumerate() {
        if members[..index].iter().any(|old| {
            old.id == member.id
                || old.directory == member.directory
                || old.private == member.private
                || old.client == member.client
        }) || members.iter().any(|old| old.private == member.client)
        {
            return Err("member IDs, directory IDs and addresses must be unique".into());
        }
    }
    if !members.iter().any(|member| member.id == local) {
        return Err("local ID missing from members".into());
    }
    Ok(Config {
        data,
        cluster,
        local,
        members,
    })
}
fn lock_identity(config: &Config) -> Result<File, String> {
    fs::create_dir_all(&config.data).map_err(|error| format!("data directory: {error}"))?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(config.data.join("serve.lock"))
        .map_err(|error| format!("data lock: {error}"))?;
    lock.try_lock()
        .map_err(|error| format!("data directory already in use or cannot be locked: {error}"))?;
    if config.data.join("serve.identity").exists() || config.data.join("catalog.journal").exists() {
        return Err("ordinary server data cannot be opened as a metadata quorum".into());
    }
    let mut identity = format!(
        "partitionline-metadata-quorum-v1\n{}\n{}\n",
        config.cluster, config.local
    );
    for member in &config.members {
        let hex: String = member
            .directory
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        use std::fmt::Write as _;
        writeln!(
            identity,
            "{}@{}@{}@{}",
            member.id, hex, member.private, member.client
        )
        .map_err(|_| "identity formatting")?;
    }
    let path = config.data.join("quorum.identity");
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            file.write_all(identity.as_bytes())
                .map_err(|error| format!("identity write: {error}"))?;
            file.sync_all()
                .map_err(|error| format!("identity sync: {error}"))?;
            #[cfg(unix)]
            File::open(&config.data)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| format!("identity directory sync: {error}"))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut actual = Vec::new();
            File::open(&path)
                .and_then(|file| file.take(16384).read_to_end(&mut actual))
                .map_err(|error| format!("identity read: {error}"))?;
            if actual != identity.as_bytes() {
                return Err("data directory quorum identity does not match".into());
            }
        }
        Err(error) => return Err(format!("identity open: {error}")),
    }
    Ok(lock)
}
pub(super) fn run(args: &[String]) -> Result<(), String> {
    let config = parse(args)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("runtime: {error}"))?;
    runtime.block_on(serve(config))
}
async fn serve(config: Config) -> Result<(), String> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| format!("SIGINT handler: {error}"))?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("SIGTERM handler: {error}"))?;
    let _lock = lock_identity(&config)?;
    let local = config
        .members
        .iter()
        .find(|member| member.id == config.local)
        .ok_or("missing local member")?;
    let voters: Vec<Voter> = config
        .members
        .iter()
        .map(|member| {
            let key = Key::new(member.id, member.directory)
                .map_err(|error| format!("member identity: {error:?}"))?;
            let endpoint = Endpoint::new(
                "CONTROLLER".into(),
                member.private.ip().to_string(),
                member.private.port(),
            )
            .map_err(|error| format!("member endpoint: {error:?}"))?;
            Voter::new(key, vec![endpoint], 0, 1).map_err(|error| format!("voter: {error:?}"))
        })
        .collect::<Result<_, String>>()?;
    let genesis = Voters::new(0, Default::default(), 1, voters.clone())
        .map_err(|error| format!("genesis: {error:?}"))?;
    let mine = voters
        .iter()
        .find(|voter| voter.key().id == config.local)
        .ok_or("missing local voter")?
        .clone();
    let bootstrap = Bootstrap::new(mine, genesis, "CONTROLLER".into())
        .map_err(|error| format!("bootstrap: {error:?}"))?;
    let peers: Vec<Peer> = config
        .members
        .iter()
        .zip(voters.iter())
        .filter(|(member, _)| member.id != config.local)
        .map(|(member, voter)| {
            Peer::new(voter.clone(), member.private).map_err(|error| format!("peer: {error}"))
        })
        .collect::<Result<_, _>>()?;
    let mut controller =
        protocol::Config::new(config.local, vec![config.local], config.cluster.clone())
            .map_err(|error| format!("controller: {error:?}"))?;
    controller.timer_seed = u64::from(config.local).saturating_mul(37).saturating_add(1);
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
        )
        .map_err(|error| format!("image limits: {error:?}"))?,
    };
    let limits = Limits {
        incoming: 2,
        client_slots: 4,
        frame_bytes: 512 * 1024,
        election_min_ms: 200,
        election_max_ms: 450,
        quorum_ms: 1500,
        tick_ms: 10,
        heartbeat_ms: 40,
        connect_ms: 250,
        rpc_ms: 500,
        transfer_ms: 1000,
        command_ms: 3000,
    };
    let raft_config = RaftConfig::new(controller, bootstrap, peers, storage, limits)
        .map_err(|error| format!("raft config: {error}"))?;
    let paths = Paths::new(
        config.data.join("content.wal"),
        config.data.join("election.wal"),
        config.data.join("images"),
    )
    .map_err(|error| format!("raft paths: {error}"))?;
    let listener = tokio::net::TcpListener::bind(local.private)
        .await
        .map_err(|error| format!("private listener: {error}"))?;
    let mut raft = Runtime::start(listener, raft_config, paths)
        .map_err(|error| format!("raft startup: {error}"))?;
    if let Err(error) = raft.wait_ready().await {
        let shutdown = raft.shutdown().await;
        return Err(format!("raft recovery: {error}; shutdown: {shutdown:?}"));
    }
    let router_config = metadata::Config::new(
        config.local as i32,
        local.client.ip().to_string(),
        local.client.port(),
        config.cluster.clone(),
    );
    let endpoints = config
        .members
        .iter()
        .map(|member| metadata::ControllerEndpoint {
            id: member.id as i32,
            host: member.client.ip().to_string(),
            port: member.client.port(),
        })
        .collect();
    let application = metadata_quorum::Limits {
        catalog: catalog::Limits::default(),
        positions: 64,
        record_bytes: 64 * 1024,
        source_bytes: 1024 * 1024,
        journal_bytes: 2 * 1024 * 1024,
    };
    let router = match metadata::Router::open_quorum_with_controllers(
        config.data.join("application.wal"),
        router_config,
        raft.handle(),
        application,
        Duration::from_secs(3),
        endpoints,
    )
    .await
    {
        Ok((router, _)) => Arc::new(router),
        Err(error) => {
            let shutdown = raft.shutdown().await;
            return Err(format!(
                "router startup: {error}; raft shutdown: {shutdown:?}"
            ));
        }
    };
    let mut listener = match transport::Transport::bind(
        local.client,
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await
    {
        Ok(listener) => listener,
        Err(error) => {
            let router_result = router.shutdown().await;
            let raft_result = raft.shutdown().await;
            return Err(format!("client listener: {error}; router shutdown: {router_result:?}; raft shutdown: {raft_result:?}"));
        }
    };
    let ready = {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        writeln!(
            output,
            "ready {} private={} profile=metadata-only-quorum",
            listener.local_addr(),
            local.private
        )
        .and_then(|()| output.flush())
    };
    let signal_result: Result<(), String> = if ready.is_ok() {
        #[cfg(unix)]
        tokio::select! { _ = interrupt.recv() => Ok(()), _ = terminate.recv() => Ok(()) }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| format!("interrupt handler: {error}"))
    } else {
        Ok(())
    };
    let transport_result = listener.shutdown().await;
    let router_result = router.shutdown().await;
    let raft_result = raft.shutdown().await;
    ready.map_err(|error| format!("ready output: {error}"))?;
    let report = transport_result.map_err(|error| format!("client listener shutdown: {error}"))?;
    router_result.map_err(|error| format!("router shutdown: {error}"))?;
    raft_result.map_err(|error| format!("raft shutdown: {error}"))?;
    signal_result?;
    if report.accepted_connections != report.joined_connections || report.worker_failures != 0 {
        return Err("client listener did not join all workers cleanly".into());
    }
    println!(
        "stopped accepted={} joined={} worker_failures={}",
        report.accepted_connections, report.joined_connections, report.worker_failures
    );
    Ok(())
}
