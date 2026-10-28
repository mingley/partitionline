//! Standalone ordinary single-node listener. This is not a Kafka cluster.

use partitionline_broker::{fetch, metadata, produce, transport};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

struct Config {
    data: PathBuf,
    listen: SocketAddr,
    advertised_host: String,
    advertised_port: u16,
    cluster: String,
    broker_id: i32,
}

fn parse(args: &[String]) -> Result<Config, String> {
    let mut options = HashMap::new();
    for pair in args[1..].chunks(2) {
        if pair.len() != 2 {
            return Err("serve options must be name/value pairs".into());
        }
        let name = pair[0].as_str();
        if !matches!(
            name,
            "--data"
                | "--listen"
                | "--advertise-host"
                | "--advertise-port"
                | "--cluster"
                | "--broker-id"
        ) {
            return Err(format!("unknown serve option {name}"));
        }
        if options.insert(name, pair[1].as_str()).is_some() {
            return Err(format!("duplicate serve option {name}"));
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
    let listen: SocketAddr = required("--listen")?
        .parse()
        .map_err(|_| "invalid listen address")?;
    if listen.port() == 0 {
        return Err("listen port must be nonzero".into());
    }
    let advertised_host = required("--advertise-host")?.to_owned();
    let advertised_port = options
        .get("--advertise-port")
        .map_or(Ok(listen.port()), |value| {
            value.parse::<u16>().map_err(|_| "invalid advertised port")
        })?;
    if advertised_port == 0 {
        return Err("advertised port must be nonzero".into());
    }
    let cluster = required("--cluster")?.to_owned();
    if cluster.is_empty() || cluster.len() > 255 || !cluster.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("cluster ID must be 1–255 printable ASCII bytes".into());
    }
    let broker_id = options.get("--broker-id").map_or(Ok(0), |value| {
        value.parse::<i32>().map_err(|_| "invalid broker ID")
    })?;
    if broker_id < 0 {
        return Err("broker ID must be nonnegative".into());
    }
    Ok(Config {
        data,
        listen,
        advertised_host,
        advertised_port,
        cluster,
        broker_id,
    })
}

// Keep the lock file open for the entire server lifetime. The identity is
// created once and synchronized; a partial or mismatched file fails closed.
fn lock_and_validate_identity(config: &Config) -> Result<File, String> {
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
    let identity = format!(
        "partitionline-rf1-v1\n{}\n{}\n",
        config.cluster, config.broker_id
    );
    let path = config.data.join("serve.identity");
    if !path.exists() && config.data.join("catalog.journal").exists() {
        return Err("existing catalog has no server identity; refusing adoption".into());
    }
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
                .and_then(|file| file.take(512).read_to_end(&mut actual))
                .map_err(|error| format!("identity read: {error}"))?;
            if actual != identity.as_bytes() {
                return Err("data directory cluster or broker identity does not match".into());
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
    // Register signal handlers before opening the listener so an immediate stop
    // can be observed and all opened resources still pass through shutdown.
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| format!("SIGINT handler: {error}"))?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("SIGTERM handler: {error}"))?;
    let _lock = lock_and_validate_identity(&config)?;
    let advertised = format!("{}:{}", config.advertised_host, config.advertised_port);
    let router_config = metadata::Config::new(
        config.broker_id,
        config.advertised_host,
        config.advertised_port,
        config.cluster,
    );
    let (router, recovery) = metadata::Router::open_with_read_store(
        config.data.join("catalog.journal"),
        router_config,
        produce::Config::new(config.data.join("partitions")),
        fetch::Limits::default(),
    )
    .await
    .map_err(|error| format!("router startup: {error}"))?;
    let router = Arc::new(router);
    let mut transport = match transport::Transport::bind(
        config.listen,
        transport::Config::default(),
        Arc::clone(&router),
    )
    .await
    {
        Ok(transport) => transport,
        Err(error) => {
            let shutdown = router.shutdown().await;
            return Err(format!(
                "listener startup: {error}; router shutdown: {shutdown:?}"
            ));
        }
    };
    let ready = {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        writeln!(
            output,
            "ready {} advertised={} recovered_entries={} profile=ordinary-rf1",
            transport.local_addr(),
            advertised,
            recovery.recovered_entries
        )
        .and_then(|()| output.flush())
    };
    let signal_result: Result<(), String> = if ready.is_ok() {
        #[cfg(unix)]
        tokio::select! {
            _ = interrupt.recv() => Ok(()),
            _ = terminate.recv() => Ok(()),
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| format!("interrupt handler: {error}"))
    } else {
        Ok(())
    };
    // Always join both owners even if one reports a failure.
    let listener = transport.shutdown().await;
    let actor = router.shutdown().await;
    ready.map_err(|error| format!("ready output: {error}"))?;
    let report = listener.map_err(|error| format!("listener shutdown: {error}"))?;
    actor.map_err(|error| format!("router shutdown: {error}"))?;
    signal_result?;
    if report.accepted_connections != report.joined_connections || report.worker_failures != 0 {
        return Err("listener did not join all workers cleanly".into());
    }
    println!(
        "stopped accepted={} joined={} worker_failures={}",
        report.accepted_connections, report.joined_connections, report.worker_failures
    );
    Ok(())
}
