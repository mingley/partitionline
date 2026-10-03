//! Bounded evidence-only composition of the actual broker TLS/SASL/store/router.
//! This separate workspace and its synthetic credential bootstrap are not a
//! production broker entry point, admin bootstrap or readiness qualification.
use partitionline_broker::{
    metadata,
    security::{
        credentials::{self, Change, Store},
        sasl::{Algorithm, Secret},
        session::{self, Profile, SASL_METADATA_API_VERSIONS},
        tls::{self, Acceptor, ClientAuth},
    },
    transport::{Config, Handler, Peer, Transport},
};
use std::{error::Error, io, path::Path, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

fn hex(raw: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if raw.len() > 2048 || raw.len() % 2 != 0 {
        return Err("bounded bootstrap hex".into());
    }
    raw.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(text, 16)?)
        })
        .collect()
}

async fn bootstrap(store: &Store, path: &Path) -> Result<(), Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    if raw.len() > 4096 {
        return Err("bounded public bootstrap fixture".into());
    }
    let mut rows = raw.lines();
    if rows.next() != Some("user\tmechanism\titerations\tsalt_hex\tsalted_password_hex") {
        return Err("exact bootstrap header".into());
    }
    for expected in ["user", "admin", "unicode"] {
        let mut changes = Vec::new();
        for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
            let row: Vec<_> = rows
                .next()
                .ok_or("missing bootstrap row")?
                .split('\t')
                .collect();
            if row.len() != 5
                || row[0] != expected
                || row[1] != algorithm.name()
                || row[2] != "4096"
            {
                return Err("exact bootstrap user/hash/iterations".into());
            }
            changes.push(Change::Upsert {
                algorithm,
                salt: hex(row[3])?,
                iterations: 4096,
                salted_password: Secret::new(hex(row[4])?),
            });
        }
        let generation = store.mutate(expected.into(), changes).await?;
        println!("{{\"event\":\"bootstrap\",\"user\":\"{expected}\",\"generation\":{generation}}}");
    }
    if rows.next().is_some() {
        return Err("unexpected bootstrap rows".into());
    }
    Ok(())
}

struct AuditedRouter(Arc<metadata::Router>);
impl Handler for AuditedRouter {
    type Error = io::Error;
    async fn handle(&self, _request: Vec<u8>) -> Result<Option<Vec<u8>>, Self::Error> {
        Err(io::Error::other(
            "evidence router requires authenticated peer",
        ))
    }
    async fn handle_with_peer(
        &self,
        peer: &Peer,
        request: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let identity = peer
            .identity()
            .ok_or_else(|| io::Error::other("no verified SASL identity"))?;
        if request.len() < 8 {
            return Err(io::Error::other("short application header"));
        }
        let key = i16::from_be_bytes([request[0], request[1]]);
        let correlation = i32::from_be_bytes([request[4], request[5], request[6], request[7]]);
        let name = match identity.name() {
            "user" | "admin" | "unicode" | "created-user" | "native-created" => identity.name(),
            _ => "other-public-test-identity",
        };
        println!(
            "{{\"event\":\"application-dispatch\",\"user\":\"{}\",\"generation\":{},\"api\":{},\"correlation\":{},\"tls\":{}}}",
            name,
            identity.generation(),
            key,
            correlation,
            peer.tls().is_some()
        );
        self.0
            .handle_with_peer(peer, request)
            .await
            .map_err(|_| io::Error::other("actual metadata router failure"))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        return Err(
            "usage stateDir fixtureDir tlsPort plainPort bootstrapFile initial|restart|default-admin".into(),
        );
    }
    let state = Path::new(&args[1]);
    let fixtures = Path::new(&args[2]);
    let tls_port: u16 = args[3].parse()?;
    let plain_port: u16 = args[4].parse()?;
    if tls_port == 0 || plain_port == 0 || tls_port == plain_port {
        return Err("explicit distinct listener ports".into());
    }
    std::fs::create_dir_all(state)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(state, std::fs::Permissions::from_mode(0o700))?;
    }
    let (store, recovery) = Store::open(
        state.join("credentials.log"),
        credentials::Limits::default(),
    )
    .await?;
    match args[6].as_str() {
        "initial" if recovery.recovered_entries == 0 => {
            bootstrap(&store, Path::new(&args[5])).await?
        }
        "restart" | "default-admin" if recovery.recovered_entries > 0 => (),
        _ => return Err("expected initial/restarted verifier history".into()),
    }
    println!(
        "{{\"event\":\"credential-recovery\",\"entries\":{},\"truncated_bytes\":{},\"healthy\":{}}}",
        recovery.recovered_entries,
        recovery.truncated_bytes,
        store.is_healthy()
    );
    let (tls_router, _) = metadata::Router::open(
        state.join("metadata-tls.log"),
        metadata::Config::new(0, "localhost".into(), tls_port, "sasl-wire-tls".into()),
    )
    .await?;
    let (plain_router, _) = metadata::Router::open(
        state.join("metadata-plain.log"),
        metadata::Config::new(0, "localhost".into(), plain_port, "sasl-wire-plain".into()),
    )
    .await?;
    let tls_router = Arc::new(tls_router);
    let plain_router = Arc::new(plain_router);
    let acceptor = Acceptor::new(
        vec![std::fs::read(fixtures.join("server1.cert.der"))?],
        std::fs::read(fixtures.join("server1.key.der"))?,
        ClientAuth::ServerOnly,
        tls::Limits::default(),
    )?;
    let config = Config::new(
        64,
        8,
        16 * 1024,
        1024 * 1024,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
    )?;
    let limits = session::Limits {
        preauth_timeout: Duration::from_secs(3),
        ..session::Limits::default()
    };
    let administrators = if args[6] == "default-admin" {
        Vec::new()
    } else {
        vec!["admin".into()]
    };
    let tls_profile = Profile::tls(store.clone(), administrators.clone(), limits)?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?;
    let plain_profile = Profile::scram_plaintext(store.clone(), administrators.clone(), limits)?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?;
    let mut tls_transport = Transport::bind_tls_sasl(
        format!("127.0.0.1:{tls_port}").parse()?,
        config,
        Arc::new(AuditedRouter(tls_router.clone())),
        acceptor,
        tls_profile,
    )
    .await?;
    let mut plain_transport = Transport::bind_sasl(
        format!("127.0.0.1:{plain_port}").parse()?,
        config,
        Arc::new(AuditedRouter(plain_router.clone())),
        plain_profile,
    )
    .await?;
    println!("{{\"event\":\"ready\",\"tls_port\":{tls_port},\"plaintext_port\":{plain_port},\"admin_allowlist_count\":{}}}", administrators.len());
    let mut command = String::new();
    if BufReader::new(tokio::io::stdin().take(17))
        .read_line(&mut command)
        .await?
        > 16
        || command.trim() != "STOP"
    {
        return Err("bounded explicit shutdown command".into());
    }
    let tls_report = tls_transport.shutdown().await?;
    let plain_report = plain_transport.shutdown().await?;
    tls_router.shutdown().await?;
    plain_router.shutdown().await?;
    store.shutdown().await?;
    if tls_report.accepted_connections != tls_report.joined_connections
        || plain_report.accepted_connections != plain_report.joined_connections
        || tls_report.worker_failures != 0
        || plain_report.worker_failures != 0
    {
        return Err("transport did not join cleanly".into());
    }
    println!(
        "{{\"event\":\"shutdown\",\"tls_accepted\":{},\"tls_joined\":{},\"plain_accepted\":{},\"plain_joined\":{},\"shutdown_connections\":{},\"worker_failures\":0,\"credential_store_joined\":true}}",
        tls_report.accepted_connections,
        tls_report.joined_connections,
        plain_report.accepted_connections,
        plain_report.joined_connections,
        tls_report.shutdown_connections + plain_report.shutdown_connections,
    );
    Ok(())
}
