//! Finite evidence-only composition of managed OIDC and the actual broker router.
//!
//! Run with `--features oidc --example oidc_live_probe -- CONFIG.json`. The
//! configuration contains public endpoints and ephemeral secret/key file paths,
//! never bearer tokens. The explicit metadata profile serves six APIs; the
//! read-write profile adds ordinary Produce, Fetch and ListOffsets. Neither
//! profile establishes groups, transactions, idempotence, replication or arbitrary
//! application authorization. `session_lifetime_ms` defaults to zero; a positive
//! whole-millisecond value through 24 hours explicitly enables bounded renewed
//! authentication. The actual response lifetime is also capped by its authority
//! lease. A bounded stop-file
//! protocol joins the listener, authority manager and router before reporting.

#[cfg(not(feature = "oidc"))]
fn main() -> std::process::ExitCode {
    eprintln!("oidc_live_probe requires --features oidc");
    std::process::ExitCode::FAILURE
}

#[cfg(feature = "oidc")]
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    live::run().await
}

#[cfg(feature = "oidc")]
mod live {
    use partitionline_broker::{
        catalog::{self, Catalog, TopicId},
        fetch, journal, metadata, produce,
        protocol::{self, ApiVersion},
        security::{
            oidc::{self, AccessToken, Algorithm, HttpsTrust, Introspection, KeySource, Policy},
            sasl::{Authority, Secret},
            session::{self, Profile, OIDC_METADATA_API_VERSIONS},
            tls::{self, Acceptor, ClientAuth},
        },
        transport::{self, Handler, Peer, Transport},
    };
    use serde_json::{json, Value};
    use std::{
        error::Error,
        fs::File,
        io::{self, Read},
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
    const MAX_EVENTS: usize = 8192;
    const READ_WRITE_APIS: [ApiVersion; 9] = [
        ApiVersion {
            api_key: 0,
            min_version: 3,
            max_version: 13,
        },
        ApiVersion {
            api_key: 1,
            min_version: 4,
            max_version: 6,
        },
        ApiVersion {
            api_key: 2,
            min_version: 1,
            max_version: 3,
        },
        OIDC_METADATA_API_VERSIONS[0],
        OIDC_METADATA_API_VERSIONS[1],
        OIDC_METADATA_API_VERSIONS[2],
        OIDC_METADATA_API_VERSIONS[3],
        OIDC_METADATA_API_VERSIONS[4],
        OIDC_METADATA_API_VERSIONS[5],
    ];

    fn read(path: &Path, maximum: usize) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(path)?
            .take(u64::try_from(maximum)? + 1)
            .read_to_end(&mut bytes)?;
        if bytes.is_empty() || bytes.len() > maximum {
            return Err("bounded nonempty probe file".into());
        }
        Ok(bytes)
    }
    fn string(config: &Value, field: &str) -> Result<String> {
        let value = config
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.len() <= 2048)
            .ok_or("bounded required probe configuration field")?;
        Ok(value.into())
    }
    fn number(
        config: &Value,
        field: &str,
        default: u64,
        minimum: u64,
        maximum: u64,
    ) -> Result<u64> {
        let value = match config.get(field) {
            Some(value) => value.as_u64().ok_or("integer probe configuration")?,
            None => default,
        };
        if !(minimum..=maximum).contains(&value) {
            return Err("bounded probe configuration integer".into());
        }
        Ok(value)
    }
    fn milliseconds(config: &Value, field: &str, default: u64, maximum: u64) -> Result<Duration> {
        Ok(Duration::from_millis(number(
            config, field, default, 50, maximum,
        )?))
    }

    struct AuditedRouter {
        router: Arc<metadata::Router>,
        dispatched: AtomicUsize,
    }
    impl Handler for AuditedRouter {
        type Error = io::Error;
        async fn handle(
            &self,
            _request: Vec<u8>,
        ) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
            Err(io::Error::other(
                "probe requires a verified socket identity",
            ))
        }
        async fn handle_with_peer(
            &self,
            peer: &Peer,
            request: Vec<u8>,
        ) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
            let identity = peer
                .identity()
                .ok_or_else(|| io::Error::other("missing verified socket identity"))?;
            let Authority::Oidc { issuer } = identity.authority() else {
                return Err(io::Error::other("probe requires issuer-scoped identity"));
            };
            if peer.tls().is_none() {
                return Err(io::Error::other("probe requires verified TLS transport"));
            }
            let header = request
                .get(..8)
                .ok_or_else(|| io::Error::other("short application header"))?;
            let api = i16::from_be_bytes(
                header
                    .get(..2)
                    .ok_or_else(|| io::Error::other("application key"))?
                    .try_into()
                    .map_err(|_| io::Error::other("application key width"))?,
            );
            let correlation = i32::from_be_bytes(
                header
                    .get(4..8)
                    .ok_or_else(|| io::Error::other("application correlation"))?
                    .try_into()
                    .map_err(|_| io::Error::other("application correlation width"))?,
            );
            let ordinal = self.dispatched.fetch_add(1, Ordering::Relaxed);
            if ordinal >= MAX_EVENTS {
                return Err(io::Error::other("finite probe dispatch history"));
            }
            println!(
                "{}",
                json!({"event":"application-dispatch","ordinal":ordinal,"api":api,
                    "correlation":correlation,"issuer":issuer,"subject":identity.name(),
                    "generation":identity.generation(),"tls":true})
            );
            self.router
                .handle_with_peer(peer, request)
                .await
                .map_err(|_| io::Error::other("actual router rejected request"))
        }
    }

    async fn shutdown(
        stage: &str,
        listener: Option<&mut Transport>,
        service: Option<&oidc::Service>,
        router: &metadata::Router,
    ) -> Result<Option<transport::Report>> {
        // Attempt every available owner even if an earlier join reports an
        // error. An error is recorded as such; it is never a successful join.
        let listener_result = match listener {
            Some(listener) => Some(listener.shutdown().await),
            None => None,
        };
        let service_result = match service {
            Some(service) => Some(service.shutdown().await),
            None => None,
        };
        let router_result = router.shutdown().await;
        println!(
            "{}",
            json!({"event":"shutdown-attempts","stage":stage,
                "listener_attempted":listener_result.is_some(),
                "listener_ok":listener_result.as_ref().map(|result|result.is_ok()),
                "service_attempted":service_result.is_some(),
                "service_ok":service_result.as_ref().map(|result|result.is_ok()),
                "router_attempted":true,"router_ok":router_result.is_ok()})
        );
        if listener_result
            .as_ref()
            .is_some_and(|result| result.is_err())
            || service_result
                .as_ref()
                .is_some_and(|result| result.is_err())
            || router_result.is_err()
        {
            return Err("probe owner shutdown failed after all available attempts".into());
        }
        Ok(match listener_result {
            Some(Ok(report)) => Some(report),
            _ => None,
        })
    }

    pub(super) async fn run() -> Result<()> {
        let mut arguments = std::env::args_os().skip(1);
        let configuration = arguments
            .next()
            .ok_or("usage: oidc_live_probe CONFIG.json")?;
        if arguments.next().is_some() {
            return Err("exactly one probe configuration argument".into());
        }
        let config: Value = serde_json::from_slice(&read(Path::new(&configuration), 16 * 1024)?)?;
        let state = PathBuf::from(string(&config, "state_dir")?);
        let stop = PathBuf::from(string(&config, "stop_file")?);
        if stop.exists() {
            return Err("fresh explicit stop-file protocol".into());
        }
        let port = u16::try_from(number(&config, "broker_port", 0, 1, 65535)?)?;
        let issuer = string(&config, "issuer")?;
        let origin = string(&config, "jwks_origin")?;
        let mode = string(&config, "profile")?;
        let advertised: &'static [ApiVersion] = match mode.as_str() {
            "metadata" => &OIDC_METADATA_API_VERSIONS,
            "read-write" => &READ_WRITE_APIS,
            _ => return Err("explicit metadata or read-write probe profile".into()),
        };
        let lifetime = Duration::from_secs(number(&config, "maximum_runtime_secs", 240, 1, 300)?);
        // Zero retains the existing compatibility profile. Positive values
        // explicitly install actual renewable authentication with enforced expiry.
        let session_lifetime_ms = number(&config, "session_lifetime_ms", 0, 0, 86_400_000)?;
        let limits = oidc::Limits {
            authority_freshness: milliseconds(&config, "key_freshness_ms", 3000, 900_000)?,
            clock_skew: Duration::ZERO,
            ..oidc::Limits::default()
        };
        let policy = Policy {
            issuer,
            audiences: vec![string(&config, "audience")?],
            algorithms: vec![Algorithm::Rs256, Algorithm::Es256],
            access_token: AccessToken::AtJwt,
            limits,
        };
        let runtime = oidc::RuntimeLimits {
            active_leases: 64,
            validations: 8,
            refresh_interval: milliseconds(&config, "key_refresh_ms", 200, 450_000)?,
            unknown_kid_cooldown: Duration::from_secs(1),
            revocation_lease: milliseconds(&config, "revocation_lease_ms", 500, 5000)?,
            revocation_refresh: milliseconds(&config, "revocation_refresh_ms", 100, 2500)?,
        };
        let authority_config = oidc::Config {
            policy,
            keys: KeySource::Discovery {
                allowed_jwks_origins: vec![origin],
            },
            introspection: Introspection {
                endpoint: string(&config, "introspection_endpoint")?,
                client_id: string(&config, "client_id")?,
                client_secret: Secret::new(read(
                    Path::new(&string(&config, "client_secret_file")?),
                    4096,
                )?),
            },
            runtime,
        };
        let trust = HttpsTrust {
            roots: vec![read(
                Path::new(&string(&config, "issuer_ca_der_file")?),
                16 * 1024,
            )?],
            limits: oidc::HttpLimits {
                timeout: milliseconds(&config, "http_timeout_ms", 1000, 30_000)?,
                ..oidc::HttpLimits::default()
            },
        };
        let acceptor = Acceptor::new(
            vec![read(
                Path::new(&string(&config, "broker_certificate_der_file")?),
                16 * 1024,
            )?],
            read(
                Path::new(&string(&config, "broker_key_der_file")?),
                16 * 1024,
            )?,
            ClientAuth::ServerOnly,
            tls::Limits::default(),
        )?;
        let address = format!("127.0.0.1:{port}").parse()?;
        let transport_config = transport::Config::new(
            64,
            8,
            1024 * 1024,
            1024 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )?;
        std::fs::create_dir_all(&state)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700))?;
        }
        let router_config = metadata::Config {
            catalog_limits: catalog::Limits::new(
                4,
                16,
                1,
                4,
                64,
                1024 * 1024,
                journal::Limits::new(1024, 1024 * 1024, 64, 4096)?,
            )?,
            protocol_limits: protocol::Limits::new(1024 * 1024, 32)?,
            max_queued_requests: 8,
            max_response_bytes: 1024 * 1024,
            ..metadata::Config::new(0, "localhost".into(), port, "oidc-live-probe".into())
        };
        let catalog_path = state.join("catalog.journal");
        let seed_path = catalog_path.clone();
        let catalog_limits = router_config.catalog_limits;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let (mut catalog, _) = Catalog::open(seed_path, catalog_limits)?;
            if catalog.by_name("oidc-probe").is_none() {
                let mut id = [0; 16];
                *id.last_mut().ok_or("topic identity")? = 2;
                let _ = catalog.create("oidc-probe", TopicId::new(id)?, 1)?;
            }
            Ok(())
        })
        .await??;
        let read_store = if mode == "read-write" {
            let mut store = produce::Config::new(state.join("partitions"));
            store.journal_limits =
                journal::Limits::new(1024 * 1024, 16 * 1024 * 1024, 4096, 2 * 1024 * 1024)?;
            store.max_stores = 4;
            store.max_disk_bytes = 64 * 1024 * 1024;
            store.max_index_bytes = 4 * 4096 * 64;
            store.max_partitions = 4;
            store.max_normalized_bytes = 2 * 1024 * 1024;
            Some((
                store,
                fetch::Limits::new(2 * 1024 * 1024, 4096, 1000)?
                    .with_retained_bytes(32 * 1024 * 1024)?,
            ))
        } else {
            None
        };
        let router_result = match read_store {
            Some((store, reads)) => {
                metadata::Router::open_with_read_store(catalog_path, router_config, store, reads)
                    .await
            }
            None => metadata::Router::open(catalog_path, router_config).await,
        };
        let (router, recovery) = match router_result {
            Ok(value) => value,
            Err(_) => {
                println!(
                    "{}",
                    json!({"event":"startup-failed","stage":"router",
                    "returned_owned_handles":false})
                );
                return Err("probe router startup failed".into());
            }
        };
        let router = Arc::new(router);
        let service = match oidc::Service::start(authority_config, trust).await {
            Ok(service) => service,
            Err(_) => {
                let _ = shutdown("startup-service", None, None, &router).await;
                return Err("probe service startup failed after cleanup attempts".into());
            }
        };
        let audited = Arc::new(AuditedRouter {
            router: router.clone(),
            dispatched: AtomicUsize::new(0),
        });
        let profile_result = Profile::oidc_tls(service.clone(), session::Limits::default())
            .and_then(|profile| profile.with_advertised(advertised))
            .and_then(|profile| {
                if session_lifetime_ms == 0 {
                    Ok(profile)
                } else {
                    profile.with_reauthentication(Duration::from_millis(session_lifetime_ms))
                }
            });
        let profile = match profile_result {
            Ok(profile) => profile,
            Err(_) => {
                let _ = shutdown("startup-profile", None, Some(&service), &router).await;
                return Err("probe profile startup failed after cleanup attempts".into());
            }
        };
        let mut server = match Transport::bind_tls_sasl(
            address,
            transport_config,
            audited.clone(),
            acceptor,
            profile,
        )
        .await
        {
            Ok(server) => server,
            Err(_) => {
                let _ = shutdown("startup-bind", None, Some(&service), &router).await;
                return Err("probe listener startup failed after cleanup attempts".into());
            }
        };
        println!(
            "{}",
            json!({"event":"ready","broker":"localhost:".to_owned()+&port.to_string(),
            "profile":mode,"api_keys":advertised.iter().map(|api|api.api_key).collect::<Vec<_>>(),
            "recovered_entries":recovery.recovered_entries,"maximum_runtime_secs":lifetime.as_secs(),
            "session_lifetime_ms":session_lifetime_ms,"topic":"oidc-probe"})
        );
        let deadline = tokio::time::Instant::now() + lifetime;
        while !stop.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let report = shutdown("served", Some(&mut server), Some(&service), &router)
            .await?
            .ok_or("probe listener shutdown report missing")?;
        if report.accepted_connections != report.joined_connections || report.worker_failures != 0 {
            return Err("probe connections did not join cleanly".into());
        }
        println!(
            "{}",
            json!({"event":"joined","accepted":report.accepted_connections,
            "joined":report.joined_connections,"worker_failures":report.worker_failures,
            "application_dispatches":audited.dispatched.load(Ordering::Relaxed),
            "handler_errors":report.handler_errors,"read_deadlines":report.read_deadlines,
            "tls_handshake_errors":report.tls_handshake_errors,
            "stop_file":stop.exists(),"absolute_lifetime_reached":tokio::time::Instant::now()>=deadline})
        );
        Ok(())
    }
}
