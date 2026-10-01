//! Owned loopback fixture for cache/coalescing and cancellation histories.
#![cfg(feature = "registry")]
use partitionline_schema::registry::{
    RegistryAuth, RegistryCacheConfig, RegistryClient, RegistryClientConfig, RegistryError,
    SchemaReference, SchemaVersion,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone)]
struct Reply {
    status: u16,
    body: String,
    gate: Option<Arc<Semaphore>>,
}
impl Reply {
    fn schema(body: &str) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            gate: None,
        }
    }
}
struct Server {
    url: String,
    routes: Arc<Mutex<HashMap<String, Reply>>>,
    paths: Arc<Mutex<Vec<String>>>,
    active: Arc<AtomicUsize>,
    task: Option<JoinHandle<()>>,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let routes: Arc<Mutex<HashMap<String, Reply>>> = Arc::default();
        let paths = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn({
            let routes = routes.clone();
            let paths = paths.clone();
            let active = active.clone();
            async move {
                let mut children = JoinSet::new();
                loop {
                    tokio::select! {
                        incoming = listener.accept() => {
                            let Ok((mut stream, _)) = incoming else { break; };
                            let routes = routes.clone(); let paths = paths.clone();
                            active.fetch_add(1, Ordering::SeqCst);
                            let guard = Active(active.clone());
                            children.spawn(async move {
                                let _guard = guard;
                                let mut bytes = Vec::new(); let mut chunk = [0u8; 1024];
                                while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                                    if bytes.len() > 65_536 { return; }
                                    let Ok(n) = stream.read(&mut chunk).await else { return; };
                                    if n == 0 { return; }
                                    bytes.extend_from_slice(&chunk[..n]);
                                }
                                let path = String::from_utf8_lossy(&bytes).split_whitespace().nth(1).unwrap().to_string();
                                paths.lock().await.push(path.clone());
                                let reply = routes.lock().await.get(&path).cloned()
                                    .unwrap_or_else(|| Reply::schema(r#"{"schema":"default"}"#));
                                if let Some(gate) = reply.gate {
                                    // A dropped lookup closes TCP even while the peer is stalled.
                                    let mut eof = [0u8; 1];
                                    tokio::select! {
                                        permit = gate.acquire() => match permit { Ok(permit) => permit.forget(), Err(_) => return },
                                        _ = stream.read(&mut eof) => return,
                                    }
                                }
                                let response = format!("HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",reply.status,reply.body.len(),reply.body);
                                drop(stream.write_all(response.as_bytes()).await);
                                drop(stream.shutdown().await);
                            });
                        }
                        _ = children.join_next(), if !children.is_empty() => {}
                    }
                }
            }
        });
        Self {
            url,
            routes,
            paths,
            active,
            task: Some(task),
        }
    }
    async fn route(&self, path: &str, reply: Reply) {
        self.routes.lock().await.insert(path.to_string(), reply);
    }
    async fn hits(&self, path: &str) -> usize {
        self.paths
            .lock()
            .await
            .iter()
            .filter(|p| p.as_str() == path)
            .count()
    }
    async fn wait_hits(&self, path: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.hits(path).await < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            drop(task.await);
        }
        for _ in 0..100 {
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
async fn same_key_concurrent_lookups_share_one_request() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    server
        .route(
            "/schemas/ids/7",
            Reply {
                gate: Some(gate.clone()),
                ..Reply::schema(r#"{"schema":"seven"}"#)
            },
        )
        .await;
    let client = RegistryClient::new(RegistryClientConfig::new(server.url.clone())).unwrap();
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    server.wait_hits("/schemas/ids/7", 1).await;
    let mut followers = Vec::new();
    for _ in 0..32 {
        followers.push(tokio::spawn({
            let client = client.clone();
            async move { client.get_schema_by_id(7).await }
        }));
    }
    wait_followers(&client, 32).await;
    assert_eq!(
        server.hits("/schemas/ids/7").await,
        1,
        "same-key requests must coalesce"
    );
    gate.add_permits(10);
    assert_eq!(first.await.unwrap().unwrap().schema, "seven");
    for follower in followers {
        assert_eq!(follower.await.unwrap().unwrap().schema, "seven");
    }
    assert_eq!(client.get_schema_by_id(7).await.unwrap().schema, "seven");
    assert_eq!(
        server.hits("/schemas/ids/7").await,
        1,
        "completed result must cache"
    );
    server.shutdown().await;
}

async fn wait_followers(client: &RegistryClient, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.cache_stats().await.coalesced_waiters != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn client(server: &Server, cache: RegistryCacheConfig) -> RegistryClient {
    RegistryClient::new(
        RegistryClientConfig::new(server.url.clone())
            .max_attempts(1)
            .cache(cache),
    )
    .unwrap()
}
async fn advance_cache_clock(duration: Duration) {
    // Advance only after requests settle, then resume before the next TCP I/O.
    tokio::time::pause();
    tokio::time::advance(duration).await;
    tokio::time::resume();
}
fn versioned(subject: &str, version: u32) -> Reply {
    Reply::schema(&format!(
        r#"{{"subject":"{subject}","version":{version},"id":{version},"schema":"v{version}"}}"#
    ))
}

#[tokio::test]
async fn unrelated_key_progresses_while_first_peer_is_stalled() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    server
        .route(
            "/schemas/ids/1",
            Reply {
                gate: Some(gate.clone()),
                ..Reply::schema(r#"{"schema":"slow"}"#)
            },
        )
        .await;
    let client = client(&server, RegistryCacheConfig::default().limits(2, 4096, 2));
    let slow = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(1).await }
    });
    server.wait_hits("/schemas/ids/1", 1).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.get_schema_by_id(2))
            .await
            .unwrap()
            .unwrap()
            .schema,
        "default"
    );
    assert_eq!(client.cache_stats().await.in_flight, 1);
    gate.add_permits(1);
    slow.await.unwrap().unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn dropped_owner_wakes_follower_for_takeover() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    server
        .route(
            "/schemas/ids/7",
            Reply {
                gate: Some(gate.clone()),
                ..Reply::schema(r#"{"schema":"takeover"}"#)
            },
        )
        .await;
    let client = client(&server, RegistryCacheConfig::default());
    let owner = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    server.wait_hits("/schemas/ids/7", 1).await;
    let follower = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    wait_followers(&client, 1).await;
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    server.wait_hits("/schemas/ids/7", 2).await;
    gate.add_permits(2);
    assert_eq!(follower.await.unwrap().unwrap().schema, "takeover");
    assert_eq!(client.cache_stats().await.in_flight, 0);
    assert_eq!(client.get_schema_by_id(7).await.unwrap().schema, "takeover");
    assert_eq!(server.hits("/schemas/ids/7").await, 2);
    server.shutdown().await;
}

#[tokio::test]
async fn dropped_follower_keeps_owner_request_alive() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    server
        .route(
            "/schemas/ids/7",
            Reply {
                gate: Some(gate.clone()),
                ..Reply::schema(r#"{"schema":"owner"}"#)
            },
        )
        .await;
    let client = client(&server, RegistryCacheConfig::default());
    let owner = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    server.wait_hits("/schemas/ids/7", 1).await;
    let follower = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    wait_followers(&client, 1).await;
    follower.abort();
    assert!(follower.await.unwrap_err().is_cancelled());
    wait_followers(&client, 0).await;
    assert_eq!(client.cache_stats().await.in_flight, 1);
    gate.add_permits(1);
    assert_eq!(owner.await.unwrap().unwrap().schema, "owner");
    assert_eq!(server.hits("/schemas/ids/7").await, 1);
    server.shutdown().await;
}

#[tokio::test]
async fn latest_freshness_is_separate_from_pinned_versions() {
    let server = Server::start().await;
    server
        .route("/subjects/s/versions/latest", versioned("s", 1))
        .await;
    server
        .route("/subjects/s/versions/1", versioned("s", 1))
        .await;
    let client = client(
        &server,
        RegistryCacheConfig::default().freshness(
            Duration::from_secs(60),
            Duration::from_millis(30),
            Duration::from_millis(30),
        ),
    );
    assert_eq!(
        client
            .get_schema_by_subject("s", SchemaVersion::Latest)
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(
        client
            .get_schema_by_subject("s", SchemaVersion::Pinned(1))
            .await
            .unwrap()
            .version,
        1
    );
    server
        .route("/subjects/s/versions/latest", versioned("s", 2))
        .await;
    assert_eq!(
        client
            .get_schema_by_subject("s", SchemaVersion::Latest)
            .await
            .unwrap()
            .version,
        1
    );
    advance_cache_clock(Duration::from_millis(31)).await;
    assert_eq!(
        client
            .get_schema_by_subject("s", SchemaVersion::Latest)
            .await
            .unwrap()
            .version,
        2
    );
    assert_eq!(
        client
            .get_schema_by_subject("s", SchemaVersion::Pinned(1))
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(server.hits("/subjects/s/versions/latest").await, 2);
    assert_eq!(server.hits("/subjects/s/versions/1").await, 1);
    server.shutdown().await;
}

#[tokio::test]
async fn negative_cache_preserves_error_and_expires() {
    let server = Server::start().await;
    server
        .route(
            "/schemas/ids/9",
            Reply {
                status: 404,
                body: r#"{"error_code":40403}"#.into(),
                gate: None,
            },
        )
        .await;
    let client = client(
        &server,
        RegistryCacheConfig::default().freshness(
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_millis(30),
        ),
    );
    let error = client.get_schema_by_id(9).await.unwrap_err();
    assert!(matches!(
        error,
        RegistryError::NotFound {
            error_code: Some(40403),
            ..
        }
    ));
    server
        .route(
            "/schemas/ids/9",
            Reply::schema(r#"{"schema":"now-exists"}"#),
        )
        .await;
    assert_eq!(client.get_schema_by_id(9).await.unwrap_err(), error);
    assert_eq!(server.hits("/schemas/ids/9").await, 1);
    advance_cache_clock(Duration::from_millis(31)).await;
    assert_eq!(
        client.get_schema_by_id(9).await.unwrap().schema,
        "now-exists"
    );
    assert_eq!(server.hits("/schemas/ids/9").await, 2);
    server.shutdown().await;
}

#[tokio::test]
async fn non_404_failures_and_malformed_or_oversize_data_never_cache() {
    let server = Server::start().await;
    let client = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone())
            .max_attempts(1)
            .max_body_bytes(1024),
    )
    .unwrap();
    for (i, (status, body)) in [
        (401, "{}".to_string()),
        (403, "{}".into()),
        (429, "{}".into()),
        (503, "{}".into()),
        (200, r#"{"schema":5}"#.into()),
        (200, "x".repeat(2048)),
    ]
    .into_iter()
    .enumerate()
    {
        let id = i as u32;
        let path = format!("/schemas/ids/{id}");
        server
            .route(
                &path,
                Reply {
                    status,
                    body,
                    gate: None,
                },
            )
            .await;
        assert!(client.get_schema_by_id(id).await.is_err());
        server
            .route(&path, Reply::schema(r#"{"schema":"recovered"}"#))
            .await;
        assert_eq!(
            client.get_schema_by_id(id).await.unwrap().schema,
            "recovered"
        );
        assert_eq!(
            client.get_schema_by_id(id).await.unwrap().schema,
            "recovered"
        );
        assert_eq!(server.hits(&path).await, 2);
    }
    server.shutdown().await;
}

#[tokio::test]
async fn coalesced_failure_is_an_error_for_every_waiter() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    server
        .route(
            "/schemas/ids/7",
            Reply {
                status: 403,
                body: "{}".into(),
                gate: Some(gate.clone()),
            },
        )
        .await;
    let client = client(&server, RegistryCacheConfig::default());
    let owner = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    server.wait_hits("/schemas/ids/7", 1).await;
    let follower = tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(7).await }
    });
    wait_followers(&client, 1).await;
    gate.add_permits(1);
    assert_eq!(owner.await.unwrap().unwrap_err(), RegistryError::Forbidden);
    assert_eq!(
        follower.await.unwrap().unwrap_err(),
        RegistryError::Forbidden
    );
    assert_eq!(client.cache_stats().await.entries, 0);
    assert_eq!(server.hits("/schemas/ids/7").await, 1);
    server.shutdown().await;
}

#[tokio::test]
async fn entry_lru_keeps_recent_hit_and_evicts_oldest() {
    let server = Server::start().await;
    let client = client(&server, RegistryCacheConfig::default().limits(2, 4096, 2));
    client.get_schema_by_id(1).await.unwrap();
    client.get_schema_by_id(2).await.unwrap();
    client.get_schema_by_id(1).await.unwrap();
    client.get_schema_by_id(3).await.unwrap();
    assert_eq!(client.cache_stats().await.entries, 2);
    assert!(client.cache_stats().await.retained_bytes <= 4096);
    client.get_schema_by_id(1).await.unwrap();
    assert_eq!(server.hits("/schemas/ids/1").await, 1);
    client.get_schema_by_id(2).await.unwrap();
    assert_eq!(server.hits("/schemas/ids/2").await, 2);
    server.shutdown().await;
}

#[tokio::test]
async fn byte_budget_evicts_and_oversized_success_bypasses_cache() {
    let server = Server::start().await;
    let client = client(&server, RegistryCacheConfig::default().limits(10, 1000, 2));
    for id in 1..=3 {
        server
            .route(
                &format!("/schemas/ids/{id}"),
                Reply::schema(&format!(r#"{{"schema":"{}"}}"#, "x".repeat(200))),
            )
            .await;
        client.get_schema_by_id(id).await.unwrap();
        assert!(client.cache_stats().await.retained_bytes <= 1000);
    }
    let before = client.cache_stats().await;
    assert!(before.entries < 3);
    server
        .route(
            "/schemas/ids/99",
            Reply::schema(&format!(r#"{{"schema":"{}"}}"#, "x".repeat(2000))),
        )
        .await;
    for _ in 0..2 {
        assert_eq!(
            client.get_schema_by_id(99).await.unwrap().schema.len(),
            2000
        );
    }
    assert_eq!(server.hits("/schemas/ids/99").await, 2);
    assert_eq!(client.cache_stats().await.entries, before.entries);
    assert!(client.cache_stats().await.retained_bytes <= 1000);
    server.shutdown().await;
}

#[tokio::test]
async fn disabled_or_zero_freshness_keeps_completed_results_uncached() {
    let server = Server::start().await;
    for config in [
        RegistryCacheConfig::disabled(),
        RegistryCacheConfig::default().freshness(Duration::ZERO, Duration::ZERO, Duration::ZERO),
    ] {
        let client = client(&server, config);
        for _ in 0..2 {
            client.get_schema_by_id(1).await.unwrap();
        }
        assert_eq!(client.cache_stats().await.entries, 0);
        assert_eq!(client.cache_stats().await.retained_bytes, 0);
    }
    assert_eq!(server.hits("/schemas/ids/1").await, 4);
    server.shutdown().await;
}

#[tokio::test]
async fn active_key_budget_and_request_deadlines_release_every_slot() {
    let server = Server::start().await;
    let gate = Arc::new(Semaphore::new(0));
    for id in 1..=3 {
        server
            .route(
                &format!("/schemas/ids/{id}"),
                Reply {
                    gate: Some(gate.clone()),
                    ..Reply::schema(r#"{"schema":"stalled"}"#)
                },
            )
            .await;
    }
    let client = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone())
            .max_attempts(1)
            .request_timeout(Duration::from_millis(500))
            .cache(RegistryCacheConfig::default().limits(2, 4096, 2)),
    )
    .unwrap();
    let mut calls = Vec::new();
    for id in 1..=2 {
        calls.push(tokio::spawn({
            let client = client.clone();
            async move { client.get_schema_by_id(id).await }
        }));
    }
    server.wait_hits("/schemas/ids/1", 1).await;
    server.wait_hits("/schemas/ids/2", 1).await;
    assert_eq!(client.cache_stats().await.in_flight, 2);
    calls.push(tokio::spawn({
        let client = client.clone();
        async move { client.get_schema_by_id(3).await }
    }));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.hits("/schemas/ids/3").await, 0);
    assert_eq!(client.cache_stats().await.in_flight, 2);
    for call in calls {
        assert_eq!(call.await.unwrap().unwrap_err(), RegistryError::Timeout);
    }
    assert_eq!(client.cache_stats().await.in_flight, 0);
    assert_eq!(client.cache_stats().await.entries, 0);
    server.shutdown().await;
}

#[tokio::test]
async fn subject_and_reference_limits_fail_before_io_or_cache_admission() {
    let server = Server::start().await;
    let client = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone())
            .max_references(1)
            .cache(RegistryCacheConfig::default().subject_limit(8)),
    )
    .unwrap();
    let error = client
        .get_schema_by_subject("SECRET_SUBJECT", SchemaVersion::Latest)
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::InvalidConfig(_)));
    assert!(!format!("{error:?}").contains("SECRET_SUBJECT"));
    assert!(server.paths.lock().await.is_empty());
    let references = vec![
        SchemaReference {
            name: "n".into(),
            subject: "s".into(),
            version: 1
        };
        2
    ];
    assert!(matches!(
        client.fetch_references(&references).await,
        Err(RegistryError::Malformed { .. })
    ));
    assert!(server.paths.lock().await.is_empty());
    server.route("/schemas/ids/1",Reply::schema(r#"{"schema":"{}","references":[{"name":"a","subject":"s","version":1},{"name":"b","subject":"s","version":1}]}"#)).await;
    for _ in 0..2 {
        assert!(matches!(
            client.get_schema_by_id(1).await,
            Err(RegistryError::Malformed { .. })
        ));
    }
    assert_eq!(server.hits("/schemas/ids/1").await, 2);
    assert_eq!(client.cache_stats().await.entries, 0);
    server.shutdown().await;
}

#[tokio::test]
async fn cyclic_declarations_are_one_level_and_duplicates_keep_input_order() {
    let server = Server::start().await;
    server.route("/subjects/self/versions/1",Reply::schema(r#"{"subject":"self","version":1,"id":1,"schema":"{}","references":[{"name":"self","subject":"self","version":1}]}"#)).await;
    let client = client(&server, RegistryCacheConfig::default());
    let reference = SchemaReference {
        name: "self".into(),
        subject: "self".into(),
        version: 1,
    };
    let schemas = client
        .fetch_references(&[reference.clone(), reference])
        .await
        .unwrap();
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0], schemas[1]);
    assert_eq!(schemas[0].references.len(), 1);
    assert_eq!(server.hits("/subjects/self/versions/1").await, 1);
    server.shutdown().await;
}

#[tokio::test]
async fn references_share_one_overall_deadline() {
    let server = Server::start().await;
    let a = Arc::new(Semaphore::new(0));
    let b = Arc::new(Semaphore::new(0));
    server
        .route(
            "/subjects/a/versions/1",
            Reply {
                gate: Some(a.clone()),
                ..versioned("a", 1)
            },
        )
        .await;
    server
        .route(
            "/subjects/b/versions/1",
            Reply {
                gate: Some(b.clone()),
                ..versioned("b", 1)
            },
        )
        .await;
    let client = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone()).request_timeout(Duration::from_millis(200)),
    )
    .unwrap();
    let batch = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .fetch_references(&[
                    SchemaReference {
                        name: "a".into(),
                        subject: "a".into(),
                        version: 1,
                    },
                    SchemaReference {
                        name: "b".into(),
                        subject: "b".into(),
                        version: 1,
                    },
                ])
                .await
        }
    });
    server.wait_hits("/subjects/a/versions/1", 1).await;
    advance_cache_clock(Duration::from_millis(120)).await;
    a.add_permits(1);
    server.wait_hits("/subjects/b/versions/1", 1).await;
    advance_cache_clock(Duration::from_millis(90)).await;
    assert_eq!(batch.await.unwrap().unwrap_err(), RegistryError::Timeout);
    assert_eq!(client.cache_stats().await.in_flight, 0);
    assert_eq!(client.cache_stats().await.entries, 1);
    b.add_permits(2);
    client
        .get_schema_by_subject("b", SchemaVersion::Pinned(1))
        .await
        .unwrap();
    assert_eq!(server.hits("/subjects/b/versions/1").await, 2);
    server.shutdown().await;
}

#[tokio::test]
async fn caches_are_per_client_credentials_and_diagnostics_omit_subjects() {
    let server = Server::start().await;
    server
        .route("/schemas/ids/1", Reply::schema(r#"{"schema":"tenant-a"}"#))
        .await;
    let first = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone()).auth(RegistryAuth::bearer("FIRST_SECRET")),
    )
    .unwrap();
    assert_eq!(first.get_schema_by_id(1).await.unwrap().schema, "tenant-a");
    server
        .route("/schemas/ids/1", Reply::schema(r#"{"schema":"tenant-b"}"#))
        .await;
    let second = RegistryClient::new(
        RegistryClientConfig::new(server.url.clone()).auth(RegistryAuth::bearer("SECOND_SECRET")),
    )
    .unwrap();
    assert_eq!(second.get_schema_by_id(1).await.unwrap().schema, "tenant-b");
    assert_eq!(first.get_schema_by_id(1).await.unwrap().schema, "tenant-a");
    assert_eq!(server.hits("/schemas/ids/1").await, 2);
    server
        .route(
            "/subjects/SECRET_SUBJECT/versions/latest",
            Reply {
                status: 404,
                body: "{}".into(),
                gate: None,
            },
        )
        .await;
    let error = first
        .get_schema_by_subject("SECRET_SUBJECT", SchemaVersion::Latest)
        .await
        .unwrap_err();
    let shown = format!("{first:?} {error:?} {:?}", first.cache_stats().await);
    for secret in [
        "FIRST_SECRET",
        "SECOND_SECRET",
        "SECRET_SUBJECT",
        "tenant-a",
    ] {
        assert!(!shown.contains(secret));
    }
    server.shutdown().await;
}

#[test]
fn cache_configuration_has_explicit_hard_limits() {
    for config in [
        RegistryCacheConfig::default().limits(0, 1, 1),
        RegistryCacheConfig::default().limits(4097, 1024, 1),
        RegistryCacheConfig::default().limits(1, 0, 1),
        RegistryCacheConfig::default().limits(1, 64 * 1024 * 1024 + 1, 1),
        RegistryCacheConfig::default().limits(1, 1024, 0),
        RegistryCacheConfig::default().limits(1, 1024, 129),
        RegistryCacheConfig::default().subject_limit(0),
        RegistryCacheConfig::default().subject_limit(4097),
        RegistryCacheConfig::default().freshness(Duration::MAX, Duration::ZERO, Duration::ZERO),
    ] {
        assert!(matches!(
            RegistryClient::new(RegistryClientConfig::new("http://127.0.0.1:9").cache(config)),
            Err(RegistryError::InvalidConfig(_))
        ));
    }
}
