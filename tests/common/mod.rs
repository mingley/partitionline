//! Mock Kafka broker for integration tests.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "mock broker is test-only; wire helpers use unwrap on trusted fixtures"
)]
#![expect(
    unreachable_pub,
    unused_results,
    reason = "mod common is private to each integration test binary; mock detaches accept loops"
)]
#![expect(
    clippy::let_underscore_must_use,
    reason = "mock discards frame length prefixes"
)]

use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use partitionline::protocol::admin::{
    decode_create_topics_request, decode_delete_topics_request, decode_describe_configs_request,
    encode_create_topics_response, encode_delete_topics_response, encode_describe_configs_response,
    ConfigEntry, DescribeConfigsResult, TopicResult, CONFIG_SOURCE_DEFAULT,
    CONFIG_SOURCE_DYNAMIC_TOPIC, RESOURCE_BROKER, RESOURCE_TOPIC,
};
use partitionline::protocol::api::{
    decode_produce_request, encode_api_versions_response, encode_metadata_response,
    encode_produce_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
    PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    API_VERSIONS, CREATE_TOPICS, DELETE_TOPICS, DESCRIBE_CONFIGS, FETCH, FIND_COORDINATOR,
    HEARTBEAT, INIT_PRODUCER_ID, JOIN_GROUP, METADATA, OFFSET_COMMIT, OFFSET_FETCH, PRODUCE,
    SASL_AUTHENTICATE, SASL_HANDSHAKE, SYNC_GROUP,
};
use partitionline::protocol::fetch::{
    decode_fetch_request, encode_fetch_response, FetchedPartition, FetchedTopic,
};
use partitionline::protocol::group::{
    decode_heartbeat_request, decode_join_group_request, decode_offset_commit_request,
    decode_offset_fetch_request, decode_sync_group_request, encode_find_coordinator_response,
    encode_heartbeat_response, encode_join_group_response, encode_offset_commit_response,
    encode_offset_fetch_response, encode_sync_group_response, JoinMember,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::idem::encode_init_producer_id_response;
use partitionline::protocol::oauth;
use partitionline::protocol::records::{Record, RecordBatch};
use partitionline::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use partitionline::protocol::scram;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct Mock {
    pub addr: String,
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
struct CreatedTopic {
    num_partitions: i32,
    configs: HashMap<String, Option<String>>,
}

struct State {
    log: HashMap<(String, i32), Vec<Record>>,
    next_offset: HashMap<(String, i32), i64>,
    assignments: HashMap<String, Vec<u8>>,
    committed: HashMap<(String, i32), i64>,
    member_seq: u32,
    sasl_user: Option<(String, String)>,
    scram_user: Option<(scram::ScramAlg, String, String)>,
    oauth_principal: Option<String>,
    next_pid: i64,
    last_producer_id: Option<i64>,
    expected_seq: HashMap<(i64, String, i32), i32>,
    produce_error: Option<i16>,
    log_start: HashMap<(String, i32), i64>,
    created_topics: HashMap<String, CreatedTopic>,
}

fn new_state(
    sasl_user: Option<(String, String)>,
    scram_user: Option<(scram::ScramAlg, String, String)>,
    oauth_principal: Option<String>,
) -> State {
    let mut created_topics = HashMap::new();
    created_topics.insert(
        "t".into(),
        CreatedTopic {
            num_partitions: 1,
            configs: HashMap::new(),
        },
    );
    State {
        log: HashMap::new(),
        next_offset: HashMap::new(),
        assignments: HashMap::new(),
        committed: HashMap::new(),
        member_seq: 0,
        sasl_user,
        scram_user,
        oauth_principal,
        next_pid: 1000,
        last_producer_id: None,
        expected_seq: HashMap::new(),
        produce_error: None,
        log_start: HashMap::new(),
        created_topics,
    }
}

fn metadata_for(host: &str, port: i32, topics: &HashMap<String, CreatedTopic>) -> MetadataResponse {
    MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![Broker {
            node_id: 1,
            host: host.to_string(),
            port,
            rack: None,
        }],
        cluster_id: Some("mock".into()),
        controller_id: 1,
        topics: topics
            .iter()
            .map(|(name, spec)| TopicMetadata {
                error_code: 0,
                name: Some(name.clone()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: (0..spec.num_partitions)
                    .map(|i| PartitionMetadata {
                        error_code: 0,
                        partition_index: i,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                    })
                    .collect(),
            })
            .collect(),
    }
}

impl Mock {
    pub async fn start() -> Self {
        Self::start_with_sasl(None).await
    }

    pub async fn start_with_sasl(creds: Option<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(new_state(creds, None, None)));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                stream.set_nodelay(true).ok();
                let st = st.clone();
                let host = host.clone();
                tokio::spawn(handle_conn(stream, host, port, st));
            }
        });
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_scram(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(new_state(
            None,
            Some((scram::ScramAlg::Sha256, creds.0, creds.1)),
            None,
        )));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                stream.set_nodelay(true).ok();
                let st = st.clone();
                let host = host.clone();
                tokio::spawn(handle_conn(stream, host, port, st));
            }
        });
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_scram_sha512(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(new_state(
            None,
            Some((scram::ScramAlg::Sha512, creds.0, creds.1)),
            None,
        )));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                stream.set_nodelay(true).ok();
                let st = st.clone();
                let host = host.clone();
                tokio::spawn(handle_conn(stream, host, port, st));
            }
        });
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_oauthbearer(principal: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(new_state(None, None, Some(principal))));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                stream.set_nodelay(true).ok();
                let st = st.clone();
                let host = host.clone();
                tokio::spawn(handle_conn(stream, host, port, st));
            }
        });
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_tls() -> (Self, partitionline::TlsConfig) {
        partitionline::net::install_crypto_provider();
        let (server, ca_pem) = tls_server_identity();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(new_state(None, None, None)));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                tcp.set_nodelay(true).ok();
                let st = st.clone();
                let host = host.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(tcp).await else {
                        return;
                    };
                    handle_conn(stream, host, port, st).await;
                });
            }
        });
        let tls = partitionline::TlsConfig {
            ca_pem: Some(ca_pem),
            client_cert_pem: None,
            client_key_pem: None,
            server_name: Some("localhost".into()),
        };
        (
            Self {
                addr: format!("127.0.0.1:{}", addr.port()),
                state,
            },
            tls,
        )
    }

    pub fn last_producer_id(&self) -> Option<i64> {
        self.state.lock().last_producer_id
    }

    pub fn set_log_start(&self, topic: &str, partition: i32, offset: i64) {
        self.state
            .lock()
            .log_start
            .insert((topic.to_string(), partition), offset);
    }

    pub fn log_len(&self, topic: &str, partition: i32) -> usize {
        self.state
            .lock()
            .log
            .get(&(topic.to_string(), partition))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn set_produce_error(&self, code: i16) {
        self.state.lock().produce_error = Some(code);
    }
}

fn tls_server_identity() -> (rustls::ServerConfig, Vec<u8>) {
    let pair = rcgen::generate_simple_self_signed(["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen");
    let ca_pem = pair.cert.pem().into_bytes();
    let cert_der = rustls::pki_types::CertificateDer::from(pair.cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(pair.key_pair.serialize_der()),
    );
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("tls server config");
    (server, ca_pem)
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<BytesMut> {
    loop {
        if buf.len() >= 4 {
            let size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            let total = 4 + size as usize;
            if buf.len() >= total {
                let mut frame = buf.split_to(total);
                let _ = frame.split_to(4);
                return Ok(frame);
            }
        }
        let n = stream.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
    }
}

async fn write_frame<S: AsyncWrite + Unpin>(stream: &mut S, payload: &[u8]) -> std::io::Result<()> {
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_i32(payload.len() as i32);
    out.extend_from_slice(payload);
    stream.write_all(&out).await
}

fn versions() -> ApiVersionsResponse {
    let keys = [
        (PRODUCE, 3, 9),
        (FETCH, 4, 11),
        (METADATA, 1, 12),
        (OFFSET_COMMIT, 2, 7),
        (OFFSET_FETCH, 1, 5),
        (FIND_COORDINATOR, 0, 2),
        (JOIN_GROUP, 0, 5),
        (HEARTBEAT, 0, 3),
        (SYNC_GROUP, 0, 3),
        (SASL_HANDSHAKE, 0, 1),
        (API_VERSIONS, 0, 4),
        (CREATE_TOPICS, 0, 4),
        (DELETE_TOPICS, 0, 3),
        (INIT_PRODUCER_ID, 0, 4),
        (DESCRIBE_CONFIGS, 0, 1),
        (SASL_AUTHENTICATE, 0, 1),
    ];
    ApiVersionsResponse {
        error_code: 0,
        api_keys: keys
            .into_iter()
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
            })
            .collect(),
        throttle_time_ms: 0,
    }
}

async fn handle_conn<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    host: String,
    port: i32,
    state: Arc<Mutex<State>>,
) {
    let mut buf = BytesMut::new();
    let mut authed = {
        let st = state.lock();
        st.sasl_user.is_none() && st.scram_user.is_none() && st.oauth_principal.is_none()
    };
    let mut scram_step: Option<(scram::ScramAlg, String, String, String)> = None;
    loop {
        let mut frame = match read_frame(&mut stream, &mut buf).await {
            Ok(f) => f,
            Err(_) => break,
        };
        let header = match decode_request_header(&mut frame) {
            Ok(h) => h,
            Err(_) => break,
        };
        if !authed
            && !matches!(
                header.api_key,
                API_VERSIONS | SASL_HANDSHAKE | SASL_AUTHENTICATE
            )
        {
            break;
        }
        let mut body = BytesMut::new();
        encode_response_header(
            &mut body,
            header.api_key,
            header.api_version,
            header.correlation_id,
        )
        .unwrap();
        match header.api_key {
            API_VERSIONS => {
                encode_api_versions_response(&mut body, header.api_version, &versions()).unwrap()
            }
            METADATA => {
                let topics = state.lock().created_topics.clone();
                encode_metadata_response(
                    &mut body,
                    header.api_version,
                    &metadata_for(&host, port, &topics),
                )
                .unwrap();
            }
            CREATE_TOPICS => {
                let req = decode_create_topics_request(&mut frame, header.api_version).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                for t in req.topics {
                    if st.created_topics.contains_key(&t.name) {
                        results.push(TopicResult {
                            name: t.name,
                            error_code: 36,
                            error_message: Some("Topic already exists.".into()),
                        });
                        continue;
                    }
                    let npart = if t.assignments.is_empty() {
                        t.num_partitions
                    } else {
                        t.assignments.len() as i32
                    };
                    let mut error_code = 0i16;
                    if npart < 1 {
                        error_code = 37;
                    } else if t.replication_factor < 1 && t.assignments.is_empty() {
                        error_code = 38;
                    }
                    if error_code == 0 && !req.validate_only {
                        let mut configs = HashMap::new();
                        for c in t.configs {
                            configs.insert(c.name, c.value);
                        }
                        st.created_topics.insert(
                            t.name.clone(),
                            CreatedTopic {
                                num_partitions: npart,
                                configs,
                            },
                        );
                    }
                    results.push(TopicResult {
                        name: t.name,
                        error_code,
                        error_message: None,
                    });
                }
                encode_create_topics_response(&mut body, header.api_version, &results).unwrap();
            }
            DELETE_TOPICS => {
                let (names, _timeout) = decode_delete_topics_request(&mut frame).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                for name in names {
                    let error_code = if st.created_topics.remove(&name).is_some() {
                        0
                    } else {
                        3
                    };
                    results.push(TopicResult {
                        name,
                        error_code,
                        error_message: None,
                    });
                }
                encode_delete_topics_response(&mut body, header.api_version, &results).unwrap();
            }
            DESCRIBE_CONFIGS => {
                let (resources, _syn) =
                    decode_describe_configs_request(&mut frame, header.api_version).unwrap();
                let st = state.lock();
                let mut results = Vec::new();
                for r in resources {
                    if r.resource_type == RESOURCE_TOPIC {
                        match st.created_topics.get(&r.name) {
                            None => results.push(DescribeConfigsResult {
                                error_code: 3,
                                error_message: Some("Unknown topic.".into()),
                                resource_type: r.resource_type,
                                name: r.name,
                                entries: Vec::new(),
                            }),
                            Some(spec) => {
                                let mut entries = Vec::new();
                                let mut seen = std::collections::HashSet::new();
                                let mut push = |name: &str, value: Option<String>, source: i8| {
                                    if let Some(keys) = &r.keys {
                                        if !keys.iter().any(|k| k == name) {
                                            return;
                                        }
                                    }
                                    if seen.insert(name.to_string()) {
                                        entries.push(ConfigEntry {
                                            name: name.to_string(),
                                            value,
                                            read_only: false,
                                            source,
                                            is_sensitive: false,
                                            synonyms: Vec::new(),
                                        });
                                    }
                                };
                                push(
                                    "cleanup.policy",
                                    spec.configs
                                        .get("cleanup.policy")
                                        .cloned()
                                        .flatten()
                                        .or_else(|| Some("delete".into())),
                                    if spec.configs.contains_key("cleanup.policy") {
                                        CONFIG_SOURCE_DYNAMIC_TOPIC
                                    } else {
                                        CONFIG_SOURCE_DEFAULT
                                    },
                                );
                                for (k, v) in &spec.configs {
                                    if k == "cleanup.policy" {
                                        continue;
                                    }
                                    push(k, v.clone(), CONFIG_SOURCE_DYNAMIC_TOPIC);
                                }
                                results.push(DescribeConfigsResult {
                                    error_code: 0,
                                    error_message: None,
                                    resource_type: r.resource_type,
                                    name: r.name,
                                    entries,
                                });
                            }
                        }
                    } else if r.resource_type == RESOURCE_BROKER {
                        results.push(DescribeConfigsResult {
                            error_code: 0,
                            error_message: None,
                            resource_type: r.resource_type,
                            name: r.name,
                            entries: vec![ConfigEntry {
                                name: "log.retention.hours".into(),
                                value: Some("168".into()),
                                read_only: true,
                                source: CONFIG_SOURCE_DEFAULT,
                                is_sensitive: false,
                                synonyms: Vec::new(),
                            }],
                        });
                    } else {
                        results.push(DescribeConfigsResult {
                            error_code: 3,
                            error_message: Some("Unknown resource.".into()),
                            resource_type: r.resource_type,
                            name: r.name,
                            entries: Vec::new(),
                        });
                    }
                }
                encode_describe_configs_response(&mut body, header.api_version, &results).unwrap();
            }
            INIT_PRODUCER_ID => {
                let mut st = state.lock();
                let pid = st.next_pid;
                st.next_pid += 1;
                encode_init_producer_id_response(&mut body, header.api_version, 0, pid, 0).unwrap();
            }
            PRODUCE => {
                let decoded = decode_produce_request(&mut frame, header.api_version).unwrap();
                let mut parts = Vec::new();
                let mut st = state.lock();
                let forced = st.produce_error;
                for topic in decoded.2 {
                    for p in topic.partitions {
                        st.last_producer_id = Some(p.records.producer_id);
                        let key = (topic.topic.clone(), p.index);
                        let nrec = p.records.records.len() as i32;
                        let mut error_code = forced.unwrap_or(0);
                        if error_code == 0 {
                            let pid = p.records.producer_id;
                            let seq = p.records.base_sequence;
                            if pid >= 0 && seq >= 0 {
                                let skey = (pid, topic.topic.clone(), p.index);
                                let expected = *st.expected_seq.get(&skey).unwrap_or(&0);
                                if seq != expected {
                                    error_code = 45;
                                } else {
                                    st.expected_seq.insert(skey, expected + nrec);
                                }
                            }
                        }
                        let start = *st.next_offset.get(&key).unwrap_or(&0);
                        if error_code == 0 {
                            let mut n = 0i64;
                            for mut rec in p.records.records {
                                rec.offset = start + n;
                                st.log.entry(key.clone()).or_default().push(rec);
                                n += 1;
                            }
                            st.next_offset.insert(key, start + n);
                            parts.push(ProducePartitionResponse {
                                topic: topic.topic.clone(),
                                partition: p.index,
                                error_code: 0,
                                base_offset: start,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        } else {
                            parts.push(ProducePartitionResponse {
                                topic: topic.topic.clone(),
                                partition: p.index,
                                error_code,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        }
                    }
                }
                encode_produce_response(&mut body, header.api_version, &parts).unwrap();
            }
            FETCH => {
                let req = decode_fetch_request(&mut frame).unwrap();
                let st = state.lock();
                let mut topics = Vec::new();
                for t in req {
                    let mut parts = Vec::new();
                    for p in t.partitions {
                        let key = (t.topic.clone(), p.partition);
                        let recs = st
                            .log
                            .get(&key)
                            .map(|v| {
                                v.iter()
                                    .filter(|r| r.offset >= p.fetch_offset)
                                    .cloned()
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        let hw = *st.next_offset.get(&key).unwrap_or(&0);
                        let log_start = *st.log_start.get(&key).unwrap_or(&0);
                        let error_code = if p.fetch_offset < log_start { 1 } else { 0 };
                        let batches = if error_code != 0 || recs.is_empty() {
                            Vec::new()
                        } else {
                            let first = recs[0].offset;
                            let mut batch = RecordBatch::from_records(recs);
                            batch.base_offset = first;
                            vec![batch]
                        };
                        parts.push(FetchedPartition {
                            partition: p.partition,
                            error_code,
                            high_watermark: hw,
                            log_start_offset: log_start,
                            records: batches,
                        });
                    }
                    topics.push(FetchedTopic {
                        topic: t.topic,
                        partitions: parts,
                    });
                }
                encode_fetch_response(&mut body, &topics).unwrap();
            }
            SASL_HANDSHAKE => {
                let _mech = decode_sasl_handshake_request(&mut frame).unwrap_or_default();
                let (scram, oauth) = {
                    let st = state.lock();
                    (st.scram_user.clone(), st.oauth_principal.clone())
                };
                if let Some((alg, _, _)) = scram {
                    encode_sasl_handshake_response(&mut body, 0, &[alg.name()]).unwrap();
                } else if oauth.is_some() {
                    encode_sasl_handshake_response(&mut body, 0, &["OAUTHBEARER"]).unwrap();
                } else {
                    encode_sasl_handshake_response(&mut body, 0, &["PLAIN"]).unwrap();
                }
            }
            SASL_AUTHENTICATE => {
                let bytes = decode_sasl_authenticate_request(&mut frame).unwrap();
                let (scram_user, oauth_principal, sasl_user) = {
                    let st = state.lock();
                    (
                        st.scram_user.clone(),
                        st.oauth_principal.clone(),
                        st.sasl_user.clone(),
                    )
                };
                if let Some((alg, _, pass)) = scram_user {
                    match scram_step.take() {
                        None => {
                            let first = String::from_utf8_lossy(&bytes);
                            match scram::server_first(
                                &first,
                                "SrvNonceMock0001",
                                b"saltsalt16bytes!",
                                4096,
                            ) {
                                Ok((sf, bare)) => {
                                    scram_step = Some((alg, pass, bare, sf.clone()));
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        0,
                                        None,
                                        sf.as_bytes(),
                                    )
                                    .unwrap();
                                }
                                Err(_) => {
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        58,
                                        Some("bad scram first"),
                                        &[],
                                    )
                                    .unwrap();
                                }
                            }
                        }
                        Some((alg, pass, bare, sf)) => {
                            let cf = String::from_utf8_lossy(&bytes);
                            match scram::server_final(alg, &pass, &bare, &sf, &cf) {
                                Ok(fin) => {
                                    authed = true;
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        0,
                                        None,
                                        fin.as_bytes(),
                                    )
                                    .unwrap();
                                }
                                Err(_) => {
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        58,
                                        Some("bad scram proof"),
                                        &[],
                                    )
                                    .unwrap();
                                }
                            }
                        }
                    }
                } else if let Some(expected) = oauth_principal {
                    let ok = oauth::token_from_initial(&bytes)
                        .and_then(|t| oauth::principal_from_jwt(&t))
                        .map(|p| p == expected)
                        .unwrap_or(false);
                    authed = ok;
                    encode_sasl_authenticate_response(
                        &mut body,
                        if ok { 0 } else { 58 },
                        if ok { None } else { Some("bad oauth token") },
                        &[],
                    )
                    .unwrap();
                } else {
                    let parsed = parse_plain_auth_bytes(&bytes);
                    let ok = match (parsed, sasl_user) {
                        (Some(got), Some(exp)) => got == exp,
                        _ => false,
                    };
                    authed = ok;
                    encode_sasl_authenticate_response(
                        &mut body,
                        if ok { 0 } else { 58 },
                        if ok { None } else { Some("bad credentials") },
                        &[],
                    )
                    .unwrap();
                }
            }
            FIND_COORDINATOR => {
                encode_find_coordinator_response(&mut body, 1, &host, port).unwrap();
            }
            JOIN_GROUP => {
                let (_g, member_id, metadata) = decode_join_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                if member_id.is_empty() {
                    st.member_seq += 1;
                    let assigned = format!("m-{}", st.member_seq);
                    encode_join_group_response(&mut body, 79, -1, "range", "", &assigned, &[])
                        .unwrap();
                } else {
                    encode_join_group_response(
                        &mut body,
                        0,
                        1,
                        "range",
                        &member_id,
                        &member_id,
                        &[JoinMember {
                            member_id: member_id.clone(),
                            metadata,
                        }],
                    )
                    .unwrap();
                }
            }
            SYNC_GROUP => {
                let (_g, member_id, assignments) = decode_sync_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                for (id, bytes) in assignments {
                    st.assignments.insert(id, bytes);
                }
                let asg = st.assignments.get(&member_id).cloned().unwrap_or_default();
                encode_sync_group_response(&mut body, 0, &asg).unwrap();
            }
            HEARTBEAT => {
                let _ = decode_heartbeat_request(&mut frame);
                encode_heartbeat_response(&mut body, 0).unwrap();
            }
            OFFSET_COMMIT => {
                let (_g, _m, partition, offset) = decode_offset_commit_request(&mut frame).unwrap();
                state
                    .lock()
                    .committed
                    .insert(("t".into(), partition), offset);
                encode_offset_commit_response(&mut body, "t", partition, 0).unwrap();
            }
            OFFSET_FETCH => {
                let (_g, topic, partition) = decode_offset_fetch_request(&mut frame).unwrap();
                let off = *state
                    .lock()
                    .committed
                    .get(&(topic.clone(), partition))
                    .unwrap_or(&-1);
                encode_offset_fetch_response(&mut body, &topic, partition, off).unwrap();
            }
            _ => break,
        }
        if write_frame(&mut stream, &body).await.is_err() {
            break;
        }
    }
}
