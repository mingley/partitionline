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
use partitionline::error;
use partitionline::protocol::acl::{
    decode_create_acls_request, decode_delete_acls_request, decode_describe_acls_request,
    encode_create_acls_response, encode_delete_acls_response, encode_describe_acls_response,
    AclBinding,
};
use partitionline::protocol::admin::{
    decode_alter_configs_request, decode_create_partitions_request, decode_create_topics_request,
    decode_delete_records_request, decode_delete_topics_request, decode_describe_cluster_request,
    decode_describe_configs_request, decode_incremental_alter_configs_request,
    encode_alter_configs_response, encode_create_partitions_response,
    encode_create_topics_response, encode_delete_records_response, encode_delete_topics_response,
    encode_describe_cluster_response, encode_describe_configs_response,
    encode_incremental_alter_configs_response, ClusterDescription, ConfigEntry,
    DescribeConfigsResult, TopicResult, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET,
    CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_TOPIC, RESOURCE_BROKER, RESOURCE_TOPIC,
};
use partitionline::protocol::api::{
    decode_produce_request, encode_api_versions_response, encode_metadata_response,
    encode_produce_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
    PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, ALTER_CONFIGS, API_VERSIONS, CREATE_ACLS,
    CREATE_PARTITIONS, CREATE_TOPICS, DELETE_ACLS, DELETE_RECORDS, DELETE_TOPICS, DESCRIBE_ACLS,
    DESCRIBE_CLUSTER, DESCRIBE_CONFIGS, END_TXN, FETCH, FIND_COORDINATOR, HEARTBEAT,
    INCREMENTAL_ALTER_CONFIGS, INIT_PRODUCER_ID, JOIN_GROUP, LEAVE_GROUP, LIST_OFFSETS, METADATA,
    OFFSET_COMMIT, OFFSET_FETCH, OFFSET_FOR_LEADER_EPOCH, PRODUCE, SASL_AUTHENTICATE,
    SASL_HANDSHAKE, SYNC_GROUP, TXN_OFFSET_COMMIT,
};
use partitionline::protocol::epoch::{
    decode_offset_for_leader_epoch_request, encode_offset_for_leader_epoch_response,
};
use partitionline::protocol::fetch::{
    decode_fetch_request, encode_fetch_response, FetchedPartition, FetchedTopic,
};
use partitionline::protocol::group::{
    decode_heartbeat_request, decode_join_group_request, decode_leave_group_request,
    decode_offset_commit_request, decode_offset_fetch_request, decode_sync_group_request,
    encode_find_coordinator_response, encode_heartbeat_response, encode_join_group_response,
    encode_leave_group_response, encode_offset_commit_response, encode_offset_fetch_response,
    encode_sync_group_response, JoinMember,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::idem::encode_init_producer_id_response;
use partitionline::protocol::oauth;
use partitionline::protocol::offsets::{
    decode_list_offsets_request, encode_list_offsets_response, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
};
use partitionline::protocol::records::{Record, RecordBatch};
use partitionline::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use partitionline::protocol::scram;
use partitionline::protocol::txn::{
    decode_add_offsets_to_txn_request, decode_add_partitions_to_txn_request,
    decode_end_txn_request, decode_txn_offset_commit_request, encode_add_offsets_to_txn_response,
    encode_add_partitions_to_txn_response, encode_end_txn_response,
    encode_txn_offset_commit_response,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

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
    committed: HashMap<(String, i32), i64>,
    member_seq: u32,
    sasl_user: Option<(String, String)>,
    scram_user: Option<(scram::ScramAlg, String, String)>,
    oauth_principal: Option<String>,
    next_pid: i64,
    last_producer_id: Option<i64>,
    expected_seq: HashMap<(i64, String, i32), i32>,
    produce_error: Option<i16>,
    produce_error_left: Option<u32>,
    log_start: HashMap<(String, i32), i64>,
    created_topics: HashMap<String, CreatedTopic>,
    brokers: Vec<Broker>,
    partition_leaders: HashMap<(String, i32), i32>,
    partition_epochs: HashMap<(String, i32), i32>,
    last_epoch_req: Option<(String, i32, i32)>,
    accepted_produce: Vec<i32>,
    accepted_fetch: Vec<i32>,
    groups: HashMap<String, GroupReg>,
    assign_notify: Arc<Notify>,
    last_fetch_isolation: i8,
    in_txn: bool,
    txn_pending: Vec<(String, i32, i64)>,
    txn_aborted: HashSet<(String, i32, i64)>,
    log_producer: HashMap<(String, i32, i64), i64>,
    last_produce_txn_id: Option<String>,
    acls: Vec<AclBinding>,
}

struct GroupReg {
    members: BTreeMap<String, Vec<u8>>,
    generation: i32,
    joined: HashSet<String>,
    assignments: HashMap<String, Vec<u8>>,
    hb_total: u32,
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
        committed: HashMap::new(),
        member_seq: 0,
        sasl_user,
        scram_user,
        oauth_principal,
        next_pid: 1000,
        last_producer_id: None,
        expected_seq: HashMap::new(),
        produce_error: None,
        produce_error_left: None,
        log_start: HashMap::new(),
        created_topics,
        brokers: Vec::new(),
        partition_leaders: HashMap::new(),
        partition_epochs: HashMap::new(),
        last_epoch_req: None,
        accepted_produce: Vec::new(),
        accepted_fetch: Vec::new(),
        groups: HashMap::new(),
        assign_notify: Arc::new(Notify::new()),
        last_fetch_isolation: 0,
        in_txn: false,
        txn_pending: Vec::new(),
        txn_aborted: HashSet::new(),
        log_producer: HashMap::new(),
        last_produce_txn_id: None,
        acls: Vec::new(),
    }
}

fn metadata_for(st: &State, fallback_host: &str, fallback_port: i32) -> MetadataResponse {
    let brokers = if st.brokers.is_empty() {
        vec![Broker {
            node_id: 1,
            host: fallback_host.to_string(),
            port: fallback_port,
            rack: None,
        }]
    } else {
        st.brokers.clone()
    };
    let replica_nodes: Vec<i32> = brokers.iter().map(|b| b.node_id).collect();
    let default_leader = brokers.first().map(|b| b.node_id).unwrap_or(1);
    let controller_id = default_leader;
    MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("mock".into()),
        controller_id,
        topics: st
            .created_topics
            .iter()
            .map(|(name, spec)| TopicMetadata {
                error_code: 0,
                name: Some(name.clone()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: (0..spec.num_partitions)
                    .map(|i| {
                        let leader_id = st
                            .partition_leaders
                            .get(&(name.clone(), i))
                            .copied()
                            .unwrap_or(default_leader);
                        PartitionMetadata {
                            error_code: 0,
                            partition_index: i,
                            leader_id,
                            leader_epoch: st
                                .partition_epochs
                                .get(&(name.clone(), i))
                                .copied()
                                .unwrap_or(0),
                            replica_nodes: replica_nodes.clone(),
                            isr_nodes: replica_nodes.clone(),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn spawn_plain(listener: TcpListener, node_id: i32, state: Arc<Mutex<State>>) {
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            stream.set_nodelay(true).ok();
            let st = state.clone();
            tokio::spawn(handle_conn(stream, node_id, st));
        }
    });
}

fn broker_host_port(st: &State, node_id: i32) -> (String, i32) {
    st.brokers
        .iter()
        .find(|b| b.node_id == node_id)
        .map(|b| (b.host.clone(), b.port))
        .unwrap_or_else(|| ("127.0.0.1".into(), 0))
}

impl Mock {
    pub async fn start() -> Self {
        Self::start_with_sasl(None).await
    }

    pub async fn start_with_sasl(creds: Option<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(creds, None, None);
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_two_node() -> Self {
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a1 = l1.local_addr().unwrap();
        let a2 = l2.local_addr().unwrap();
        let mut st = new_state(None, None, None);
        st.brokers = vec![
            Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: a1.port() as i32,
                rack: None,
            },
            Broker {
                node_id: 2,
                host: "127.0.0.1".into(),
                port: a2.port() as i32,
                rack: None,
            },
        ];
        st.partition_leaders.insert(("t".into(), 0), 2);
        let state = Arc::new(Mutex::new(st));
        spawn_plain(l1, 1, state.clone());
        spawn_plain(l2, 2, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", a1.port()),
            state,
        }
    }

    pub async fn start_with_scram(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(
            None,
            Some((scram::ScramAlg::Sha256, creds.0, creds.1)),
            None,
        );
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_scram_sha512(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(
            None,
            Some((scram::ScramAlg::Sha512, creds.0, creds.1)),
            None,
        );
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_oauthbearer(principal: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(None, None, Some(principal));
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
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
        let port = addr.port() as i32;
        let mut st = new_state(None, None, None);
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                tcp.set_nodelay(true).ok();
                let st = st.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(tcp).await else {
                        return;
                    };
                    handle_conn(stream, 1, st).await;
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
        let mut st = self.state.lock();
        st.produce_error = Some(code);
        st.produce_error_left = None;
    }

    pub fn set_produce_error_times(&self, code: i16, n: u32) {
        let mut st = self.state.lock();
        st.produce_error = Some(code);
        st.produce_error_left = Some(n);
    }

    pub fn produce_nodes(&self) -> Vec<i32> {
        self.state.lock().accepted_produce.clone()
    }

    pub fn fetch_nodes(&self) -> Vec<i32> {
        self.state.lock().accepted_fetch.clone()
    }

    pub fn last_fetch_isolation(&self) -> i8 {
        self.state.lock().last_fetch_isolation
    }

    pub fn last_produce_txn_id(&self) -> Option<String> {
        self.state.lock().last_produce_txn_id.clone()
    }

    pub fn bump_leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        let mut st = self.state.lock();
        let slot = st
            .partition_epochs
            .entry((topic.to_string(), partition))
            .or_insert(0);
        *slot += 1;
        *slot
    }

    pub fn last_offset_for_leader_epoch(&self) -> Option<(String, i32, i32)> {
        self.state.lock().last_epoch_req.clone()
    }

    pub fn heartbeat_total(&self, group_id: &str) -> u32 {
        self.state
            .lock()
            .groups
            .get(group_id)
            .map(|g| g.hb_total)
            .unwrap_or(0)
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
        (LIST_OFFSETS, 0, 5),
        (METADATA, 1, 12),
        (OFFSET_COMMIT, 2, 7),
        (OFFSET_FETCH, 1, 5),
        (FIND_COORDINATOR, 0, 2),
        (JOIN_GROUP, 0, 5),
        (HEARTBEAT, 0, 3),
        (SYNC_GROUP, 0, 3),
        (LEAVE_GROUP, 0, 2),
        (SASL_HANDSHAKE, 0, 1),
        (API_VERSIONS, 0, 4),
        (CREATE_TOPICS, 0, 4),
        (DELETE_TOPICS, 0, 3),
        (CREATE_PARTITIONS, 0, 1),
        (DELETE_RECORDS, 0, 1),
        (ALTER_CONFIGS, 0, 1),
        (DESCRIBE_CLUSTER, 0, 0),
        (DESCRIBE_ACLS, 0, 1),
        (CREATE_ACLS, 0, 1),
        (DELETE_ACLS, 0, 1),
        (INCREMENTAL_ALTER_CONFIGS, 0, 0),
        (INIT_PRODUCER_ID, 0, 4),
        (ADD_PARTITIONS_TO_TXN, 0, 1),
        (ADD_OFFSETS_TO_TXN, 0, 1),
        (END_TXN, 0, 1),
        (TXN_OFFSET_COMMIT, 0, 2),
        (OFFSET_FOR_LEADER_EPOCH, 0, 2),
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
    node_id: i32,
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
                let st = state.lock();
                let (host, port) = broker_host_port(&st, node_id);
                encode_metadata_response(
                    &mut body,
                    header.api_version,
                    &metadata_for(&st, &host, port),
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
            CREATE_PARTITIONS => {
                let (topics, validate_only) = decode_create_partitions_request(&mut frame).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                for (name, count) in topics {
                    match st.created_topics.get_mut(&name) {
                        None => results.push(TopicResult {
                            name,
                            error_code: 3,
                            error_message: Some("Unknown topic.".into()),
                        }),
                        Some(spec) => {
                            let mut err = 0i16;
                            if count < spec.num_partitions {
                                err = 37;
                            } else if !validate_only {
                                spec.num_partitions = count;
                            }
                            results.push(TopicResult {
                                name,
                                error_code: err,
                                error_message: None,
                            });
                        }
                    }
                }
                encode_create_partitions_response(&mut body, &results).unwrap();
            }
            INCREMENTAL_ALTER_CONFIGS => {
                let (rt, name, configs, validate_only) =
                    decode_incremental_alter_configs_request(&mut frame).unwrap();
                let mut err = 0i16;
                let mut st = state.lock();
                if rt != RESOURCE_TOPIC {
                    err = 3;
                } else if let Some(spec) = st.created_topics.get_mut(&name) {
                    if !validate_only {
                        for c in configs {
                            if c.op == ALTER_CONFIG_DELETE {
                                spec.configs.remove(&c.name);
                            } else if c.op == ALTER_CONFIG_SET {
                                spec.configs.insert(c.name, c.value);
                            }
                        }
                    }
                } else {
                    err = 3;
                }
                encode_incremental_alter_configs_response(&mut body, err, &name).unwrap();
            }
            ALTER_CONFIGS => {
                let (rt, name, configs, validate_only) =
                    decode_alter_configs_request(&mut frame).unwrap();
                let mut err = 0i16;
                let mut st = state.lock();
                if rt != RESOURCE_TOPIC {
                    err = 3;
                } else if let Some(spec) = st.created_topics.get_mut(&name) {
                    if !validate_only {
                        for c in configs {
                            if let Some(val) = c.value {
                                spec.configs.insert(c.name, Some(val));
                            } else {
                                spec.configs.remove(&c.name);
                            }
                        }
                    }
                } else {
                    err = 3;
                }
                encode_alter_configs_response(&mut body, header.api_version, err, &name).unwrap();
            }
            DELETE_RECORDS => {
                let (topic, partition, offset, _timeout) =
                    decode_delete_records_request(&mut frame).unwrap();
                let mut st = state.lock();
                let key = (topic.clone(), partition);
                let (low, err) = if st.created_topics.contains_key(&topic) {
                    let hw = *st.next_offset.get(&key).unwrap_or(&0);
                    let start = *st.log_start.get(&key).unwrap_or(&0);
                    let low = offset.clamp(start, hw);
                    st.log_start.insert(key.clone(), low);
                    if let Some(recs) = st.log.get_mut(&key) {
                        recs.retain(|r| r.offset >= low);
                    }
                    (low, 0i16)
                } else {
                    (0i64, 3i16)
                };
                encode_delete_records_response(
                    &mut body,
                    header.api_version,
                    &topic,
                    partition,
                    low,
                    err,
                )
                .unwrap();
            }
            DESCRIBE_CLUSTER => {
                let _include = decode_describe_cluster_request(&mut frame).unwrap();
                let st = state.lock();
                let brokers = if st.brokers.is_empty() {
                    vec![Broker {
                        node_id,
                        host: "127.0.0.1".into(),
                        port: 0,
                        rack: None,
                    }]
                } else {
                    st.brokers.clone()
                };
                let controller_id = brokers.first().map(|b| b.node_id).unwrap_or(node_id);
                encode_describe_cluster_response(
                    &mut body,
                    &ClusterDescription {
                        error_code: 0,
                        error_message: None,
                        cluster_id: Some("mock".into()),
                        controller_id,
                        brokers,
                    },
                )
                .unwrap();
            }
            CREATE_ACLS => {
                let acls = decode_create_acls_request(&mut frame).unwrap();
                let n = acls.len();
                state.lock().acls.extend(acls);
                encode_create_acls_response(&mut body, &vec![0; n]).unwrap();
            }
            DESCRIBE_ACLS => {
                let rt = decode_describe_acls_request(&mut frame).unwrap();
                let st = state.lock();
                let acls: Vec<AclBinding> = st
                    .acls
                    .iter()
                    .filter(|a| rt == 1 || a.resource_type == rt)
                    .cloned()
                    .collect();
                encode_describe_acls_response(&mut body, &acls).unwrap();
            }
            DELETE_ACLS => {
                let rt = decode_delete_acls_request(&mut frame).unwrap();
                let mut st = state.lock();
                let before = st.acls.len();
                st.acls.retain(|a| rt != 1 && a.resource_type != rt);
                let removed = i32::try_from(before.saturating_sub(st.acls.len())).unwrap_or(0);
                encode_delete_acls_response(&mut body, removed).unwrap();
            }
            LIST_OFFSETS => {
                let (iso, topic, partition, timestamp) =
                    decode_list_offsets_request(&mut frame, header.api_version).unwrap();
                let _ = iso;
                let st = state.lock();
                let key = (topic.clone(), partition);
                let log_start = *st.log_start.get(&key).unwrap_or(&0);
                let hw = *st.next_offset.get(&key).unwrap_or(&0);
                let offset = if timestamp == EARLIEST_TIMESTAMP {
                    log_start
                } else if timestamp == LATEST_TIMESTAMP {
                    hw
                } else {
                    st.log
                        .get(&key)
                        .and_then(|recs| recs.iter().find(|r| r.timestamp >= timestamp))
                        .map(|r| r.offset)
                        .unwrap_or(-1)
                };
                encode_list_offsets_response(
                    &mut body,
                    header.api_version,
                    &topic,
                    partition,
                    0,
                    timestamp,
                    offset,
                )
                .unwrap();
            }
            INIT_PRODUCER_ID => {
                let mut st = state.lock();
                let pid = st.next_pid;
                st.next_pid += 1;
                encode_init_producer_id_response(&mut body, header.api_version, 0, pid, 0).unwrap();
            }
            ADD_PARTITIONS_TO_TXN => {
                let _ = decode_add_partitions_to_txn_request(&mut frame);
                state.lock().in_txn = true;
                encode_add_partitions_to_txn_response(&mut body, 0).unwrap();
            }
            ADD_OFFSETS_TO_TXN => {
                let _ = decode_add_offsets_to_txn_request(&mut frame);
                encode_add_offsets_to_txn_response(&mut body, 0).unwrap();
            }
            END_TXN => {
                let (_tid, _pid, _epoch, committed) = decode_end_txn_request(&mut frame).unwrap();
                let mut st = state.lock();
                if !committed {
                    let pending = std::mem::take(&mut st.txn_pending);
                    for rec in pending {
                        st.txn_aborted.insert(rec);
                    }
                } else {
                    st.txn_pending.clear();
                }
                st.in_txn = false;
                encode_end_txn_response(&mut body, 0).unwrap();
            }
            TXN_OFFSET_COMMIT => {
                let (_tid, _gid, part, off) = decode_txn_offset_commit_request(&mut frame).unwrap();
                state.lock().committed.insert(("t".into(), part), off);
                encode_txn_offset_commit_response(&mut body, 0).unwrap();
            }
            PRODUCE => {
                let decoded = decode_produce_request(&mut frame, header.api_version).unwrap();
                let txn_id = decoded.0;
                let mut parts = Vec::new();
                let mut st = state.lock();
                let forced = match (st.produce_error, st.produce_error_left) {
                    (Some(_), Some(0)) => {
                        st.produce_error = None;
                        st.produce_error_left = None;
                        None
                    }
                    (Some(c), Some(left)) => {
                        st.produce_error_left = Some(left.saturating_sub(1));
                        if left <= 1 {
                            st.produce_error = None;
                            st.produce_error_left = None;
                        }
                        Some(c)
                    }
                    (Some(c), None) => Some(c),
                    (None, _) => None,
                };
                for topic in decoded.3 {
                    for p in topic.partitions {
                        st.last_producer_id = Some(p.records.producer_id);
                        let key = (topic.topic.clone(), p.index);
                        let nrec = p.records.records.len() as i32;
                        let leader = st
                            .partition_leaders
                            .get(&(topic.topic.clone(), p.index))
                            .copied()
                            .unwrap_or(node_id);
                        let mut error_code = if leader != node_id {
                            6
                        } else if st.in_txn && txn_id.is_none() {
                            error::INVALID_TXN_STATE
                        } else {
                            forced.unwrap_or(0)
                        };
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
                            st.accepted_produce.push(node_id);
                            st.last_produce_txn_id = txn_id.clone();
                            let pid = p.records.producer_id;
                            let mut n = 0i64;
                            for mut rec in p.records.records {
                                rec.offset = start + n;
                                st.log_producer
                                    .insert((topic.topic.clone(), p.index, rec.offset), pid);
                                st.log.entry(key.clone()).or_default().push(rec);
                                n += 1;
                            }
                            st.next_offset.insert(key, start + n);
                            if st.in_txn {
                                for o in 0..n {
                                    st.txn_pending
                                        .push((topic.topic.clone(), p.index, start + o));
                                }
                            }
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
                let (iso, req) = decode_fetch_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.last_fetch_isolation = iso;
                let mut topics = Vec::new();
                for t in req {
                    let mut parts = Vec::new();
                    for p in t.partitions {
                        let leader = st
                            .partition_leaders
                            .get(&(t.topic.clone(), p.partition))
                            .copied()
                            .unwrap_or(node_id);
                        if leader != node_id {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: 6,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                records: Vec::new(),
                            });
                            continue;
                        }
                        let current_epoch = st
                            .partition_epochs
                            .get(&(t.topic.clone(), p.partition))
                            .copied()
                            .unwrap_or(0);
                        if p.current_leader_epoch != -1 && p.current_leader_epoch < current_epoch {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: error::FENCED_LEADER_EPOCH,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                records: Vec::new(),
                            });
                            continue;
                        }
                        if p.current_leader_epoch != -1 && p.current_leader_epoch > current_epoch {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: error::UNKNOWN_LEADER_EPOCH,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                records: Vec::new(),
                            });
                            continue;
                        }
                        st.accepted_fetch.push(node_id);
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
                        let lso = if iso == 1 {
                            st.txn_pending
                                .iter()
                                .filter(|(tn, pn, _)| tn == &t.topic && *pn == p.partition)
                                .map(|(_, _, o)| *o)
                                .min()
                                .unwrap_or(hw)
                        } else {
                            hw
                        };
                        let mut aborted_transactions = Vec::new();
                        if iso == 1 {
                            let mut first_off: HashMap<i64, i64> = HashMap::new();
                            for (tn, pn, off) in &st.txn_aborted {
                                if tn == &t.topic && *pn == p.partition {
                                    if let Some(pid) =
                                        st.log_producer.get(&(tn.clone(), *pn, *off)).copied()
                                    {
                                        let e = first_off.entry(pid).or_insert(*off);
                                        if *off < *e {
                                            *e = *off;
                                        }
                                    }
                                }
                            }
                            aborted_transactions = first_off.into_iter().collect();
                        }
                        let error_code = if p.fetch_offset < log_start { 1 } else { 0 };
                        let batches = if error_code != 0 || recs.is_empty() {
                            Vec::new()
                        } else {
                            let first = recs[0].offset;
                            let pid = st
                                .log_producer
                                .get(&(t.topic.clone(), p.partition, first))
                                .copied()
                                .unwrap_or(-1);
                            let mut batch = RecordBatch::from_records(recs);
                            batch.base_offset = first;
                            batch.producer_id = pid;
                            vec![batch]
                        };
                        parts.push(FetchedPartition {
                            partition: p.partition,
                            error_code,
                            high_watermark: hw,
                            last_stable_offset: lso,
                            log_start_offset: log_start,
                            aborted_transactions,
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
            OFFSET_FOR_LEADER_EPOCH => {
                let (topic, partition, _current, leader_epoch) =
                    decode_offset_for_leader_epoch_request(&mut frame, header.api_version).unwrap();
                let mut st = state.lock();
                st.last_epoch_req = Some((topic.clone(), partition, leader_epoch));
                let epoch = st
                    .partition_epochs
                    .get(&(topic.clone(), partition))
                    .copied()
                    .unwrap_or(0);
                let end = *st
                    .next_offset
                    .get(&(topic.clone(), partition))
                    .unwrap_or(&0);
                encode_offset_for_leader_epoch_response(
                    &mut body,
                    header.api_version,
                    &topic,
                    partition,
                    0,
                    epoch,
                    end,
                )
                .unwrap();
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
                let st = state.lock();
                let (host, port) = broker_host_port(&st, node_id);
                encode_find_coordinator_response(&mut body, node_id, &host, port).unwrap();
            }
            JOIN_GROUP => {
                let (gid, member_id, metadata) = decode_join_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                if member_id.is_empty() {
                    st.member_seq += 1;
                    let assigned = format!("m-{}", st.member_seq);
                    encode_join_group_response(&mut body, 79, -1, "range", "", &assigned, &[])
                        .unwrap();
                } else {
                    let notify = st.assign_notify.clone();
                    let g = st.groups.entry(gid).or_insert_with(|| GroupReg {
                        members: BTreeMap::new(),
                        generation: 0,
                        joined: HashSet::new(),
                        assignments: HashMap::new(),
                        hb_total: 0,
                    });
                    let mut bumped = false;
                    if !g.members.contains_key(&member_id) {
                        g.generation += 1;
                        g.joined.clear();
                        g.assignments.clear();
                        bumped = true;
                    }
                    g.members.insert(member_id.clone(), metadata.clone());
                    g.joined.insert(member_id.clone());
                    let leader = g.members.keys().next().cloned().unwrap_or_default();
                    let members: Vec<JoinMember> = g
                        .members
                        .iter()
                        .map(|(id, md)| JoinMember {
                            member_id: id.clone(),
                            metadata: md.clone(),
                        })
                        .collect();
                    let gen = g.generation;
                    drop(st);
                    if bumped {
                        notify.notify_waiters();
                    }
                    encode_join_group_response(
                        &mut body, 0, gen, "range", &leader, &member_id, &members,
                    )
                    .unwrap();
                }
            }
            SYNC_GROUP => {
                let (gid, member_id, assignments) = decode_sync_group_request(&mut frame).unwrap();
                let notify = state.lock().assign_notify.clone();
                if !assignments.is_empty() {
                    let mut st = state.lock();
                    if let Some(g) = st.groups.get_mut(&gid) {
                        g.assignments.clear();
                        for (id, bytes) in assignments {
                            g.assignments.insert(id, bytes);
                        }
                    }
                    notify.notify_waiters();
                }
                let mut asg = Vec::new();
                for _ in 0..40 {
                    {
                        let st = state.lock();
                        if let Some(g) = st.groups.get(&gid) {
                            if let Some(b) = g.assignments.get(&member_id) {
                                asg = b.clone();
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                encode_sync_group_response(&mut body, 0, &asg).unwrap();
            }
            HEARTBEAT => {
                let (gid, _gen, member_id) = decode_heartbeat_request(&mut frame).unwrap();
                let mut st = state.lock();
                let mut err = 0i16;
                if let Some(g) = st.groups.get_mut(&gid) {
                    g.hb_total += 1;
                    if g.members.contains_key(&member_id) && !g.joined.contains(&member_id) {
                        err = 27;
                    }
                }
                encode_heartbeat_response(&mut body, err).unwrap();
            }
            LEAVE_GROUP => {
                let (gid, member_id) = decode_leave_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                if let Some(g) = st.groups.get_mut(&gid) {
                    g.members.remove(&member_id);
                    g.joined.remove(&member_id);
                    g.generation += 1;
                    g.joined.clear();
                    g.assignments.clear();
                }
                st.assign_notify.notify_waiters();
                encode_leave_group_response(&mut body, 0).unwrap();
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

/// RFC 6749 token endpoint. Valid Basic credentials get an unsecured JWT for `principal`.
pub async fn start_oidc_token_endpoint(
    client_id: String,
    client_secret: String,
    principal: String,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            let id = client_id.clone();
            let secret = client_secret.clone();
            let principal = principal.clone();
            tokio::spawn(async move {
                serve_oidc_token(sock, &id, &secret, &principal).await;
            });
        }
    });
    format!("http://{addr}/oauth/token")
}

async fn serve_oidc_token(
    mut sock: tokio::net::TcpStream,
    client_id: &str,
    client_secret: &str,
    principal: &str,
) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        let n = match sock.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => return,
        };
        buf.extend_from_slice(tmp.get(..n).unwrap_or(&[]));
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16 * 1024 {
            break;
        }
    }
    let req = String::from_utf8_lossy(&buf);
    let expected = {
        let raw = format!("{client_id}:{client_secret}");
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw.as_bytes())
    };
    let auth_ok = req.lines().any(|l| {
        let line = l.trim_end_matches('\r');
        let Some((k, v)) = line.split_once(':') else {
            return false;
        };
        k.eq_ignore_ascii_case("authorization") && v.trim() == format!("Basic {expected}")
    });
    let ok = auth_ok && req.contains("grant_type=client_credentials");
    let (status, body) = if ok {
        let token = oauth::unsecured_jwt_now(principal);
        (
            "200 OK",
            format!("{{\"access_token\":\"{token}\",\"token_type\":\"Bearer\"}}"),
        )
    } else {
        ("401 Unauthorized", "{\"error\":\"invalid_client\"}".into())
    };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = sock.write_all(resp.as_bytes()).await;
}
