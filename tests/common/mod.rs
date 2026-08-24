use bytes::{BufMut, BytesMut};
use partitionline::protocol::api::{
    decode_produce_request, encode_api_versions_response, encode_metadata_response,
    encode_produce_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
    PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    API_VERSIONS, FETCH, FIND_COORDINATOR, HEARTBEAT, INIT_PRODUCER_ID, JOIN_GROUP, METADATA,
    OFFSET_COMMIT, OFFSET_FETCH, PRODUCE, SASL_AUTHENTICATE, SASL_HANDSHAKE, SYNC_GROUP,
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
use partitionline::protocol::records::{Record, RecordBatch};
use partitionline::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct Mock {
    pub addr: String,
    #[allow(dead_code)]
    state: Arc<Mutex<State>>,
}

struct State {
    log: HashMap<(String, i32), Vec<Record>>,
    next_offset: HashMap<(String, i32), i64>,
    assignments: HashMap<String, Vec<u8>>,
    committed: HashMap<(String, i32), i64>,
    member_seq: u32,
    sasl_user: Option<(String, String)>,
    next_pid: i64,
    last_producer_id: Option<i64>,
    expected_seq: HashMap<(i64, String, i32), i32>,
    produce_error: Option<i16>,
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
        let state = Arc::new(Mutex::new(State {
            log: HashMap::new(),
            next_offset: HashMap::new(),
            assignments: HashMap::new(),
            committed: HashMap::new(),
            member_seq: 0,
            sasl_user: creds,
            next_pid: 1000,
            last_producer_id: None,
            expected_seq: HashMap::new(),
            produce_error: None,
        }));
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

    #[allow(dead_code)]
    pub async fn start_tls() -> (Self, partitionline::TlsConfig) {
        partitionline::net::install_crypto_provider();
        let (server, ca_pem) = tls_server_identity();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host = addr.ip().to_string();
        let port = addr.port() as i32;
        let state = Arc::new(Mutex::new(State {
            log: HashMap::new(),
            next_offset: HashMap::new(),
            assignments: HashMap::new(),
            committed: HashMap::new(),
            member_seq: 0,
            sasl_user: None,
            next_pid: 1000,
            last_producer_id: None,
            expected_seq: HashMap::new(),
            produce_error: None,
        }));
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

    #[allow(dead_code)]
    pub fn last_producer_id(&self) -> Option<i64> {
        self.state.lock().unwrap().last_producer_id
    }

    #[allow(dead_code)]
    pub fn log_len(&self, topic: &str, partition: i32) -> usize {
        self.state
            .lock()
            .unwrap()
            .log
            .get(&(topic.to_string(), partition))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn set_produce_error(&self, code: i16) {
        self.state.lock().unwrap().produce_error = Some(code);
    }
}

#[allow(dead_code)]
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
        (INIT_PRODUCER_ID, 0, 4),
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
    let mut authed = state.lock().unwrap().sasl_user.is_none();
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
        );
        match header.api_key {
            API_VERSIONS => {
                encode_api_versions_response(&mut body, header.api_version, &versions())
            }
            METADATA => {
                encode_metadata_response(
                    &mut body,
                    header.api_version,
                    &MetadataResponse {
                        throttle_time_ms: 0,
                        brokers: vec![Broker {
                            node_id: 1,
                            host: host.clone(),
                            port,
                            rack: None,
                        }],
                        cluster_id: Some("mock".into()),
                        controller_id: 1,
                        topics: vec![TopicMetadata {
                            error_code: 0,
                            name: Some("t".into()),
                            topic_id: [0u8; 16],
                            is_internal: false,
                            partitions: vec![PartitionMetadata {
                                error_code: 0,
                                partition_index: 0,
                                leader_id: 1,
                                leader_epoch: 0,
                                replica_nodes: vec![1],
                                isr_nodes: vec![1],
                            }],
                        }],
                    },
                );
            }
            INIT_PRODUCER_ID => {
                let mut st = state.lock().unwrap();
                let pid = st.next_pid;
                st.next_pid += 1;
                encode_init_producer_id_response(&mut body, header.api_version, 0, pid, 0);
            }
            PRODUCE => {
                let decoded = decode_produce_request(&mut frame, header.api_version).unwrap();
                let mut parts = Vec::new();
                let mut st = state.lock().unwrap();
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
                encode_produce_response(&mut body, header.api_version, &parts);
            }
            FETCH => {
                let req = decode_fetch_request(&mut frame).unwrap();
                let st = state.lock().unwrap();
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
                        let batches = if recs.is_empty() {
                            Vec::new()
                        } else {
                            let first = recs[0].offset;
                            let mut batch = RecordBatch::from_records(recs);
                            batch.base_offset = first;
                            vec![batch]
                        };
                        parts.push(FetchedPartition {
                            partition: p.partition,
                            error_code: 0,
                            high_watermark: hw,
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
                let _ = decode_sasl_handshake_request(&mut frame);
                encode_sasl_handshake_response(&mut body, 0, &["PLAIN"]);
            }
            SASL_AUTHENTICATE => {
                let bytes = decode_sasl_authenticate_request(&mut frame).unwrap();
                let parsed = parse_plain_auth_bytes(&bytes);
                let expected = state.lock().unwrap().sasl_user.clone();
                let ok = match (parsed, expected) {
                    (Some(got), Some(exp)) => got == exp,
                    _ => false,
                };
                authed = ok;
                encode_sasl_authenticate_response(
                    &mut body,
                    if ok { 0 } else { 58 },
                    if ok { None } else { Some("bad credentials") },
                );
            }
            FIND_COORDINATOR => {
                encode_find_coordinator_response(&mut body, 1, &host, port);
            }
            JOIN_GROUP => {
                let (_g, member_id, metadata) = decode_join_group_request(&mut frame).unwrap();
                let mut st = state.lock().unwrap();
                if member_id.is_empty() {
                    st.member_seq += 1;
                    let assigned = format!("m-{}", st.member_seq);
                    encode_join_group_response(&mut body, 79, -1, "range", "", &assigned, &[]);
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
                    );
                }
            }
            SYNC_GROUP => {
                let (_g, member_id, assignments) = decode_sync_group_request(&mut frame).unwrap();
                let mut st = state.lock().unwrap();
                for (id, bytes) in assignments {
                    st.assignments.insert(id, bytes);
                }
                let asg = st.assignments.get(&member_id).cloned().unwrap_or_default();
                encode_sync_group_response(&mut body, 0, &asg);
            }
            HEARTBEAT => {
                let _ = decode_heartbeat_request(&mut frame);
                encode_heartbeat_response(&mut body, 0);
            }
            OFFSET_COMMIT => {
                let (_g, _m, partition, offset) = decode_offset_commit_request(&mut frame).unwrap();
                state
                    .lock()
                    .unwrap()
                    .committed
                    .insert(("t".into(), partition), offset);
                encode_offset_commit_response(&mut body, "t", partition, 0);
            }
            OFFSET_FETCH => {
                let (_g, topic, partition) = decode_offset_fetch_request(&mut frame).unwrap();
                let off = *state
                    .lock()
                    .unwrap()
                    .committed
                    .get(&(topic.clone(), partition))
                    .unwrap_or(&-1);
                encode_offset_fetch_response(&mut body, &topic, partition, off);
            }
            _ => break,
        }
        if write_frame(&mut stream, &body).await.is_err() {
            break;
        }
    }
}
