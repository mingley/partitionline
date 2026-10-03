//! Bounded scripted two-node socket peer for API-specific Admin tests.
#![allow(
    dead_code,
    reason = "each API-specific test target uses a subset of the peer controls"
)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use partitionline::protocol::api::{
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    API_VERSIONS, DESCRIBE_SHARE_GROUP_OFFSETS, FIND_COORDINATOR, METADATA, WRITE_TXN_MARKERS,
};
use partitionline::protocol::group::{
    decode_find_coordinator_request_keys, encode_find_coordinator_response_coordinators,
    CoordinatorResult,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{Admin, AdminConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

pub const BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct Observed {
    pub node: i32,
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
    pub body: Vec<u8>,
    pub request_payload: Vec<u8>,
    pub response_payload: Option<Vec<u8>>,
    pub response_written: bool,
}

#[derive(Default)]
pub struct State {
    pub observed: Vec<Observed>,
    pub marker_responses: VecDeque<Vec<u8>>,
    pub share_responses: VecDeque<Vec<u8>>,
    pub hold_markers: bool,
    pub hold_share: bool,
}

pub struct Peer {
    pub bootstrap: String,
    pub state: Arc<Mutex<State>>,
    pub seen: Arc<Notify>,
    pub release: Arc<Notify>,
    tasks: Vec<JoinHandle<()>>,
}

impl Peer {
    pub async fn start(marker: Option<(i16, i16)>, share: Option<(i16, i16)>) -> Self {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        let brokers: Vec<Broker> = addresses
            .iter()
            .enumerate()
            .map(|(i, address)| {
                Broker::new(
                    i32::try_from(i + 1).unwrap(),
                    "127.0.0.1",
                    i32::from(address.port()),
                    None,
                )
            })
            .collect();
        let mut ranges = vec![
            ApiVersion {
                api_key: API_VERSIONS,
                min_version: 0,
                max_version: 4,
            },
            ApiVersion {
                api_key: METADATA,
                min_version: 1,
                max_version: 13,
            },
            ApiVersion {
                api_key: FIND_COORDINATOR,
                min_version: 1,
                max_version: 6,
            },
        ];
        for (key, range) in [
            (WRITE_TXN_MARKERS, marker),
            (DESCRIBE_SHARE_GROUP_OFFSETS, share),
        ] {
            if let Some((min_version, max_version)) = range {
                ranges.push(ApiVersion {
                    api_key: key,
                    min_version,
                    max_version,
                });
            }
        }
        let state = Arc::new(Mutex::new(State::default()));
        let seen = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let tasks = [first, second]
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let node = i32::try_from(i + 1).unwrap();
                let state = state.clone();
                let seen = seen.clone();
                let release = release.clone();
                let brokers = brokers.clone();
                let ranges = ranges.clone();
                tokio::spawn(async move {
                    let mut workers = JoinSet::new();
                    let mut connections = 0;
                    loop {
                        let accepted = listener.accept().await;
                        let Ok((mut stream, _)) = accepted else { break };
                        connections += 1;
                        assert!(connections <= 32, "finite socket peer connection history");
                        let state = state.clone();
                        let seen = seen.clone();
                        let release = release.clone();
                        let brokers = brokers.clone();
                        let ranges = ranges.clone();
                        workers.spawn(async move {
                            let mut frames = 0;
                            while let Ok(length) = stream.read_i32().await {
                                assert!((8..=65_536).contains(&length));
                                frames += 1;
                                assert!(frames <= 64, "finite per-connection frame history");
                                let mut frame = vec![0; usize::try_from(length).unwrap()];
                                if stream.read_exact(&mut frame).await.is_err() {
                                    break;
                                }
                                let mut cursor = frame.as_slice();
                                let header = decode_request_header(&mut cursor).unwrap();
                                let observation_index = {
                                    let mut state = state.lock().unwrap();
                                    assert!(state.observed.len() < 256);
                                    let observation_index = state.observed.len();
                                    state.observed.push(Observed {
                                        node,
                                        key: header.api_key,
                                        version: header.api_version,
                                        correlation: header.correlation_id,
                                        body: cursor.to_vec(),
                                        request_payload: frame.clone(),
                                        response_payload: None,
                                        response_written: false,
                                    });
                                    observation_index
                                };
                                let mut body = BytesMut::new();
                                match header.api_key {
                                    API_VERSIONS => encode_api_versions_response(
                                        &mut body,
                                        header.api_version,
                                        &ApiVersionsResponse {
                                            api_keys: ranges.clone(),
                                            ..ApiVersionsResponse::default()
                                        },
                                    )
                                    .unwrap(),
                                    METADATA => encode_metadata_response(
                                        &mut body,
                                        header.api_version,
                                        &MetadataResponse {
                                            throttle_time_ms: 0,
                                            brokers: brokers.clone(),
                                            cluster_id: Some("capabilities".into()),
                                            controller_id: 1,
                                            topics: vec![TopicMetadata {
                                                error_code: 0,
                                                name: Some("t".into()),
                                                topic_id: [1; 16],
                                                is_internal: false,
                                                partitions: vec![PartitionMetadata::new(
                                                    0,
                                                    0,
                                                    Some(2),
                                                    Some(7),
                                                    vec![2],
                                                    vec![2],
                                                    vec![],
                                                )],
                                                topic_authorized_operations: i32::MIN,
                                            }],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        },
                                    )
                                    .unwrap(),
                                    FIND_COORDINATOR => {
                                        let (keys, key_type) =
                                            decode_find_coordinator_request_keys(
                                                &mut cursor,
                                                header.api_version,
                                            )
                                            .unwrap();
                                        assert!(cursor.is_empty());
                                        assert_eq!(
                                            key_type, 0,
                                            "public share offsets use GROUP coordinator"
                                        );
                                        let coordinator = &brokers[1];
                                        let rows: Vec<_> = keys
                                            .into_iter()
                                            .map(|key| CoordinatorResult {
                                                key,
                                                node_id: 2,
                                                host: coordinator.host.clone(),
                                                port: coordinator.port,
                                                error_code: 0,
                                                error_message: None,
                                            })
                                            .collect();
                                        encode_find_coordinator_response_coordinators(
                                            &mut body,
                                            header.api_version,
                                            &rows,
                                        )
                                        .unwrap();
                                    }
                                    WRITE_TXN_MARKERS | DESCRIBE_SHARE_GROUP_OFFSETS => {
                                        assert_eq!(
                                            node, 2,
                                            "operation reaches selected leader/coordinator"
                                        );
                                        let (response, hold) = {
                                            let mut state = state.lock().unwrap();
                                            let hold = if header.api_key == WRITE_TXN_MARKERS {
                                                state.hold_markers
                                            } else {
                                                state.hold_share
                                            };
                                            let queue = if header.api_key == WRITE_TXN_MARKERS {
                                                &mut state.marker_responses
                                            } else {
                                                &mut state.share_responses
                                            };
                                            let response = if queue.len() > 1 {
                                                queue.pop_front().unwrap()
                                            } else {
                                                queue.front().expect("declared response").clone()
                                            };
                                            (response, hold)
                                        };
                                        seen.notify_one();
                                        if hold {
                                            release.notified().await;
                                        }
                                        body.extend_from_slice(&response);
                                    }
                                    other => panic!("unexpected API {other}"),
                                }
                                let mut response = BytesMut::new();
                                encode_response_header(
                                    &mut response,
                                    header.api_key,
                                    header.api_version,
                                    header.correlation_id,
                                )
                                .unwrap();
                                response.extend_from_slice(&body);
                                let mut packet = BytesMut::new();
                                packet.put_i32(i32::try_from(response.len()).unwrap());
                                packet.extend_from_slice(&response);
                                let response_written = stream.write_all(&packet).await.is_ok();
                                {
                                    let mut state = state.lock().unwrap();
                                    state.observed[observation_index].response_payload =
                                        Some(response.to_vec());
                                    state.observed[observation_index].response_written =
                                        response_written;
                                }
                                if !response_written {
                                    break;
                                }
                            }
                        });
                    }
                })
            })
            .collect();
        Self {
            bootstrap: addresses[0].to_string(),
            state,
            seen,
            release,
            tasks,
        }
    }

    pub async fn admin(&self) -> Admin {
        Admin::new(
            AdminConfig::bootstrap([self.bootstrap.clone()])
                .connect_timeout(BUDGET)
                .request_timeout(BUDGET)
                .retry_backoff(Duration::from_millis(2))
                .retry_backoff_max(Duration::from_millis(4)),
        )
        .await
        .unwrap()
    }

    pub fn requests(&self, key: i16) -> Vec<Observed> {
        self.state
            .lock()
            .unwrap()
            .observed
            .iter()
            .filter(|row| row.key == key)
            .cloned()
            .collect()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
