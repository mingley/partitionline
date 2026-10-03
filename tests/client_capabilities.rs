//! Limited and mixed broker capability negotiation through public client APIs.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded test peers and assertions fail immediately on unexpected wire behavior"
)]

mod common;

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use partitionline::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
use partitionline::protocol::api_keys::{
    API_VERSIONS, CREATE_TOPICS, DELETE_GROUPS, DELETE_TOPICS, DESCRIBE_CONFIGS, DESCRIBE_GROUPS,
    FETCH, INIT_PRODUCER_ID, LIST_OFFSETS, METADATA, PRODUCE,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{
    AclBinding, AclResourceType, Admin, AdminConfig, ConfigResource, Consumer, ConsumerConfig,
    Error, NewPartitions, NewTopic, ProduceRecord, Producer, ProducerConfig, TopicPartition,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const DEADLINE: Duration = Duration::from_secs(2);
const BASIC: &[(i16, i16, i16)] = &[
    (PRODUCE, 3, 13),
    (FETCH, 4, 6),
    (LIST_OFFSETS, 1, 3),
    (METADATA, 0, 13),
    (API_VERSIONS, 0, 4),
    (CREATE_TOPICS, 2, 4),
    (DELETE_TOPICS, 1, 6),
];

/// Only answers negotiation; any later frame is observed and closes the socket.
/// This deliberately cannot make an unavailable operation appear successful.
struct NegotiationPeer {
    addr: String,
    requests: mpsc::Receiver<(i16, i16)>,
    task: JoinHandle<()>,
}

impl NegotiationPeer {
    async fn start(extra: &[(i16, i16, i16)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind peer");
        let addr = listener.local_addr().unwrap().to_string();
        let keys: Vec<ApiVersion> = BASIC
            .iter()
            .chain(extra)
            .map(|&(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
            })
            .collect();
        let (send, requests) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            while let Ok(length) = stream.read_i32().await {
                assert!((8..=65_536).contains(&length));
                let mut frame = vec![0; usize::try_from(length).unwrap()];
                let _read = stream.read_exact(&mut frame).await.unwrap();
                let header = decode_request_header(&mut frame.as_slice()).unwrap();
                send.send((header.api_key, header.api_version))
                    .await
                    .unwrap();
                if header.api_key != API_VERSIONS {
                    break;
                }
                let mut response = BytesMut::new();
                encode_response_header(
                    &mut response,
                    API_VERSIONS,
                    header.api_version,
                    header.correlation_id,
                )
                .unwrap();
                encode_api_versions_response(
                    &mut response,
                    header.api_version,
                    &ApiVersionsResponse {
                        api_keys: keys.clone(),
                        ..ApiVersionsResponse::default()
                    },
                )
                .unwrap();
                let mut packet = BytesMut::new();
                packet.put_i32(i32::try_from(response.len()).unwrap());
                packet.extend_from_slice(&response);
                stream.write_all(&packet).await.unwrap();
            }
        });
        Self {
            addr,
            requests,
            task,
        }
    }

    async fn negotiated(&mut self) {
        assert_eq!(
            tokio::time::timeout(DEADLINE, self.requests.recv())
                .await
                .unwrap(),
            Some((API_VERSIONS, 4))
        );
    }

    async fn no_more_requests(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), self.requests.recv())
                .await
                .is_err(),
            "an unsupported operation sent a frame or closed the connection"
        );
    }
}

impl Drop for NegotiationPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn producer_config(addr: &str) -> ProducerConfig {
    ProducerConfig::bootstrap([addr])
        .connections(1)
        .max_in_flight(1)
        .buffer_memory(1 << 20)
        .batch_bytes(4096)
        .max_request_size(65_536)
        .linger(Duration::ZERO)
        .request_timeout(DEADLINE)
        .connect_timeout(DEADLINE)
        .delivery_timeout(Duration::from_secs(5))
        .max_block(DEADLINE)
}

fn admin_config(addr: &str) -> AdminConfig {
    AdminConfig::bootstrap([addr])
        .request_timeout(DEADLINE)
        .connect_timeout(DEADLINE)
}

fn limit_mock(mock: &common::Mock, extra: &[i16]) {
    for key in 0..128 {
        if !BASIC.iter().any(|&(allowed, _, _)| allowed == key) && !extra.contains(&key) {
            mock.hide_api(key);
        }
    }
    for &(key, _, max) in BASIC {
        mock.set_api_max(key, max);
    }
}

fn unsupported<T>(result: partitionline::Result<T>, capability: &str) {
    match result {
        Err(Error::Unsupported(message)) => assert!(message.contains(capability), "{message}"),
        Err(error) => panic!("expected Unsupported({capability}), received {error}"),
        Ok(_) => panic!("unavailable {capability} unexpectedly succeeded"),
    }
}

#[tokio::test]
async fn ordinary_produce_and_manual_fetch_use_only_the_limited_profile() {
    let mock = common::Mock::start().await;
    limit_mock(&mock, &[]);
    let producer = Producer::new(producer_config(&mock.addr)).await.unwrap();
    let sent = producer
        .send(
            ProduceRecord::to("t")
                .partition(0)
                .timestamp(1000)
                .value(&b"ordinary"[..]),
        )
        .await
        .unwrap();
    assert_eq!(sent.offset, 0);
    assert_eq!(mock.last_producer_id(), Some(-1));
    assert_eq!(mock.find_coordinator_calls(), 0);
    assert!(mock.init_producer_id_nodes().is_empty());
    assert_eq!(mock.last_produce_version(), Some(13));
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .fetch_max_bytes(65_536)
            .max_partition_fetch_bytes(4096)
            .buffer_memory(1 << 20)
            .max_wait_ms(10)
            .request_timeout(DEADLINE),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let records = consumer.fetch().await.unwrap();
    assert_eq!(records.count(), 1);
    let record = records.records(("t", 0)).next().unwrap();
    assert_eq!(record.offset, 0);
    assert_eq!(record.timestamp, 1000);
    assert_eq!(record.value.as_deref(), Some(&b"ordinary"[..]));
    assert_eq!(mock.last_fetch_version(), Some(6));
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn nontransactional_idempotence_uses_init_producer_id_without_a_coordinator() {
    let mock = common::Mock::start().await;
    limit_mock(&mock, &[INIT_PRODUCER_ID]);
    let producer = Producer::new(producer_config(&mock.addr).idempotent(true))
        .await
        .unwrap();
    let sent = producer
        .send(ProduceRecord::to("t").value(&b"idempotent"[..]))
        .await
        .unwrap();
    assert_eq!(sent.offset, 0);
    assert!(mock.last_producer_id().unwrap() >= 0);
    assert_eq!(mock.find_coordinator_calls(), 0);
    assert_eq!(mock.init_producer_id_nodes(), vec![1]);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn missing_identity_and_transaction_capabilities_fail_before_discovery() {
    for transactional in [false, true] {
        let mut peer = NegotiationPeer::start(&[]).await;
        let cfg = if transactional {
            producer_config(&peer.addr).transactional_id("limited-transaction")
        } else {
            producer_config(&peer.addr).idempotent(true)
        };
        unsupported(
            Producer::new(cfg).await,
            if transactional {
                "FindCoordinator"
            } else {
                "InitProducerId"
            },
        );
        peer.negotiated().await;
        assert_eq!(peer.requests.recv().await, None);
    }
}

#[tokio::test]
async fn basic_admin_create_describe_and_delete_work_without_unrelated_apis() {
    let mock = common::Mock::start().await;
    limit_mock(&mock, &[]);
    let mut admin = Admin::new(admin_config(&mock.addr)).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("limited", 2, 1)], 1000, false)
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created.first().unwrap().error_code, 0);
    let topics = admin.describe_topics(["limited"]).await.unwrap();
    assert_eq!(topics.first().unwrap().partitions.len(), 2);
    assert_eq!(
        admin
            .delete_topics(&["limited"], 1000)
            .await
            .unwrap()
            .first()
            .unwrap()
            .error_code,
        0
    );
    assert_eq!(mock.find_coordinator_calls(), 0);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn unavailable_admin_operations_send_no_frames_and_keep_the_socket_open() {
    let mut peer = NegotiationPeer::start(&[]).await;
    let mut admin = Admin::new(admin_config(&peer.addr)).await.unwrap();
    peer.negotiated().await;
    let resource = ConfigResource::topic("t");
    unsupported(
        admin
            .describe_configs(std::slice::from_ref(&resource), false)
            .await,
        "DescribeConfigs",
    );
    unsupported(
        admin
            .create_partitions(&[NewPartitions::increase_to("t", 2)], 1000, false)
            .await,
        "CreatePartitions",
    );
    unsupported(
        admin.alter_configs(&resource, &[], false).await,
        "AlterConfigs",
    );
    unsupported(
        admin
            .delete_records(TopicPartition::new("t", 0), 1, 1000)
            .await,
        "DeleteRecords",
    );
    unsupported(
        admin
            .create_acls(&[AclBinding::allow_topic("t", "User:test")])
            .await,
        "CreateAcls",
    );
    unsupported(
        admin.describe_acls(AclResourceType::Topic).await,
        "DescribeAcls",
    );
    unsupported(
        admin.delete_acls(AclResourceType::Topic).await,
        "DeleteAcls",
    );
    unsupported(
        admin.describe_groups(&["group"], false).await,
        "DescribeGroups",
    );
    unsupported(admin.list_groups(&[], &[]).await, "ListGroups");
    unsupported(admin.delete_groups(&["group"]).await, "DeleteGroups");
    peer.no_more_requests().await;
    admin.close().await.unwrap();
}

#[tokio::test]
async fn advertised_group_operation_without_coordinator_fails_before_metadata() {
    let mut peer = NegotiationPeer::start(&[(DESCRIBE_GROUPS, 0, 6), (DELETE_GROUPS, 0, 2)]).await;
    let mut admin = Admin::new(admin_config(&peer.addr)).await.unwrap();
    peer.negotiated().await;
    unsupported(
        admin.describe_groups(&["group"], false).await,
        "FindCoordinator",
    );
    unsupported(admin.delete_groups(&["group"]).await, "FindCoordinator");
    peer.no_more_requests().await;
    admin.close().await.unwrap();
}

#[tokio::test]
async fn a_supported_configuration_api_keeps_its_negotiated_version() {
    let mock = common::Mock::start().await;
    limit_mock(&mock, &[DESCRIBE_CONFIGS]);
    mock.set_api_max(DESCRIBE_CONFIGS, 0);
    let mut admin = Admin::new(admin_config(&mock.addr)).await.unwrap();
    let results = admin
        .describe_configs(&[ConfigResource::topic("t")], false)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results.first().unwrap().error_code, 0);
    assert_eq!(
        admin.versions().get(&DESCRIBE_CONFIGS).unwrap().max_version,
        0
    );
    unsupported(admin.list_groups(&[], &[]).await, "ListGroups");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn nonoverlapping_optional_versions_are_unavailable_locally() {
    let mut peer = NegotiationPeer::start(&[(DESCRIBE_CONFIGS, 5, 6)]).await;
    let mut admin = Admin::new(admin_config(&peer.addr)).await.unwrap();
    peer.negotiated().await;
    unsupported(
        admin
            .describe_configs(&[ConfigResource::topic("t")], false)
            .await,
        "DescribeConfigs",
    );
    peer.no_more_requests().await;
    admin.close().await.unwrap();
}

#[tokio::test]
async fn documented_empty_group_and_delete_inputs_remain_no_ops() {
    let mut peer = NegotiationPeer::start(&[]).await;
    let mut admin = Admin::new(admin_config(&peer.addr)).await.unwrap();
    peer.negotiated().await;
    assert!(admin.describe_groups(&[], false).await.unwrap().is_empty());
    assert!(admin.delete_groups(&[]).await.unwrap().is_empty());
    assert!(admin.delete_acls_with(&[]).await.unwrap().is_empty());
    assert!(admin
        .delete_records_for(Vec::<(TopicPartition, i64)>::new())
        .await
        .unwrap()
        .is_empty());
    peer.no_more_requests().await;
    admin.close().await.unwrap();
}

#[tokio::test]
async fn transactional_producer_still_discovers_and_commits_when_supported() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(producer_config(&mock.addr).transactional_id("supported-transaction"))
            .await
            .unwrap();
    producer.begin_transaction().await.unwrap();
    let sent = producer
        .send(ProduceRecord::to("t").value(&b"transaction"[..]))
        .await
        .unwrap();
    assert_eq!(sent.offset, 0);
    producer.commit_transaction().await.unwrap();
    assert!(mock.find_coordinator_calls() > 0);
    assert_eq!(
        mock.last_produce_txn_id().as_deref(),
        Some("supported-transaction")
    );
    producer.close().await.unwrap();
}
