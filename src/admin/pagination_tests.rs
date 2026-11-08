use super::*;
use crate::protocol::admin::{
    decode_describe_topic_partitions_request, encode_describe_topic_partitions_response,
};
use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
use crate::protocol::header::{decode_request_header, encode_response_header, RequestHeader};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn admin_and_listener() -> (Admin, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let conn = BrokerConn::connect(&addr, "dtp-test", Duration::from_secs(5))
        .await
        .unwrap();
    let mut admin = Admin::test_holding(conn);
    admin.describe_topic_partitions_version = Some(0);
    (admin, listener)
}

async fn request(socket: &mut TcpStream) -> (RequestHeader, Bytes) {
    let len = usize::try_from(socket.read_i32().await.unwrap()).unwrap();
    assert!(len < 1024 * 1024);
    let mut data = vec![0; len];
    let _read = socket.read_exact(&mut data).await.unwrap();
    let mut body = Bytes::from(data);
    let header = decode_request_header(&mut body).unwrap();
    (header, body)
}

async fn dtp_request(socket: &mut TcpStream, expected: Option<(&str, i32)>) -> RequestHeader {
    let (header, mut body) = request(socket).await;
    assert_eq!(
        (header.api_key, header.api_version),
        (DESCRIBE_TOPIC_PARTITIONS, 0)
    );
    let (names, limit, cursor) = decode_describe_topic_partitions_request(&mut body).unwrap();
    assert_eq!(names, vec!["orders"]);
    assert_eq!(limit, 1);
    assert_eq!(
        cursor
            .as_ref()
            .map(|c| (c.topic_name.as_str(), c.partition_index)),
        expected
    );
    assert!(body.is_empty());
    header
}

fn page(partition: i32, next: Option<(&str, i32)>) -> DescribeTopicPartitionsResponse {
    let mut part = DescribedTopicPartition::new(0);
    part.partition_index = partition;
    part.leader_id = 1;
    let mut topic = DescribedTopicPartitions::new("orders", 0);
    topic.topic_id = [7; 16];
    topic.partitions = vec![part];
    topic.topic_authorized_operations = 37;
    let mut response = DescribeTopicPartitionsResponse::new(vec![topic]);
    response.next_cursor = next.map(|(name, index)| TopicPartitionCursor::new(name, index));
    response
}

async fn send_page(
    socket: &mut TcpStream,
    header: &RequestHeader,
    response: &DescribeTopicPartitionsResponse,
) -> std::io::Result<()> {
    let mut body = BytesMut::new();
    encode_response_header(
        &mut body,
        header.api_key,
        header.api_version,
        header.correlation_id,
    )
    .unwrap();
    encode_describe_topic_partitions_response(&mut body, response).unwrap();
    socket.write_i32(i32::try_from(body.len()).unwrap()).await?;
    socket.write_all(&body).await
}

#[tokio::test]
async fn dtp_pagination_accumulates_complete_pages() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        send_page(&mut socket, &second, &page(1, None))
            .await
            .unwrap();
    });
    let names = ["orders"];
    let result = admin
        .describe_topics_with_partition_limit_timeout(names.iter(), true, 1, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name(), "orders");
    assert_eq!(result[0].topic_id(), Uuid::from([7; 16]));
    assert_eq!(result[0].authorized_operations(), 37);
    assert_eq!(
        result[0]
            .partitions()
            .iter()
            .map(|p| p.partition)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    peer.await.unwrap();
}

async fn repeated_cursor_case(cycle: bool) {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let next = if cycle { 2 } else { 1 };
        send_page(&mut socket, &second, &page(1, Some(("orders", next))))
            .await
            .unwrap();
        if cycle {
            let third = dtp_request(&mut socket, Some(("orders", 2))).await;
            send_page(&mut socket, &third, &page(2, Some(("orders", 1))))
                .await
                .unwrap();
        }
    });
    let names = ["orders"];
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            names.iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    assert!(matches!(got, Err(Error::Protocol(_))), "got {got:?}");
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_pagination_rejects_an_immediate_repeat() {
    repeated_cursor_case(false).await;
}

#[tokio::test]
async fn dtp_pagination_nonadjacent_cycle_times_out_without_partial_result() {
    let (mut admin, listener) = admin_and_listener().await;
    let (fourth_tx, fourth_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        send_page(&mut socket, &second, &page(1, Some(("orders", 2))))
            .await
            .unwrap();
        let third = dtp_request(&mut socket, Some(("orders", 2))).await;
        send_page(&mut socket, &third, &page(2, Some(("orders", 1))))
            .await
            .unwrap();
        let _fourth = dtp_request(&mut socket, Some(("orders", 1))).await;
        fourth_tx.send(()).unwrap();
        release_rx.await.unwrap();
    });
    let names = ["orders"];
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(1),
    ));
    tokio::select! {
        result = &mut call => panic!("finished before the fourth request: {result:?}"),
        got = fourth_rx => got.unwrap(),
    }
    let result = call.await;
    assert!(matches!(result, Err(Error::Timeout)), "got {result:?}");
    release_tx.send(()).unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_pagination_rejects_oversized_timeout_without_panicking() {
    let (mut admin, _listener) = admin_and_listener().await;
    let names = ["orders"];
    let got = admin
        .describe_topics_with_partition_limit_timeout(names.iter(), false, 1, Duration::MAX)
        .await;
    assert!(matches!(got, Err(Error::Timeout)), "got {got:?}");
}

#[tokio::test]
async fn dtp_pagination_deadline_retires_inflight_socket_and_reconnects() {
    let (mut admin, listener) = admin_and_listener().await;
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let (late_tx, late_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        let first = dtp_request(&mut old, None).await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        send_page(&mut old, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut old, Some(("orders", 1))).await;
        second_tx.send(()).unwrap();
        late_rx.await.unwrap();
        let _late_write = send_page(&mut old, &second, &page(999, None)).await;

        let (mut new, _) = listener.accept().await.unwrap();
        let (versions_header, _) = request(&mut new).await;
        assert_eq!(versions_header.api_key, API_VERSIONS);
        let versions = ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: METADATA,
                    min_version: 0,
                    max_version: 12,
                },
                ApiVersion {
                    api_key: DESCRIBE_TOPIC_PARTITIONS,
                    min_version: 0,
                    max_version: 0,
                },
            ],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        encode_response_header(
            &mut body,
            versions_header.api_key,
            versions_header.api_version,
            versions_header.correlation_id,
        )
        .unwrap();
        encode_api_versions_response(&mut body, versions_header.api_version, &versions).unwrap();
        new.write_i32(i32::try_from(body.len()).unwrap())
            .await
            .unwrap();
        new.write_all(&body).await.unwrap();
        let retry = dtp_request(&mut new, None).await;
        send_page(&mut new, &retry, &page(42, None)).await.unwrap();
    });
    let names = ["orders"];
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(2),
    ));
    tokio::select! {
        got = &mut call => panic!("finished before second page: {got:?}"),
        got = second_rx => got.unwrap(),
    }
    let got = tokio::time::timeout(Duration::from_millis(1400), &mut call)
        .await
        .expect("the second page reset the shared deadline");
    assert!(matches!(got, Err(Error::Timeout)), "got {got:?}");
    drop(call);
    assert!(
        admin.conn.is_closed(),
        "timed out connection was not retired"
    );
    late_tx.send(()).unwrap();
    let retry = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(retry.topics[0].partitions[0].partition_index, 42);
    assert!(retry.next_cursor.is_none());
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_raw_request_preserves_next_cursor_and_rejects_zero_timeout() {
    let (mut admin, listener) = admin_and_listener().await;
    let got = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::ZERO)
        .await;
    assert!(matches!(got, Err(Error::Timeout)), "got {got:?}");
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &header, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
    });
    let result = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        result.next_cursor,
        Some(TopicPartitionCursor::new("orders", 1))
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_raw_cancel_during_encoding_keeps_socket_usable() {
    use std::future::Future;

    let (mut admin, listener) = admin_and_listener().await;
    let names = vec!["orders"; 8193];
    let mut call =
        Box::pin(admin.describe_topic_partitions_timeout(&names, 1, None, Duration::from_secs(5)));
    std::future::poll_fn(|cx| {
        assert!(call.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(call);
    assert!(
        !admin.conn.is_closed(),
        "encoding cancellation retired an idle socket"
    );
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &header, &page(42, None))
            .await
            .unwrap();
    });
    let result = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(result.topics[0].partitions[0].partition_index, 42);
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_raw_cancel_after_request_retires_socket() {
    let (mut admin, listener) = admin_and_listener().await;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_request(&mut socket, None).await;
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let _late = send_page(&mut socket, &header, &page(9, None)).await;
    });
    let names = ["orders"];
    let mut call =
        Box::pin(admin.describe_topic_partitions_timeout(&names, 1, None, Duration::from_secs(5)));
    tokio::select! {
        got = &mut call => panic!("finished before cancellation: {got:?}"),
        got = ready_rx => got.unwrap(),
    }
    drop(call);
    assert!(
        admin.conn.is_closed(),
        "inflight cancellation kept the socket usable"
    );
    release_tx.send(()).unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_raw_unsupported_precedes_zero_and_oversized_timeout() {
    let (mut admin, _listener) = admin_and_listener().await;
    admin.describe_topic_partitions_version = None;
    for duration in [Duration::ZERO, Duration::MAX] {
        let got = admin
            .describe_topic_partitions_timeout(&["orders"], 1, None, duration)
            .await;
        assert!(matches!(got, Err(Error::Unsupported(_))), "got {got:?}");
    }
}

#[tokio::test]
async fn dtp_empty_describe_stays_a_noop_at_extreme_timeouts() {
    let (mut admin, _listener) = admin_and_listener().await;
    let names: [&str; 0] = [];
    for duration in [Duration::ZERO, Duration::MAX] {
        let got = admin
            .describe_topics_with_partition_limit_timeout(names.iter(), false, 1, duration)
            .await
            .unwrap();
        assert!(got.is_empty());
    }
}

async fn later_topic_error_case(code: i16) {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut failed = page(1, None);
        failed.topics[0].error_code = code;
        failed.topics[0].topic_id = [0; 16];
        failed.topics[0].partitions.clear();
        send_page(&mut socket, &second, &failed).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            true,
            1,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name(), "orders");
    assert_eq!(result[0].error_code(), code);
    assert!(
        result[0].partitions().is_empty(),
        "failed topic retained partial partitions"
    );
    assert_eq!(result[0].topic_id(), Uuid::from([0; 16]));
}

#[tokio::test]
async fn dtp_pagination_propagates_later_topic_deletion() {
    later_topic_error_case(crate::error::UNKNOWN_TOPIC_OR_PARTITION).await;
}

#[tokio::test]
async fn dtp_pagination_propagates_later_topic_denial() {
    later_topic_error_case(crate::error::TOPIC_AUTHORIZATION_FAILED).await;
}

#[tokio::test]
async fn dtp_pagination_rejects_recreated_topic_with_different_id() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut recreated = page(1, None);
        recreated.topics[0].topic_id = [8; 16];
        send_page(&mut socket, &second, &recreated).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(
        matches!(&result, Err(Error::Protocol(message))
            if message == "DescribeTopicPartitions returned different topic IDs for the same topic"),
        "got {result:?}"
    );
}

#[tokio::test]
async fn dtp_pagination_keeps_first_topic_error_across_later_success() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        for position in 0..2 {
            let (header, mut body) = request(&mut socket).await;
            assert_eq!(
                (header.api_key, header.api_version),
                (DESCRIBE_TOPIC_PARTITIONS, 0)
            );
            let (names, limit, cursor) =
                decode_describe_topic_partitions_request(&mut body).unwrap();
            assert_eq!(names, vec!["orders", "payments"]);
            assert_eq!(limit, 1);
            assert_eq!(
                cursor
                    .as_ref()
                    .map(|c| (c.topic_name.as_str(), c.partition_index)),
                if position == 0 {
                    None
                } else {
                    Some(("orders", 1))
                }
            );
            assert!(body.is_empty());
            let mut response = page(
                position,
                if position == 0 {
                    Some(("orders", 1))
                } else {
                    None
                },
            );
            let payment = if position == 0 {
                let mut denied = DescribedTopicPartitions::new(
                    "payments",
                    crate::error::TOPIC_AUTHORIZATION_FAILED,
                );
                denied.topic_id = [9; 16];
                denied
            } else {
                let mut success = DescribedTopicPartitions::new("payments", 0);
                success.topic_id = [8; 16];
                success.partitions = vec![DescribedTopicPartition::new(0)];
                success
            };
            response.topics.push(payment);
            send_page(&mut socket, &header, &response).await.unwrap();
        }
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders", "payments"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(
        result
            .iter()
            .map(TopicDescription::name)
            .collect::<Vec<_>>(),
        ["orders", "payments"]
    );
    assert_eq!(
        result[0]
            .partitions()
            .iter()
            .map(|p| p.partition)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        result[1].error_code(),
        crate::error::TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(result[1].topic_id(), Uuid::from([9; 16]));
    assert!(
        result[1].partitions().is_empty(),
        "error topic retained later success partitions"
    );
}

async fn unknown_id_case(final_id: [u8; 16], expect_conflict: bool) {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let ids = [[0; 16], [7; 16], [0; 16], final_id];
        for (i, id) in ids.into_iter().enumerate() {
            let index = i32::try_from(i).unwrap();
            let header = dtp_request(
                &mut socket,
                if i == 0 {
                    None
                } else {
                    Some(("orders", index))
                },
            )
            .await;
            let mut response = page(
                index,
                if i == 3 {
                    None
                } else {
                    Some(("orders", index + 1))
                },
            );
            response.topics[0].topic_id = id;
            send_page(&mut socket, &header, &response).await.unwrap();
        }
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    if expect_conflict {
        assert!(
            matches!(&got, Err(Error::Protocol(message))
                if message == "DescribeTopicPartitions returned different topic IDs for the same topic"),
            "got {got:?}"
        );
    } else {
        let result = got.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].topic_id(), Uuid::from([0; 16]));
        assert_eq!(
            result[0]
                .partitions()
                .iter()
                .map(|p| p.partition)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
    }
}

#[tokio::test]
async fn dtp_pagination_checks_known_ids_despite_unknown_pages() {
    unknown_id_case([8; 16], true).await;
}

#[tokio::test]
async fn dtp_pagination_keeps_unknown_id_when_known_pages_agree() {
    unknown_id_case([7; 16], false).await;
}

#[test]
fn dtp_discard_partitions_yields_after_making_progress() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    let make_partitions = || -> Vec<crate::PartitionInfo> {
        (0..16_385)
            .map(|partition| crate::PartitionInfo {
                topic: "orders".into(),
                partition,
                leader: 1,
                leader_epoch: 0,
                replicas: vec![1],
                isr: vec![1],
                offline_replicas: Vec::new(),
            })
            .collect()
    };
    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);

    for polls in 1..=2 {
        let mut partitions = make_partitions();
        let mut future = Box::pin(discard_described_partitions(&mut partitions));
        for _ in 0..polls {
            poll_cleanup_yield(future.as_mut(), &mut context, &wake);
        }
        drop(future);
        assert_eq!(partitions.len(), 16_385 - polls * 8_192);
    }
    let mut partitions = make_partitions();
    let mut future = Box::pin(discard_described_partitions(&mut partitions));
    poll_cleanup_yield(future.as_mut(), &mut context, &wake);
    poll_cleanup_yield(future.as_mut(), &mut context, &wake);
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(())
    ));
    drop(future);
    assert!(partitions.is_empty());
}

#[tokio::test]
async fn dtp_pagination_later_error_with_known_id_keeps_order() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        for position in 0..2 {
            let (header, mut body) = request(&mut socket).await;
            assert_eq!(
                (header.api_key, header.api_version),
                (DESCRIBE_TOPIC_PARTITIONS, 0)
            );
            let (names, limit, cursor) =
                decode_describe_topic_partitions_request(&mut body).unwrap();
            assert_eq!(names, vec!["payments", "orders"]);
            assert_eq!(limit, 1);
            assert_eq!(
                cursor
                    .as_ref()
                    .map(|c| (c.topic_name.as_str(), c.partition_index)),
                if position == 0 {
                    None
                } else {
                    Some(("orders", 1))
                }
            );
            assert!(body.is_empty());
            let response = if position == 0 {
                page(0, Some(("orders", 1)))
            } else {
                let mut response = page(1, None);
                response.topics[0].error_code = crate::error::UNKNOWN_TOPIC_OR_PARTITION;
                response.topics[0].topic_id = [9; 16];
                response.topics[0].partitions.clear();
                let mut payment = DescribedTopicPartitions::new("payments", 0);
                payment.topic_id = [8; 16];
                payment.partitions = vec![DescribedTopicPartition::new(0)];
                response.topics.push(payment);
                response
            };
            send_page(&mut socket, &header, &response).await.unwrap();
        }
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["payments", "orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(
        result
            .iter()
            .map(TopicDescription::name)
            .collect::<Vec<_>>(),
        ["orders", "payments"]
    );
    assert_eq!(
        result[0].error_code(),
        crate::error::UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(result[0].topic_id(), Uuid::from([9; 16]));
    assert!(result[0].partitions().is_empty());
    assert_eq!(result[1].error_code(), 0);
    assert_eq!(result[1].topic_id(), Uuid::from([8; 16]));
    assert_eq!(result[1].partitions().len(), 1);
}

#[test]
fn dtp_pagination_error_and_conflict_cleanup_make_cooperative_progress() {
    use std::sync::Arc;
    use std::task::{Context, Waker};

    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    for identity_conflict in [false, true] {
        for polls in 1..=2 {
            let partitions = (0..16_385)
                .map(|partition| crate::PartitionInfo {
                    topic: "orders".into(),
                    partition,
                    leader: 1,
                    leader_epoch: 0,
                    replicas: vec![1],
                    isr: vec![1],
                    offline_replicas: Vec::new(),
                })
                .collect();
            let mut out = vec![TopicDescription::new(
                "orders", [7; 16], false, 0, partitions,
            )];
            let mut index = DescribedTopicIndex::from([("orders".into(), (0, [7; 16]))]);
            let incoming = if identity_conflict {
                TopicDescription::new("orders", [8; 16], false, 0, Vec::new())
            } else {
                TopicDescription::new(
                    "orders",
                    [0; 16],
                    false,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                    Vec::new(),
                )
            };
            let mut future = Box::pin(extend_described_topics(
                &mut out,
                &mut index,
                vec![incoming],
            ));
            for _ in 0..polls {
                poll_cleanup_yield(future.as_mut(), &mut context, &wake);
            }
            drop(future);
            assert_eq!(out[0].partitions.len(), 16_385 - polls * 8_192);
        }
    }
}

#[tokio::test]
async fn dtp_pagination_id_conflict_fails_the_call_and_keeps_socket_usable() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        for position in 0..3 {
            let (header, mut body) = request(&mut socket).await;
            let (names, limit, cursor) =
                decode_describe_topic_partitions_request(&mut body).unwrap();
            assert_eq!(
                (header.api_key, header.api_version),
                (DESCRIBE_TOPIC_PARTITIONS, 0)
            );
            assert_eq!(names, vec!["a-payments", "orders"]);
            assert_eq!(limit, 1);
            let expected = match position {
                0 => None,
                1 => Some(("orders", 0)),
                _ => Some(("orders", 1)),
            };
            assert_eq!(
                cursor
                    .as_ref()
                    .map(|c| (c.topic_name.as_str(), c.partition_index)),
                expected
            );
            assert!(body.is_empty());
            let response = match position {
                0 => {
                    let mut response = page(0, Some(("orders", 0)));
                    response.topics[0].name = Some("a-payments".into());
                    response.topics[0].topic_id = [6; 16];
                    response
                }
                1 => page(0, Some(("orders", 1))),
                _ => {
                    let mut response = page(1, None);
                    response.topics[0].topic_id = [8; 16];
                    response
                }
            };
            send_page(&mut socket, &header, &response).await.unwrap();
        }
        let header = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &header, &page(42, None))
            .await
            .unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["a-payments", "orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    let error = got.expect_err("a conflict must not return a partial list of topic descriptions");
    assert!(matches!(&error, Error::Protocol(message)
        if message == "DescribeTopicPartitions returned different topic IDs for the same topic"));
    assert!(!error.is_retriable());
    assert!(!admin.conn.is_closed());
    let next = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(next.topics[0].partitions[0].partition_index, 42);
    peer.await.unwrap();
}

#[derive(Default)]
struct CleanupWake(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CleanupWake {
    fn wake(self: std::sync::Arc<Self>) {
        let _previous = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn poll_cleanup_yield<F: std::future::Future>(
    future: std::pin::Pin<&mut F>,
    context: &mut std::task::Context<'_>,
    wake: &CleanupWake,
) {
    let before = wake.0.load(std::sync::atomic::Ordering::SeqCst);
    assert!(future.poll(context).is_pending());
    assert!(wake.0.load(std::sync::atomic::Ordering::SeqCst) > before);
}

#[tokio::test]
async fn dtp_pagination_error_precedes_different_successful_id() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();

        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut error = page(1, Some(("orders", 2)));
        error.topics[0].error_code = crate::error::UNKNOWN_TOPIC_OR_PARTITION;
        error.topics[0].topic_id = [9; 16];
        error.topics[0].partitions.clear();
        send_page(&mut socket, &second, &error).await.unwrap();

        let third = dtp_request(&mut socket, Some(("orders", 2))).await;
        let mut recreated = page(2, None);
        recreated.topics[0].topic_id = [8; 16];
        send_page(&mut socket, &third, &recreated).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name(), "orders");
    assert_eq!(
        result[0].error_code(),
        crate::error::UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(result[0].topic_id(), Uuid::from([9; 16]));
    assert!(result[0].partitions().is_empty());
}

#[tokio::test]
async fn dtp_pagination_repeated_cursor_precedes_topic_error() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut denied = page(1, Some(("orders", 1)));
        denied.topics[0].error_code = crate::error::TOPIC_AUTHORIZATION_FAILED;
        denied.topics[0].topic_id = [0; 16];
        denied.topics[0].partitions.clear();
        send_page(&mut socket, &second, &denied).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(
        matches!(&result, Err(Error::Protocol(message))
            if message == "repeated DescribeTopicPartitions cursor"),
        "got {result:?}"
    );
}

#[tokio::test]
async fn dtp_aggregate_rejects_unnamed_topics_and_raw_preserves_them() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        for _ in 0..2 {
            let (header, mut body) = request(&mut socket).await;
            assert_eq!(
                (header.api_key, header.api_version),
                (DESCRIBE_TOPIC_PARTITIONS, 0)
            );
            let (names, limit, cursor) =
                decode_describe_topic_partitions_request(&mut body).unwrap();
            assert_eq!(names, vec!["orders", "payments"]);
            assert_eq!(limit, 1);
            assert!(cursor.is_none());
            assert!(body.is_empty());

            let mut first = DescribedTopicPartitions::new("orders", 0);
            first.name = None;
            first.topic_id = [7; 16];
            let mut second = DescribedTopicPartitions::new("payments", 0);
            second.name = None;
            second.topic_id = [8; 16];
            let response = DescribeTopicPartitionsResponse::new(vec![first, second]);
            send_page(&mut socket, &header, &response).await.unwrap();
        }
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders", "payments"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    let error = got.expect_err("unnamed topics cannot be associated with requested names");
    assert!(matches!(&error, Error::Unsupported(message)
        if message == "cannot aggregate DescribeTopicPartitions without a topic name"));
    assert!(!error.is_retriable());
    assert!(!admin.conn.is_closed());

    let raw = admin
        .describe_topic_partitions_timeout(&["orders", "payments"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(raw.topics.len(), 2);
    assert!(raw.topics[0].name.is_none());
    assert!(raw.topics[1].name.is_none());
    assert_eq!(raw.topics[0].topic_id, [7; 16]);
    assert_eq!(raw.topics[1].topic_id, [8; 16]);
    assert!(raw.next_cursor.is_none());
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_aggregate_rejects_unnamed_topic_after_partial_result() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut unnamed = page(1, None);
        unnamed.topics[0].name = None;
        send_page(&mut socket, &second, &unnamed).await.unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(matches!(&got, Err(Error::Unsupported(message))
        if message == "cannot aggregate DescribeTopicPartitions without a topic name"));
}

#[tokio::test]
async fn dtp_aggregate_distinguishes_empty_name_from_null() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        let mut first_page = page(0, Some(("orders", 1)));
        first_page.topics[0].name = Some(String::new());
        send_page(&mut socket, &first, &first_page).await.unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut second_page = page(1, None);
        second_page.topics[0].name = Some(String::new());
        send_page(&mut socket, &second, &second_page).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name(), "");
    assert_eq!(result[0].topic_id(), Uuid::from([7; 16]));
    assert_eq!(
        result[0]
            .partitions()
            .iter()
            .map(|p| p.partition)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[tokio::test]
async fn dtp_repeated_cursor_precedes_unnamed_topic() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut unnamed = page(1, Some(("orders", 1)));
        unnamed.topics[0].name = None;
        send_page(&mut socket, &second, &unnamed).await.unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "repeated DescribeTopicPartitions cursor"));
}

#[test]
fn dtp_unnamed_rejection_cleans_cooperatively_and_checks_deadline() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    for polls in 1..=2 {
        let partitions = (0..16_385)
            .map(|partition| crate::PartitionInfo {
                topic: "orders".into(),
                partition,
                leader: 1,
                leader_epoch: 0,
                replicas: vec![1],
                isr: vec![1],
                offline_replicas: Vec::new(),
            })
            .collect();
        let mut out = vec![TopicDescription::new(
            "orders", [7; 16], false, 0, partitions,
        )];
        let mut page = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut future = Box::pin(reject_unnamed_described_topic(
            &mut page, &mut out, deadline,
        ));
        for _ in 0..polls {
            poll_cleanup_yield(future.as_mut(), &mut context, &wake);
        }
        drop(future);
        assert_eq!(out[0].partitions.len(), 16_385 - polls * 8_192);
    }

    let expired = tokio::time::Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap();
    let mut page = Vec::new();
    let mut out = Vec::new();
    let mut future = Box::pin(reject_unnamed_described_topic(&mut page, &mut out, expired));
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Error::Timeout)
    ));
}

#[tokio::test]
async fn dtp_unnamed_topic_precedes_same_page_identity_conflict() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let mut response = page(1, None);
        response.topics[0].topic_id = [8; 16];
        let mut unnamed = DescribedTopicPartitions::new("payments", 0);
        unnamed.name = None;
        unnamed.topic_id = [9; 16];
        response.topics.push(unnamed);
        send_page(&mut socket, &second, &response).await.unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(matches!(&got, Err(Error::Unsupported(message))
        if message == "cannot aggregate DescribeTopicPartitions without a topic name"));
}

/// Run the public Admin operation against a single mock response and time each
/// poll. The peer checks every requested name and returns the same names.
#[tokio::test(flavor = "current_thread")]
async fn dtp_public_row_growth_poll_witness() {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let count: usize = std::env::var("PL_ROW_PUBLIC_COUNT").unwrap().parse().unwrap();
    assert!(count > 0);
    let kind = std::env::var("PL_ROW_PUBLIC_KIND").unwrap();
    let error = match kind.as_str() {
        "empty" => 0,
        "error" => crate::error::TOPIC_AUTHORIZATION_FAILED,
        _ => panic!("unknown row kind"),
    };
    let names: Vec<String> = (0..count).map(|i| format!("topic-{i:08}")).collect();
    let topics = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let mut topic = DescribedTopicPartitions::new(name.clone(), error);
            if error == 0 {
                topic.topic_id[..8].copy_from_slice(&(i as u64).to_be_bytes());
                topic.topic_id[15] = 7;
            }
            topic
        })
        .collect();
    let response = DescribeTopicPartitionsResponse::new(topics);
    let mut response_body = BytesMut::new();
    encode_describe_topic_partitions_response(&mut response_body, &response).unwrap();
    let wire_body_bytes = response_body.len();
    assert!(wire_body_bytes + 32 < usize::try_from(crate::net::MAX_FRAME).unwrap());
    drop(response);

    let (mut admin, listener) = admin_and_listener().await;
    let response_written = Arc::new(AtomicBool::new(false));
    let peer_written = Arc::clone(&response_written);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let request_len = usize::try_from(socket.read_i32().await.unwrap()).unwrap();
        assert!(request_len < usize::try_from(crate::net::MAX_FRAME).unwrap());
        let mut request_bytes = vec![0; request_len];
        let _read = socket.read_exact(&mut request_bytes).await.unwrap();
        let mut request_body = Bytes::from(request_bytes);
        let header = decode_request_header(&mut request_body).unwrap();
        assert_eq!((header.api_key, header.api_version), (DESCRIBE_TOPIC_PARTITIONS, 0));
        let (requested, limit, cursor) =
            decode_describe_topic_partitions_request(&mut request_body).unwrap();
        assert_eq!(requested.len(), count);
        for (i, name) in requested.iter().enumerate() {
            assert_eq!(name, &format!("topic-{i:08}"));
        }
        assert_eq!(limit, 1);
        assert!(cursor.is_none());
        assert!(request_body.is_empty());
        drop(requested);
        let mut response_header = BytesMut::new();
        encode_response_header(
            &mut response_header,
            header.api_key,
            header.api_version,
            header.correlation_id,
        )
        .unwrap();
        socket.write_i32(i32::try_from(response_header.len() + response_body.len()).unwrap()).await.unwrap();
        socket.write_all(&response_header).await.unwrap();
        socket.write_all(&response_body).await.unwrap();
        peer_written.store(true, Ordering::SeqCst);
    });

    struct ResetTrace;
    impl Drop for ResetTrace {
        fn drop(&mut self) {
            DTP_TRACE_ACTIVE.with(|value| value.set(false));
            DTP_TRACE_PREALLOC.with(|value| value.set(false));
            DTP_TRACE_RESPONSE_START.with(|value| value.set(None));
            DTP_TRACE_INDEX_START.with(|value| value.set(None));
            DTP_TRACE_EVENTS.with(|events| events.borrow_mut().clear());
        }
    }

    let prealloc = std::env::var("PL_DTP_PREALLOC").as_deref() == Ok("1");
    DTP_TRACE_ACTIVE.with(|value| value.set(true));
    DTP_TRACE_PREALLOC.with(|value| value.set(prealloc));
    DTP_TRACE_RESPONSE_START.with(|value| value.set(None));
    DTP_TRACE_INDEX_START.with(|value| value.set(None));
    DTP_TRACE_EVENTS.with(|events| *events.borrow_mut() = Vec::with_capacity(64));
    let _reset_trace = ResetTrace;
    for response in [true, false] {
        for _ in 0..16 {
            let _end = DtpDropPhase { response, start: false };
            let _start = DtpDropPhase { response, start: true };
        }
    }
    let calibration = DTP_TRACE_EVENTS.with(|events| events.borrow().clone());
    assert_eq!(calibration.len(), 32);
    let response_calibration = calibration.iter().filter(|event| event.kind == "response_drop").map(|event| event.elapsed_ns).max().unwrap();
    let index_calibration = calibration.iter().filter(|event| event.kind == "index_drop").map(|event| event.elapsed_ns).max().unwrap();
    DTP_TRACE_EVENTS.with(|events| events.borrow_mut().clear());
    let trace_capacity_start = DTP_TRACE_EVENTS.with(|events| events.borrow().capacity());
    let start = std::time::Instant::now();
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(120),
    ));
    let mut all_polls = 0usize;
    let mut after_write_polls = 0usize;
    let mut max_before_write_ns = 0u128;
    let mut max_after_write_ns = 0u128;
    let mut max_pending_after_write_ns = 0u128;
    let mut ready_poll_ns = 0u128;
    let mut ready_after_write = false;
    let mut max_pending_events = (0usize, 0usize);
    let mut ready_events = (0usize, 0usize);
    let result = std::future::poll_fn(|cx| {
        let after_write = response_written.load(Ordering::SeqCst);
        let event_before = DTP_TRACE_EVENTS.with(|events| events.borrow().len());
        let poll_start = std::time::Instant::now();
        let polled = call.as_mut().poll(cx);
        let elapsed_ns = poll_start.elapsed().as_nanos();
        let event_after = DTP_TRACE_EVENTS.with(|events| events.borrow().len());
        all_polls += 1;
        if polled.is_ready() {
            ready_poll_ns = elapsed_ns;
            ready_after_write = after_write;
            ready_events = (event_before, event_after);
        } else if after_write && elapsed_ns > max_pending_after_write_ns {
            max_pending_after_write_ns = elapsed_ns;
            max_pending_events = (event_before, event_after);
        }
        if after_write {
            after_write_polls += 1;
            max_after_write_ns = max_after_write_ns.max(elapsed_ns);
        } else {
            max_before_write_ns = max_before_write_ns.max(elapsed_ns);
        }
        polled
    }).await.unwrap();
    let total_ns = start.elapsed().as_nanos();
    drop(call);
    peer.await.unwrap();
    assert!(response_written.load(Ordering::SeqCst));
    assert!(after_write_polls > 0);
    assert!(ready_after_write);
    assert_eq!(result.len(), count);
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row.name, names[i]);
        assert_eq!(row.error_code, error);
        assert!(row.partitions.is_empty());
        assert_eq!(row.authorized_operations, AUTHORIZED_OPERATIONS_OMITTED);
        if error == 0 {
            assert_eq!(&row.topic_id[..8], &(i as u64).to_be_bytes());
            assert_eq!(row.topic_id[15], 7);
        }
    }
    let events = DTP_TRACE_EVENTS.with(|events| events.borrow().clone());
    let growth_count = events.iter().filter(|event| event.kind == "growth").count();
    let reserve_count = events.iter().filter(|event| event.kind == "reserve").count();
    let response_positions: Vec<_> = events.iter().enumerate().filter_map(|(index, event)| (event.kind == "response_drop").then_some(index)).collect();
    let index_positions: Vec<_> = events.iter().enumerate().filter_map(|(index, event)| (event.kind == "index_drop").then_some(index)).collect();
    assert_eq!(response_positions.len(), 1);
    assert_eq!(index_positions.len(), 1);
    assert!(response_positions[0] < index_positions[0]);
    assert!((ready_events.0..ready_events.1).contains(&response_positions[0]));
    assert!((ready_events.0..ready_events.1).contains(&index_positions[0]));
    assert_eq!(events.iter().filter(|event| event.kind == "hash_fingerprint").count(), 1);
    let trace_capacity_end = DTP_TRACE_EVENTS.with(|events| events.borrow().capacity());
    assert_eq!(trace_capacity_end, trace_capacity_start, "trace event storage reallocated");
    if prealloc {
        assert_eq!(reserve_count, 1);
        assert_eq!(growth_count, 0);
    } else {
        assert_eq!(reserve_count, 0);
        assert!(growth_count > 0);
    }
    println!(
        "ROW_GROWTH rows={count} kind={kind} prealloc={prealloc} all_polls={all_polls} after_write_polls={after_write_polls} max_before_write_ns={max_before_write_ns} max_after_write_ns={max_after_write_ns} max_pending_after_write_ns={max_pending_after_write_ns} ready_poll_ns={ready_poll_ns} total_ns={total_ns} wire_body_bytes={wire_body_bytes} growth_count={growth_count} reserve_count={reserve_count} trace_capacity_start={trace_capacity_start} trace_capacity_end={trace_capacity_end} response_calibration_max_ns={response_calibration} index_calibration_max_ns={index_calibration} max_pending_event_start={} max_pending_event_end={} ready_event_start={} ready_event_end={}",
        max_pending_events.0,
        max_pending_events.1,
        ready_events.0,
        ready_events.1,
    );
    for (event_index, event) in events.iter().enumerate() {
        println!(
            "DTP_EVENT rows={count} kind={kind} prealloc={prealloc} index={event_index} phase={} len={} before={} after={} elapsed_ns={} hash_fingerprint={} in_max_pending={} in_ready={}",
            event.kind,
            event.len,
            event.before,
            event.after,
            event.elapsed_ns,
            event.hash_fingerprint,
            (max_pending_events.0..max_pending_events.1).contains(&event_index),
            (ready_events.0..ready_events.1).contains(&event_index),
        );
    }
}
