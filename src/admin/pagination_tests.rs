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
        let first = dtp_named_request(&mut socket, &[""], None).await;
        let mut first_page = page(0, Some(("", 1)));
        first_page.topics[0].name = Some(String::new());
        send_page(&mut socket, &first, &first_page).await.unwrap();
        let second = dtp_named_request(&mut socket, &[""], Some(("", 1))).await;
        let mut second_page = page(1, None);
        second_page.topics[0].name = Some(String::new());
        send_page(&mut socket, &second, &second_page).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout([""].iter(), false, 1, Duration::from_secs(5))
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

async fn dtp_named_request(
    socket: &mut TcpStream,
    expected_names: &[&str],
    expected_cursor: Option<(&str, i32)>,
) -> RequestHeader {
    let (header, mut body) = request(socket).await;
    assert_eq!(
        (header.api_key, header.api_version),
        (DESCRIBE_TOPIC_PARTITIONS, 0)
    );
    let (names, limit, cursor) = decode_describe_topic_partitions_request(&mut body).unwrap();
    let expected_names: Vec<String> = expected_names
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    assert_eq!(names, expected_names);
    assert_eq!(limit, 1);
    assert_eq!(
        cursor
            .as_ref()
            .map(|c| (c.topic_name.as_str(), c.partition_index)),
        expected_cursor
    );
    assert!(body.is_empty());
    header
}

async fn unrequested_topic_case(name: &'static str, error_code: i16, after_partial: bool) {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        let header = if after_partial {
            send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
                .await
                .unwrap();
            dtp_named_request(&mut socket, &["orders"], Some(("orders", 1))).await
        } else {
            first
        };
        let mut unexpected = page(1, None);
        unexpected.topics[0].name = Some(name.into());
        unexpected.topics[0].error_code = error_code;
        if error_code != 0 {
            unexpected.topics[0].partitions.clear();
        }
        send_page(&mut socket, &header, &unexpected).await.unwrap();
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
    assert!(
        matches!(&got, Err(Error::Protocol(message))
            if message == "DescribeTopicPartitions returned an unrequested topic"),
        "got {got:?}"
    );
    if let Err(error) = &got {
        assert!(!error.is_retriable());
    }
}

#[tokio::test]
async fn dtp_requested_names_reject_unsolicited_success() {
    unrequested_topic_case("payments", 0, false).await;
}

#[tokio::test]
async fn dtp_requested_names_reject_unsolicited_error() {
    unrequested_topic_case("payments", crate::error::TOPIC_AUTHORIZATION_FAILED, false).await;
}

#[tokio::test]
async fn dtp_requested_names_reject_unsolicited_after_partial_result() {
    unrequested_topic_case("payments", 0, true).await;
}

#[tokio::test]
async fn dtp_requested_names_reject_unrequested_empty_name() {
    unrequested_topic_case("", 0, false).await;
}

#[tokio::test]
async fn dtp_requested_names_raw_empty_request_still_means_all() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &[], None).await;
        let mut response = page(0, None);
        response.topics[0].name = Some("payments".into());
        send_page(&mut socket, &header, &response).await.unwrap();
    });
    let raw = admin
        .describe_topic_partitions_timeout(&[], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(raw.topics[0].name.as_deref(), Some("payments"));
}

#[tokio::test]
async fn dtp_requested_names_allow_duplicate_inputs_and_requested_errors() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders", "orders", "payments"], None).await;
        let mut response = page(0, None);
        response.topics.push(DescribedTopicPartitions::new(
            "payments",
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        ));
        send_page(&mut socket, &header, &response).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders", "orders", "payments"].iter(),
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
    assert_eq!(result[0].error_code(), 0);
    assert_eq!(
        result[1].error_code(),
        crate::error::TOPIC_AUTHORIZATION_FAILED
    );
}

#[tokio::test]
async fn dtp_requested_names_metadata_fallback_preserves_projection() {
    use crate::protocol::api::{decode_metadata_request, encode_metadata_response, TopicMetadata};
    let (mut admin, listener) = admin_and_listener().await;
    admin.describe_topic_partitions_version = None;
    admin.metadata_version = 4;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (header, mut body) = request(&mut socket).await;
        assert_eq!((header.api_key, header.api_version), (METADATA, 4));
        let (names, allow_auto, _include_topic, _include_cluster) =
            decode_metadata_request(&mut body, 4).unwrap();
        assert_eq!(
            names,
            Some(vec!["orders".into(), "orders".into(), "missing".into()])
        );
        assert!(!allow_auto);
        assert!(body.is_empty());
        let mut response = MetadataResponse::blank();
        response.topics = vec![
            TopicMetadata::new(0, "orders", false, Vec::new()),
            TopicMetadata::new(0, "unrequested", false, Vec::new()),
            TopicMetadata::new(
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                "missing",
                false,
                Vec::new(),
            ),
        ];
        let mut encoded = BytesMut::new();
        encode_response_header(
            &mut encoded,
            header.api_key,
            header.api_version,
            header.correlation_id,
        )
        .unwrap();
        encode_metadata_response(&mut encoded, 4, &response).unwrap();
        socket
            .write_i32(i32::try_from(encoded.len()).unwrap())
            .await
            .unwrap();
        socket.write_all(&encoded).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders", "orders", "missing"].iter(),
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
        ["orders", "orders", "missing"]
    );
    assert_eq!(result[0].error_code(), 0);
    assert_eq!(result[1].error_code(), 0);
    assert_eq!(
        result[2].error_code(),
        crate::error::UNKNOWN_TOPIC_OR_PARTITION
    );
}

#[tokio::test]
async fn dtp_requested_names_reject_unsolicited_cursor() {
    let (mut admin, listener) = admin_and_listener().await;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("payments", 0))))
            .await
            .unwrap();
        tokio::select! {
            _ = done_rx => {}
            header = dtp_named_request(&mut socket, &["orders"], Some(("payments", 0))) => {
                send_page(&mut socket, &header, &page(1, None)).await.unwrap();
            }
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
    let _sent = done_tx.send(());
    peer.await.unwrap();
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "DescribeTopicPartitions returned an unrequested cursor topic"));
}

#[tokio::test]
async fn dtp_requested_names_raw_keeps_unsolicited_cursor() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("payments", 0))))
            .await
            .unwrap();
    });
    let raw = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(
        raw.next_cursor,
        Some(TopicPartitionCursor::new("payments", 0))
    );
}

#[tokio::test]
async fn dtp_requested_names_null_precedes_unsolicited_row_and_cursor() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        let mut response = page(0, Some(("another-unrequested", 0)));
        response.topics[0].name = Some("payments".into());
        let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
        unnamed.name = None;
        response.topics.push(unnamed);
        send_page(&mut socket, &header, &response).await.unwrap();
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
async fn dtp_requested_names_unsolicited_row_precedes_unsolicited_cursor() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        let mut response = page(0, Some(("another-unrequested", 0)));
        response.topics[0].name = Some("payments".into());
        send_page(&mut socket, &header, &response).await.unwrap();
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
        if message == "DescribeTopicPartitions returned an unrequested topic"));
}

#[tokio::test]
async fn dtp_requested_names_repeated_cursor_precedes_unsolicited_rows() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_named_request(&mut socket, &["orders"], Some(("orders", 1))).await;
        let mut response = page(1, Some(("orders", 1)));
        response.topics[0].name = Some("payments".into());
        let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
        unnamed.name = None;
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
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "repeated DescribeTopicPartitions cursor"));
}

#[tokio::test]
async fn dtp_requested_names_cursor_precedes_same_page_id_conflict() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_named_request(&mut socket, &["orders"], Some(("orders", 1))).await;
        let mut response = page(1, Some(("payments", 0)));
        response.topics[0].topic_id = [8; 16];
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
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "DescribeTopicPartitions returned an unrequested cursor topic"));
}

#[tokio::test]
async fn dtp_requested_names_row_precedes_same_page_id_conflict() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_named_request(&mut socket, &["orders"], Some(("orders", 1))).await;
        let mut response = page(1, None);
        response.topics[0].topic_id = [8; 16];
        response
            .topics
            .push(DescribedTopicPartitions::new("payments", 0));
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
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "DescribeTopicPartitions returned an unrequested topic"));
}

#[tokio::test]
async fn dtp_requested_names_errors_do_not_copy_long_unrequested_names() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        let mut response = page(0, None);
        response.topics[0].name = Some("x".repeat(128 * 1024));
        send_page(&mut socket, &header, &response).await.unwrap();
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
        if message == "DescribeTopicPartitions returned an unrequested topic"));
}

#[tokio::test]
async fn dtp_requested_names_large_input_accepts_members_and_duplicates() {
    let (mut admin, listener) = admin_and_listener().await;
    let mut names: Vec<String> = (0..12).map(|n| format!("topic-{n}")).collect();
    names.push(names[0].clone());
    let peer_names = names.clone();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let expected: Vec<&str> = peer_names.iter().map(String::as_str).collect();
        let header = dtp_named_request(&mut socket, &expected, None).await;
        let mut response = page(0, None);
        response.topics[0].name = Some(peer_names[11].clone());
        response.topics.push(DescribedTopicPartitions::new(
            peer_names[0].as_str(),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        ));
        send_page(&mut socket, &header, &response).await.unwrap();
    });
    let result = admin
        .describe_topics_with_partition_limit_timeout(
            names.iter(),
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
        ["topic-11", "topic-0"]
    );
    assert_eq!(
        result[1].error_code(),
        crate::error::TOPIC_AUTHORIZATION_FAILED
    );
}

#[test]
fn dtp_requested_names_index_yields_after_progress() {
    use std::sync::Arc;
    use std::task::{Context, Waker};

    let owned: Vec<String> = (0..16_385).map(|n| format!("topic-{n}")).collect();
    let names: Vec<&str> = owned.iter().map(String::as_str).collect();
    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    for polls in 1..=2 {
        let mut index = HashSet::new();
        let mut future = Box::pin(index_requested_dtp_names(&names, &mut index));
        for _ in 0..polls {
            poll_cleanup_yield(future.as_mut(), &mut context, &wake);
        }
        drop(future);
        assert_eq!(index.len(), polls * 8_192);
    }
}

#[tokio::test]
async fn dtp_requested_names_index_stores_only_distinct_borrowed_names() {
    let names = vec!["orders"; 16_385];
    let mut index = HashSet::new();
    index_requested_dtp_names(&names, &mut index).await;
    assert_eq!(index.len(), 1);
    assert!(index.contains("orders"));
}

#[test]
fn dtp_requested_names_scan_yields_after_progress() {
    use std::cell::Cell;
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    let mut topics: Vec<_> = (0..16_385)
        .map(|_| DescribedTopicPartitions::new("orders", 0))
        .collect();
    topics.last_mut().unwrap().name = None;
    let seen = Cell::new(0usize);
    let iter = topics.iter().inspect(|_| seen.set(seen.get() + 1));
    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(remaining_dtp_has_unnamed(iter));
    poll_cleanup_yield(future.as_mut(), &mut context, &wake);
    assert_eq!(seen.get(), 8_192);
    poll_cleanup_yield(future.as_mut(), &mut context, &wake);
    assert_eq!(seen.get(), 16_384);
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(true)
    ));
    assert_eq!(seen.get(), 16_385);
}

#[tokio::test]
async fn dtp_requested_names_cancel_during_index_keeps_socket_usable() {
    use std::future::Future;

    let (mut admin, listener) = admin_and_listener().await;
    let names = vec!["orders"; 8_193];
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(5),
    ));
    std::future::poll_fn(|cx| {
        assert!(call.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(call);
    assert!(!admin.conn.is_closed());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &header, &page(42, None))
            .await
            .unwrap();
    });
    let raw = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(raw.topics[0].partitions[0].partition_index, 42);
    peer.await.unwrap();
}

#[test]
fn dtp_requested_names_rejection_cleans_and_checks_deadline() {
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
        let mut future = Box::pin(reject_unrequested_described_topic(
            &mut page,
            &mut out,
            deadline,
            "fixed rejection",
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
    let mut future = Box::pin(reject_unrequested_described_topic(
        &mut page,
        &mut out,
        expired,
        "fixed rejection",
    ));
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Error::Timeout)
    ));
}

#[tokio::test]
async fn dtp_requested_names_raw_keeps_unsolicited_topic() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        let mut response = page(0, None);
        response.topics[0].name = Some("payments".into());
        send_page(&mut socket, &header, &response).await.unwrap();
    });
    let raw = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(raw.topics[0].name.as_deref(), Some("payments"));
}

#[tokio::test]
async fn dtp_requested_names_are_case_sensitive() {
    unrequested_topic_case("Orders", 0, false).await;
}

#[tokio::test]
async fn dtp_requested_names_rejection_keeps_socket_usable_after_partial_result() {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let first = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1))))
            .await
            .unwrap();
        let second = dtp_named_request(&mut socket, &["orders"], Some(("orders", 1))).await;
        let mut unexpected = page(1, None);
        unexpected.topics[0].name = Some("payments".into());
        send_page(&mut socket, &second, &unexpected).await.unwrap();
        let raw = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &raw, &page(42, None)).await.unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            ["orders"].iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "DescribeTopicPartitions returned an unrequested topic"));
    assert!(!admin.conn.is_closed());
    let raw = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(raw.topics[0].partitions[0].partition_index, 42);
    peer.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dtp_requested_names_cancel_during_scan_keeps_socket_usable() {
    struct ResetScanProbe(Option<usize>);
    impl Drop for ResetScanProbe {
        fn drop(&mut self) {
            DTP_REMAINING_SCAN_PROGRESS.with(|probe| probe.set(self.0));
        }
    }

    let previous = DTP_REMAINING_SCAN_PROGRESS.with(|probe| probe.replace(Some(0)));
    let _reset = ResetScanProbe(previous);
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let header = dtp_named_request(&mut socket, &["orders"], None).await;
        let mut unexpected = DescribedTopicPartitions::new("payments", 0);
        unexpected.topic_id = [8; 16];
        let mut topics = Vec::with_capacity(40_001);
        topics.push(unexpected);
        topics.extend((0..40_000).map(|_| {
            DescribedTopicPartitions::new("orders", crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        }));
        let response = DescribeTopicPartitionsResponse::new(topics);
        send_page(&mut socket, &header, &response).await.unwrap();
        let raw = dtp_named_request(&mut socket, &["orders"], None).await;
        send_page(&mut socket, &raw, &page(42, None)).await.unwrap();
    });
    let names = ["orders"];
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(5),
    ));
    loop {
        tokio::select! {
            biased;
            result = &mut call => panic!("finished before scan cancellation: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        let scanned = DTP_REMAINING_SCAN_PROGRESS.with(|probe| probe.get().unwrap_or(0));
        if scanned >= 8_192 {
            break;
        }
    }
    drop(call);
    assert!(!admin.conn.is_closed());
    let raw = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(raw.topics[0].partitions[0].partition_index, 42);
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_requested_names_large_input_rejects_unsolicited_topic() {
    let (mut admin, listener) = admin_and_listener().await;
    let names: Vec<String> = (0..12).map(|n| format!("topic-{n}")).collect();
    let peer_names = names.clone();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let expected: Vec<&str> = peer_names.iter().map(String::as_str).collect();
        let header = dtp_named_request(&mut socket, &expected, None).await;
        let mut response = page(0, None);
        response.topics[0].name = Some("unrequested".into());
        send_page(&mut socket, &header, &response).await.unwrap();
    });
    let got = admin
        .describe_topics_with_partition_limit_timeout(
            names.iter(),
            false,
            1,
            Duration::from_secs(5),
        )
        .await;
    peer.await.unwrap();
    assert!(matches!(&got, Err(Error::Protocol(message))
        if message == "DescribeTopicPartitions returned an unrequested topic"));
}

async fn convert_described_topic_page(
    topics: &[DescribedTopicPartitions],
    include_authorized_operations: bool,
    page: &mut Vec<TopicDescription>,
    out: &mut [TopicDescription],
    deadline: tokio::time::Instant,
    converted_rows: &mut usize,
) -> Result<()> {
    let mut topics = topics.iter();
    let unrequested = convert_described_requested_page(
        &mut topics,
        |_| true,
        include_authorized_operations,
        page,
        out,
        deadline,
        converted_rows,
    )
    .await?;
    assert!(!unrequested);
    Ok(())
}

fn dtp_conversion_rows(start: usize, count: usize) -> Vec<DescribedTopicPartitions> {
    (start..start + count)
        .map(|index| {
            let mut topic = DescribedTopicPartitions::new(format!("topic-{index:08}"), 0);
            topic.topic_id[15] = 7;
            topic.topic_authorized_operations = 37;
            topic
        })
        .collect()
}

#[test]
fn dtp_conversion_yields_on_the_exact_row_boundary() {
    use std::future::Future;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    for count in [8191, 8192, 8193] {
        let topics = dtp_conversion_rows(0, count);
        let mut page = Vec::with_capacity(count);
        let mut out: Vec<TopicDescription> = Vec::new();
        let mut budget = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let wake = Arc::new(CleanupWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(convert_described_topic_page(
            &topics,
            true,
            &mut page,
            &mut out,
            deadline,
            &mut budget,
        ));
        let first = future.as_mut().poll(&mut context);
        if count < 8192 {
            assert!(matches!(first, Poll::Ready(Ok(()))));
            assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        } else {
            assert!(matches!(first, Poll::Pending));
            assert!(wake.0.load(Ordering::SeqCst) > 0);
            assert!(matches!(
                future.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
        }
        drop(future);
        assert_eq!(budget, count % 8192);
        assert_eq!(page.len(), count);
        assert!(out.is_empty());
        for (index, result) in page.iter().enumerate() {
            assert_eq!(result.name, format!("topic-{index:08}"));
            assert_eq!(result.topic_id[15], 7);
            assert_eq!(result.error_code, 0);
            assert!(result.partitions.is_empty());
            assert_eq!(result.authorized_operations, 37);
        }
    }

    // Drop after the first pending poll to inspect the exact conversion frontier.
    for count in [8192, 8193] {
        let topics = dtp_conversion_rows(0, count);
        let mut page = Vec::with_capacity(count);
        let mut out: Vec<TopicDescription> = Vec::new();
        let mut budget = 0;
        let wake = Arc::new(CleanupWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(convert_described_topic_page(
            &topics,
            false,
            &mut page,
            &mut out,
            tokio::time::Instant::now() + Duration::from_secs(30),
            &mut budget,
        ));
        assert!(future.as_mut().poll(&mut context).is_pending());
        drop(future);
        assert_eq!(page.len(), 8192);
        assert_eq!(budget, 0);
        assert!(out.is_empty());
        assert!(page
            .iter()
            .all(|row| row.authorized_operations == AUTHORIZED_OPERATIONS_OMITTED));
    }
}

#[test]
fn dtp_conversion_carries_the_row_budget_across_pages() {
    use std::future::Future;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    let first = dtp_conversion_rows(0, 8191);
    let last = dtp_conversion_rows(8191, 1);
    let next = dtp_conversion_rows(8192, 1);
    let mut page = Vec::new();
    let mut out: Vec<TopicDescription> = Vec::new();
    let mut budget = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);

    let mut initial = Box::pin(convert_described_topic_page(
        &first,
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(matches!(
        initial.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(initial);
    assert_eq!(budget, 8191);
    assert_eq!(page.len(), 8191);

    page.clear();
    let mut at_boundary = Box::pin(convert_described_topic_page(
        &last,
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(at_boundary.as_mut().poll(&mut context).is_pending());
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        at_boundary.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(at_boundary);
    assert_eq!(budget, 0);
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, "topic-00008191");

    page.clear();
    let mut after = Box::pin(convert_described_topic_page(
        &next,
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(matches!(
        after.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(after);
    assert_eq!(budget, 1);
    assert_eq!(page[0].name, "topic-00008192");
}

#[test]
fn dtp_conversion_keeps_partition_yields_and_ignores_error_partitions() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    let mut topic = DescribedTopicPartitions::new("orders", 0);
    topic.partitions = (0..8192)
        .map(|partition| {
            let mut item = DescribedTopicPartition::new(0);
            item.partition_index = partition;
            item.replica_nodes = vec![1, 2];
            item.isr_nodes = vec![2];
            item
        })
        .collect();
    let mut page = Vec::new();
    let mut out: Vec<TopicDescription> = Vec::new();
    let mut budget = 0;
    let wake = Arc::new(CleanupWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    let mut future = Box::pin(convert_described_topic_page(
        std::slice::from_ref(&topic),
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(future.as_mut().poll(&mut context).is_pending());
    drop(future);
    assert!(page.is_empty());
    assert_eq!(budget, 0);

    let mut future = Box::pin(convert_described_topic_page(
        std::slice::from_ref(&topic),
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(future.as_mut().poll(&mut context).is_pending());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(future);
    assert_eq!(budget, 1);
    assert_eq!(page[0].partitions.len(), 8192);
    assert_eq!(page[0].partitions[0].replicas, [1, 2]);
    assert_eq!(page[0].partitions[8191].partition, 8191);

    page.clear();
    topic.error_code = crate::error::TOPIC_AUTHORIZATION_FAILED;
    let mut future = Box::pin(convert_described_topic_page(
        std::slice::from_ref(&topic),
        false,
        &mut page,
        &mut out,
        deadline,
        &mut budget,
    ));
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(future);
    assert_eq!(page[0].error_code, crate::error::TOPIC_AUTHORIZATION_FAILED);
    assert!(page[0].partitions.is_empty());
    assert_eq!(budget, 2);
}

#[test]
fn dtp_conversion_unnamed_after_checkpoint_keeps_error_and_deadline_rules() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    for expired in [false, true] {
        let mut topics = dtp_conversion_rows(0, 8192);
        let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
        unnamed.name = None;
        topics.push(unnamed);
        let mut page = Vec::new();
        let mut out: Vec<TopicDescription> = Vec::new();
        let mut budget = 0;
        let wake = Arc::new(CleanupWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let deadline = if expired {
            tokio::time::Instant::now() - Duration::from_secs(1)
        } else {
            tokio::time::Instant::now() + Duration::from_secs(30)
        };
        let mut future = Box::pin(convert_described_topic_page(
            &topics,
            false,
            &mut page,
            &mut out,
            deadline,
            &mut budget,
        ));
        assert!(future.as_mut().poll(&mut context).is_pending());
        // Rejection cleanup may itself yield after visiting 8,192 descriptions.
        let got = loop {
            if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
                break result;
            }
        };
        if expired {
            assert!(matches!(got, Err(Error::Timeout)));
        } else {
            assert!(matches!(got, Err(Error::Unsupported(_))));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dtp_requested_and_unnamed_after_conversion_checkpoint_keep_precedence() {
    // Test the direct-name path as well as the indexed path. Duplicate rows
    // in the direct case are synthetic and keep the request to one name.
    for indexed in [false, true] {
        for count in [8191, 8192] {
            for tail in 0..3 {
                let names: Vec<String> = if indexed {
                    (0..count)
                        .map(|index| format!("topic-{index:08}"))
                        .collect()
                } else {
                    vec!["orders".into()]
                };
                let (mut admin, listener) = admin_and_listener().await;
                let peer = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let (header, mut body) = request(&mut socket).await;
                    let (requested, limit, cursor) =
                        decode_describe_topic_partitions_request(&mut body).unwrap();
                    if indexed {
                        assert_eq!(requested.len(), count);
                        for (index, name) in requested.iter().enumerate() {
                            assert_eq!(name, &format!("topic-{index:08}"));
                        }
                    } else {
                        assert_eq!(requested, ["orders"]);
                    }
                    assert_eq!(limit, 1);
                    assert!(cursor.is_none());
                    assert!(body.is_empty());
                    let mut topics = if indexed {
                        dtp_conversion_rows(0, count)
                    } else {
                        (0..count)
                            .map(|_| DescribedTopicPartitions::new("orders", 0))
                            .collect()
                    };
                    let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
                    unnamed.name = None;
                    if tail == 1 {
                        topics.push(unnamed);
                        topics.push(DescribedTopicPartitions::new("unrequested", 0));
                    } else {
                        topics.push(DescribedTopicPartitions::new("unrequested", 0));
                        if tail == 2 {
                            topics.push(unnamed);
                        } else {
                            let name = if indexed { "topic-00000000" } else { "orders" };
                            topics.push(DescribedTopicPartitions::new(name, 0));
                        }
                    }
                    let response = DescribeTopicPartitionsResponse::new(topics);
                    send_page(&mut socket, &header, &response).await.unwrap();
                    let next = dtp_request(&mut socket, None).await;
                    send_page(&mut socket, &next, &page(42, None))
                        .await
                        .unwrap();
                });
                let result = admin
                    .describe_topics_with_partition_limit_timeout(
                        names.iter(),
                        false,
                        1,
                        Duration::from_secs(5),
                    )
                    .await;
                if tail == 0 {
                    assert!(matches!(&result, Err(Error::Protocol(message))
                        if message == "DescribeTopicPartitions returned an unrequested topic"));
                } else {
                    assert!(matches!(&result, Err(Error::Unsupported(message))
                        if message == "cannot aggregate DescribeTopicPartitions without a topic name"));
                }
                assert!(!admin.conn.is_closed());
                let next = admin
                    .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
                    .await
                    .unwrap();
                assert_eq!(next.topics[0].partitions[0].partition_index, 42);
                peer.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn dtp_requested_public_describe_future_is_send() {
    fn require_send<T: Send>(_: &T) {}

    let (mut admin, _listener) = admin_and_listener().await;
    let names = ["orders"];
    let call = admin.describe_topics_with_partition_limit_timeout(
        names.iter(),
        false,
        1,
        Duration::from_secs(5),
    );
    require_send(&call);
}

#[test]
fn dtp_requested_mismatch_iterator_and_null_cleanup_at_row_boundary() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    for count in [8191, 8192] {
        let mut topics = dtp_conversion_rows(0, count);
        topics.push(DescribedTopicPartitions::new("unrequested", 0));
        let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
        unnamed.name = None;
        topics.push(unnamed);
        let mut iter = topics.iter();
        let mut page = Vec::new();
        let mut out: Vec<TopicDescription> = Vec::new();
        let mut converted_rows = 0;
        let wake = Arc::new(CleanupWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut call = Box::pin(convert_described_requested_page(
            &mut iter,
            |name| name != "unrequested",
            false,
            &mut page,
            &mut out,
            deadline,
            &mut converted_rows,
        ));
        if count == 8192 {
            assert!(call.as_mut().poll(&mut context).is_pending());
        }
        assert!(matches!(
            call.as_mut().poll(&mut context),
            Poll::Ready(Ok(true))
        ));
        drop(call);
        assert!(iter.next().unwrap().name.is_none());
        assert!(iter.next().is_none());
        assert_eq!(page.len(), count);
        assert_eq!(converted_rows, count % 8192);

        let mut topics = dtp_conversion_rows(0, count);
        let mut unnamed = DescribedTopicPartitions::new("ignored", 0);
        unnamed.name = None;
        topics.push(unnamed);
        let mut iter = topics.iter();
        let mut page = Vec::new();
        let mut out: Vec<TopicDescription> = Vec::new();
        let mut converted_rows = 0;
        let mut call = Box::pin(convert_described_requested_page(
            &mut iter,
            |_| true,
            false,
            &mut page,
            &mut out,
            deadline,
            &mut converted_rows,
        ));
        let mut pending = 0;
        let result = loop {
            match call.as_mut().poll(&mut context) {
                Poll::Pending => pending += 1,
                Poll::Ready(result) => break result,
            }
        };
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert_eq!(pending, if count == 8192 { 2 } else { 0 });
    }
}
