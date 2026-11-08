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
    assert_eq!((header.api_key, header.api_version), (DESCRIBE_TOPIC_PARTITIONS, 0));
    let (names, limit, cursor) = decode_describe_topic_partitions_request(&mut body).unwrap();
    assert_eq!(names, vec!["orders"]);
    assert_eq!(limit, 1);
    assert_eq!(
        cursor.as_ref().map(|c| (c.topic_name.as_str(), c.partition_index)),
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
    encode_response_header(&mut body, header.api_key, header.api_version, header.correlation_id)
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
        send_page(&mut socket, &first, &page(0, Some(("orders", 1)))).await.unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        send_page(&mut socket, &second, &page(1, None)).await.unwrap();
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
        result[0].partitions().iter().map(|p| p.partition).collect::<Vec<_>>(),
        vec![0, 1]
    );
    peer.await.unwrap();
}

async fn repeated_cursor_case(cycle: bool) {
    let (mut admin, listener) = admin_and_listener().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &first, &page(0, Some(("orders", 1)))).await.unwrap();
        let second = dtp_request(&mut socket, Some(("orders", 1))).await;
        let next = if cycle { 2 } else { 1 };
        send_page(&mut socket, &second, &page(1, Some(("orders", next)))).await.unwrap();
        if cycle {
            let third = dtp_request(&mut socket, Some(("orders", 2))).await;
            send_page(&mut socket, &third, &page(2, Some(("orders", 1)))).await.unwrap();
        }
    });
    let names = ["orders"];
    let got = admin
        .describe_topics_with_partition_limit_timeout(names.iter(), false, 1, Duration::from_secs(5))
        .await;
    assert!(matches!(got, Err(Error::Protocol(_))), "got {got:?}");
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_pagination_rejects_an_immediate_repeat() {
    repeated_cursor_case(false).await;
}

#[tokio::test]
async fn dtp_pagination_rejects_a_nonadjacent_cycle() {
    repeated_cursor_case(true).await;
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
        send_page(&mut old, &first, &page(0, Some(("orders", 1)))).await.unwrap();
        let second = dtp_request(&mut old, Some(("orders", 1))).await;
        second_tx.send(()).unwrap();
        late_rx.await.unwrap();
        let _late_write = send_page(&mut old, &second, &page(999, None)).await;

        let (mut new, _) = listener.accept().await.unwrap();
        let (versions_header, _) = request(&mut new).await;
        assert_eq!(versions_header.api_key, API_VERSIONS);
        let versions = ApiVersionsResponse {
            api_keys: vec![
                ApiVersion { api_key: METADATA, min_version: 0, max_version: 12 },
                ApiVersion { api_key: DESCRIBE_TOPIC_PARTITIONS, min_version: 0, max_version: 0 },
            ],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        encode_response_header(
            &mut body,
            versions_header.api_key,
            versions_header.api_version,
            versions_header.correlation_id,
        ).unwrap();
        encode_api_versions_response(&mut body, versions_header.api_version, &versions).unwrap();
        new.write_i32(i32::try_from(body.len()).unwrap()).await.unwrap();
        new.write_all(&body).await.unwrap();
        let retry = dtp_request(&mut new, None).await;
        send_page(&mut new, &retry, &page(42, None)).await.unwrap();
    });
    let names = ["orders"];
    let mut call = Box::pin(admin.describe_topics_with_partition_limit_timeout(
        names.iter(), false, 1, Duration::from_secs(2)
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
    assert!(admin.conn.is_closed(), "timed out connection was not retired");
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
        send_page(&mut socket, &header, &page(0, Some(("orders", 1)))).await.unwrap();
    });
    let result = admin
        .describe_topic_partitions_timeout(&["orders"], 1, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(result.next_cursor, Some(TopicPartitionCursor::new("orders", 1)));
    peer.await.unwrap();
}

#[tokio::test]
async fn dtp_raw_cancel_during_encoding_keeps_socket_usable() {
    use std::future::Future;

    let (mut admin, listener) = admin_and_listener().await;
    let names = vec!["orders"; 8193];
    let mut call = Box::pin(admin.describe_topic_partitions_timeout(
        &names,
        1,
        None,
        Duration::from_secs(5),
    ));
    std::future::poll_fn(|cx| {
        assert!(call.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    }).await;
    drop(call);
    assert!(!admin.conn.is_closed(), "encoding cancellation retired an idle socket");
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let header = dtp_request(&mut socket, None).await;
        send_page(&mut socket, &header, &page(42, None)).await.unwrap();
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
    let mut call = Box::pin(admin.describe_topic_partitions_timeout(
        &names,
        1,
        None,
        Duration::from_secs(5),
    ));
    tokio::select! {
        got = &mut call => panic!("finished before cancellation: {got:?}"),
        got = ready_rx => got.unwrap(),
    }
    drop(call);
    assert!(admin.conn.is_closed(), "inflight cancellation kept the socket usable");
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
