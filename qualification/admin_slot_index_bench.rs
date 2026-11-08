/// Run the public Admin operation against a single mock response and time each
/// poll. The peer checks every requested name and returns the same names.
#[tokio::test(flavor = "current_thread")]
async fn dtp_public_slot_index_witness() {
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
    let unique: usize = std::env::var("PL_ROW_UNIQUE").unwrap().parse().unwrap();
    assert!(unique > 0 && unique <= count);
    let names: Vec<String> = (0..unique).map(|i| format!("topic-{i:08}")).collect();
    let topics = (0..count)
        .map(|i| {
            let slot = i % unique;
            let mut topic = DescribedTopicPartitions::new(names[slot].clone(), error);
            if error == 0 {
                topic.topic_id[..8].copy_from_slice(&(slot as u64).to_be_bytes());
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
    let (release_peer, peer_release) = tokio::sync::oneshot::channel();
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
        assert_eq!(requested.len(), unique);
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
        let _release = peer_release.await;
        drop(response_body);
        drop(socket);
    });

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
    let result = std::future::poll_fn(|cx| {
        let after_write = response_written.load(Ordering::SeqCst);
        let poll_start = std::time::Instant::now();
        let polled = call.as_mut().poll(cx);
        let elapsed_ns = poll_start.elapsed().as_nanos();
        all_polls += 1;
        if polled.is_ready() {
            ready_poll_ns = elapsed_ns;
            ready_after_write = after_write;
        } else if after_write {
            max_pending_after_write_ns = max_pending_after_write_ns.max(elapsed_ns);
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
    assert!(response_written.load(Ordering::SeqCst));
    assert!(after_write_polls > 0);
    assert!(ready_after_write);
    assert_eq!(result.len(), unique);
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row.name, names[i]);
        assert_eq!(row.error_code, error);
        assert!(row.partitions.is_empty());
        assert_eq!(row.authorized_operations, AUTHORIZED_OPERATIONS_OMITTED);
        let mut expected_id = [0; 16];
        if error == 0 {
            expected_id[..8].copy_from_slice(&(i as u64).to_be_bytes());
            expected_id[15] = 7;
        }
        assert_eq!(row.topic_id, expected_id);
        assert!(!row.is_internal);
    }
    assert_eq!(std::hint::black_box(&result).len(), unique);
    release_peer.send(()).unwrap();
    peer.await.unwrap();
    println!(
        "ROW_SLOT rows={count} unique={unique} kind={kind} all_polls={all_polls} after_write_polls={after_write_polls} max_before_write_ns={max_before_write_ns} max_after_write_ns={max_after_write_ns} max_pending_after_write_ns={max_pending_after_write_ns} ready_poll_ns={ready_poll_ns} ready_after_write={ready_after_write} total_ns={total_ns} wire_body_bytes={wire_body_bytes}"
    );
}
