
#[tokio::test]
async fn dtp_active_cycle_control() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (mut admin, listener) = admin_and_listener().await;
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        loop {
            let Ok(len) = socket.read_i32().await else { break };
            let Ok(len) = usize::try_from(len) else { break };
            if len > 1024 * 1024 { break; }
            let mut bytes = vec![0; len];
            if socket.read_exact(&mut bytes).await.is_err() { break; }
            let mut body = Bytes::from(bytes);
            let Ok(header) = decode_request_header(&mut body) else { break };
            if header.api_key != DESCRIBE_TOPIC_PARTITIONS { break; }
            let Ok((names, limit, cursor)) = decode_describe_topic_partitions_request(&mut body) else { break };
            assert_eq!(names, vec!["orders"]);
            assert_eq!(limit, 1);
            let seq = counter.fetch_add(1, Ordering::SeqCst);
            let expected = match seq {
                0 => None,
                n if n % 2 == 1 => Some(("orders", 1)),
                _ => Some(("orders", 2)),
            };
            assert_eq!(cursor.as_ref().map(|c| (c.topic_name.as_str(), c.partition_index)), expected);
            let next = if seq % 2 == 0 { 1 } else { 2 };
            if send_page(&mut socket, &header, &page(0, Some(("orders", next)))).await.is_err() { break; }
        }
    });
    let names = ["orders"];
    let started = std::time::Instant::now();
    let result = admin
        .describe_topics_with_partition_limit_timeout(names.iter(), false, 1, Duration::from_millis(250))
        .await;
    let elapsed = started.elapsed();
    let count = requests.load(Ordering::SeqCst);
    match std::env::var("DTP_CYCLE_MODE").unwrap().as_str() {
        "history" => {
            assert!(matches!(result, Err(Error::Protocol(_))), "got {result:?}");
            assert_eq!(count, 3);
        }
        "constant" => {
            assert!(matches!(result, Err(Error::Timeout)), "got {result:?}");
            assert!(count >= 4, "only {count} requests");
        }
        other => panic!("unknown mode {other}"),
    }
    drop(admin);
    tokio::time::timeout(Duration::from_secs(5), peer).await.unwrap().unwrap();
    println!("ACTIVE_CYCLE elapsed_ns={} requests={count}", elapsed.as_nanos());
}
