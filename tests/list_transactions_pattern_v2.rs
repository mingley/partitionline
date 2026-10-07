//! API66 v2 must retain pattern filters and the complete broker response.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite codec and owned wire-peer regression assertions"
)]
#[path = "fixtures/list-transactions-pattern-v2/socket_peer.rs"]
mod socket_peer;

use bytes::BytesMut;
use partitionline::protocol::admin::{
    decode_list_transactions_request_data, decode_list_transactions_response,
    encode_list_transactions_request, encode_list_transactions_request_data,
    encode_list_transactions_response, ListTransactionsRequestData, TransactionListing,
};
use socket_peer::{finish, listing, response, Peer, Reply, BUDGET};
use std::time::{Duration, Instant};

#[test]
fn v2_unfiltered_request_includes_nullable_pattern() {
    let mut bytes = BytesMut::new();
    encode_list_transactions_request(&mut bytes, 2, &[], &[], -1).unwrap();
    assert_eq!(
        bytes.as_ref(),
        &[1, 1, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0]
    );
}

#[test]
fn v2_unfiltered_response_keeps_full_layout() {
    let raw = response(0, &[listing("txn-one", 42, "Ongoing")]);
    let mut cursor = raw.as_ref();
    let decoded = decode_list_transactions_response(&mut cursor, 2).unwrap();
    assert!(cursor.is_empty());
    assert_eq!(
        decoded.transaction_states[0],
        TransactionListing {
            transactional_id: "txn-one".into(),
            producer_id: 42,
            transaction_state: "Ongoing".into()
        }
    );
}

#[tokio::test]
async fn v2_unfiltered_public_query_negotiates_every_broker() {
    let peer = Peer::start([Some((2, 2)); 2], false).await;
    peer.script(
        1,
        [Reply::Body(response(
            0,
            &[listing("txn-one", 1, "Ongoing")],
        ))],
    )
    .await;
    peer.script(
        2,
        [Reply::Body(response(
            0,
            &[listing("txn-two", 2, "CompleteCommit")],
        ))],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(admin, peer, "v2-unfiltered-public").await;
    assert_eq!(result.unwrap().len(), 2);
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66)
            .map(|r| (r.node, r.api_version))
            .collect::<Vec<_>>(),
        [(1, 2), (2, 2)]
    );
}

#[test]
fn raw_empty_pattern_requires_v2_and_invalid_regex_is_broker_authority() {
    for pattern in ["", "[", "only-.*"] {
        let request = ListTransactionsRequestData {
            transactional_id_pattern: Some(pattern.into()),
            ..Default::default()
        };
        for version in [0, 1] {
            let mut output = BytesMut::from(&b"prefix"[..]);
            assert!(matches!(
                encode_list_transactions_request_data(&mut output, version, &request),
                Err(partitionline::Error::Unsupported(_))
            ));
            assert_eq!(output.as_ref(), b"prefix");
        }
        let mut output = BytesMut::new();
        encode_list_transactions_request_data(&mut output, 2, &request).unwrap();
        let mut cursor = output.as_ref();
        assert_eq!(
            decode_list_transactions_request_data(&mut cursor, 2).unwrap(),
            request
        );
        assert!(cursor.is_empty());
    }
}

#[test]
fn request_limits_and_null_fields_fail_before_output_or_allocation() {
    let huge = ListTransactionsRequestData {
        transactional_id_pattern: Some("x".repeat(256 * 1024 + 1)),
        ..Default::default()
    };
    let mut output = BytesMut::from(&b"prefix"[..]);
    assert!(encode_list_transactions_request_data(&mut output, 2, &huge).is_err());
    assert_eq!(output.as_ref(), b"prefix");
    // The claimed string is over the local bound even though no payload exists.
    let mut raw = BytesMut::from(&b"\x01\x01\xff\xff\xff\xff\xff\xff\xff\xff"[..]);
    partitionline::protocol::buf::put_unsigned_varint(&mut raw, 256 * 1024 + 2);
    let error = decode_list_transactions_request_data(&mut raw.as_ref(), 2).unwrap_err();
    assert!(error.to_string().contains("local limit"));
    for bytes in [&[0][..], &[1, 0][..], &[2, 0][..], &[1, 2, 0][..]] {
        let mut cursor = bytes;
        assert!(decode_list_transactions_request_data(&mut cursor, 2).is_err());
    }
}

#[tokio::test]
async fn typed_public_empty_pattern_preserves_v0_and_nonempty_refuses_old_broker() {
    for pattern in ["", "only-.*"] {
        let peer = Peer::start([Some((0, 0)), Some((2, 2))], false).await;
        peer.script(1, [Reply::Body(response(0, &[]))]).await;
        peer.script(2, [Reply::Body(response(0, &[]))]).await;
        let mut admin = peer.admin().await.unwrap();
        let options = partitionline::ListTransactionsOptions {
            transactional_id_pattern: Some(pattern.into()),
            ..Default::default()
        };
        let result = admin
            .list_transactions_by_broker_with_options_timeout(&options, BUDGET)
            .await;
        let closed = finish(
            admin,
            peer,
            if pattern.is_empty() {
                "public-empty-v0"
            } else {
                "public-nonempty-v0"
            },
        )
        .await;
        let brokers = result.unwrap();
        assert!(brokers[1].listings.is_ok());
        if pattern.is_empty() {
            assert!(brokers[0].listings.is_ok());
        } else {
            assert!(matches!(
                brokers[0].listings,
                Err(partitionline::Error::Unsupported(_))
            ));
            assert!(closed
                .observations
                .iter()
                .all(|r| r.node != 1 || r.api_key != 66));
        }
    }
}

#[test]
#[ignore = "requires actual SDK-generated144 profiles; runner supplies mandatory fixture paths"]
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous external fixture replay runs outside any Tokio runtime"
)]
fn actual_sdk_pattern_frames() {
    let fixtures =
        std::path::PathBuf::from(std::env::var_os("PARTITIONLINE_PATTERN_FIXTURES").unwrap());
    let reverse =
        std::path::PathBuf::from(std::env::var_os("PARTITIONLINE_PATTERN_REVERSE").unwrap());
    std::fs::create_dir(&reverse).unwrap();
    let mut supported = 0;
    let mut unsupported = 0;
    for line in std::fs::read_to_string(fixtures.join("raw.tsv"))
        .unwrap()
        .lines()
    {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        let version = fields[1].parse::<i16>().unwrap();
        let pattern = if fields[2] == "-" {
            None
        } else {
            let bytes = (0..fields[2].len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&fields[2][i..i + 2], 16).unwrap())
                .collect();
            Some(String::from_utf8(bytes).unwrap())
        };
        let request = ListTransactionsRequestData {
            state_filters: if fields[4] == "0" {
                vec![]
            } else {
                vec!["Ongoing".into(), "PrepareCommit".into()]
            },
            producer_id_filters: if fields[4] == "0" {
                vec![]
            } else {
                vec![1001, i64::MAX]
            },
            duration_ms: fields[3].parse().unwrap(),
            transactional_id_pattern: pattern,
        };
        let mut encoded = BytesMut::from(&b"prefix"[..]);
        let result = encode_list_transactions_request_data(&mut encoded, version, &request);
        if fields[5] == "unsupported" {
            assert!(matches!(result, Err(partitionline::Error::Unsupported(_))));
            assert_eq!(encoded.as_ref(), b"prefix");
            unsupported += 1;
            continue;
        }
        result.unwrap();
        let mut frame = std::fs::read(fixtures.join(format!("{}.request.bin", fields[0]))).unwrap();
        let client = i16::from_be_bytes(frame[12..14].try_into().unwrap());
        let start = 15 + usize::try_from(client.max(0)).unwrap();
        assert_eq!(&encoded[6..], &frame[start..]);
        let mut cursor = &frame[start..];
        assert_eq!(
            decode_list_transactions_request_data(&mut cursor, version).unwrap(),
            request
        );
        assert!(cursor.is_empty());
        frame.truncate(start);
        frame.extend_from_slice(&encoded[6..]);
        std::fs::write(reverse.join(format!("{}.request.bin", fields[0])), frame).unwrap();
        let mut response =
            std::fs::read(fixtures.join(format!("{}.response.bin", fields[0]))).unwrap();
        let mut body = &response[9..];
        let decoded = decode_list_transactions_response(&mut body, version).unwrap();
        assert!(body.is_empty());
        assert_eq!(decoded.throttle_time_ms, 42);
        assert_eq!(decoded.unknown_state_filters, ["UnknownState"]);
        assert_eq!(decoded.transaction_states[0].producer_id, i64::MAX);
        let mut output = BytesMut::new();
        encode_list_transactions_response(&mut output, version, &decoded).unwrap();
        assert_eq!(output.as_ref(), &response[9..]);
        response.truncate(9);
        response.extend_from_slice(&output);
        std::fs::write(
            reverse.join(format!("{}.response.bin", fields[0])),
            response,
        )
        .unwrap();
        supported += 1;
    }
    assert_eq!(supported + unsupported, 144);
    assert!(supported > 0 && unsupported > 0);
    println!(
        "actual SDK request/response pairs={supported}; explicit unsupported cells={unsupported}"
    );
}

#[tokio::test]
async fn oversized_pattern_and_zero_deadline_do_not_dispatch() {
    let peer = Peer::start([Some((2, 2)); 2], false).await;
    let mut admin = peer.admin().await.unwrap();
    let oversized = partitionline::ListTransactionsOptions {
        transactional_id_pattern: Some("x".repeat(256 * 1024 + 1)),
        ..Default::default()
    };
    let limit = admin
        .list_transactions_with_options_timeout(&oversized, BUDGET)
        .await;
    let zero = admin
        .list_transactions_with_options_timeout(&Default::default(), Duration::ZERO)
        .await;
    let closed = finish(admin, peer, "oversized-pattern-zero-deadline").await;
    assert!(matches!(limit, Err(partitionline::Error::Protocol(_))));
    assert!(matches!(zero, Err(partitionline::Error::Timeout)));
    assert!(closed.observations.iter().all(|r| r.api_key == 18));
}

/// Explicit external lane: the runner supplies actual SDK response bodies.
#[tokio::test]
#[ignore = "requires source-bound actual SDK fixtures and owned process runner"]
async fn serve_list_pattern_probe() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("PARTITIONLINE_PATTERN_PEER_DIR").unwrap());
    let fixtures =
        std::path::PathBuf::from(std::env::var_os("PARTITIONLINE_PATTERN_FIXTURES").unwrap());
    let mode = std::env::var("PARTITIONLINE_PATTERN_CASE").unwrap();
    let ranges = match mode.as_str() {
        "v0" => [Some((0, 0)); 2],
        "v1" => [Some((1, 1)); 2],
        "mixed" => [Some((1, 1)), Some((2, 2))],
        _ => [Some((2, 2)); 2],
    };
    let peer = Peer::start(ranges, false).await;
    async fn load(fixtures: &std::path::Path, name: &str) -> std::sync::Arc<[u8]> {
        let frame = tokio::fs::read(fixtures.join(name)).await.unwrap();
        assert_eq!(
            i32::from_be_bytes(frame[..4].try_into().unwrap()),
            i32::try_from(frame.len() - 4).unwrap()
        );
        assert_eq!(frame[8], 0);
        std::sync::Arc::<[u8]>::from(&frame[9..])
    }
    let first = load(&fixtures, &format!("public-{mode}-node-1.response.bin")).await;
    let second = load(&fixtures, &format!("public-{mode}-node-2.response.bin")).await;
    peer.script(
        1,
        (0..4).map(|_| Reply::Body(std::sync::Arc::clone(&first))),
    )
    .await;
    let mut replies = Vec::new();
    if mode == "disconnect" || mode == "downgrade" {
        replies.push(Reply::Disconnect);
    }
    replies.extend((0..4).map(|_| Reply::Body(std::sync::Arc::clone(&second))));
    peer.script(2, replies).await;
    if mode == "downgrade" {
        peer.state.lock().await.downgrade_second_on_disconnect = true;
    }
    let mut options = partitionline::ListTransactionsOptions {
        transactional_id_pattern: match mode.as_str() {
            "null" | "v0" | "v1" => None,
            "empty" => Some(String::new()),
            "no-match" => Some("missing-.*".into()),
            "invalid" => Some("[".into()),
            _ => Some("only-.*".into()),
        },
        ..Default::default()
    };
    if mode == "combined" {
        options.state_filters = vec!["Ongoing".into()];
        options.producer_id_filters = vec![1001];
        options.duration_ms = 0;
    }
    tokio::fs::write(directory.join("ready"), &peer.bootstrap)
        .await
        .unwrap();
    let closed = if std::env::var_os("PARTITIONLINE_PATTERN_INVOKE").is_some() {
        let mut admin = peer.admin().await.unwrap();
        let start = Instant::now();
        let partial = admin
            .list_transactions_by_broker_with_options_timeout(&options, BUDGET)
            .await;
        let mut outcome = String::new();
        match partial {
            Ok(brokers) => {
                for broker in brokers {
                    match broker.listings {
                        Ok(rows) => {
                            for row in rows {
                                outcome.push_str(&format!(
                                    "listing\t{}\t{}\t{}\t{}\n",
                                    broker.broker_id,
                                    row.transactional_id,
                                    row.producer_id,
                                    row.state()
                                ));
                            }
                        }
                        Err(error) => outcome.push_str(&format!(
                            "broker-error\t{}\t{:?}\t{}\n",
                            broker.broker_id,
                            error.broker_code(),
                            error
                        )),
                    }
                }
            }
            Err(error) => outcome.push_str(&format!("discovery-error\t{error}\n")),
        }
        outcome.push_str(&format!(
            "partial-elapsed-ms\t{}\n",
            start.elapsed().as_millis()
        ));
        let start = Instant::now();
        let complete = admin
            .list_transactions_with_options_timeout(&options, BUDGET)
            .await;
        outcome.push_str(&match complete {
            Ok(rows) => format!("complete-count\t{}\n", rows.len()),
            Err(error) => format!("complete-error\t{:?}\t{error}\n", error.broker_code()),
        });
        outcome.push_str(&format!(
            "complete-elapsed-ms\t{}\n",
            start.elapsed().as_millis()
        ));
        tokio::fs::write(directory.join("rust-outcome.tsv"), outcome)
            .await
            .unwrap();
        finish(admin, peer, "probe").await
    } else {
        let stop = directory.join("stop");
        tokio::time::timeout(Duration::from_secs(16), async {
            while !stop.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let closed = peer.shutdown().await;
        socket_peer::retain(&closed, "probe").await;
        closed
    };
    assert_eq!(closed.listener_tasks_joined, 3);
    assert!(
        closed.shutdown_failures.is_empty(),
        "{:?}",
        closed.shutdown_failures
    );
    assert!(closed.observations.iter().all(|r| r.api_key != 10));
    assert_eq!(
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        0
    );
    tokio::fs::write(
        directory.join("closure.txt"),
        "listeners_joined=3\nworkers_joined=true\nports_closed_and_rebound=3\nruntime_tasks=0\n",
    )
    .await
    .unwrap();
}
