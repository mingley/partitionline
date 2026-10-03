//! ListTransactions must query the broker membership, preserve origins and obey
//! one caller deadline. Independently scripted peers do not import API66 codecs.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite regression assertions and scripted schema peers"
)]

#[path = "fixtures/list-transactions-routing/socket_peer.rs"]
mod socket_peer;

use partitionline::Error;
use socket_peer::{filters, finish, listing, response, Peer, Reply, BUDGET};
use std::time::{Duration, Instant};

include!("fixtures/list-transactions-routing/all_brokers_regression.rs");

#[tokio::test]
async fn partial_results_preserve_broker_origin_and_complete_results_reject_terminal_errors() {
    for code in [15, 16, 29, 53] {
        let peer = Peer::start([Some((0, 1)); 2], false).await;
        peer.script(
            1,
            [
                Reply::Body(response(0, &broker_rows(1))),
                Reply::Body(response(0, &broker_rows(1))),
            ],
        )
        .await;
        peer.script(
            2,
            [
                Reply::Body(response(code, &[])),
                Reply::Body(response(code, &[])),
            ],
        )
        .await;
        let mut admin = peer.admin().await.unwrap();
        let partial = admin
            .list_transactions_by_broker_timeout(&[], &[], -1, BUDGET)
            .await;
        let complete = admin.list_transactions_all_timeout(BUDGET).await;
        let closed = finish(admin, peer, &format!("terminal-code-{code}")).await;
        let partial = partial.unwrap();
        assert_eq!(
            partial.iter().map(|r| r.broker_id).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(partial[0].listings.as_ref().unwrap().len(), 2);
        let error = partial[1].listings.as_ref().unwrap_err();
        assert_eq!(error.broker_code(), Some(code));
        assert!(error.to_string().contains("broker 2"));
        assert_eq!(complete.unwrap_err().broker_code(), Some(code));
        assert_eq!(
            closed
                .observations
                .iter()
                .filter(|r| r.api_key == 66 && r.node == 2)
                .count(),
            2,
            "terminal errors must not relookup or retry"
        );
        assert_no_empty_coordinator(&closed);
    }
}

#[tokio::test]
async fn mixed_broker_versions_preserve_unfiltered_v0_and_filtered_v1() {
    let peer = Peer::start([Some((0, 0)), Some((1, 1))], false).await;
    peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
        .await;
    peer.script(
        2,
        [
            Reply::Body(response(0, &broker_rows(2))),
            Reply::Body(response(0, &[listing("only-two", 1002, "Ongoing")])),
        ],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let unfiltered = admin.list_transactions_all_timeout(BUDGET).await;
    let filtered = admin
        .list_transactions_by_broker_timeout(&["Ongoing"], &[1002], 5000, BUDGET)
        .await;
    let closed = finish(
        admin,
        peer,
        "mixed_broker_versions_preserve_unfiltered_v0_and_filtered_v1",
    )
    .await;
    assert_eq!(unfiltered.unwrap().len(), 4);
    let filtered = filtered.unwrap();
    assert!(matches!(filtered[0].listings, Err(Error::Unsupported(_))));
    assert!(filtered[1].listings.is_ok());
    let requests: Vec<_> = closed
        .observations
        .iter()
        .filter(|r| r.api_key == 66)
        .collect();
    assert_eq!(
        requests
            .iter()
            .map(|r| (r.node, r.api_version))
            .collect::<Vec<_>>(),
        [(1, 0), (2, 1), (2, 1)]
    );
    assert_eq!(
        filters(&requests[2].request_body, 1),
        (vec!["Ongoing".into()], vec![1002], 5000)
    );
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn duration_on_v0_and_absent_api_fail_complete_but_remain_typed_in_partial() {
    for first_range in [Some((0, 0)), None, Some((2, 2))] {
        let peer = Peer::start([first_range, Some((1, 1))], false).await;
        peer.script(
            2,
            [
                Reply::Body(response(0, &broker_rows(2))),
                Reply::Body(response(0, &broker_rows(2))),
            ],
        )
        .await;
        let mut admin = peer.admin().await.unwrap(); // unrelated APIs are absent
        let partial = admin
            .list_transactions_by_broker_timeout(&[], &[], 0, BUDGET)
            .await;
        let complete = admin
            .list_transactions_with_duration_timeout(&[], &[], 0, BUDGET)
            .await;
        let closed = finish(
            admin,
            peer,
            &format!(
                "duration-range-{}",
                match first_range {
                    Some((0, 0)) => "v0",
                    None => "absent",
                    _ => "v2",
                }
            ),
        )
        .await;
        let partial = partial.unwrap();
        assert!(matches!(partial[0].listings, Err(Error::Unsupported(_))));
        assert!(partial[1].listings.is_ok());
        assert!(matches!(complete, Err(Error::Unsupported(_))));
        assert!(closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66)
            .all(|r| r.node == 2));
        assert_no_empty_coordinator(&closed);
    }
}

#[tokio::test]
async fn load_retry_is_per_broker_and_never_replays_successful_broker() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
        .await;
    peer.script(
        2,
        [
            Reply::Body(response(14, &[])),
            Reply::Body(response(14, &[])),
            Reply::Body(response(0, &broker_rows(2))),
        ],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let outcome = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "load_retry_is_per_broker_and_never_replays_successful_broker",
    )
    .await;
    assert_eq!(outcome.unwrap().len(), 4);
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 1)
            .count(),
        1
    );
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 2)
            .count(),
        3
    );
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 3)
            .count(),
        1
    );
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn persistent_loading_has_at_most_eight_actual_attempts() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(1, [Reply::Body(response(0, &[]))]).await;
    peer.script(2, (0..8).map(|_| Reply::Body(response(14, &[]))))
        .await;
    let mut admin = peer.admin().await.unwrap();
    let outcome = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "persistent_loading_has_at_most_eight_actual_attempts",
    )
    .await;
    assert_eq!(outcome.unwrap_err().broker_code(), Some(14));
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 2)
            .count(),
        8
    );
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 1)
            .count(),
        1
    );
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn disconnect_refreshes_only_affected_address_with_at_most_three_refreshes() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.state.lock().await.move_second_on_disconnect = true;
    peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
        .await;
    peer.script(
        2,
        [
            Reply::Disconnect,
            Reply::Disconnect,
            Reply::Disconnect,
            Reply::Disconnect,
            Reply::Body(response(0, &broker_rows(2))),
        ],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let outcome = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "disconnect_refreshes_only_affected_address_with_at_most_three_refreshes",
    )
    .await;
    assert_eq!(outcome.unwrap().len(), 4);
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 1)
            .count(),
        1
    );
    let second: Vec<_> = closed
        .observations
        .iter()
        .filter(|r| r.api_key == 66 && r.node == 2)
        .collect();
    assert_eq!(second.len(), 5);
    assert_eq!(second[0].listener_slot, 1);
    assert!(second[1..].iter().all(|r| r.listener_slot == 2));
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 3)
            .count(),
        4
    );
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn partial_deadline_keeps_success_and_times_out_stalled_broker() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
        .await;
    peer.script(2, [Reply::Stall]).await;
    let mut admin = peer.admin().await.unwrap();
    let started = Instant::now();
    let outcome = admin
        .list_transactions_by_broker_timeout(&[], &[], -1, Duration::from_millis(80))
        .await;
    let elapsed = started.elapsed();
    let closed = finish(
        admin,
        peer,
        "partial_deadline_keeps_success_and_times_out_stalled_broker",
    )
    .await;
    let rows = outcome.unwrap();
    assert!(rows[0].listings.is_ok());
    assert!(matches!(rows[1].listings, Err(Error::Timeout)));
    assert!(elapsed < Duration::from_secs(1));
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66 && r.node == 2)
            .count(),
        1
    );
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn delayed_metadata_uses_total_budget_and_sends_no_post_deadline_listing() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.state.lock().await.metadata_delay = Duration::from_millis(300);
    let mut admin = peer.admin().await.unwrap();
    let started = Instant::now();
    let result = admin
        .list_transactions_all_timeout(Duration::from_millis(70))
        .await;
    let elapsed = started.elapsed();
    let closed = finish(
        admin,
        peer,
        "delayed_metadata_uses_total_budget_and_sends_no_post_deadline_listing",
    )
    .await;
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(elapsed < Duration::from_secs(1));
    assert!(closed.observations.iter().any(|r| r.api_key == 3));
    assert!(closed.observations.iter().all(|r| r.api_key != 66));
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn listing_rounds_cannot_reset_budget_or_send_to_next_broker_after_expiry() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(
        1,
        [Reply::Delay(
            Duration::from_millis(300),
            response(0, &broker_rows(1)),
        )],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let started = Instant::now();
    let result = admin
        .list_transactions_by_broker_timeout(&[], &[], -1, Duration::from_millis(70))
        .await;
    let elapsed = started.elapsed();
    let closed = finish(
        admin,
        peer,
        "listing_rounds_cannot_reset_budget_or_send_to_next_broker_after_expiry",
    )
    .await;
    let rows = result.unwrap();
    assert!(rows
        .iter()
        .all(|r| matches!(r.listings, Err(Error::Timeout))));
    assert!(elapsed < Duration::from_secs(1));
    assert!(closed
        .observations
        .iter()
        .filter(|r| r.api_key == 66)
        .all(|r| r.node == 1));
    assert_no_empty_coordinator(&closed);
}

#[tokio::test]
async fn zero_deadline_and_invalid_filters_do_not_send_discovery_or_listings() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    let mut admin = peer.admin().await.unwrap();
    let zero = admin.list_transactions_all_timeout(Duration::ZERO).await;
    let excessive_states = vec!["Ongoing"; 8193];
    let states = admin
        .list_transactions_timeout(&excessive_states, &[], BUDGET)
        .await;
    let excessive_pids = vec![1i64; 8193];
    let pids = admin
        .list_transactions_timeout(&[], &excessive_pids, BUDGET)
        .await;
    let text = "x".repeat(256 * 1024 + 1);
    let text_result = admin
        .list_transactions_timeout(&[text.as_str()], &[], BUDGET)
        .await;
    let closed = finish(
        admin,
        peer,
        "zero_deadline_and_invalid_filters_do_not_send_discovery_or_listings",
    )
    .await;
    assert!(matches!(zero, Err(Error::Timeout)));
    for outcome in [states, pids, text_result] {
        assert!(matches!(outcome, Err(Error::Protocol(_))));
    }
    assert!(closed.observations.iter().all(|r| matches!(r.api_key, 18)));
}

#[tokio::test]
async fn count_trailing_and_byte_limits_cannot_become_complete_success() {
    let mut hostiles = socket_peer::required_null_responses();
    hostiles.extend([
        socket_peer::huge_count_response(),
        socket_peer::trailing_response(),
        socket_peer::oversized_response(),
    ]);
    for (index, hostile) in hostiles.into_iter().enumerate() {
        let peer = Peer::start([Some((0, 1)); 2], false).await;
        peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
            .await;
        peer.script(2, [Reply::Body(hostile)]).await;
        let mut admin = peer.admin().await.unwrap();
        let outcome = admin.list_transactions_all_timeout(BUDGET).await;
        let closed = finish(admin, peer, &format!("listing-hostile-{index}")).await;
        assert!(matches!(outcome, Err(Error::Protocol(_))));
        assert_eq!(
            closed
                .observations
                .iter()
                .filter(|r| r.api_key == 66)
                .count(),
            2
        );
        assert_no_empty_coordinator(&closed);
    }
}

#[tokio::test]
async fn required_metadata_fields_topics_counts_duplicates_and_trailing_fail_closed() {
    for (index, hostile) in socket_peer::hostile_metadata_bodies()
        .into_iter()
        .enumerate()
    {
        let peer = Peer::start([Some((0, 1)); 2], false).await;
        peer.state.lock().await.metadata_override = Some(hostile);
        let mut admin = peer.admin().await.unwrap();
        let outcome = admin.list_transactions_all_timeout(BUDGET).await;
        let closed = finish(admin, peer, &format!("metadata-hostile-{index}")).await;
        assert!(matches!(outcome, Err(Error::Protocol(_))));
        assert!(closed.observations.iter().all(|r| r.api_key != 66));
        assert_no_empty_coordinator(&closed);
    }
}

#[tokio::test]
async fn cumulative_listing_limit_covers_successful_brokers_together() {
    let first = vec![listing("", -1, ""); 60_001];
    let second = vec![listing("", -1, ""); 40_000];
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(1, [Reply::Body(response(0, &first))]).await;
    peer.script(2, [Reply::Body(response(0, &second))]).await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "cumulative_listing_limit_covers_successful_brokers_together",
    )
    .await;
    assert!(matches!(result, Err(Error::Protocol(_))));
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66)
            .count(),
        2
    );
}

#[tokio::test]
async fn cumulative_response_bytes_include_every_broker_not_just_each_frame() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    let body = socket_peer::large_unknown_filters_response();
    peer.script(1, [Reply::Body(body.clone())]).await;
    peer.script(2, [Reply::Body(body)]).await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "cumulative_response_bytes_include_every_broker_not_just_each_frame",
    )
    .await;
    assert!(matches!(result, Err(Error::Protocol(_))));
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66)
            .count(),
        2
    );
}

#[tokio::test]
async fn excessive_broker_count_fails_before_any_broker_listing() {
    for count in [0, 257, i32::MAX] {
        let peer = Peer::start([Some((0, 1)); 2], false).await;
        peer.state.lock().await.metadata_override =
            Some(socket_peer::invalid_metadata_count(count));
        let mut admin = peer.admin().await.unwrap();
        let outcome = admin.list_transactions_all_timeout(BUDGET).await;
        let closed = finish(admin, peer, &format!("broker-count-{count}")).await;
        assert!(matches!(outcome, Err(Error::Protocol(_))));
        assert!(closed.observations.iter().all(|r| r.api_key != 66));
    }
}

#[tokio::test]
async fn dropping_public_future_cancels_active_wait_then_all_peer_tasks_join() {
    let peer = Peer::start([Some((0, 1)); 2], false).await;
    peer.script(1, [Reply::Stall]).await;
    let mut admin = peer.admin().await.unwrap();
    let mut call = Box::pin(admin.list_transactions_all_timeout(BUDGET));
    tokio::select! {
        result = &mut call => panic!("held public future unexpectedly completed: {result:?}"),
        () = peer.await_requests(66,1,1) => {},
    }
    drop(call);
    let closed = finish(
        admin,
        peer,
        "dropping_public_future_cancels_active_wait_then_all_peer_tasks_join",
    )
    .await;
    assert_eq!(
        closed
            .observations
            .iter()
            .filter(|r| r.api_key == 66)
            .count(),
        1
    );
    assert!(closed.observations.iter().all(|r| r.api_key != 10));
}
