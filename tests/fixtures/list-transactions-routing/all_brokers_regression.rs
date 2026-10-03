// Same existing-public-API undercount assertion in the old and corrected targets.

fn broker_rows(node: i32) -> Vec<socket_peer::Listing> {
    match node {
        1 => vec![
            listing("only-one", 1001, "Ongoing"),
            listing("same-id", 41, "PrepareCommit"),
        ],
        2 => vec![
            listing("only-two", 1002, "CompleteCommit"),
            listing("same-id", 42, "PrepareAbort"),
        ],
        _ => panic!("unexpected broker ID{node}"),
    }
}

fn assert_no_empty_coordinator(closed: &socket_peer::Closed) {
    assert!(closed.observations.iter().all(|r| r.api_key != 10));
}

#[tokio::test]
async fn all_queries_two_distinct_brokers_and_preserves_conflicting_duplicates() {
    // This exact public-existing-API test is the failing-first control for old
    // main: it permits the real FindCoordinator6(empty) reply to broker1, then
    // asserts the independently known complete four-row result. It must fail on
    // the old implementation's two-row success, without a test-only old branch.
    let peer = Peer::start([Some((0, 1)), Some((0, 1))], true).await;
    peer.script(1, [Reply::Body(response(0, &broker_rows(1)))])
        .await;
    peer.script(2, [Reply::Body(response(0, &broker_rows(2)))])
        .await;
    let mut admin = peer.admin().await.unwrap();
    let outcome = admin.list_transactions_all_timeout(BUDGET).await;
    let closed = finish(
        admin,
        peer,
        "all_queries_two_distinct_brokers_and_preserves_conflicting_duplicates",
    )
    .await;
    let actual = outcome.unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|r| (r.transactional_id.as_str(), r.producer_id, r.state()))
            .collect::<Vec<_>>(),
        vec![
            ("only-one", 1001, "Ongoing"),
            ("same-id", 41, "PrepareCommit"),
            ("only-two", 1002, "CompleteCommit"),
            ("same-id", 42, "PrepareAbort")
        ]
    );
    let requests: Vec<_> = closed
        .observations
        .iter()
        .filter(|r| r.api_key == 66)
        .collect();
    assert_eq!(requests.iter().map(|r| r.node).collect::<Vec<_>>(), [1, 2]);
    for r in requests {
        assert_eq!(
            filters(&r.request_body, r.api_version),
            (vec![], vec![], -1)
        );
    }
    assert_no_empty_coordinator(&closed);
}
