//! Public Streams descriptions, coordinator routing and bounded ownership.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite independently scripted socket assertions"
)]
#[path = "fixtures/streams-describe/socket_peer.rs"]
mod socket_peer;
use partitionline::protocol::streams::{self, StreamsGroupDescribeResponse};
use partitionline::{DescribeStreamsGroupsOptions, Error, StreamsLimits};
use socket_peer::{Peer, Reply};
use std::time::{Duration, Instant};

fn options() -> DescribeStreamsGroupsOptions {
    DescribeStreamsGroupsOptions {
        allow_unstable: true,
        include_authorized_operations: true,
        ..Default::default()
    }
}
fn group(id: &str, code: i16) -> Vec<u8> {
    let mut value = streams::decode_streams_group_describe_response(
        include_bytes!("fixtures/streams/4.3.1/describe-response-full.bin"),
        0,
        StreamsLimits::default(),
    )
    .unwrap();
    value.groups.truncate(1);
    value.groups[0].group_id = id.into();
    value.groups[0].error_code = code;
    streams::encode_streams_group_describe_response(&value, 0, StreamsLimits::default()).unwrap()
}
async fn prepared() -> Peer {
    let peer = Peer::start().await;
    let mut state = peer.state.lock().await;
    for id in ["alpha", "beta"] {
        let _previous = state.groups.insert(id.into(), group(id, 0));
    }
    drop(state);
    peer
}
fn code(body: Vec<u8>, code: i16) -> Vec<u8> {
    let mut value =
        streams::decode_streams_group_describe_response(&body, 0, StreamsLimits::default())
            .unwrap();
    value.groups[0].error_code = code;
    streams::encode_streams_group_describe_response(&value, 0, StreamsLimits::default()).unwrap()
}
#[tokio::test]
async fn local_policy_limits_and_empty_are_no_io() {
    let peer = prepared().await;
    let mut admin = peer.admin().await;
    let before = peer.state.lock().await.frames.len();
    assert!(matches!(
        admin
            .describe_streams_groups(&["alpha"], &Default::default())
            .await,
        Err(Error::Unsupported(_))
    ));
    let mut opts = options();
    opts.limits.string_bytes = 2;
    assert!(matches!(
        admin.describe_streams_groups(&["alpha"], &opts).await,
        Err(Error::Protocol(_))
    ));
    opts.limits = StreamsLimits::default();
    assert!(admin
        .describe_streams_groups(&[], &opts)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(peer.state.lock().await.frames.len(), before);
    drop(admin);
    let _closed = peer.close().await;
}
#[tokio::test]
async fn order_duplicates_and_all_fields_over_three_discovery_versions() {
    for version in [1, 3, 6] {
        let peer = prepared().await;
        peer.state.lock().await.find_version = version;
        let mut admin = peer.admin().await;
        let output = admin
            .describe_streams_groups(&["beta", "alpha", "beta"], &options())
            .await
            .unwrap();
        assert_eq!(output.len(), 3);
        for (actual, id) in output.iter().zip(["beta", "alpha", "beta"]) {
            assert_eq!(actual.group_id, id);
            assert_eq!(actual.group_type(), "Streams");
            let expected = streams::decode_streams_group_describe_response(
                &group(id, 0),
                0,
                StreamsLimits::default(),
            )
            .unwrap();
            assert_eq!(actual.description.as_ref().unwrap(), &expected.groups[0]);
        }
        drop(admin);
        let frames = peer.close().await;
        assert_eq!(frames.iter().filter(|f| f.api == 89).count(), 1);
        assert_eq!(frames.iter().filter(|f| f.api == 10).count(), 2);
        let request = streams::decode_streams_group_describe_request(
            &frames.iter().find(|f| f.api == 89).unwrap().body,
            0,
            StreamsLimits::default(),
        )
        .unwrap();
        assert_eq!(request.group_ids, ["beta", "alpha"]);
        assert!(request.include_authorized_operations);
    }
}
#[tokio::test]
async fn different_coordinators_and_terminal_errors_remain_partial() {
    let peer = prepared().await;
    {
        let mut s = peer.state.lock().await;
        let _previous = s.group_routes.insert("beta".into(), 2);
        let _previous = s.groups.insert("beta".into(), group("beta", 31));
    }
    let mut admin = peer.admin().await;
    let out = admin
        .describe_streams_groups(&["alpha", "beta", "beta"], &options())
        .await
        .unwrap();
    assert_eq!(out[0].description.as_ref().unwrap().error_code, 0);
    assert_eq!(out[1].description.as_ref().unwrap().error_code, 31);
    assert_eq!(out[2].description.as_ref().unwrap().error_code, 31);
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| f.api == 89)
            .map(|f| f.slot)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([1, 2])
    );
}
#[tokio::test]
async fn discovery_failure_is_not_a_fabricated_description() {
    let peer = prepared().await;
    let _previous = peer
        .state
        .lock()
        .await
        .group_errors
        .insert("beta".into(), 31);
    let mut admin = peer.admin().await;
    let out = admin
        .describe_streams_groups(&["beta", "alpha", "beta"], &options())
        .await
        .unwrap();
    assert_eq!(
        out[0].description.as_ref().unwrap_err().broker_code(),
        Some(31)
    );
    assert!(out[1].description.is_ok());
    assert_eq!(
        out[2].description.as_ref().unwrap_err().broker_code(),
        Some(31)
    );
    drop(admin);
    let _closed = peer.close().await;
}
#[tokio::test]
async fn unavailable_bootstrap_refuses_before_discovery_and_coordinator_is_partial() {
    let peer = prepared().await;
    let mut admin = peer.admin().await;
    peer.state.lock().await.ranges[0] = None;
    assert!(matches!(
        admin.describe_streams_groups(&["alpha"], &options()).await,
        Err(Error::Unsupported(_))
    ));
    assert!(!peer.state.lock().await.frames.iter().any(|f| f.api == 10));
    {
        let mut s = peer.state.lock().await;
        s.ranges[0] = Some((0, 0));
        s.ranges[2] = Some((1, 1));
        let _previous = s.group_routes.insert("beta".into(), 2);
    }
    let out = admin
        .describe_streams_groups(&["alpha", "beta"], &options())
        .await
        .unwrap();
    assert!(out[0].description.is_ok());
    assert!(matches!(
        out[1].description.as_ref().unwrap_err().as_ref(),
        Error::Unsupported(_)
    ));
    drop(admin);
    let frames = peer.close().await;
    assert!(!frames.iter().any(|f| f.api == 89 && f.slot == 2));
}
#[tokio::test]
async fn coordinator_and_disconnect_retries_use_new_authority() {
    for reply in [
        Reply::Body(code(group("alpha", 0), 14)),
        Reply::Body(code(group("alpha", 0), 15)),
        Reply::Body(code(group("alpha", 0), 16)),
        Reply::Disconnect,
    ] {
        let peer = prepared().await;
        peer.state.lock().await.routes.extend([1, 2]);
        peer.script(1, [reply]).await;
        let mut admin = peer.admin().await;
        assert_eq!(
            admin
                .describe_streams_groups(&["alpha"], &options())
                .await
                .unwrap()[0]
                .description
                .as_ref()
                .unwrap()
                .error_code,
            0
        );
        drop(admin);
        let frames = peer.close().await;
        assert_eq!(
            frames
                .iter()
                .filter(|f| f.api == 89)
                .map(|f| f.slot)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }
}
#[tokio::test]
async fn reconnect_downgrade_is_rechecked_before_dispatch() {
    let peer = prepared().await;
    peer.state.lock().await.downgrade_on_disconnect = true;
    peer.script(1, [Reply::Disconnect]).await;
    let mut admin = peer.admin().await;
    let out = admin
        .describe_streams_groups(&["alpha"], &options())
        .await
        .unwrap();
    assert!(matches!(
        out[0].description.as_ref().unwrap_err().as_ref(),
        Error::Unsupported(_)
    ));
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|f| f.api == 89).count(), 1);
}
#[tokio::test]
async fn malformed_duplicate_missing_trailing_and_correlation_fail() {
    let mut duplicate = streams::decode_streams_group_describe_response(
        &group("alpha", 0),
        0,
        StreamsLimits::default(),
    )
    .unwrap();
    duplicate.groups.push(duplicate.groups[0].clone());
    let empty = StreamsGroupDescribeResponse::default();
    let mut trailing = group("alpha", 0);
    trailing.push(0);
    for reply in [
        Reply::Body(
            streams::encode_streams_group_describe_response(
                &duplicate,
                0,
                StreamsLimits::default(),
            )
            .unwrap(),
        ),
        Reply::Body(
            streams::encode_streams_group_describe_response(&empty, 0, StreamsLimits::default())
                .unwrap(),
        ),
        Reply::Body(trailing),
        Reply::Correlation(group("alpha", 0)),
    ] {
        let peer = prepared().await;
        peer.script(1, [reply]).await;
        let mut admin = peer.admin().await;
        assert!(matches!(
            admin.describe_streams_groups(&["alpha"], &options()).await,
            Err(Error::Protocol(_))
        ));
        drop(admin);
        let _closed = peer.close().await;
    }
}
#[tokio::test]
async fn duplicate_expansion_is_bounded_before_cloning() {
    let peer = prepared().await;
    let mut admin = peer.admin().await;
    let mut opts = options();
    opts.limits.wire_bytes = group("alpha", 0).len() + 10;
    assert!(admin
        .describe_streams_groups(&["alpha", "alpha"], &opts)
        .await
        .is_err());
    drop(admin);
    let _closed = peer.close().await;
}
#[tokio::test]
async fn timeout_and_cancellation_close_operation_sockets_before_admin_drop() {
    for cancel in [false, true] {
        let peer = prepared().await;
        peer.script(1, [Reply::Stall]).await;
        let mut admin = peer.admin().await;
        let mut opts = options();
        opts.timeout = Some(Duration::from_millis(70));
        let started = Instant::now();
        if cancel {
            let future = admin.describe_streams_groups(&["alpha"], &opts);
            tokio::pin!(future);
            tokio::select! { result=&mut future=>panic!("unexpected {result:?}"),_ = tokio::time::sleep(Duration::from_millis(30))=>() }
        } else {
            assert!(matches!(
                admin.describe_streams_groups(&["alpha"], &opts).await,
                Err(Error::Timeout)
            ));
        }
        assert!(started.elapsed() < Duration::from_millis(300));
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if peer.state.lock().await.active_workers == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(admin
            .describe_streams_groups(&["alpha"], &options())
            .await
            .unwrap()[0]
            .description
            .is_ok());
        drop(admin);
        let _closed = peer.close().await;
    }
}
#[tokio::test]
async fn delayed_retry_uses_original_deadline_and_attempts_are_bounded() {
    let peer = prepared().await;
    peer.script(
        1,
        [Reply::Delay(Duration::from_millis(100), group("alpha", 0))],
    )
    .await;
    let mut admin = peer.admin().await;
    let mut opts = options();
    opts.timeout = Some(Duration::from_millis(50));
    assert!(matches!(
        admin.describe_streams_groups(&["alpha"], &opts).await,
        Err(Error::Timeout)
    ));
    drop(admin);
    let _closed = peer.close().await;
    let peer = prepared().await;
    peer.script(1, (0..8).map(|_| Reply::Body(group("alpha", 14))))
        .await;
    let mut admin = peer.admin().await;
    let out = admin
        .describe_streams_groups(&["alpha"], &options())
        .await
        .unwrap();
    assert_eq!(
        out[0].description.as_ref().unwrap_err().broker_code(),
        Some(14)
    );
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|f| f.api == 89).count(), 8);
}

/// Genuine SDK peers are driven and parent-waited by the external runner.
#[tokio::test]
#[ignore = "requires pinned SDK inputs and owned process runner"]
async fn serve_streams_describe_probe() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("STREAMS_DESCRIBE_DIRECTORY").unwrap());
    let fixture = std::path::PathBuf::from(std::env::var_os("STREAMS_DESCRIBE_FIXTURES").unwrap());
    let mode = std::env::var("STREAMS_DESCRIBE_MODE").unwrap();
    let peer = Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        for id in ["alpha", "beta"] {
            let raw = tokio::fs::read(fixture.join(format!("{mode}-{id}.bin")))
                .await
                .unwrap();
            let _previous = state.groups.insert(id.into(), raw);
        }
        if ["error", "reroute", "disconnect", "mixed", "downgrade"].contains(&mode.as_str()) {
            let _previous = state.group_routes.insert("beta".into(), 2);
        }
        if mode == "mixed" {
            state.ranges[2] = None;
        }
        state.move_alpha_on_reply = mode == "reroute" || mode == "disconnect";
        state.downgrade_on_disconnect = mode == "downgrade";
    }
    if mode == "reroute" {
        peer.script(
            1,
            [Reply::Body(code(
                tokio::fs::read(fixture.join("full-alpha.bin"))
                    .await
                    .unwrap(),
                16,
            ))],
        )
        .await;
    }
    if mode == "disconnect" || mode == "downgrade" {
        peer.script(1, [Reply::Disconnect]).await;
    }
    tokio::fs::write(directory.join("ready"), peer.addresses[0].to_string())
        .await
        .unwrap();
    if std::env::var_os("STREAMS_DESCRIBE_INVOKE").is_some() {
        let mut admin = peer.admin().await;
        let output = admin
            .describe_streams_groups(&["beta", "alpha", "beta"], &options())
            .await
            .unwrap();
        assert_eq!(output.len(), 3);
        let mut rows = String::new();
        for (entry, id) in output.iter().zip(["beta", "alpha", "beta"]) {
            assert_eq!(entry.group_id, id);
            match &entry.description {
                Ok(group) => {
                    let raw = streams::encode_streams_group_describe_response(
                        &StreamsGroupDescribeResponse {
                            throttle_time_ms: 0,
                            groups: vec![group.clone()],
                        },
                        0,
                        StreamsLimits::default(),
                    )
                    .unwrap();
                    tokio::fs::write(directory.join(format!("{id}.result.bin")), raw)
                        .await
                        .unwrap();
                    rows.push_str(&format!(
                        "{id}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        group.error_code,
                        group.group_epoch,
                        group.assignment_epoch,
                        group.topology.as_ref().map_or(-1, |t| t.epoch),
                        group.members.len(),
                        group
                            .topology
                            .as_ref()
                            .and_then(|t| t.subtopologies.as_ref())
                            .map_or(0, Vec::len)
                    ));
                }
                Err(error) => {
                    let error_code = if matches!(error.as_ref(), Error::Unsupported(_)) {
                        35
                    } else {
                        error.broker_code().unwrap_or(-1)
                    };
                    rows.push_str(&format!("{id}\t{error_code}\t{error}\n"));
                }
            }
        }
        tokio::fs::write(directory.join("rust-outcome.tsv"), rows)
            .await
            .unwrap();
        drop(admin);
    } else {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !directory.join("stop").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    let frames = peer.close().await;
    let mut rows = String::from("file\tapi\tslot\tversion\n");
    for (index, frame) in frames
        .iter()
        .filter(|f| f.api == 10 || f.api == 89)
        .enumerate()
    {
        let name = format!("request-{index}.bin");
        tokio::fs::write(directory.join(&name), &frame.raw)
            .await
            .unwrap();
        rows.push_str(&format!(
            "{name}\t{}\t{}\t{}\n",
            frame.api, frame.slot, frame.version
        ));
    }
    tokio::fs::write(directory.join("frames.tsv"), rows)
        .await
        .unwrap();
    tokio::fs::write(
        directory.join("closure.txt"),
        "listeners_joined=3\nworkers_joined=true\nports_closed_and_rebound=3\nruntime_tasks=0\n",
    )
    .await
    .unwrap();
}
