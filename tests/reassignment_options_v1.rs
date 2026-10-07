//! Reassignment policy negotiation and independent Apache wire regressions.
#![expect(
    clippy::disallowed_methods,
    clippy::panic,
    clippy::unreachable,
    reason = "finite test assertions and bounded fixture/artifact I/O; production lint unchanged"
)]
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "bounded regression assertions and finite wire peers"
)]

#[path = "fixtures/reassignment-options-v1/socket_peer.rs"]
mod socket_peer;

use bytes::BytesMut;
use partitionline::protocol::admin::*;
use partitionline::{AlterPartitionReassignmentsOptions, Error, PartitionReassignment};
use socket_peer::{finish, fixture, request, Peer, Reply, BUDGET};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn assignments() -> Vec<PartitionReassignment> {
    vec![
        PartitionReassignment::assign(("topic", 0), [1, 2, 3]),
        PartitionReassignment::assign(("topic", 1), [2, 3]),
        PartitionReassignment::cancel(("topic", 2)),
    ]
}
fn options(allow: bool) -> AlterPartitionReassignmentsOptions {
    AlterPartitionReassignmentsOptions::default()
        .allow_replication_factor_change(allow)
        .timeout(BUDGET)
}
fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reassignment-options-v1")
}
fn emit(release: &str, name: &str, bytes: &[u8]) {
    if let Some(root) = std::env::var_os("PL_REASSIGNMENT_WIRE_OUTPUT") {
        let out = PathBuf::from(root).join(release);
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join(name), bytes).unwrap();
    }
}

#[test]
fn all_three_actual_apache_serializers_preserve_policy_and_result_tree() {
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        for version in 0..=1 {
            for cell in ["true", "false", "empty", "mixed", "top-error", "tagged"] {
                let allow = !matches!(cell, "false" | "mixed");
                if version == 0 && !allow {
                    continue;
                }
                let prefix = format!("v{version}-{cell}");
                let input = std::fs::read(
                    fixtures()
                        .join(release)
                        .join(format!("{prefix}-request.bin")),
                )
                .unwrap();
                let reply = std::fs::read(
                    fixtures()
                        .join(release)
                        .join(format!("{prefix}-response.bin")),
                )
                .unwrap();
                let req = decode_alter_partition_reassignments_request_data(
                    &mut input.as_slice(),
                    version,
                )
                .unwrap();
                let resp = decode_alter_partition_reassignments_response_data(
                    &mut reply.as_slice(),
                    version,
                )
                .unwrap();
                assert_eq!(req.timeout_ms, 2000);
                assert_eq!(req.allow_replication_factor_change, allow);
                assert_eq!(resp.allow_replication_factor_change, allow);
                assert_eq!(resp.response.throttle_time_ms, 7);
                assert_eq!(
                    resp.response.error_code,
                    if cell == "top-error" { 41 } else { 0 }
                );
                assert_eq!(req.topics.len(), usize::from(cell != "empty"));
                assert_eq!(resp.response.results.len(), usize::from(cell != "empty"));
                if cell != "empty" {
                    assert_eq!(req.topics[0].partitions[2].replicas, None);
                    assert_eq!(
                        resp.response.results[0]
                            .partitions
                            .iter()
                            .map(|p| p.error_code)
                            .collect::<Vec<_>>(),
                        [0, if allow { 0 } else { 38 }, 0]
                    );
                }
                let mut output = BytesMut::new();
                let mut response = BytesMut::new();
                encode_alter_partition_reassignments_request_data(&mut output, version, &req)
                    .unwrap();
                encode_alter_partition_reassignments_response_data(&mut response, version, &resp)
                    .unwrap();
                if cell != "tagged" {
                    assert_eq!(output.as_ref(), input);
                    assert_eq!(response.as_ref(), reply);
                }
                emit(release, &format!("{prefix}-request.bin"), &output);
                emit(release, &format!("{prefix}-response.bin"), &response);
            }
        }
    }
}

#[test]
fn truncation_trailing_required_null_arrays_and_caps_fail_before_allocation() {
    for version in 0..=1 {
        let req = std::fs::read(
            fixtures()
                .join("4.3.1")
                .join(format!("v{version}-true-request.bin")),
        )
        .unwrap();
        let resp = fixture(version, "true");
        for end in 0..req.len() {
            assert!(
                decode_alter_partition_reassignments_request_data(&mut &req[..end], version)
                    .is_err()
            );
        }
        for end in 0..resp.len() {
            assert!(
                decode_alter_partition_reassignments_response_data(&mut &resp[..end], version)
                    .is_err()
            );
        }
        let mut trailing = req.clone();
        trailing.push(0);
        assert!(decode_alter_partition_reassignments_request_data(
            &mut trailing.as_slice(),
            version
        )
        .is_err());
        let mut trailing = resp.to_vec();
        trailing.push(0);
        assert!(decode_alter_partition_reassignments_response_data(
            &mut trailing.as_slice(),
            version
        )
        .is_err());
        for count in [
            &[0][..],
            &[0x92, 0x4e][..],
            &[0xff, 0xff, 0xff, 0xff, 0x07][..],
        ] {
            let mut bad = req[..4 + usize::from(version == 1)].to_vec();
            bad.extend_from_slice(count);
            bad.push(0);
            assert!(decode_alter_partition_reassignments_request_data(
                &mut bad.as_slice(),
                version
            )
            .is_err());
            let mut bad = resp[..7 + usize::from(version == 1)].to_vec();
            bad.extend_from_slice(count);
            bad.push(0);
            assert!(decode_alter_partition_reassignments_response_data(
                &mut bad.as_slice(),
                version
            )
            .is_err());
        }
    }
    let mut bad = fixture(1, "true").to_vec();
    bad[4] = 2;
    assert!(decode_alter_partition_reassignments_response_data(&mut bad.as_slice(), 1).is_err());
    let mut output = BytesMut::from(&b"prefix"[..]);
    let req = AlterPartitionReassignmentsRequestData {
        allow_replication_factor_change: false,
        ..Default::default()
    };
    assert!(matches!(
        encode_alter_partition_reassignments_request_data(&mut output, 0, &req),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(output.as_ref(), b"prefix");
    for version in [-1, 2, i16::MAX] {
        assert!(
            encode_alter_partition_reassignments_request_data(&mut output, version, &req).is_err()
        );
        assert_eq!(output.as_ref(), b"prefix");
    }
}

#[tokio::test]
async fn public_admin_accepts_a_version_one_only_controller() {
    let peer = Peer::start([Some((1, 1)); 2]).await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin
        .alter_partition_reassignments(&assignments(), 2000)
        .await;
    let closed = finish(admin, peer, "v1-only-default-true").await;
    assert_eq!(
        result
            .unwrap()
            .iter()
            .map(|r| r.error_code)
            .collect::<Vec<_>>(),
        [0, 0, 0]
    );
    let frames: Vec<_> = closed.observed.iter().filter(|r| r.api == 45).collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].version, 1);
    assert!(request(&frames[0].body, 1).allow);
    assert_eq!(closed.listener_tasks_joined, 2);
    assert!(closed.connection_tasks_joined > 0 && closed.ports_closed_and_reusable);
}

#[tokio::test]
async fn false_policy_preserves_same_factor_factor_change_and_cancellation_results() {
    let peer = Peer::start([Some((0, 1)); 2]).await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin
        .alter_partition_reassignments_with_options(&assignments(), options(false))
        .await;
    let closed = finish(admin, peer, "v1-false-mixed").await;
    let rows = result.unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.partition, r.error_code))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 38), (2, 0)]
    );
    assert_eq!(rows[1].error_message.as_deref(), Some("factor change"));
    let frame = closed.observed.iter().find(|r| r.api == 45).unwrap();
    assert_eq!(frame.version, 1);
    assert!(!request(&frame.body, 1).allow);
}

#[tokio::test]
async fn false_on_zero_rejects_before_mutation_and_default_true_remains_compatible() {
    let peer = Peer::start([Some((0, 0)); 2]).await;
    let mut admin = peer.admin().await.unwrap();
    let rejected = admin
        .alter_partition_reassignments_with_options(&assignments(), options(false))
        .await;
    let allowed = admin
        .alter_partition_reassignments(&assignments(), 2000)
        .await;
    let closed = finish(admin, peer, "v0-false-rejection-then-true").await;
    assert!(matches!(rejected, Err(Error::Unsupported(_))));
    assert!(allowed.is_ok());
    let frames: Vec<_> = closed.observed.iter().filter(|r| r.api == 45).collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].version, 0);
    assert!(request(&frames[0].body, 0).allow);
}

#[tokio::test]
async fn missing_or_disjoint_api_does_not_block_unrelated_admin_startup() {
    for (index, range) in [None, Some((2, 3))].into_iter().enumerate() {
        let peer = Peer::start([range; 2]).await;
        let mut admin = peer.admin().await.unwrap();
        let result = admin
            .alter_partition_reassignments_with_options(&assignments(), options(true))
            .await;
        let closed = finish(admin, peer, &format!("unsupported-{index}")).await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert!(closed.observed.iter().all(|r| r.api != 45));
    }
}

#[tokio::test]
async fn controller_failover_renegotiates_and_preserves_false_policy() {
    let peer = Peer::start([Some((0, 1)), Some((1, 1))]).await;
    peer.script(1, [Reply::ControllerMoved(fixture(1, "top-error"))])
        .await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin
        .alter_partition_reassignments_with_options(&assignments(), options(false))
        .await;
    let closed = finish(admin, peer, "controller-move-false").await;
    assert!(result.is_ok());
    let frames: Vec<_> = closed.observed.iter().filter(|r| r.api == 45).collect();
    assert_eq!(
        frames
            .iter()
            .map(|r| (r.node, r.version))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 1)]
    );
    let parsed: Vec<_> = frames.iter().map(|r| request(&r.body, r.version)).collect();
    assert!(parsed.iter().all(|r| !r.allow));
    assert_eq!(parsed[0].assignments, parsed[1].assignments);
    assert!(parsed[1].timeout_ms <= parsed[0].timeout_ms);
}

#[tokio::test]
async fn lost_ack_reconnects_to_mixed_versions_without_dropping_policy() {
    for allow in [false, true] {
        let peer = Peer::start([Some((1, 1)), Some((0, 0))]).await;
        peer.script(1, [Reply::Disconnect(true)]).await;
        let mut admin = peer.admin().await.unwrap();
        let result = admin
            .alter_partition_reassignments_with_options(&assignments(), options(allow))
            .await;
        let closed = finish(admin, peer, &format!("lost-ack-allow-{allow}")).await;
        let frames: Vec<_> = closed.observed.iter().filter(|r| r.api == 45).collect();
        if allow {
            assert!(result.is_ok());
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[1].version, 0);
        } else {
            assert!(matches!(result, Err(Error::Unsupported(_))));
            assert_eq!(frames.len(), 1);
        }
        assert!(frames
            .iter()
            .all(|r| request(&r.body, r.version).allow == allow));
    }
}

#[tokio::test]
async fn metadata_delay_and_retries_share_the_original_deadline() {
    let peer = Peer::start([Some((1, 1)); 2]).await;
    peer.state.lock().await.metadata_delay = Duration::from_millis(500);
    let mut admin = peer.admin().await.unwrap();
    let start = Instant::now();
    let result = admin
        .alter_partition_reassignments_with_options(
            &assignments(),
            options(false).timeout(Duration::from_millis(80)),
        )
        .await;
    let elapsed = start.elapsed();
    let closed = finish(admin, peer, "metadata-deadline").await;
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(elapsed < Duration::from_millis(300));
    assert!(closed.observed.iter().all(|r| r.api != 45));

    let peer = Peer::start([Some((1, 1)); 2]).await;
    peer.script(
        1,
        [
            Reply::Delay(Duration::from_millis(30), fixture(1, "top-error")),
            Reply::Delay(Duration::from_millis(30), fixture(1, "top-error")),
        ],
    )
    .await;
    let mut admin = peer.admin().await.unwrap();
    let result = admin
        .alter_partition_reassignments_with_options(&assignments(), options(false))
        .await;
    let closed = finish(admin, peer, "retry-deadline").await;
    assert!(result.is_ok());
    let parsed: Vec<_> = closed
        .observed
        .iter()
        .filter(|r| r.api == 45)
        .map(|r| request(&r.body, r.version))
        .collect();
    assert_eq!(parsed.len(), 3);
    assert!(parsed.iter().all(|r| !r.allow));
    assert!(parsed.windows(2).all(|p| p[1].timeout_ms < p[0].timeout_ms));
}

#[tokio::test]
async fn cancelled_call_discards_poisoned_controller_connection_before_renegotiation() {
    let peer = Peer::start([Some((1, 1)); 2]).await;
    peer.script(1, [Reply::Stall]).await;
    let mut admin = peer.admin().await.unwrap();
    let input = assignments();
    {
        let pending = admin.alter_partition_reassignments_with_options(&input, options(false));
        tokio::pin!(pending);
        tokio::select! { ()=tokio::time::sleep(Duration::from_millis(80))=>{}, result=&mut pending=>panic!("unexpected completion: {result:?}") }
    }
    peer.state.lock().await.ranges[0] = Some((0, 0));
    let result = admin
        .alter_partition_reassignments_with_options(&input, options(false))
        .await;
    let closed = finish(admin, peer, "cancelled-then-downgraded").await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    let frames: Vec<_> = closed.observed.iter().filter(|r| r.api == 45).collect();
    assert_eq!(frames.len(), 1);
    assert!(!request(&frames[0].body, 1).allow);
}

#[tokio::test]
async fn local_assignment_and_replica_caps_fail_without_discovery_or_mutation() {
    let peer = Peer::start([Some((1, 1)); 2]).await;
    let mut admin = peer.admin().await.unwrap();
    let inputs = [
        vec![PartitionReassignment::cancel(("topic", 0)); MAX_REASSIGNMENT_PARTITIONS + 1],
        vec![PartitionReassignment::assign(
            ("topic", 0),
            vec![1; MAX_REASSIGNMENT_REPLICAS + 1],
        )],
        vec![PartitionReassignment::cancel(("x".repeat(250), 0))],
    ];
    let mut results = Vec::new();
    for input in inputs {
        results.push(
            admin
                .alter_partition_reassignments_with_options(&input, options(true))
                .await,
        );
    }
    let closed = finish(admin, peer, "request-caps").await;
    assert!(results.iter().all(|r| matches!(r, Err(Error::Protocol(_)))));
    assert!(closed.observed.iter().all(|r| r.api != 3 && r.api != 45));
}

#[tokio::test]
async fn malformed_result_count_and_trailing_bytes_fail_as_protocol_errors() {
    for (index, body) in [vec![0, 0, 0, 0, 1, 0, 0, 0, 0x92, 0x4e, 0], {
        let mut body = fixture(1, "true").to_vec();
        body.push(0);
        body
    }]
    .into_iter()
    .enumerate()
    {
        let peer = Peer::start([Some((1, 1)); 2]).await;
        peer.script(1, [Reply::Body(body.into())]).await;
        let mut admin = peer.admin().await.unwrap();
        let result = admin
            .alter_partition_reassignments_with_options(&assignments(), options(true))
            .await;
        let _closed = finish(admin, peer, &format!("response-cap-{index}")).await;
        assert!(matches!(result, Err(Error::Protocol(_))));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicitly pinned Java SDK jars, libraries and an artifact directory"]
async fn actual_public_java_options_match_bounded_policy_peer() {
    let jars =
        PathBuf::from(std::env::var_os("PL_REASSIGNMENT_JARS").expect("pinned SDK directory"));
    let libs =
        PathBuf::from(std::env::var_os("PL_REASSIGNMENT_LIBS").expect("Java dependency directory"));
    let output =
        PathBuf::from(std::env::var_os("PL_REASSIGNMENT_OUTPUT").expect("artifact directory"));
    std::fs::create_dir_all(&output).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ReassignmentOptionsV1.java");
    let mut receipts = Vec::new();
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let jar = jars.join(format!("kafka-clients-{release}.jar"));
        for (profile, range) in [
            ("public-v1-true", Some((1, 1))),
            ("public-v1-false", Some((1, 1))),
            ("public-v0-true", Some((0, 0))),
            ("public-v0-false", Some((0, 0))),
            ("public-missing-false", None),
        ] {
            let label = format!("java-{release}-{profile}");
            let peer = Peer::start([range; 2]).await;
            let log_path = output.join(format!("{label}.log"));
            let log = std::fs::File::create(&log_path).unwrap();
            let classpath = format!("{}:{}/*", jar.display(), libs.display());
            let mut child = OwnedChild(
                std::process::Command::new("java")
                    .args(["-Xmx128m", "--class-path", &classpath])
                    .arg(&source)
                    .arg(release)
                    .arg(&jar)
                    .arg(&peer.bootstrap)
                    .arg(profile)
                    .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
                    .stderr(std::process::Stdio::from(log))
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(20);
            let (status, expired) = loop {
                if child.0.try_wait().unwrap().is_some() {
                    break (child.0.wait().unwrap(), false);
                }
                if Instant::now() >= deadline {
                    child.0.kill().unwrap();
                    break (child.0.wait().unwrap(), true);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            drop(child);
            let closed = peer.close(&label).await;
            let applications: Vec<_> = closed.observed.iter().filter(|row| row.api == 45).collect();
            receipts.push(format!(
                "{{\"release\":\"{release}\",\"profile\":\"{profile}\",\"exit_code\":{},\"deadline_expired\":{expired},\"parent_waited\":true,\"listeners_joined\":{},\"connections_joined\":{},\"ports_closed_and_reusable\":{},\"mutation_frames\":{}}}",
                status.code().unwrap_or(-1), closed.listener_tasks_joined,
                closed.connection_tasks_joined, closed.ports_closed_and_reusable, applications.len(),
            ));
            std::fs::write(
                output.join("java-public-receipts.json"),
                format!("[{}]\n", receipts.join(",\n")),
            )
            .unwrap();
            assert!(
                status.success() && !expired,
                "{label}: {}",
                std::fs::read_to_string(&log_path).unwrap()
            );
            let log = std::fs::read_to_string(log_path).unwrap();
            assert!(
                log.contains("\"public_admin\":true")
                    && log.contains("\"admin_closed\":true")
                    && log.contains("\"partitions\":3")
            );
            if matches!(profile, "public-v0-false" | "public-missing-false") {
                assert!(applications.is_empty());
            } else {
                assert_eq!(applications.len(), 1);
                assert_eq!(
                    request(&applications[0].body, applications[0].version).allow,
                    profile.ends_with("true")
                );
            }
        }
    }
    assert_eq!(receipts.len(), 15);
}

struct OwnedChild(std::process::Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _killed = self.0.kill();
        let _waited = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires a fresh pinned three-broker reference cluster and isolated topics"]
async fn native_reference_cluster_preserves_factor_policy_and_cancels_pending_move() {
    let bootstrap = std::env::var("PL_REASSIGNMENT_NATIVE_BOOTSTRAP").expect("reference cluster");
    let topic = std::env::var("PL_REASSIGNMENT_NATIVE_TOPIC").expect("isolated topic");
    let mode = std::env::var("PL_REASSIGNMENT_NATIVE_MODE").expect("false|true|cancel");
    let input = if mode == "cancel" {
        vec![PartitionReassignment::cancel((topic.as_str(), 0))]
    } else {
        vec![
            PartitionReassignment::assign((topic.as_str(), 0), [2, 3, 1]),
            PartitionReassignment::assign((topic.as_str(), 1), [2, 3]),
            PartitionReassignment::cancel((topic.as_str(), 2)),
        ]
    };
    assert!(matches!(mode.as_str(), "false" | "true" | "cancel"));
    let config =
        partitionline::AdminConfig::bootstrap([bootstrap]).request_timeout(Duration::from_secs(15));
    let mut admin = partitionline::Admin::new(config).await.unwrap();
    let result = admin
        .alter_partition_reassignments_with_options(
            &input,
            options(mode == "true").timeout(Duration::from_secs(10)),
        )
        .await;
    let closed = admin.close().await;
    println!("native_public_result={result:?}");
    closed.unwrap();
    let rows = result.unwrap();
    let expected: &[i16] = match mode.as_str() {
        "false" => &[0, 38, 85],
        "true" => &[0, 0, 85],
        "cancel" => &[0],
        _ => unreachable!(),
    };
    assert_eq!(
        rows.iter().map(|row| row.error_code).collect::<Vec<_>>(),
        expected
    );
    println!(
        "{{\"status\":\"pass\",\"mode\":\"{mode}\",\"partitions\":{},\"admin_closed\":true}}",
        rows.len()
    );
}
