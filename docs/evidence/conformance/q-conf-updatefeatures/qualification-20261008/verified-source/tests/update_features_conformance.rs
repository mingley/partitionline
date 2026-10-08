//! Independent UpdateFeatures safety and wire conformance checks.
mod common;

use bytes::{Buf, Bytes, BytesMut};
use partitionline::protocol::admin::{
    decode_update_features_request, decode_update_features_response,
    encode_update_features_request, encode_update_features_response, FeatureUpdateKey,
};
use partitionline::protocol::api_keys::UPDATE_FEATURES;
use partitionline::{Admin, AdminConfig, Error, FeatureUpdate};

#[test]
fn v0_validation_request_rejected_before_body_changes() {
    let mut body = BytesMut::from(&b"existing prefix"[..]);
    let original = body.clone();
    let outcome = encode_update_features_request(
        &mut body,
        0,
        10_000,
        &[FeatureUpdateKey::new("metadata.version", 17, false)],
        true,
    );
    assert!(
        matches!(outcome, Err(Error::Unsupported(_))),
        "v0 validation-only must be unsupported before a request is written; got {outcome:?}"
    );
    assert_eq!(body, original, "rejected validation changed caller bytes");
}

#[tokio::test]
#[ignore = "requires guarded Apache SDK fixtures and independent reverse parsing"]
async fn actual_sdk_update_features_bodies() {
    let fixtures = std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_FIXTURES").unwrap());
    let reverse = std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_REVERSE").unwrap());
    tokio::fs::create_dir(&reverse).await.unwrap();
    let mut files = tokio::fs::read_dir(fixtures).await.unwrap();
    let mut count = 0;
    while let Some(item) = files.next_entry().await.unwrap() {
        let path = item.path();
        if path.extension().and_then(|v| v.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let version: i16 = name[1..name.find('-').unwrap()].parse().unwrap();
        let data = tokio::fs::read(&path).await.unwrap();
        assert!(data.len() <= 65_536);
        for length in 0..data.len() {
            if name.ends_with("request.bin") {
                assert!(decode_update_features_request(&mut &data[..length], version).is_err());
            } else {
                assert!(decode_update_features_response(&mut &data[..length], version).is_err());
            }
        }
        let mut cursor = Bytes::from(data);
        let mut encoded = BytesMut::new();
        if name.ends_with("request.bin") {
            let (timeout, updates, validate) =
                decode_update_features_request(&mut cursor, version).unwrap();
            encode_update_features_request(&mut encoded, version, timeout, &updates, validate)
                .unwrap();
        } else {
            let value = decode_update_features_response(&mut cursor, version).unwrap();
            encode_update_features_response(&mut encoded, version, &value).unwrap();
        }
        assert!(!cursor.has_remaining());
        tokio::fs::write(reverse.join(name), encoded).await.unwrap();
        count += 1;
    }
    assert_eq!(count, 101);
}

#[tokio::test]
#[ignore = "requires an owned Apache Kafka broker and independent public Admin caller"]
async fn native_public_update_features_history() {
    use std::collections::BTreeMap;
    use std::time::Duration;
    let address = std::env::var("UPDATE_FEATURES_BROKER").unwrap();
    let output = std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_OUTPUT").unwrap());
    let mut admin =
        Admin::new(AdminConfig::bootstrap([address]).request_timeout(Duration::from_secs(2)))
            .await
            .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(25), async {
        let before = admin.describe_features().await.unwrap();
        let level = before
            .finalized_features
            .iter()
            .find(|value| value.name == "metadata.version")
            .unwrap()
            .max_version_level;
        let mut rows = Vec::new();
        for index in 0..6 {
            let updates = match index {
                0 | 1 => vec![FeatureUpdate::new("metadata.version", level)],
                2 | 3 => vec![FeatureUpdate::new("partitionline.conformance.unknown", 1)],
                4 => vec![
                    FeatureUpdate::new("metadata.version", level),
                    FeatureUpdate::new("partitionline.conformance.unknown", 1),
                ],
                _ => vec![FeatureUpdate::new("", 1)],
            };
            let validate = index != 1 && index != 3;
            let result = admin.update_features_with(&updates, 2000, validate).await;
            let mut errors = BTreeMap::new();
            let rejected_locally = match result {
                Ok(values) => {
                    for value in values {
                        let _ = errors.insert(value.name, value.error_code);
                    }
                    false
                }
                Err(Error::Protocol(_)) if index == 5 => true,
                Err(error) => {
                    let code = error.broker_code().unwrap();
                    for update in &updates {
                        let _ = errors.insert(update.name.clone(), code);
                    }
                    false
                }
            };
            assert_eq!(rejected_locally, index == 5);
            if index < 5 {
                assert_eq!(errors.len(), updates.len());
                assert!(updates.iter().all(|update| errors.contains_key(&update.name)));
            }
            if (2..=4).contains(&index) {
                assert!(errors.values().any(|code| *code != 0));
            }
            let fields = errors
                .iter()
                .map(|(name, code)| {
                    assert!(name == "metadata.version" || name == "partitionline.conformance.unknown");
                    format!("\"{name}\":{code}")
                })
                .collect::<Vec<_>>()
                .join(",");
            rows.push(format!(
                "{{\"case\":{index},\"validate_only\":{validate},\"rejected_locally\":{rejected_locally},\"errors\":{{{fields}}}}}"
            ));
        }
        assert!(matches!(
            admin.update_features_with(&[], 2000, true).await,
            Err(Error::Protocol(_))
        ));
        let after = admin.describe_features().await.unwrap();
        assert_eq!(before.finalized_features, after.finalized_features);
        format!(
            "{{\"actual_public_Admin\":true,\"metadata_version\":{level},\"cases\":[{}],\"empty_rejected\":true,\"finalized_features_unchanged\":true}}",
            rows.join(",")
        )
    })
    .await;
    admin.close().await.unwrap();
    let observed = outcome.unwrap();
    tokio::fs::write(output, observed).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the guarded Apache SDK TCP controller peer"]
async fn public_update_features_controller_and_deadline_history() {
    use std::time::{Duration, Instant};
    let address = std::env::var("UPDATE_FEATURES_BROKER").unwrap();
    let scenario = std::env::var("UPDATE_FEATURES_SCENARIO").unwrap();
    let output = std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_OUTPUT").unwrap());
    let mut admin = Admin::new(
        AdminConfig::bootstrap([address])
            .client_id("update-features-rust")
            .request_timeout(Duration::from_secs(2))
            .retry_backoff(Duration::from_millis(10))
            .retry_backoff_max(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    let timeout = if scenario == "deadline" { 250 } else { 2000 };
    let started = Instant::now();
    let result = admin
        .update_features_with_timeout(
            &[
                FeatureUpdate::new("test_feature_1", 2),
                FeatureUpdate::new("test_feature_2", 3).allow_downgrade(true),
            ],
            Duration::from_millis(timeout),
            false,
        )
        .await;
    let elapsed = started.elapsed();
    let code = match &result {
        Ok(values) => {
            assert_eq!(values.len(), 2);
            let mut names = values
                .iter()
                .map(|value| value.name.as_str())
                .collect::<Vec<_>>();
            names.sort_unstable();
            assert_eq!(names, ["test_feature_1", "test_feature_2"]);
            if scenario == "feature-error" {
                assert_eq!(
                    values
                        .iter()
                        .find(|value| value.name == "test_feature_1")
                        .unwrap()
                        .error_code,
                    41
                );
                assert_eq!(
                    values
                        .iter()
                        .find(|value| value.name == "test_feature_2")
                        .unwrap()
                        .error_code,
                    0
                );
                41
            } else {
                assert!(values.iter().all(|value| value.error_code == 0));
                values[0].error_code
            }
        }
        Err(Error::Timeout) => 7,
        Err(error) => error.broker_code().unwrap(),
    };
    let recovery_code = if scenario == "deadline" {
        let recovered = admin
            .update_features_with_timeout(
                &[
                    FeatureUpdate::new("test_feature_1", 2),
                    FeatureUpdate::new("test_feature_2", 3).allow_downgrade(true),
                ],
                Duration::from_secs(2),
                false,
            )
            .await;
        assert!(
            recovered.is_ok(),
            "cancelled connection did not recover: {recovered:?}"
        );
        assert_eq!(recovered.unwrap()[0].error_code, 0);
        Some(0)
    } else {
        None
    };
    admin.close().await.unwrap();
    tokio::fs::write(
        output,
        format!(
            "{{\"error_code\":{code},\"elapsed_us\":{},\"recovery_code\":{}}}",
            elapsed.as_micros(),
            recovery_code.map_or("null".to_string(), |value| value.to_string())
        ),
    )
    .await
    .unwrap();
    let expected = match scenario.as_str() {
        "deadline" => 7,
        "top-error" => 42,
        "success" | "empty-success" | "retry" | "legacy" => 0,
        "feature-error" => 41,
        _ => panic!("undeclared scripted scenario"),
    };
    assert_eq!(
        code, expected,
        "actual public outcome {result:?}, elapsed {elapsed:?}"
    );
}

#[tokio::test]
#[ignore = "requires independently rejected Apache SDK malformed messages"]
async fn actual_sdk_update_features_malformed_bodies() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_MALFORMED").unwrap());
    let mut files = tokio::fs::read_dir(directory).await.unwrap();
    let mut accepted = Vec::new();
    let mut count = 0;
    while let Some(item) = files.next_entry().await.unwrap() {
        let path = item.path();
        if path.extension().and_then(|value| value.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let version = name[1..name.find('-').unwrap()].parse().unwrap();
        let body = tokio::fs::read(&path).await.unwrap();
        let rejected = if name.contains("request") {
            decode_update_features_request(&mut &body[..], version).is_err()
        } else {
            decode_update_features_response(&mut &body[..], version).is_err()
        };
        if !rejected {
            accepted.push(name.to_owned());
        }
        count += 1;
    }
    assert_eq!(count, 10);
    assert!(
        accepted.is_empty(),
        "accepted SDK-rejected malformed fields: {accepted:?}"
    );
}

#[tokio::test]
#[ignore = "requires independent Apache public argument checks and a physical TCP peer"]
async fn public_update_features_name_validation_history() {
    use std::time::Duration;
    let address = std::env::var("UPDATE_FEATURES_BROKER").unwrap();
    let output = std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_OUTPUT").unwrap());
    let mut admin = Admin::new(
        AdminConfig::bootstrap([address])
            .client_id("update-features-rust")
            .request_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let mut rows = Vec::new();
    for name in ["", " ", "\0", "\u{1f}", "\u{a0}", "\u{2000}", "feature"] {
        let result = admin
            .update_features_with(&[FeatureUpdate::new(name, 1)], 1000, false)
            .await;
        let rejected = matches!(result, Err(Error::Protocol(_)));
        if !rejected {
            assert!(result.is_ok(), "unexpected name outcome: {result:?}");
        }
        let code = name.chars().next().map_or(-1, |character| character as i32);
        rows.push(format!(
            "{{\"name_code\":{code},\"rejected_locally\":{rejected}}}"
        ));
    }
    for (code, updates) in [
        (-2, Vec::new()),
        (
            -3,
            vec![FeatureUpdate::new("feature", 2), FeatureUpdate::new("", 2)],
        ),
    ] {
        assert!(matches!(
            admin.update_features_with(&updates, 1000, false).await,
            Err(Error::Protocol(_))
        ));
        rows.push(format!(
            "{{\"name_code\":{code},\"rejected_locally\":true}}"
        ));
    }
    admin.close().await.unwrap();
    tokio::fs::write(output, format!("[{}]", rows.join(",")))
        .await
        .unwrap();
}

#[tokio::test]
async fn v0_public_validation_does_not_dispatch_or_mutate() {
    let mock = common::Mock::start().await;
    mock.set_api_max(UPDATE_FEATURES, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let outcome = admin
        .update_features_with(&[FeatureUpdate::new("metadata.version", 17)], 10_000, true)
        .await;
    let dispatched = mock.last_update_features_version();
    let finalized = mock.feature_level("metadata.version");
    admin.close().await.unwrap();
    assert!(
        matches!(outcome, Err(Error::Unsupported(_))),
        "v0 validation-only must fail locally: outcome={outcome:?}, dispatched={dispatched:?}, finalized={finalized:?}"
    );
    assert_eq!(
        dispatched, None,
        "validation-only sent a mutating v0 request"
    );
    assert_eq!(
        finalized, None,
        "validation-only changed finalized features"
    );
}

#[tokio::test]
#[ignore = "requires actual Apache source-case bodies"]
async fn actual_sdk_update_features_source_cases() {
    use std::collections::HashMap;
    let directory =
        std::path::PathBuf::from(std::env::var_os("UPDATE_FEATURES_SOURCE_CASES").unwrap());
    let mut count = 0;
    let mut files = tokio::fs::read_dir(directory).await.unwrap();
    while let Some(entry) = files.next_entry().await.unwrap() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let version = name[1..name.find('-').unwrap()].parse().unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        let mut body = &bytes[..];
        if name.ends_with("request.bin") {
            let (timeout, updates, validate) =
                decode_update_features_request(&mut body, version).unwrap();
            assert_eq!(timeout, 10000);
            assert!(!validate);
            assert_eq!(updates.len(), 2);
            assert_eq!(
                (
                    &*updates[0].name,
                    updates[0].max_version_level,
                    updates[0].upgrade_type
                ),
                ("foo", 1, 2)
            );
            assert_eq!(
                (
                    &*updates[1].name,
                    updates[1].max_version_level,
                    updates[1].upgrade_type
                ),
                ("bar", 3, 1)
            );
        } else {
            let response = decode_update_features_response(&mut body, version).unwrap();
            if name.contains("counts") {
                assert_eq!(
                    response.error_counts(),
                    if version < 2 {
                        HashMap::from([(42, 1), (-1, 2), (96, 1)])
                    } else {
                        HashMap::from([(42, 1)])
                    }
                );
                if version < 2 {
                    assert_eq!(
                        response
                            .results
                            .iter()
                            .map(|value| (&*value.name, value.error_code))
                            .collect::<Vec<_>>(),
                        [("foo", -1), ("bar", -1), ("baz", 96)]
                    );
                }
            } else if name.contains("factory") {
                assert_eq!(response.error_counts(), HashMap::from([(-1, 1)]));
                assert!(response.results.is_empty());
            } else if name.contains("success") {
                assert_eq!(response.error_code, 0);
                assert_eq!(response.results.len(), if version < 2 { 2 } else { 0 });
                let mut names = response
                    .results
                    .iter()
                    .map(|value| value.name.as_str())
                    .collect::<Vec<_>>();
                names.sort_unstable();
                if version < 2 {
                    assert_eq!(names, ["feature-1", "feature-2"]);
                }
            } else {
                assert_eq!(
                    response.error_code,
                    partitionline::error::INVALID_UPDATE_VERSION
                );
                assert!(response.results.is_empty());
            }
        }
        assert!(body.is_empty());
        count += 1;
    }
    assert_eq!(count, 12);
    for version in 0..=2 {
        let mut body = BytesMut::from(&b"unchanged"[..]);
        let before = body.clone();
        assert!(encode_update_features_request(
            &mut body,
            version,
            1000,
            &[FeatureUpdateKey::new("feature", 0, false)],
            false
        )
        .is_err());
        assert_eq!(
            body, before,
            "Java rejects deletion without downgrade before serialization"
        );
    }
}
