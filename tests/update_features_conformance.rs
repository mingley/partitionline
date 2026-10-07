//! Independent UpdateFeatures safety and wire conformance checks.
mod common;

use bytes::{Buf, Bytes, BytesMut};
use partitionline::protocol::admin::{
    decode_update_features_request, decode_update_features_response, encode_update_features_request,
    encode_update_features_response, FeatureUpdateKey,
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
    let mut admin = Admin::new(
        AdminConfig::bootstrap([address]).request_timeout(Duration::from_secs(2)),
    )
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
            }
            if (2..=4).contains(&index) {
                assert!(errors.values().any(|code| *code != 0));
            }
            rows.push(serde_json::json!({"case":index,"validate_only":validate,
                "rejected_locally":rejected_locally,"errors":errors}));
        }
        assert!(matches!(
            admin.update_features_with(&[], 2000, true).await,
            Err(Error::Protocol(_))
        ));
        let after = admin.describe_features().await.unwrap();
        assert_eq!(before.finalized_features, after.finalized_features);
        serde_json::json!({"actual_public_Admin":true,"metadata_version":level,"cases":rows,
            "empty_rejected":true,"finalized_features_unchanged":true})
    })
    .await;
    admin.close().await.unwrap();
    let observed = outcome.unwrap();
    tokio::fs::write(output, serde_json::to_vec_pretty(&observed).unwrap())
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
    assert_eq!(dispatched, None, "validation-only sent a mutating v0 request");
    assert_eq!(finalized, None, "validation-only changed finalized features");
}
