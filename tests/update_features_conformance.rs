mod common;

use bytes::BytesMut;
use partitionline::protocol::admin::{encode_update_features_request, FeatureUpdateKey};
use partitionline::protocol::api::UPDATE_FEATURES;
use partitionline::{Admin, Error, FeatureUpdate};

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
