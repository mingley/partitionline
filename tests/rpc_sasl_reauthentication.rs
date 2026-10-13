//! KL06-12: Consumer, ConsumerGroup, ShareGroup, and Admin honor broker
//! SaslAuthenticate session lifetimes on the shared exclusive roundtrip path.
#![expect(
    unused_results,
    reason = "tests discard describe_cluster and poll results after asserting side effects"
)]

mod common;

use std::time::{Duration, Instant};

use partitionline::net::{is_reserved_correlation_id, BrokerConn};
use partitionline::protocol::api::negotiate_api_versions;
use partitionline::protocol::api_keys::{DESCRIBE_CLUSTER, METADATA, SASL_AUTHENTICATE};
use partitionline::protocol::sasl::{apply_api_keys, authenticate};
use partitionline::{
    Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup, Error, OidcConfig, Sasl,
    ShareGroup,
};

const SESSION_MS: i64 = 220;
const AFTER_REAUTH: Duration = Duration::from_millis(210);

fn plain_admin(addr: &str) -> AdminConfig {
    AdminConfig::bootstrap([addr.to_string()]).sasl(Sasl::plain("alice", "secret"))
}

fn plain_consumer(addr: &str) -> ConsumerConfig {
    let mut cfg = ConsumerConfig::bootstrap([addr.to_string()]);
    cfg.sasl_plain = Some(("alice".into(), "secret".into()));
    cfg.max_wait_ms = 10;
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg
}

#[tokio::test]
async fn admin_roundtrip_reauthenticates_before_describe_cluster() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut admin = Admin::new(plain_admin(&mock.addr)).await.unwrap();
    admin.describe_cluster().await.unwrap();
    assert_eq!(mock.describe_cluster_calls(), 1);
    let auths = mock.sasl_authenticate_calls();

    tokio::time::sleep(AFTER_REAUTH).await;
    admin.describe_cluster().await.unwrap();

    assert_eq!(mock.describe_cluster_calls(), 2);
    assert!(
        mock.sasl_authenticate_calls() > auths,
        "Admin describe_cluster must send SaslAuthenticate before the second RPC"
    );
    assert!(is_reserved_correlation_id(
        mock.last_sasl_authenticate_correlation()
            .expect("reauth records a SaslAuthenticate correlation")
    ));
    assert_eq!(mock.last_application_api_key(), Some(DESCRIBE_CLUSTER));
}

#[tokio::test]
async fn consumer_roundtrip_reauthenticates_before_partitions_for() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut consumer = Consumer::new(plain_consumer(&mock.addr)).await.unwrap();
    let first = consumer.partitions_for("t").await.unwrap();
    assert!(!first.is_empty());
    let auths = mock.sasl_authenticate_calls();

    tokio::time::sleep(AFTER_REAUTH).await;
    let second = consumer.partitions_for("t").await.unwrap();
    assert_eq!(second.len(), first.len());
    assert!(
        mock.sasl_authenticate_calls() > auths,
        "manual Consumer metadata must reauth before the second partitions_for"
    );
    assert_eq!(mock.last_application_api_key(), Some(METADATA));
}

#[tokio::test]
async fn classic_group_roundtrip_reauthenticates_before_membership_rpc() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut group = ConsumerGroup::join(plain_consumer(&mock.addr), "rpc-reauth-classic", "t")
        .await
        .unwrap();
    let auths = mock.sasl_authenticate_calls();
    common::wait_pred("classic group SASL reauth", || {
        mock.sasl_authenticate_calls() > auths
    })
    .await;
    let _recs = group.poll().await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_group_roundtrip_reauthenticates_before_membership_rpc() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut group = ConsumerGroup::join_consumer(plain_consumer(&mock.addr), "rpc-reauth-848", "t")
        .await
        .unwrap();
    let auths = mock.sasl_authenticate_calls();
    common::wait_pred("KIP-848 SASL reauth", || {
        mock.sasl_authenticate_calls() > auths
    })
    .await;
    let _recs = group.poll().await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_group_roundtrip_reauthenticates_before_membership_rpc() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut group = ShareGroup::join(plain_consumer(&mock.addr), "rpc-reauth-share", "t")
        .await
        .unwrap();
    let auths = mock.sasl_authenticate_calls();
    common::wait_pred("share group SASL reauth", || {
        mock.sasl_authenticate_calls() > auths
    })
    .await;
    let _recs = group.poll().await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn admin_oidc_roundtrip_reauthenticates_with_shared_provider() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let cfg = AdminConfig::bootstrap([mock.addr.clone()])
        .sasl(Sasl::oidc(OidcConfig::new(token_url, "cid", "csecret")));
    let mut admin = Admin::new(cfg).await.unwrap();
    admin.describe_cluster().await.unwrap();
    let auths = mock.sasl_authenticate_calls();
    tokio::time::sleep(AFTER_REAUTH).await;
    admin.describe_cluster().await.unwrap();
    assert!(
        mock.sasl_authenticate_calls() > auths,
        "OIDC Admin reauth must send another SaslAuthenticate"
    );
    assert!(is_reserved_correlation_id(
        mock.last_sasl_authenticate_correlation()
            .expect("OIDC reauth records a SaslAuthenticate correlation")
    ));
    assert_eq!(mock.describe_cluster_calls(), 2);
}

#[tokio::test]
async fn admin_failed_reauth_closes_without_application_fallback() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_sasl_session_lifetime_ms(SESSION_MS);
    let mut admin = Admin::new(plain_admin(&mock.addr)).await.unwrap();
    admin.describe_cluster().await.unwrap();
    let describes = mock.describe_cluster_calls();
    mock.set_sasl_user(Some(("alice".into(), "other".into())));

    tokio::time::sleep(AFTER_REAUTH).await;
    let err = admin.describe_cluster().await.unwrap_err();
    assert!(
        matches!(err, Error::Broker { code: 58, .. }) || matches!(err, Error::Closed),
        "failed reauth must be explicit, got {err:?}"
    );
    assert_eq!(
        mock.describe_cluster_calls(),
        describes,
        "failed reauth must not send the application DescribeCluster"
    );

    let again = admin.describe_cluster().await.unwrap_err();
    assert!(
        matches!(again, Error::Closed) || matches!(again, Error::Broker { code: 58, .. }),
        "closed after failed reauth, got {again:?}"
    );
}

#[tokio::test]
async fn v0_peer_reauth_is_unsupported_and_closes() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_api_max(SASL_AUTHENTICATE, 0);
    let mut conn = BrokerConn::connect(&mock.addr, "rpc-reauth-v0", Duration::from_secs(2))
        .await
        .unwrap();
    let versions = negotiate_api_versions(&mut conn, Duration::from_secs(2))
        .await
        .unwrap();
    apply_api_keys(&mut conn, &versions.api_keys);
    authenticate(
        &mut conn,
        Some(&("alice".into(), "secret".into())),
        None,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(conn.sasl_authenticate_version(), 0);
    conn.record_sasl_session_lifetime_at(50, Instant::now() - Duration::from_millis(80));
    assert!(conn.is_session_expired());

    let err = conn
        .roundtrip(METADATA, 12, |_| Ok(()), Duration::from_secs(2))
        .await
        .unwrap_err();
    match err {
        Error::Unsupported(message) => {
            assert!(
                message.contains("reauthentication") || message.contains("v0"),
                "{message}"
            );
        }
        other => panic!("v0 reauth must be Unsupported, got {other:?}"),
    }
    assert!(
        conn.is_closed(),
        "unsupported reauth must close rather than send plaintext"
    );
}
