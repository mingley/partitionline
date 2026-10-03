//! Actual TLS authority, refresh, revocation and ownership regressions.
#![cfg(feature = "oidc")]

mod oidc_support;
use oidc_support::{at_least, Harness, Result};
use partitionline_broker::security::oidc::{Error, Service};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{task::JoinSet, time::timeout};

#[tokio::test]
async fn actual_https_discovery_trust_whole_json_and_size_policy() -> Result {
    let harness = Harness::start().await?;
    let mut untrusted = harness.trust();
    untrusted.roots = vec![include_bytes!("fixtures/tls/ca2.cert.der").to_vec()];
    assert_eq!(
        Service::start(harness.config(), untrusted)
            .await
            .unwrap_err(),
        Error::Unavailable
    );
    for mode in [1, 2, 3, 4, 11, 12, 13] {
        harness
            .authority
            .discovery_mode
            .store(mode, Ordering::Release);
        assert!(
            Service::start(harness.config(), harness.trust())
                .await
                .is_err(),
            "bad discovery accepted"
        );
    }
    harness.authority.discovery_mode.store(0, Ordering::Release);
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert_eq!(lease.subject(), "test-user");
    assert_eq!(lease.issuer(), harness.authority.issuer);
    assert!(lease.failure().is_none());
    service.shutdown().await?;
    assert_eq!(lease.invalidated().await, Error::Unavailable);
    harness.shutdown().await
}

#[tokio::test]
async fn revocation_binding_and_outage_never_extend_a_lease() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    // Both audiences are locally allowed, but the provider's active response
    // must still bind the exact audience carried by this verified token.
    config.policy.audiences.push("other-service".to_owned());
    let service = Service::start(config, harness.trust()).await?;
    for mode in [5, 6, 7, 8, 9, 11, 14, 15, 16, 17] {
        harness
            .authority
            .introspection_mode
            .store(mode, Ordering::Release);
        assert!(
            service
                .validate(harness.token(0, "shared-key", 30)?)
                .await
                .is_err(),
            "bad introspection accepted"
        );
    }
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    let original = lease.deadline();
    let checks = harness.authority.checks.load(Ordering::Acquire);
    harness
        .authority
        .introspection_mode
        .store(1, Ordering::Release);
    at_least(&harness.authority.checks, checks + 2).await?;
    assert_eq!(lease.deadline(), original);
    assert_eq!(
        timeout(Duration::from_secs(2), lease.invalidated()).await?,
        Error::Expired
    );
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    assert_eq!(lease.failure(), Some(Error::Expired));
    let fresh = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(5, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(2), fresh.invalidated()).await?,
        Error::Revoked
    );
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn removed_or_changed_key_interrupts_stalled_introspection() -> Result {
    let harness = Harness::start().await?;
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    // Same kid, independently generated replacement public material: a kid-only
    // cache comparison would incorrectly leave this lease authenticated.
    harness.authority.generation.store(1, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(2), lease.invalidated()).await?,
        Error::Revoked
    );
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    harness.authority.release.add_permits(1);
    let replacement = service
        .validate(harness.token(1, "shared-key", 30)?)
        .await?;
    assert!(replacement.generation() > lease.generation());
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn key_rotation_during_initial_introspection_cannot_publish_old_authentication() -> Result {
    let harness = Harness::start().await?;
    let mut trust = harness.trust();
    trust.limits.timeout = Duration::from_secs(2);
    let service = Service::start(harness.config(), trust).await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    let token = harness.token(0, "shared-key", 30)?;
    let other = service.clone();
    let validating = tokio::spawn(async move { other.validate(token).await });
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    harness.authority.generation.store(1, Ordering::Release);
    // Two manager fetches prove that its preceding generation was published,
    // before allowing the old proof's introspection to complete.
    at_least(&harness.authority.key_requests, before + 2).await?;
    harness.authority.release.add_permits(1);
    assert_eq!(validating.await?.unwrap_err(), Error::Revoked);
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn failed_refresh_keeps_finite_generation_but_valid_empty_keys_revoke() -> Result {
    let harness = Harness::start().await?;
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    harness.authority.key_mode.store(11, Ordering::Release);
    at_least(&harness.authority.key_requests, before + 2).await?;
    assert!(lease.failure().is_none());
    let same = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert_eq!(same.generation(), lease.generation());
    harness.authority.key_mode.store(18, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(2), lease.invalidated()).await?,
        Error::Revoked
    );
    assert_eq!(same.invalidated().await, Error::Revoked);
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    harness.authority.key_mode.store(0, Ordering::Release);
    at_least(&harness.authority.key_requests, before + 2).await?;
    let fresh = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert!(fresh.generation() > lease.generation());
    assert_eq!(lease.failure(), Some(Error::Revoked));
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn authority_outage_expiry_admission_and_joined_shutdown() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    config.runtime.active_leases = 1;
    let service = Service::start(config, harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert_eq!(
        service
            .validate(harness.token(0, "shared-key", 30)?)
            .await
            .unwrap_err(),
        Error::Busy
    );
    harness.authority.key_mode.store(1, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(3), lease.invalidated()).await?,
        Error::Expired
    );
    // Failure publication wakes the socket before the task's final permit drop.
    // Wait for that bounded cleanup, while preserving Busy as a valid outcome.
    timeout(Duration::from_secs(1), async {
        loop {
            let error = service
                .validate(harness.token(0, "shared-key", 30)?)
                .await
                .unwrap_err();
            if error != Error::Busy {
                assert_eq!(error, Error::Expired);
                break;
            }
            tokio::task::yield_now().await;
        }
        Result::Ok(())
    })
    .await??;
    service.shutdown().await?;
    harness.authority.key_mode.store(0, Ordering::Release);
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service.validate(harness.token(0, "shared-key", 2)?).await?;
    assert_eq!(
        timeout(Duration::from_secs(3), lease.invalidated()).await?,
        Error::Expired
    );
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    timeout(Duration::from_secs(1), service.shutdown()).await??;
    assert_eq!(lease.failure(), Some(Error::Unavailable));
    harness.shutdown().await
}

#[tokio::test]
async fn unknown_kid_requests_are_single_flight_and_globally_bounded() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    config.runtime.refresh_interval = Duration::from_secs(1);
    let service = Service::start(config, harness.trust()).await?;
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    let mut requests = JoinSet::new();
    for index in 0..8 {
        let token = harness.token(1, &format!("unknown-{index}"), 30)?;
        let other = service.clone();
        requests.spawn(async move { other.validate(token).await });
    }
    while let Some(request) = requests.join_next().await {
        assert!(matches!(
            request?.unwrap_err(),
            Error::Authentication | Error::Busy
        ));
    }
    assert_eq!(
        harness.authority.key_requests.load(Ordering::Acquire),
        before + 1
    );
    assert_eq!(harness.authority.checks.load(Ordering::Acquire), 0);
    service.shutdown().await?;
    harness.shutdown().await
}
