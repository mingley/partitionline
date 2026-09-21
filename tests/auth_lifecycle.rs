//! KL06-03 integration tests: Bounded cache and refresh owner lifecycle following docs/auth-refresh.md.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::error::Error;
use partitionline::net::BrokerConn;
use partitionline::protocol::api::negotiate_api_versions;
use partitionline::protocol::oauth::unsecured_jwt_now;
use partitionline::protocol::oidc::{
    FixedJitter, MockClock, MockTokenFetcher, OidcConfig, OidcRefreshConfig, OidcTokenManager,
    OidcTokenResponse, SystemClock, TokenData, TokenLifecycleState, ZeroJitter,
};
use partitionline::protocol::sasl::{apply_api_keys, authenticate_with_token_provider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn wait_for_state(manager: &OidcTokenManager, expected: TokenLifecycleState) {
    for _ in 0..100 {
        if manager.state() == expected {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        manager.state(),
        expected,
        "timed out waiting for state {expected:?}"
    );
}

async fn wait_for_condition<F: Fn() -> bool>(f: F) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(f(), "timed out waiting for condition");
}

#[tokio::test]
async fn concurrent_connection_demand_single_flight_coalescing() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(OidcTokenResponse::new(
                format!("shared-token-{count}"),
                "Bearer",
                Some(3600),
            ))
        })
    }));

    let clock = Arc::new(MockClock::new(Instant::now()));
    let jitter = Arc::new(ZeroJitter);
    let manager =
        OidcTokenManager::with_fetcher(fetcher, OidcRefreshConfig::default(), clock, jitter);

    assert_eq!(manager.state(), TokenLifecycleState::Uninitialized);

    let mut handles = Vec::new();
    for _ in 0..25 {
        let mgr = manager.clone();
        handles.push(tokio::spawn(async move {
            mgr.token(Duration::from_secs(5)).await.unwrap()
        }));
    }

    for h in handles {
        let token = h.await.unwrap();
        assert_eq!(
            token, "shared-token-0",
            "all callers should receive the coalesced token"
        );
    }

    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        1,
        "concurrent demand must trigger exactly one single-flight fetch"
    );
    assert_eq!(manager.state(), TokenLifecycleState::Active);
}

#[tokio::test]
async fn early_proactive_refresh_with_deterministic_clock() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(OidcTokenResponse::new(
                format!("token-gen-{count}"),
                "Bearer",
                Some(3600),
            ))
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    let jitter = Arc::new(ZeroJitter);
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    // Initial acquisition: token-gen-0 (expires in 3600s, scheduled at 3480s)
    let tok1 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok1, "token-gen-0");
    assert_eq!(manager.state(), TokenLifecycleState::Active);
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Advance clock to 3470s (before refresh point 3480s)
    clock.advance(Duration::from_secs(3470));
    tokio::task::yield_now().await;

    // Caller receives cached token-gen-0 without refetch
    let tok_cached = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok_cached, "token-gen-0");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(manager.state(), TokenLifecycleState::Active);

    // Advance clock past scheduled refresh point (3480s) to 3485s
    clock.advance(Duration::from_secs(15));
    // Allow background refresh task to run
    let fc = fetch_count.clone();
    wait_for_condition(move || fc.load(Ordering::SeqCst) >= 2).await;

    // Next caller receives refreshed token-gen-1
    let tok2 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok2, "token-gen-1");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn deterministic_jitter_subtracted_from_refresh_point() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(OidcTokenResponse::new(
                format!("jitter-tok-{count}"),
                "Bearer",
                Some(3600),
            ))
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    // Fixed jitter of 20 seconds
    let jitter = Arc::new(FixedJitter(Duration::from_secs(20)));
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    let _ = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Refresh point = 3480s. Scheduled = 3480s - 20s jitter = 3460s.
    // Advance to 3455s (before scheduled jitter point):
    clock.advance(Duration::from_secs(3455));
    tokio::task::yield_now().await;
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Advance past 3460s to 3465s:
    clock.advance(Duration::from_secs(10));
    let fc = fetch_count.clone();
    wait_for_condition(move || fc.load(Ordering::SeqCst) >= 2).await;

    let tok = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok, "jitter-tok-1");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn clock_skew_and_hard_expiration_ceiling() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(OidcTokenResponse::new(
                format!("skew-tok-{count}"),
                "Bearer",
                Some(60), // 60s lifetime -> skew is 6s -> hard ceiling at 54s
            ))
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    let jitter = Arc::new(ZeroJitter);
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    let tok1 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok1, "skew-tok-0");

    // Advance clock to 40s (< 48s refresh, < 54s hard ceiling): token is still usable
    clock.advance(Duration::from_secs(40));
    let tok_still_valid = manager.cached_token().unwrap();
    assert_eq!(tok_still_valid.token(), "skew-tok-0");

    // Advance clock past 54s hard ceiling (to 55s total): old token expires
    clock.advance(Duration::from_secs(15));
    let tok2 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_ne!(tok2, "skew-tok-0", "expired token must not be returned");
}

#[tokio::test]
async fn transient_refresh_failure_retains_valid_old_token() {
    let fail_flag = Arc::new(AtomicUsize::new(0));
    let ff = fail_flag.clone();
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        let should_fail = ff.load(Ordering::SeqCst) > 0;
        Box::pin(async move {
            if should_fail {
                Err(Error::protocol("oidc token endpoint HTTP 503"))
            } else {
                Ok(OidcTokenResponse::new(
                    format!("resilient-tok-{count}"),
                    "Bearer",
                    Some(3600), // Refresh at 3480s, ceiling at 3540s
                ))
            }
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    let jitter = Arc::new(ZeroJitter);
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    let tok1 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok1, "resilient-tok-0");

    // Trigger failure on next fetch
    fail_flag.store(1, Ordering::SeqCst);

    // Advance to refresh point (3480s)
    clock.advance(Duration::from_secs(3480));
    wait_for_state(&manager, TokenLifecycleState::Degraded).await;

    // Caller requesting token during transient refresh failure receives valid old token
    let caller_token = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        caller_token, "resilient-tok-0",
        "caller must retain valid old token during transient refresh failure"
    );

    // Clear failure flag so retry succeeds
    fail_flag.store(0, Ordering::SeqCst);

    // Advance clock by 1s (retry backoff delay)
    clock.advance(Duration::from_secs(2));
    wait_for_state(&manager, TokenLifecycleState::Active).await;

    // After successful recovery, callers receive fresh token
    let fresh_tok = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(fresh_tok, "resilient-tok-2");
}

#[tokio::test]
async fn outage_persisting_past_expiry_fails_closed() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if count == 0 {
                Ok(OidcTokenResponse::new(
                    "initial-tok",
                    "Bearer",
                    Some(100), // skew = 10s -> hard ceiling at 90s
                ))
            } else {
                // Outage continues
                Err(Error::protocol("oidc token endpoint HTTP 503"))
            }
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    let jitter = Arc::new(ZeroJitter);
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    let tok1 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok1, "initial-tok");

    // Advance to refresh point (80s): refresh fails, state becomes Degraded
    clock.advance(Duration::from_secs(80));
    wait_for_state(&manager, TokenLifecycleState::Degraded).await;

    // Advance past hard expiration ceiling (90s) to 95s
    clock.advance(Duration::from_secs(15));
    wait_for_state(&manager, TokenLifecycleState::Expired).await;

    // New connection attempt MUST fail closed and MUST NOT return initial-tok
    let res = manager.token(Duration::from_secs(1)).await;
    assert!(
        res.is_err(),
        "must fail closed when expired token cannot be refreshed"
    );
    match res.unwrap_err() {
        Error::Protocol(m) => assert!(m.contains("503") || m.contains("oidc")),
        Error::Timeout => {}
        other => panic!("expected Protocol or Timeout error, got {other:?}"),
    }
}

#[tokio::test]
async fn non_transient_http_401_invalidates_active_token_and_fails_closed() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if count == 0 {
                Ok(OidcTokenResponse::new("active-tok-1", "Bearer", Some(3600)))
            } else {
                // IdP revokes client credentials
                Err(Error::protocol("oidc token endpoint HTTP 401"))
            }
        })
    }));

    let start = Instant::now();
    let clock = Arc::new(MockClock::new(start));
    let jitter = Arc::new(ZeroJitter);
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        jitter,
    );

    let tok1 = manager.token(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tok1, "active-tok-1");

    // Advance to refresh point (3480s): IdP returns 401
    clock.advance(Duration::from_secs(3480));
    wait_for_state(&manager, TokenLifecycleState::FailedClosed).await;

    // Active token must be invalidated immediately
    assert!(
        manager.cached_token().is_none(),
        "cached token must be cleared on 401"
    );

    // Subsequent connection attempts fail closed immediately
    let res = manager.token(Duration::from_secs(1)).await;
    assert!(
        res.is_err(),
        "subsequent connection attempts must fail closed"
    );
}

#[tokio::test]
async fn cancellation_safety_does_not_leak_tasks_or_corrupt_cache() {
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fc = fetch_count.clone();

    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let count = fc.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(OidcTokenResponse::new(
                format!("cancellation-tok-{count}"),
                "Bearer",
                Some(3600),
            ))
        })
    }));

    let clock = Arc::new(SystemClock);
    let jitter = Arc::new(ZeroJitter);
    let manager =
        OidcTokenManager::with_fetcher(fetcher, OidcRefreshConfig::default(), clock, jitter);

    // Caller 1 times out quickly (cancels its future)
    let mgr1 = manager.clone();
    drop(tokio::time::timeout(Duration::from_millis(5), mgr1.token(Duration::from_secs(5))).await);

    // Caller 2 awaits the same manager
    let mgr2 = manager.clone();
    let tok = mgr2.token(Duration::from_secs(2)).await.unwrap();
    assert_eq!(tok, "cancellation-tok-0");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(manager.state(), TokenLifecycleState::Active);
}

#[tokio::test]
async fn credential_redaction_and_error_hygiene() {
    let token = "super-sensitive-jwt-token-kl06-secret";
    let td = TokenData::new(token, Instant::now() + Duration::from_secs(3600));

    let dbg = format!("{td:?}");
    assert!(!dbg.contains(token), "TokenData Debug leaked token: {dbg}");
    assert!(
        dbg.contains("<redacted>"),
        "TokenData Debug must show <redacted>: {dbg}"
    );

    let cfg = OidcConfig::new("https://idp.example/token", "client-id", "my-client-secret");
    let mgr = OidcTokenManager::new(cfg);
    let mgr_dbg = format!("{mgr:?}");
    assert!(
        !mgr_dbg.contains("my-client-secret"),
        "Manager Debug leaked client_secret: {mgr_dbg}"
    );
}

#[tokio::test]
async fn real_http_token_endpoint_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let request_count = Arc::new(AtomicUsize::new(0));
    let rc = request_count.clone();

    drop(tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0u8; 1024];
            let _ = sock.read(&mut buf).await.unwrap();
            let count = rc.fetch_add(1, Ordering::SeqCst);
            let body = format!(
                "{{\"access_token\":\"real-http-tok-{count}\",\"token_type\":\"Bearer\",\"expires_in\":3600}}"
            );
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }
    }));

    let cfg = OidcConfig::new(format!("http://{addr}/token"), "my-cid", "my-secret");
    let manager = OidcTokenManager::new(cfg);

    // Call token concurrently from 5 callers
    let mut handles = Vec::new();
    for _ in 0..5 {
        let mgr = manager.clone();
        handles.push(tokio::spawn(async move {
            mgr.token(Duration::from_secs(5)).await.unwrap()
        }));
    }

    for h in handles {
        let token = h.await.unwrap();
        assert_eq!(token, "real-http-tok-0");
    }

    assert_eq!(
        request_count.load(Ordering::SeqCst),
        1,
        "all 5 callers coalesced onto 1 HTTP POST"
    );
}

#[tokio::test]
async fn sasl_oauthbearer_handshake_with_token_provider() {
    let principal = "alice";
    let mock = common::Mock::start_with_oauthbearer(principal.into()).await;

    let valid_jwt = unsecured_jwt_now(principal);
    let fetcher = Arc::new(MockTokenFetcher::new(move |_timeout| {
        let tok = valid_jwt.clone();
        Box::pin(async move { Ok(OidcTokenResponse::new(tok, "Bearer", Some(3600))) })
    }));

    let clock = Arc::new(SystemClock);
    let jitter = Arc::new(ZeroJitter);
    let manager =
        OidcTokenManager::with_fetcher(fetcher, OidcRefreshConfig::default(), clock, jitter);

    // Connect raw broker connection
    let mut conn = BrokerConn::connect_tls(&mock.addr, "test-client", Duration::from_secs(5), None)
        .await
        .unwrap();

    let vers = negotiate_api_versions(&mut conn, Duration::from_secs(5))
        .await
        .unwrap();
    apply_api_keys(&mut conn, &vers.api_keys);

    // Authenticate using the refresh owner
    authenticate_with_token_provider(&mut conn, &manager, Duration::from_secs(5))
        .await
        .unwrap();

    // Verify token was cached
    let cached = manager.cached_token().unwrap();
    assert!(!cached.token().is_empty());
    assert_eq!(manager.state(), TokenLifecycleState::Active);

    // Second connection uses the same provider and succeeds immediately
    let mut conn2 =
        BrokerConn::connect_tls(&mock.addr, "test-client-2", Duration::from_secs(5), None)
            .await
            .unwrap();
    let vers2 = negotiate_api_versions(&mut conn2, Duration::from_secs(5))
        .await
        .unwrap();
    apply_api_keys(&mut conn2, &vers2.api_keys);
    authenticate_with_token_provider(&mut conn2, &manager, Duration::from_secs(5))
        .await
        .unwrap();
}
