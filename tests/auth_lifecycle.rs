//! KL06-03 & KL06-04 integration tests: Bounded cache and refresh owner lifecycle,
//! broker SASL session lifetimes, in-place reauthentication, and pipelined traffic quiescing.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use partitionline::error::Error;
use partitionline::net::{
    is_reserved_correlation_id, BrokerConn, BrokerPipeline, Deadline, PipelineState,
};
use partitionline::protocol::api::{encode_api_versions_response, negotiate_api_versions};
use partitionline::protocol::api_keys::{API_VERSIONS, SASL_AUTHENTICATE, SASL_HANDSHAKE};
use partitionline::protocol::oauth::unsecured_jwt_now;
use partitionline::protocol::oidc::{
    FixedJitter, MockClock, MockTokenFetcher, OidcConfig, OidcRefreshConfig, OidcTokenManager,
    OidcTokenResponse, SystemClock, TokenData, TokenLifecycleState, ZeroJitter,
};
use partitionline::protocol::sasl::{
    apply_api_keys, authenticate_oauthbearer_token, authenticate_with_token_provider,
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, reauthenticate,
    reauthenticate_oauthbearer_token, reauthenticate_plain, reauthenticate_scram,
    reauthenticate_with_token_provider, should_reconnect_after_reauth,
};
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

fn make_response_frame(
    correlation_id: i32,
    body: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let frame_len = 4 + body.len();
    // Test frames carry one small encoded response; fail loudly instead of
    // truncating if a test ever builds a frame past i32::MAX.
    let len_prefix = i32::try_from(frame_len)?;
    let mut frame = Vec::with_capacity(4 + frame_len);
    frame.extend_from_slice(&len_prefix.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(body);
    Ok(frame)
}

fn dummy_api_versions_response() -> partitionline::protocol::api::ApiVersionsResponse {
    partitionline::protocol::api::ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![],
        throttle_time_ms: 0,
        supported_features: vec![],
        finalized_features_epoch: None,
        finalized_features: vec![],
        zk_migration_ready: false,
    }
}

async fn read_request_frame(
    socket: &mut tokio::net::TcpStream,
) -> Result<(partitionline::protocol::header::RequestHeader, Vec<u8>), Box<dyn std::error::Error>> {
    let mut size_buf = [0u8; 4];
    // read_exact fills the buffer or errors; the byte count itself is unneeded.
    let _ = socket.read_exact(&mut size_buf).await?;
    let size_i32 = i32::from_be_bytes(size_buf);
    // Loopback test frames are small and non-negative; reject anything else
    // loudly instead of losing the sign or truncating.
    let size = usize::try_from(size_i32)?;
    let mut req_buf = vec![0u8; size];
    let _ = socket.read_exact(&mut req_buf).await?;
    let mut cur = &req_buf[..];
    let header = partitionline::protocol::header::decode_request_header(&mut cur)?;
    let body = cur.to_vec();
    Ok((header, body))
}

#[tokio::test]
async fn sasl_session_lifetime_captured_and_timing_calculated() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let start = Instant::now();
    conn.record_sasl_session_lifetime_at(10000, start);
    assert_eq!(conn.session_lifetime_ms(), Some(10000));
    assert_eq!(conn.authenticated_at(), Some(start));
    assert_eq!(
        conn.session_expiry(),
        Some(start + Duration::from_millis(10000))
    );
    assert_eq!(conn.reauth_at(), Some(start + Duration::from_millis(8500)));

    assert!(!conn.needs_reauth_at(start + Duration::from_millis(8499)));
    assert!(conn.needs_reauth_at(start + Duration::from_millis(8500)));
    assert!(!conn.is_session_expired_at(start + Duration::from_millis(9999)));
    assert!(conn.is_session_expired_at(start + Duration::from_millis(10000)));

    // Zero / v0 lifetime: no expiry
    conn.record_sasl_session_lifetime_at(0, start);
    assert_eq!(conn.session_lifetime_ms(), None);
    assert_eq!(conn.session_expiry(), None);
    assert_eq!(conn.reauth_at(), None);
    assert!(!conn.needs_reauth_at(start + Duration::from_secs(3600)));
    assert!(!conn.is_session_expired_at(start + Duration::from_secs(3600)));

    // Negative lifetime: cleared
    conn.record_sasl_session_lifetime_at(10000, start);
    assert_eq!(conn.session_lifetime_ms(), Some(10000));
    conn.record_sasl_session_lifetime_at(-1, start);
    assert_eq!(conn.session_lifetime_ms(), None);
    assert_eq!(conn.session_expiry(), None);
    assert_eq!(conn.reauth_at(), None);

    // clear_sasl_session
    conn.record_sasl_session_lifetime_at(10000, start);
    conn.clear_sasl_session();
    assert_eq!(conn.session_lifetime_ms(), None);
    assert_eq!(conn.session_expiry(), None);
    assert_eq!(conn.reauth_at(), None);
}

#[tokio::test]
async fn sasl_reauthenticate_v1_and_v2_wire_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Initial Handshake request
        let (h1, b1) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h1.api_key(), SASL_HANDSHAKE);
        let mech = decode_sasl_handshake_request(&mut &b1[..], h1.api_version()).unwrap();
        assert_eq!(mech, "OAUTHBEARER");
        let mut resp = BytesMut::new();
        encode_sasl_handshake_response(
            &mut resp,
            h1.api_version(),
            0,
            &["OAUTHBEARER", "PLAIN", "SCRAM-SHA-256"],
        )
        .unwrap();
        socket
            .write_all(&make_response_frame(h1.correlation_id(), &resp).unwrap())
            .await
            .unwrap();

        // 2. Initial Authenticate request
        let (h2, b2) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h2.api_key(), SASL_AUTHENTICATE);
        let auth_bytes = decode_sasl_authenticate_request(&mut &b2[..], h2.api_version()).unwrap();
        assert!(!auth_bytes.is_empty());
        let mut resp2 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp2, h2.api_version(), 0, None, &[], 5000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h2.correlation_id(), &resp2).unwrap())
            .await
            .unwrap();

        // 3. Reauthenticate request (SaslAuthenticate ONLY, NO Handshake!)
        let (h3, b3) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(
            h3.api_key(),
            SASL_AUTHENTICATE,
            "reauth must send SaslAuthenticate without handshake"
        );
        assert!(
            is_reserved_correlation_id(h3.correlation_id()),
            "reauth must use reserved SASL correlation ID"
        );
        let reauth_bytes =
            decode_sasl_authenticate_request(&mut &b3[..], h3.api_version()).unwrap();
        assert!(!reauth_bytes.is_empty());
        let mut resp3 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp3, h3.api_version(), 0, None, &[], 25000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h3.correlation_id(), &resp3).unwrap())
            .await
            .unwrap();

        // 4. Reauthenticate PLAIN
        let (h4, b4) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h4.api_key(), SASL_AUTHENTICATE);
        assert!(is_reserved_correlation_id(h4.correlation_id()));
        let plain_bytes = decode_sasl_authenticate_request(&mut &b4[..], h4.api_version()).unwrap();
        assert!(!plain_bytes.is_empty());
        let mut resp4 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp4, h4.api_version(), 0, None, &[], 35000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h4.correlation_id(), &resp4).unwrap())
            .await
            .unwrap();

        // 5. Reauthenticate with token provider
        let (h5, b5) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h5.api_key(), SASL_AUTHENTICATE);
        assert!(is_reserved_correlation_id(h5.correlation_id()));
        let tok_bytes = decode_sasl_authenticate_request(&mut &b5[..], h5.api_version()).unwrap();
        assert!(!tok_bytes.is_empty());
        let mut resp5 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp5, h5.api_version(), 0, None, &[], 45000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h5.correlation_id(), &resp5).unwrap())
            .await
            .unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(2))
        .await
        .unwrap();
    conn.set_sasl_versions(1, 1);

    // Initial auth
    authenticate_oauthbearer_token(&mut conn, "initial-token", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(conn.session_lifetime_ms(), Some(5000));

    // Mid-connection reauth OAUTHBEARER
    reauthenticate_oauthbearer_token(&mut conn, "refreshed-token", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(conn.session_lifetime_ms(), Some(25000));

    // Mid-connection reauth PLAIN
    reauthenticate_plain(&mut conn, "user", "pass", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(conn.session_lifetime_ms(), Some(35000));

    // Mid-connection reauth with TokenProvider
    let fetcher = Arc::new(MockTokenFetcher::new(|_timeout| {
        Box::pin(async {
            Ok(OidcTokenResponse::new(
                "provider-token",
                "Bearer",
                Some(3600),
            ))
        })
    }));
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        Arc::new(SystemClock),
        Arc::new(ZeroJitter),
    );
    reauthenticate_with_token_provider(&mut conn, &manager, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(conn.session_lifetime_ms(), Some(45000));

    server_task.await.unwrap();
}

#[tokio::test]
async fn unsupported_sasl_mechanisms_and_versions_fail_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    // v0 peer does not support reauth (KIP-368 is v1+)
    conn.set_sasl_authenticate_version(0);
    let res = reauthenticate_oauthbearer_token(&mut conn, "token", Duration::from_secs(1)).await;
    assert!(
        matches!(res, Err(Error::Unsupported(_))),
        "v0 peer must return Unsupported, got {res:?}"
    );

    let res_plain = reauthenticate_plain(&mut conn, "u", "p", Duration::from_secs(1)).await;
    assert!(matches!(res_plain, Err(Error::Unsupported(_))));

    let res_scram = reauthenticate_scram(
        &mut conn,
        partitionline::protocol::scram::ScramAlg::Sha256,
        "u",
        "p",
        Duration::from_secs(1),
    )
    .await;
    assert!(matches!(res_scram, Err(Error::Unsupported(_))));

    // unset version (-1)
    conn.set_sasl_authenticate_version(-1);
    let res = reauthenticate_oauthbearer_token(&mut conn, "token", Duration::from_secs(1)).await;
    assert!(matches!(res, Err(Error::Unsupported(_))));

    // unified reauthenticate with no mechanisms configured
    conn.set_sasl_authenticate_version(1);
    let res = reauthenticate(
        &mut conn,
        None,
        None,
        None,
        None,
        None,
        Duration::from_secs(1),
    )
    .await;
    assert!(
        matches!(res, Err(Error::Unsupported(_))),
        "no mechanism configured must return Unsupported, got {res:?}"
    );

    // multiple mechanisms configured
    let plain = ("u".into(), "p".into());
    let res = reauthenticate(
        &mut conn,
        Some(&plain),
        None,
        None,
        Some("alice"),
        None,
        Duration::from_secs(1),
    )
    .await;
    assert!(matches!(res, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn quiesce_and_resume_pipelined_traffic_without_mixing_correlation_ids() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Initial Handshake + Authenticate
        let (h1, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp1 = BytesMut::new();
        encode_sasl_handshake_response(&mut resp1, h1.api_version(), 0, &["OAUTHBEARER"]).unwrap();
        socket
            .write_all(&make_response_frame(h1.correlation_id(), &resp1).unwrap())
            .await
            .unwrap();

        let (h2, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp2 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp2, h2.api_version(), 0, None, &[], 10000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h2.correlation_id(), &resp2).unwrap())
            .await
            .unwrap();

        // 2. Read 2 pipelined application requests (API_VERSIONS)
        let (app1, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(app1.api_key(), API_VERSIONS);
        assert!(
            !is_reserved_correlation_id(app1.correlation_id()),
            "application request must not use reserved SASL correlation ID"
        );

        let (app2, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(app2.api_key(), API_VERSIONS);
        assert!(!is_reserved_correlation_id(app2.correlation_id()));

        // Delay responses slightly so reauth is triggered while in-flight
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Respond to app1
        let mut app_resp1 = BytesMut::new();
        encode_api_versions_response(&mut app_resp1, 0, &dummy_api_versions_response()).unwrap();
        socket
            .write_all(&make_response_frame(app1.correlation_id(), &app_resp1).unwrap())
            .await
            .unwrap();

        // Respond to app2
        let mut app_resp2 = BytesMut::new();
        encode_api_versions_response(&mut app_resp2, 0, &dummy_api_versions_response()).unwrap();
        socket
            .write_all(&make_response_frame(app2.correlation_id(), &app_resp2).unwrap())
            .await
            .unwrap();

        // 3. Next request received MUST be SaslAuthenticate with reserved SASL correlation ID!
        let (sasl_req, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(sasl_req.api_key(), SASL_AUTHENTICATE);
        assert!(
            is_reserved_correlation_id(sasl_req.correlation_id()),
            "SASL reauth must use reserved SASL correlation ID"
        );

        let mut sasl_resp = BytesMut::new();
        encode_sasl_authenticate_response(
            &mut sasl_resp,
            sasl_req.api_version(),
            0,
            None,
            &[],
            60000,
        )
        .unwrap();
        socket
            .write_all(&make_response_frame(sasl_req.correlation_id(), &sasl_resp).unwrap())
            .await
            .unwrap();

        // 4. After reauth completes, queued request app3 arrives with application correlation ID!
        let (app3, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(app3.api_key(), API_VERSIONS);
        assert!(
            !is_reserved_correlation_id(app3.correlation_id()),
            "resumed request must use application correlation ID"
        );

        let mut app_resp3 = BytesMut::new();
        encode_api_versions_response(&mut app_resp3, 0, &dummy_api_versions_response()).unwrap();
        socket
            .write_all(&make_response_frame(app3.correlation_id(), &app_resp3).unwrap())
            .await
            .unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(2))
        .await
        .unwrap();
    conn.set_sasl_versions(1, 1);

    // Initial auth
    authenticate_oauthbearer_token(&mut conn, "initial-token", Duration::from_secs(2))
        .await
        .unwrap();

    let pipeline = BrokerPipeline::new(conn);

    // Send 2 requests concurrently
    let p1 = pipeline.clone();
    let f1 = tokio::spawn(async move {
        p1.request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(2))
            .await
    });
    let p2 = pipeline.clone();
    let f2 = tokio::spawn(async move {
        p2.request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(2))
            .await
    });

    // Give requests time to reach server
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Trigger reauthentication on pipeline
    let p_reauth = pipeline.clone();
    let reauth_handle = tokio::spawn(async move {
        p_reauth
            .reauthenticate_oauthbearer_token("refreshed-token", Duration::from_secs(2))
            .await
    });

    // While quiescing/reauthenticating, enqueue request 3
    let p3 = pipeline.clone();
    let f3 = tokio::spawn(async move {
        p3.request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(2))
            .await
    });

    // All must complete successfully!
    let r1 = f1.await.unwrap().unwrap();
    let r2 = f2.await.unwrap().unwrap();
    reauth_handle.await.unwrap().unwrap();
    let r3 = f3.await.unwrap().unwrap();

    assert!(!r1.is_empty());
    assert!(!r2.is_empty());
    assert!(!r3.is_empty());
    assert_eq!(pipeline.session_lifetime_ms().await.unwrap(), Some(60000));
    assert_eq!(pipeline.in_flight_count().await.unwrap(), 0);
    assert_eq!(pipeline.queued_count().await.unwrap(), 0);

    server_task.await.unwrap();
}

#[tokio::test]
async fn failed_reauthentication_closes_connection_and_fails_accepted_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Initial Handshake + Authenticate
        let (h1, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp1 = BytesMut::new();
        encode_sasl_handshake_response(&mut resp1, h1.api_version(), 0, &["OAUTHBEARER"]).unwrap();
        socket
            .write_all(&make_response_frame(h1.correlation_id(), &resp1).unwrap())
            .await
            .unwrap();

        let (h2, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp2 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp2, h2.api_version(), 0, None, &[], 5000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h2.correlation_id(), &resp2).unwrap())
            .await
            .unwrap();

        // 2. Reauth arrives -> respond with error 58 (SASL_AUTHENTICATION_FAILED)
        let (h3, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h3.api_key(), SASL_AUTHENTICATE);
        let mut resp3 = BytesMut::new();
        encode_sasl_authenticate_response(
            &mut resp3,
            h3.api_version(),
            58,
            Some("bad credentials"),
            &[],
            0,
        )
        .unwrap();
        socket
            .write_all(&make_response_frame(h3.correlation_id(), &resp3).unwrap())
            .await
            .unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(2))
        .await
        .unwrap();
    conn.set_sasl_versions(1, 1);

    authenticate_oauthbearer_token(&mut conn, "initial-token", Duration::from_secs(2))
        .await
        .unwrap();

    let pipeline = BrokerPipeline::new(conn);

    // Trigger reauth which will fail
    let p_reauth = pipeline.clone();
    let reauth_handle = tokio::spawn(async move {
        p_reauth
            .reauthenticate_oauthbearer_token("bad-token", Duration::from_secs(2))
            .await
    });

    // Enqueue request while reauth is pending
    let p_req = pipeline.clone();
    let req_handle = tokio::spawn(async move {
        p_req
            .request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(2))
            .await
    });

    let reauth_err = reauth_handle.await.unwrap().unwrap_err();
    assert!(
        matches!(reauth_err, Error::Broker { code: 58, .. }),
        "expected broker error 58, got {reauth_err:?}"
    );

    let req_err = req_handle.await.unwrap().unwrap_err();
    assert!(
        pipeline.is_closed(),
        "pipeline must be marked closed after failed reauth"
    );
    assert!(
        matches!(req_err, Error::Broker { code: 58, .. } | Error::Closed),
        "queued request must fail, got {req_err:?}"
    );

    // Subsequent request must fail immediately with Closed
    let sub = pipeline
        .request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(1))
        .await;
    assert!(matches!(sub, Err(Error::Closed)));

    server_task.await.unwrap();
}

#[tokio::test]
async fn session_expiration_blocks_new_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    // Record session lifetime of 50ms in the past (already expired)
    let past = Instant::now() - Duration::from_millis(100);
    conn.record_sasl_session_lifetime_at(50, past);

    assert!(conn.is_session_expired());

    // Direct send_deadline must fail
    let res = conn
        .send_deadline(
            API_VERSIONS,
            0,
            |_| Ok(()),
            Deadline::from_timeout(Duration::from_secs(1)),
        )
        .await;
    assert!(
        matches!(res, Err(Error::Protocol(_))),
        "expected Protocol error on expired session, got {res:?}"
    );

    // Pipeline request must also fail
    let pipeline = BrokerPipeline::new(conn);
    let pres = pipeline
        .request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(1))
        .await;
    assert!(
        matches!(pres, Err(Error::Protocol(_))),
        "pipeline request must fail on expired session, got {pres:?}"
    );
}

#[tokio::test]
async fn reauth_disconnect_closes_connection_and_fails_accepted_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Initial Handshake + Authenticate
        let (h1, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp1 = BytesMut::new();
        encode_sasl_handshake_response(&mut resp1, h1.api_version(), 0, &["OAUTHBEARER"]).unwrap();
        socket
            .write_all(&make_response_frame(h1.correlation_id(), &resp1).unwrap())
            .await
            .unwrap();

        let (h2, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp2 = BytesMut::new();
        encode_sasl_authenticate_response(&mut resp2, h2.api_version(), 0, None, &[], 5000)
            .unwrap();
        socket
            .write_all(&make_response_frame(h2.correlation_id(), &resp2).unwrap())
            .await
            .unwrap();

        // 2. Read reauth frame then abruptly drop socket (EOF / disconnect)
        let _ = read_request_frame(&mut socket).await.unwrap();
        drop(socket);
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(2))
        .await
        .unwrap();
    conn.set_sasl_versions(1, 1);

    authenticate_oauthbearer_token(&mut conn, "token", Duration::from_secs(2))
        .await
        .unwrap();

    let pipeline = BrokerPipeline::new(conn);

    let p_reauth = pipeline.clone();
    let reauth_handle = tokio::spawn(async move {
        p_reauth
            .reauthenticate_oauthbearer_token("refreshed", Duration::from_secs(2))
            .await
    });

    let p_req = pipeline.clone();
    let req_handle = tokio::spawn(async move {
        p_req
            .request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(2))
            .await
    });

    let reauth_err = reauth_handle.await.unwrap().unwrap_err();
    assert!(
        matches!(reauth_err, Error::Io(_)),
        "expected Io error on disconnect, got {reauth_err:?}"
    );

    let req_err = req_handle.await.unwrap().unwrap_err();
    assert!(matches!(req_err, Error::Io(_) | Error::Closed));
    assert!(pipeline.is_closed());

    server_task.await.unwrap();
}

#[tokio::test]
async fn pipeline_shutdown_cleanly_terminates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();
    let pipeline = BrokerPipeline::new(conn);

    assert_eq!(pipeline.state(), PipelineState::Active);
    assert!(!pipeline.is_closed());

    pipeline.shutdown().await;

    assert!(pipeline.is_closed());
    assert_eq!(pipeline.state(), PipelineState::Closed);

    let res = pipeline
        .request(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(1))
        .await;
    assert!(matches!(res, Err(Error::Closed)));
}

#[tokio::test]
async fn direct_broker_conn_quiesce_guard() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (h1, _) = read_request_frame(&mut socket).await.unwrap();
        // Delay response
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut resp = BytesMut::new();
        encode_api_versions_response(&mut resp, 0, &dummy_api_versions_response()).unwrap();
        socket
            .write_all(&make_response_frame(h1.correlation_id(), &resp).unwrap())
            .await
            .unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let corr = conn
        .send_deadline(
            API_VERSIONS,
            0,
            |_| Ok(()),
            Deadline::from_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert_eq!(conn.in_flight(), 1);
    assert!(!conn.can_reauthenticate());

    // Calling roundtrip_sasl while in_flight > 0 MUST fail immediately with protocol error
    let sasl_res = conn
        .roundtrip_sasl(SASL_AUTHENTICATE, 1, |_| Ok(()), Duration::from_secs(1))
        .await;
    assert!(
        matches!(sasl_res, Err(Error::Protocol(_))),
        "must reject SASL request while requests in flight, got {sasl_res:?}"
    );

    // Read response -> in_flight becomes 0
    let _ = conn
        .read_response_deadline(
            API_VERSIONS,
            0,
            corr,
            Deadline::from_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert_eq!(conn.in_flight(), 0);
    assert!(conn.can_reauthenticate());

    server_task.await.unwrap();
}

#[tokio::test]
async fn credential_redaction_and_response_body_hygiene_on_reauth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let secret_token = "sentinel-super-secret-token-xyz-987654";

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (h, _) = read_request_frame(&mut socket).await.unwrap();
        let mut resp = BytesMut::new();
        // Broker failure JSON echoing sensitive material
        let failure_json = format!("{{\"error\":\"invalid token {secret_token}\"}}");
        encode_sasl_authenticate_response(
            &mut resp,
            h.api_version(),
            0,
            None,
            failure_json.as_bytes(),
            0,
        )
        .unwrap();
        socket
            .write_all(&make_response_frame(h.correlation_id(), &resp).unwrap())
            .await
            .unwrap();

        // Read final SOH
        let (h2, _) = read_request_frame(&mut socket).await.unwrap();
        assert_eq!(h2.api_key(), SASL_AUTHENTICATE);
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();
    conn.set_sasl_authenticate_version(1);

    let err = reauthenticate_oauthbearer_token(&mut conn, secret_token, Duration::from_secs(1))
        .await
        .unwrap_err();

    let disp = format!("{err}");
    let dbg = format!("{err:?}");
    assert!(
        !disp.contains(secret_token),
        "Display leaked secret: {disp}"
    );
    assert!(!dbg.contains(secret_token), "Debug leaked secret: {dbg}");
    assert!(
        !disp.contains("invalid token"),
        "Display leaked broker body: {disp}"
    );
    assert!(
        !dbg.contains("invalid token"),
        "Debug leaked broker body: {dbg}"
    );

    server_task.await.unwrap();
}

#[tokio::test]
async fn should_reconnect_after_reauth_checks_idle_and_reauth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();
    conn.set_sasl_authenticate_version(1);

    // Fresh session: no reconnect needed
    conn.record_sasl_session_lifetime(100_000);
    let rec = should_reconnect_after_reauth(
        &mut conn,
        None,
        None,
        None,
        None,
        None,
        Duration::from_secs(60),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(!rec, "fresh connection should not reconnect");

    // Session needing reauth with no mechanism configured fails reauth -> triggers reconnect
    conn.record_sasl_session_lifetime_at(100, Instant::now() - Duration::from_millis(90));
    assert!(conn.needs_reauth());
    let rec = should_reconnect_after_reauth(
        &mut conn,
        None,
        None,
        None,
        None,
        None,
        Duration::from_secs(60),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(rec, "failed reauth must indicate reconnect is needed");
}
