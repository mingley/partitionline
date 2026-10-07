//! Cancellation of OIDC tasks when the final public manager owner retires.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::protocol::oidc::{
    MockClock, MockTokenFetcher, OidcRefreshConfig, OidcTokenManager, OidcTokenResponse, ZeroJitter,
};
use tokio::sync::Notify;

struct FetchGuard(Arc<Notify>);

impl Drop for FetchGuard {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn last_owner_cancels_in_flight_acquisition() {
    let entered = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let fetcher = Arc::new(MockTokenFetcher::new({
        let entered = entered.clone();
        let cancelled = cancelled.clone();
        move |_| {
            let entered = entered.clone();
            let cancelled = cancelled.clone();
            Box::pin(async move {
                let _guard = FetchGuard(cancelled);
                entered.notify_one();
                std::future::pending().await
            })
        }
    }));
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        Arc::new(MockClock::new(Instant::now())),
        Arc::new(ZeroJitter),
    );
    let caller = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.token(Duration::from_secs(5)).await })
    };
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // The acquisition belongs to the remaining manager, not the cancelled caller.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), cancelled.notified())
            .await
            .is_err()
    );
    drop(manager);
    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("last owner must cancel acquisition");
}

#[tokio::test]
async fn last_owner_cancels_in_flight_refresh() {
    let entered = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let fetcher = Arc::new(MockTokenFetcher::new({
        let entered = entered.clone();
        let cancelled = cancelled.clone();
        move |_| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            let entered = entered.clone();
            let cancelled = cancelled.clone();
            Box::pin(async move {
                if first {
                    return Ok(OidcTokenResponse::new("initial", "Bearer", Some(100)));
                }
                let _guard = FetchGuard(cancelled);
                entered.notify_one();
                std::future::pending().await
            })
        }
    }));
    let clock = Arc::new(MockClock::new(Instant::now()));
    let manager = OidcTokenManager::with_fetcher(
        fetcher,
        OidcRefreshConfig::default(),
        clock.clone(),
        Arc::new(ZeroJitter),
    );
    assert_eq!(
        manager.token(Duration::from_secs(1)).await.unwrap(),
        "initial"
    );
    clock.advance(Duration::from_secs(80));
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let survivor = manager.clone();
    drop(manager);
    assert_eq!(
        survivor.token(Duration::from_secs(1)).await.unwrap(),
        "initial"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), cancelled.notified())
            .await
            .is_err()
    );
    drop(survivor);
    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("last owner must cancel refresh");
}
