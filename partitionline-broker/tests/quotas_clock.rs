//! Public quota window behavior with reordered clock samples.
#![allow(clippy::unwrap_used)]

use partitionline_broker::quotas::{Error, Kind, Limits, Window};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

fn limits() -> Limits {
    Limits::new(100, 100, 100, 1, 4, 10_000, 1_000).unwrap()
}

fn charge(window: &mut Window, name: Option<&str>, kind: Kind, bytes: u64, at: u64) -> u64 {
    let admission = window.admit(name, kind, bytes, at).unwrap();
    assert_eq!(window.complete(admission.work_id).unwrap(), bytes);
    admission.throttle_ms
}

#[test]
fn stale_new_identity_cannot_get_a_second_burst_at_the_observed_time() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 10_000), 0);
    assert_eq!(charge(&mut window, Some("late"), Kind::Produce, 100, 0), 0);
    assert_eq!(
        charge(&mut window, Some("late"), Kind::Produce, 100, 10_000),
        1_000
    );
}

#[test]
fn stale_client_charge_uses_the_latest_observed_time() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 10_000), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 50, 0), 0);
    assert_eq!(
        charge(&mut window, Some("a"), Kind::Produce, 100, 10_000),
        500
    );
}

#[test]
fn stale_replication_charge_uses_the_latest_observed_time() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 100, 0), 0);
    assert_eq!(
        charge(&mut window, Some("a"), Kind::Produce, 1, 10_000),
        0
    );
    assert_eq!(charge(&mut window, None, Kind::Replication, 50, 0), 0);
    assert_eq!(
        charge(&mut window, None, Kind::Replication, 100, 10_000),
        500
    );
}

#[test]
fn stale_timed_rate_change_refills_at_the_old_rate_first() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 1_000), 0);
    window.set_rates_at(1_000, 1_000, 1_000, 0).unwrap();
    assert_eq!(
        charge(&mut window, Some("a"), Kind::Produce, 200, 1_000),
        100
    );
}

#[test]
fn stale_connect_reclaims_only_fully_refilled_idle_identity() {
    let limits = Limits::new(100, 100, 100, 1, 2, 10_000, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    window.connect_at("a", 0).unwrap();
    let accepted = window.admit(Some("a"), Kind::Produce, 150, 0).unwrap();
    assert_eq!(accepted.throttle_ms, 500);
    window.disconnect("a").unwrap();
    window.connect_at("b", 0).unwrap();
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 1_000), 0);
    assert!(matches!(window.connect_at("c", 0), Err(Error::Throttled)));
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 2_000), 0);
    window.connect_at("c", 0).unwrap();
    assert_eq!(window.connections(), 2);
    assert!(matches!(
        window.connect_at("d", 2_000),
        Err(Error::Throttled)
    ));
    assert!(window.accepted(accepted.work_id));
    assert_eq!(window.complete(accepted.work_id).unwrap(), 150);
    assert_eq!(window.inflight_bytes(), 0);
}

#[test]
fn serialized_concurrent_callers_do_not_rewind_a_sampled_time() {
    let window = Arc::new(Mutex::new(Window::new(limits(), 0)));
    let (tx, rx) = mpsc::channel();
    let delayed_window = Arc::clone(&window);
    let delayed = thread::spawn(move || {
        let sampled_at = 0;
        rx.recv().unwrap();
        let mut guard = delayed_window.lock().unwrap();
        let first = charge(&mut guard, Some("late"), Kind::Produce, 100, sampled_at);
        let second = charge(&mut guard, Some("late"), Kind::Produce, 100, 10_000);
        (first, second)
    });
    let advancing_window = Arc::clone(&window);
    let advancing = thread::spawn(move || {
        {
            let mut guard = advancing_window.lock().unwrap();
            assert_eq!(charge(&mut guard, None, Kind::Replication, 1, 10_000), 0);
        }
        tx.send(()).unwrap();
    });
    advancing.join().unwrap();
    assert_eq!(delayed.join().unwrap(), (0, 1_000));
}

#[test]
fn advancing_time_and_timed_rates_keep_independent_credit() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Fetch, 100, 0), 0);
    window.set_rates_at(200, 200, 200, 500).unwrap();
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 500), 250);
    assert_eq!(charge(&mut window, Some("a"), Kind::Fetch, 50, 500), 0);
    assert_eq!(charge(&mut window, Some("b"), Kind::Produce, 200, 500), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 100, 500), 0);
}

#[test]
fn early_invalid_calls_do_not_advance_the_observed_clock() {
    for case in 0..3 {
        let mut window = Window::new(limits(), 0);
        assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
        match case {
            0 => assert!(matches!(window.connect_at("", 10_000), Err(Error::InvalidRequest))),
            1 => assert!(matches!(
                window.admit(Some("a"), Kind::Produce, 0, 10_000),
                Err(Error::InvalidRequest)
            )),
            _ => assert!(matches!(
                window.set_rates_at(0, 100, 100, 10_000),
                Err(Error::InvalidLimits)
            )),
        }
        assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 1_000);
    }
}

#[test]
fn byte_cap_rejection_happens_before_observing_time() {
    let limits = Limits::new(100, 100, 100, 1, 4, 100, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    let accepted = window.admit(Some("a"), Kind::Produce, 100, 0).unwrap();
    assert!(matches!(
        window.admit(None, Kind::Replication, 1, 10_000),
        Err(Error::Throttled)
    ));
    assert_eq!(window.complete(accepted.work_id).unwrap(), 100);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 1_000);
}

#[test]
fn later_rejection_observes_time_without_spending_tokens() {
    let mut missing_name = Window::new(limits(), 0);
    assert_eq!(charge(&mut missing_name, Some("a"), Kind::Produce, 100, 0), 0);
    assert!(matches!(
        missing_name.admit(None, Kind::Produce, 1, 10_000),
        Err(Error::InvalidRequest)
    ));
    assert_eq!(
        charge(&mut missing_name, Some("a"), Kind::Produce, 100, 0),
        0
    );

    let mut refused_quota = Window::new(limits(), 0);
    assert_eq!(charge(&mut refused_quota, Some("a"), Kind::Produce, 100, 0), 0);
    assert!(matches!(
        refused_quota.admit(Some("a"), Kind::Produce, 201, 10_000),
        Err(Error::Throttled)
    ));
    assert_eq!(
        charge(&mut refused_quota, Some("a"), Kind::Produce, 100, 0),
        0
    );
    assert_eq!(
        charge(&mut refused_quota, Some("a"), Kind::Produce, 100, 10_000),
        1_000
    );
}

#[test]
fn connection_cap_rejection_observes_valid_time() {
    let limits = Limits::new(100, 100, 100, 1, 1, 10_000, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    window.connect_at("a", 0).unwrap();
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
    assert!(matches!(
        window.connect_at("b", 10_000),
        Err(Error::Throttled)
    ));
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
}

#[test]
fn untimed_rate_update_retains_legacy_refill_semantics() {
    let mut window = Window::new(limits(), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 100, 0), 0);
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, 500), 0);
    window.set_rates(1_000, 1_000, 1_000).unwrap();
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 200, 500), 0);
}

#[test]
fn clamped_time_preserves_fractional_refill_at_u64_max() {
    let mut window = Window::new(limits(), u64::MAX - 1);
    assert_eq!(
        charge(&mut window, Some("a"), Kind::Produce, 100, u64::MAX - 1),
        0
    );
    assert_eq!(charge(&mut window, None, Kind::Replication, 1, u64::MAX), 0);
    assert_eq!(charge(&mut window, Some("a"), Kind::Produce, 1, 0), 9);
}
