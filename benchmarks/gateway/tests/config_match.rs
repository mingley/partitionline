use gateway::cells::{find, AckMode, CELLS};
use gateway::settings::{refuse_ack_modes, refuse_compare, ComparableSettings, TlsMode};
use gateway::{compare_effective, partitionline_emit_config};
use serde_json::json;

fn base() -> ComparableSettings {
    ComparableSettings::for_cell(find("lb-gateway-produce-gzip").unwrap(), TlsMode::Plain)
}

#[test]
fn matching_settings_compare() {
    let left = base();
    let right = left.clone();
    refuse_compare(&left, &right).unwrap();
}

#[test]
fn each_gated_field_refuses() {
    let left = base();
    let mut codec = left.clone();
    codec.codec = "lz4".into();
    let mut level = left.clone();
    level.level = 6;
    let mut acks = left.clone();
    acks.acks = 1;
    let mut inflight = left.clone();
    inflight.max_in_flight = 5;
    let mut linger = left.clone();
    linger.linger_ms = 5;
    let mut batch = left.clone();
    batch.batch_size_bytes = 1000;
    let mut tls = left.clone();
    tls.tls = TlsMode::Mtls;
    for (field, right) in [
        ("codec", codec),
        ("level", level),
        ("acks", acks),
        ("max_in_flight", inflight),
        ("linger_ms", linger),
        ("batch_size_bytes", batch),
        ("tls", tls),
    ] {
        let error = refuse_compare(&left, &right).unwrap_err();
        assert_eq!(error.field, field, "{field}");
    }
}

#[test]
fn fire_and_forget_is_never_compared_with_per_record() {
    let error = refuse_ack_modes(
        AckMode::FireAndForgetTimedFlush,
        AckMode::PerRecordCompletion,
    )
    .unwrap_err();
    assert_eq!(error.field, "acknowledgment_mode");
}

#[test]
fn effective_json_round_trip_and_peer_keys() {
    let cell = CELLS[0];
    let emitted = partitionline_emit_config(cell, TlsMode::Plain);
    let parsed = ComparableSettings::from_effective(&emitted).unwrap();
    assert_eq!(parsed, ComparableSettings::for_cell(cell, TlsMode::Plain));
    let peer = json!({
        "compression.codec": "gzip",
        "compression.level": "-1",
        "request.required.acks": "-1",
        "max.in.flight.requests.per.connection": "1",
        "queue.buffering.max.ms": "100",
        "batch.size": "1048576",
        "security.protocol": "plaintext"
    });
    compare_effective(
        &emitted,
        &peer,
        AckMode::FireAndForgetTimedFlush,
        AckMode::FireAndForgetTimedFlush,
    )
    .unwrap();
}
