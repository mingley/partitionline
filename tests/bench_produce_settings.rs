//! Producer benchmark configuration and independent C-generator regression checks.
#[path = "../examples/common/bench_produce_settings.rs"]
#[expect(
    dead_code,
    reason = "environment entry point is checked through the standalone example"
)]
mod settings;
use settings::{id_key, seeded_payload, KeyMode, PayloadMode, Settings};

fn parse(values: &[(&str, &str)]) -> partitionline::Result<Settings> {
    Settings::parse(|name| {
        values
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).into())
    })
}

#[test]
fn knobs_survive_into_the_actual_producer_configuration() {
    let settings = parse(&[
        ("BATCH_SIZE", "8192"),
        ("BATCH_RECORDS", "7"),
        ("WARMUP", "17"),
        ("CONNECTIONS", "2"),
        ("MAX_IN_FLIGHT", "3"),
        ("ACKS", "-1"),
        ("IDEMPOTENT", "1"),
        ("QUEUE_KBYTES", "1024"),
        ("KEY_MODE", "id"),
        ("RECORD_SEED", "0xffff"),
    ])
    .unwrap();
    assert_eq!(settings.producer.batch_bytes, 8192);
    assert_eq!(settings.producer.batch_records, 7);
    assert_eq!(settings.warmup_records, Some(17));
    assert!(settings.warmup.is_zero());
    assert_eq!(settings.producer.connections, 2);
    assert_eq!(settings.producer.max_in_flight, 3);
    assert_eq!(settings.producer.buffer_memory, 1024 * 1024);
    assert_eq!(settings.producer.acks, -1);
    assert!(settings.producer.enable_idempotence);
    assert_eq!(settings.seed, 65535);
}
#[test]
fn aliases_agree_and_conflicts_never_silently_win() {
    for values in [
        vec![("BATCH_SIZE", "8192"), ("BATCH_BYTES", "8192")],
        vec![("SEED", "65535"), ("RECORD_SEED", "0xffff")],
        vec![("BUFFER_MEMORY", "1024"), ("QUEUE_KBYTES", "1")],
    ] {
        assert!(parse(&values).is_ok());
    }
    for values in [
        vec![("BATCH_SIZE", "8192"), ("BATCH_BYTES", "4096")],
        vec![("SEED", "1"), ("RECORD_SEED", "2")],
        vec![("BUFFER_MEMORY", "1025"), ("QUEUE_KBYTES", "1")],
    ] {
        assert!(parse(&values).is_err());
    }
}
#[test]
fn invalid_or_implicitly_clamped_settings_are_rejected_before_io() {
    for values in [
        vec![("COUNT", "0")],
        vec![("COUNT", "oops")],
        vec![("WARMUP", "oops")],
        vec![("BATCH_BYTES", "0")],
        vec![("BATCH_RECORDS", "0")],
        vec![("MAX_IN_FLIGHT", "0")],
        vec![("CONNECTIONS", "0")],
        vec![("QUEUE_KBYTES", "18446744073709551615")],
        vec![("BATCH_BYTES", "2097152")],
        vec![("IDEMPOTENT", "1"), ("ACKS", "1")],
        vec![("IDEMPOTENT", "1"), ("ACKS", "-1"), ("MAX_IN_FLIGHT", "6")],
        vec![("ACKS", "2")],
        vec![("KEY_MODE", "invented")],
        vec![("PAYLOAD_MODE", "invented")],
        vec![("RUN_TIMEOUT_MS", "0")],
        vec![("WARMUP_SECS", "3601")],
        vec![("PARTITIONS", "0")],
    ] {
        assert!(parse(&values).is_err(), "accepted {values:?}");
    }
    let settings = parse(&[("IDEMPOTENT", "1"), ("ACKS", "-1")]).unwrap();
    assert_eq!(settings.producer.max_in_flight, 5);
}
#[test]
fn seeded_default_and_explicit_legacy_history_modes_are_distinct() {
    let seeded = parse(&[]).unwrap();
    assert_eq!(seeded.payload_mode, PayloadMode::Seeded);
    assert_eq!(seeded.key_mode, KeyMode::Id);
    let legacy = parse(&[("PAYLOAD_MODE", "constant-x"), ("KEY_MODE", "none")]).unwrap();
    assert_eq!(legacy.payload_mode, PayloadMode::ConstantX);
    let history = parse(&[("RECORD_HISTORY", "receipt.jsonl"), ("COUNT", "3")]).unwrap();
    assert_eq!(history.payload_mode, PayloadMode::History);
    assert_eq!(history.key_mode, KeyMode::History);
    assert!(parse(&[
        ("RECORD_HISTORY", "receipt.jsonl"),
        ("COUNT", "3"),
        ("KEY_MODE", "id")
    ])
    .is_err());
}
#[test]
fn independent_c_peer_generator_goldens_match_and_ids_change_the_payload() {
    // These constants are also checked by a separately compiled C implementation.
    assert_eq!(
        &id_key(0x5eed0001, 17)[..],
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0xb1, 0x1d, 0xe1, 0xe1, 0x66, 0xb5,
            0xef, 0x61
        ]
    );
    assert_eq!(
        &seeded_payload(0x5eed0001, 17, 16)[..],
        &[
            0x19, 0x4a, 0xfd, 0xdc, 0x7f, 0x77, 0xef, 0x73, 0xa7, 0x41, 0x53, 0x6b, 0x8c, 0x15,
            0xa5, 0x2f
        ]
    );
    assert_eq!(&id_key(0x5eed0001, 17)[..8], &17u64.to_be_bytes());
    assert_eq!(seeded_payload(0x5eed0001, 17, 17).len(), 17);
    assert_ne!(
        seeded_payload(0x5eed0001, 17, 100),
        seeded_payload(0x5eed0001, 18, 100)
    );
    assert_ne!(seeded_payload(1, 17, 100), seeded_payload(2, 17, 100));
    assert_eq!(
        &seeded_payload(0x5eed0001, 17, 9)[..8],
        &seeded_payload(0x5eed0001, 17, 8)[..]
    );
}
