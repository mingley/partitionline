use partitionline_rust_rdkafka_peer::{config::Config, native};
use rdkafka::producer::{BaseProducer, DefaultProducerContext, Producer};

fn config() -> Config {
    serde_json::from_value(serde_json::json!({
        "bootstrap":"127.0.0.1:1","topic":"offline-test","count":12,"warmup":0,
        "payload_bytes":100,"partitions":6,"acks":-1,"linger_ms":5,
        "batch_size_bytes":1048576,"batch_num_messages":32768,"max_in_flight":5,
        "queue_max_messages":1000000,"queue_max_kbytes":32768,"delivery_timeout_ms":30000,
        "flush_timeout_ms":35000,"run_timeout_ms":120000,"consume_timeout_ms":30000,
        "record_seed":1592590337_u64,"latency_samples":12,"idempotence":true,
        "compression":"none","isolation_level":"read_committed","payload_mode":"seeded",
        "key_mode":"id","security_protocol":"PLAINTEXT","sasl_mechanism":""
    }))
    .unwrap()
}

#[test]
fn reject_implicit_idempotence_adjustments() {
    let mut c = config();
    assert!(c.validate().is_ok());
    c.acks = 1;
    assert!(c.validate().is_err());
    c.acks = -1;
    c.max_in_flight = 6;
    assert!(c.validate().is_err());
}

#[test]
fn bounded_workload_and_absolute_phase_deadlines_are_required() {
    for mutate in [
        |c: &mut Config| c.count = 0,
        |c: &mut Config| c.count = 1_000_000_001,
        |c: &mut Config| c.partitions = 0,
        |c: &mut Config| c.partitions = 10001,
        |c: &mut Config| c.payload_bytes = 10_000_001,
        |c: &mut Config| c.queue_max_messages = 0,
        |c: &mut Config| c.delivery_timeout_ms = 0,
        |c: &mut Config| c.flush_timeout_ms = 0,
        |c: &mut Config| c.run_timeout_ms = 0,
        |c: &mut Config| c.consume_timeout_ms = 0,
    ] {
        let mut c = config();
        mutate(&mut c);
        assert!(c.validate().is_err());
    }
}

#[test]
fn null_keys_cannot_suggest_unique_id_verification() {
    let mut c = config();
    c.key_mode = "none".into();
    assert!(c.validate().is_err());
    c.payload_mode = "constant-x".into();
    assert!(c.validate().is_ok());
}

#[test]
fn unsupported_protocol_and_compression_settings_fail_closed() {
    for (field, value) in [
        ("compression", "made-up"),
        ("isolation", "snapshot"),
        ("security", "INSECURE_SSL"),
        ("sasl", "GSSAPI"),
    ] {
        let mut c = config();
        match field {
            "compression" => c.compression = value.into(),
            "isolation" => c.isolation_level = value.into(),
            "security" => c.security_protocol = value.into(),
            _ => c.sasl_mechanism = value.into(),
        }
        assert!(c.validate().is_err());
    }
}

#[test]
fn effective_post_creation_settings_and_no_secret_dump() {
    let c = config();
    let producer: BaseProducer<DefaultProducerContext> = c.client(false).create().unwrap();
    let effective = native::effective(producer.client()).unwrap();
    assert_eq!(effective["enable.idempotence"], "true");
    assert_eq!(effective["request.required.acks"], "-1");
    assert_eq!(effective["max.in.flight.requests.per.connection"], "5");
    assert_eq!(effective["batch.size"], "1048576");
    assert_eq!(effective["socket.nagle.disable"], "true");
    assert!(!effective.keys().any(|key| key.contains("password")
        || key.contains("username")
        || key.contains("key.location")));
}

#[test]
fn native_pin_checks_fail_before_any_broker_request() {
    assert!(native::runtime("missing-hash").is_err());
    let failure = native::runtime(&"0".repeat(64)).unwrap_err();
    assert!(failure.contains("SHA256 differs"), "{failure}");
    assert_eq!(
        rdkafka::util::get_rdkafka_version().1,
        native::NATIVE_VERSION
    );
    assert_eq!(native::BINDING_VERSION, "4.10.0+2.12.1");
    assert_eq!(native::WRAPPER_VERSION, "0.39.0");
}
