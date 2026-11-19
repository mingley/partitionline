#[path = "../support/live_registry.rs"]
mod live_registry;
#[path = "../support/selected_avro.rs"]
mod selected_avro;

use std::{env, net::SocketAddrV4, time::Duration};

use apache_avro::types::Value;
use live_registry::{assert_avro, RegistryFixture};
use partitionline::{Acks, Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};
use partitionline_schema::{
    avro::{self, Adapter, Limits, Reference, Schema},
    registry::{RegistryClient, SchemaReference, SchemaVersion},
};
use selected_avro::SelectedAvro;
use serde_json::json;

const TOPIC: &str = "pl-live-avro";
const SUBJECT: &str = "pl-live-avro-value";
const PLACE_SUBJECT: &str = "pl-live-avro-place";
const PLACE_NAME: &str = "fixture.Place";
const KEY: &[u8] = b"event-42";
// Avro long 42 and a seven-byte UTF-8 string in the nested Place record.
const DATUM: &[u8] = b"\x54\x0eZ\xc3\xbcrich";

fn limits() -> Limits {
    Limits::new(256, 4096, 2).unwrap()
}

async fn adapter(
    registry: &RegistryClient,
    schema_id: u32,
    schema: &str,
    references: &[SchemaReference],
    place_id: u32,
    place_json: &serde_json::Value,
) -> Adapter<SelectedAvro> {
    let [place] = references else {
        panic!("expected the single named Place reference");
    };
    assert_eq!(place.name, PLACE_NAME);
    assert_eq!(place.subject, PLACE_SUBJECT);
    assert_eq!(place.version, 1);

    let resolved = registry.fetch_references(references).await.unwrap();
    let [resolved] = resolved.as_slice() else {
        panic!("expected the referenced Place schema");
    };
    assert_eq!(resolved.subject, PLACE_SUBJECT);
    assert_eq!(resolved.version, 1);
    assert_eq!(resolved.id, place_id);
    assert!(resolved.references.is_empty());
    assert_avro(
        &resolved.schema,
        resolved.schema_type.as_deref(),
        place_json,
    );

    let selection = Schema {
        json: schema,
        references: &[Reference {
            name: &place.name,
            json: &resolved.schema,
        }],
    };
    Adapter::new(
        schema_id,
        selection,
        selection,
        SelectedAvro::default(),
        limits(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the owned HTTPS Schema Registry and a fresh loopback Kafka topic"]
async fn avro_produce_and_fetch_against_redpanda() {
    let bootstrap: SocketAddrV4 = env::var("PARTITIONLINE_SCHEMA_LIVE_KAFKA")
        .expect("set the loopback Kafka listener")
        .parse()
        .expect("use 127.0.0.1:<port> for the Kafka fixture");
    assert!(bootstrap.ip().is_loopback() && bootstrap.port() > 0);
    let bootstrap = bootstrap.to_string();

    let fixture = RegistryFixture::from_env();
    fixture.assert_https();
    let place = json!({
        "type": "record", "name": "Place", "namespace": "fixture",
        "fields": [{"name": "city", "type": "string"}],
    });
    let event = json!({
        "type": "record", "name": "Event",
        "fields": [{"name": "id", "type": "long"}, {"name": "ship", "type": PLACE_NAME}],
    });
    let place_id = fixture.register(PLACE_SUBJECT, &place, &[]);
    let writer_id = fixture.register(
        SUBJECT,
        &event,
        &[json!({"name": PLACE_NAME, "subject": PLACE_SUBJECT, "version": 1})],
    );
    assert_ne!(place_id, writer_id);

    let config = fixture.client_config().await;
    let writer_registry = RegistryClient::new(config.clone()).unwrap();
    let writer = writer_registry
        .get_schema_by_subject(SUBJECT, SchemaVersion::Pinned(1))
        .await
        .unwrap();
    assert_eq!(writer.subject, SUBJECT);
    assert_eq!(writer.version, 1);
    assert_eq!(writer.id, writer_id);
    assert_avro(&writer.schema, writer.schema_type.as_deref(), &event);
    let encoder = adapter(
        &writer_registry,
        writer.id,
        &writer.schema,
        &writer.references,
        place_id,
        &place,
    )
    .await;
    let input = Value::Record(vec![
        ("id".into(), Value::Long(42)),
        (
            "ship".into(),
            Value::Record(vec![("city".into(), Value::String("Zürich".into()))]),
        ),
    ]);
    let frame = encoder.encode(&input).unwrap();
    let mut expected_frame = vec![0];
    expected_frame.extend_from_slice(&writer_id.to_be_bytes());
    expected_frame.extend_from_slice(DATUM);
    assert_eq!(frame, expected_frame);
    println!("HTTPS schema id={writer_id}, Place id={place_id}, Avro frame verified");

    let mut producer_config = ProducerConfig::bootstrap([bootstrap.clone()])
        .acks(Acks::All)
        .linger(Duration::ZERO);
    producer_config.request_timeout = Duration::from_secs(5);
    producer_config.max_block = Duration::from_secs(5);
    producer_config.delivery_timeout = Duration::from_secs(10);
    let producer = Producer::new(producer_config)
        .await
        .expect("connect the Kafka producer");
    println!("Kafka producer connected");
    let ack = producer
        .send(ProduceRecord::to(TOPIC).partition(0).key(KEY).value(frame))
        .await
        .expect("produce the Avro frame");
    println!(
        "Kafka write acknowledged at {}@{}",
        ack.partition, ack.offset
    );
    assert_eq!(ack.topic.as_ref(), TOPIC);
    assert_eq!(ack.partition, 0);
    assert_eq!(ack.offset, 0);
    producer.close().await.expect("close the Kafka producer");

    let mut consumer_config = ConsumerConfig::bootstrap([bootstrap]);
    consumer_config.request_timeout = Duration::from_secs(5);
    consumer_config.max_wait_ms = 500;
    let mut consumer = Consumer::new(consumer_config)
        .await
        .expect("connect the Kafka consumer");
    println!("Kafka consumer connected");
    consumer
        .assign(TOPIC, ack.partition, ack.offset)
        .await
        .expect("assign the acknowledged Kafka partition and offset");
    let fetched = consumer.fetch().await.expect("fetch the Avro frame");
    let [record] = &*fetched else {
        panic!("expected one Kafka record at the acknowledged offset");
    };
    assert_eq!(record.topic, TOPIC);
    assert_eq!(record.partition, ack.partition);
    assert_eq!(record.offset, ack.offset);
    assert_eq!(record.key.as_deref(), Some(KEY));
    let received = record.value.as_deref().expect("a non-null Avro frame");
    assert_eq!(received, expected_frame.as_slice());

    let framed = avro::decode(received, limits()).unwrap();
    assert_eq!(framed.schema_id, writer_id);
    assert_eq!(framed.payload, DATUM);
    let reader_registry = RegistryClient::new(config).unwrap();
    let registered = reader_registry
        .get_schema_by_id(framed.schema_id)
        .await
        .unwrap();
    assert_eq!(registered.id, writer_id);
    assert_eq!(registered.references, writer.references);
    assert_avro(
        &registered.schema,
        registered.schema_type.as_deref(),
        &event,
    );
    let decoder = adapter(
        &reader_registry,
        registered.id,
        &registered.schema,
        &registered.references,
        place_id,
        &place,
    )
    .await;
    let output = decoder.decode(received).unwrap();
    assert_eq!(
        selected_avro::json(&output),
        json!({"id": 42, "ship": {"city": "Zürich"}})
    );
    consumer.close().await.unwrap();
    println!(
        "Redpanda round trip {TOPIC}-{}@{} with schema id={writer_id}, Place id={place_id}, key=event-42 and Avro city=Zürich",
        ack.partition, ack.offset
    );
}
