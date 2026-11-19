#![cfg(feature = "registry")]

#[path = "support/live_registry.rs"]
mod live_registry;

use std::time::Duration;

use live_registry::{assert_avro, RegistryFixture};

use partitionline_schema::registry::{
    RegistryCacheConfig, RegistryClient, RegistryError, SchemaReference, SchemaVersion,
};
use serde_json::json;

const ADDRESS_SUBJECT: &str = "pl-live-address";
const ORDER_SUBJECT: &str = "pl-live-order";

#[tokio::test]
#[ignore = "requires an empty loopback Schema Registry and curl"]
async fn lookup_and_cache_against_redpanda() {
    let fixture = RegistryFixture::from_env();
    assert_eq!(fixture.request_json("/subjects", None), json!([]));

    let address = json!({
        "type": "record", "name": "Address",
        "fields": [{"name": "city", "type": "string"}],
    });
    let order_v1 = json!({
        "type": "record", "name": "Order",
        "fields": [{"name": "id", "type": "long"}, {"name": "ship", "type": "Address"}],
    });
    let mut order_v2 = order_v1.clone();
    order_v2["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name": "note", "type": ["null", "string"], "default": null}));
    let reference_json = json!({"name": "Address", "subject": ADDRESS_SUBJECT, "version": 1});
    let reference = SchemaReference {
        name: "Address".into(),
        subject: ADDRESS_SUBJECT.into(),
        version: 1,
    };

    let address_id = fixture.register(ADDRESS_SUBJECT, &address, &[]);
    let order_v1_id = fixture.register(
        ORDER_SUBJECT,
        &order_v1,
        std::slice::from_ref(&reference_json),
    );
    assert_ne!(address_id, order_v1_id);

    let latest_ttl = Duration::from_secs(60);
    let cache = RegistryCacheConfig::default().freshness(
        Duration::from_secs(600),
        latest_ttl,
        Duration::from_secs(1),
    );
    let client = RegistryClient::new(fixture.client_config().await.cache(cache)).unwrap();
    let latest_v1 = client
        .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Latest)
        .await
        .unwrap();
    assert_eq!(latest_v1.subject, ORDER_SUBJECT);
    assert_eq!(latest_v1.version, 1);
    assert_eq!(latest_v1.id, order_v1_id);
    assert_eq!(latest_v1.references, [reference]);
    assert_avro(
        &latest_v1.schema,
        latest_v1.schema_type.as_deref(),
        &order_v1,
    );
    let pinned = client
        .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Pinned(1))
        .await
        .unwrap();
    assert_eq!(pinned, latest_v1);

    let by_id = client.get_schema_by_id(order_v1_id).await.unwrap();
    assert_eq!(by_id.id, order_v1_id);
    assert_eq!(by_id.references, latest_v1.references);
    assert_avro(&by_id.schema, by_id.schema_type.as_deref(), &order_v1);
    let resolved = client.fetch_references(&by_id.references).await.unwrap();
    let [resolved] = resolved.as_slice() else {
        panic!("expected one referenced schema");
    };
    assert_eq!(resolved.subject, ADDRESS_SUBJECT);
    assert_eq!(resolved.version, 1);
    assert_eq!(resolved.id, address_id);
    assert!(resolved.references.is_empty());
    assert_avro(&resolved.schema, resolved.schema_type.as_deref(), &address);

    let order_v2_id = fixture.register(
        ORDER_SUBJECT,
        &order_v2,
        std::slice::from_ref(&reference_json),
    );
    assert_ne!(order_v2_id, order_v1_id);
    let live = fixture.request_json(
        &format!("/subjects/{ORDER_SUBJECT}/versions/latest"),
        None,
    );
    assert_eq!(live["id"], order_v2_id);
    assert_eq!(live["version"], 2);

    let cached_v1 = client
        .clone()
        .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Latest)
        .await
        .unwrap();
    assert_eq!(cached_v1, latest_v1);

    tokio::time::pause();
    tokio::time::advance(latest_ttl + Duration::from_secs(1)).await;
    tokio::time::resume();

    let latest_v2 = client
        .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Latest)
        .await
        .unwrap();
    assert_eq!(latest_v2.subject, ORDER_SUBJECT);
    assert_eq!(latest_v2.version, 2);
    assert_eq!(latest_v2.id, order_v2_id);
    assert_eq!(latest_v2.references, latest_v1.references);
    assert_avro(
        &latest_v2.schema,
        latest_v2.schema_type.as_deref(),
        &order_v2,
    );
    assert_eq!(
        client
            .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Pinned(1))
            .await
            .unwrap(),
        pinned
    );

    assert!(matches!(
        client
            .get_schema_by_subject("pl-live-missing", SchemaVersion::Latest)
            .await,
        Err(RegistryError::NotFound {
            error_code: Some(40401),
            ..
        })
    ));
    assert_eq!(client.cache_stats().await.in_flight, 0);
    println!(
        "Redpanda registry verified address id={address_id}, order v1 id={order_v1_id}, order v2 id={order_v2_id}, latest cache expiry and missing-subject error"
    );
}

#[tokio::test]
#[ignore = "requires an isolated HTTPS Schema Registry, its CA PEM, and curl"]
async fn lookup_over_tls_against_redpanda() {
    const SUBJECT: &str = "pl-live-tls";

    let fixture = RegistryFixture::from_env();
    fixture.assert_https();
    let expected = json!({
        "type": "record", "name": "TlsEvent",
        "fields": [{"name": "id", "type": "long"}],
    });
    let id = fixture.register(SUBJECT, &expected, &[]);
    let client = RegistryClient::new(fixture.client_config().await).unwrap();

    let version = client
        .get_schema_by_subject(SUBJECT, SchemaVersion::Pinned(1))
        .await
        .unwrap();
    assert_eq!(version.subject, SUBJECT);
    assert_eq!(version.version, 1);
    assert_eq!(version.id, id);
    assert!(version.references.is_empty());
    assert_avro(&version.schema, version.schema_type.as_deref(), &expected);
    let by_id = client.get_schema_by_id(id).await.unwrap();
    assert_eq!(by_id.id, id);
    assert!(by_id.references.is_empty());
    assert_avro(&by_id.schema, by_id.schema_type.as_deref(), &expected);
    println!("Redpanda HTTPS registry verified localhost with the generated CA and schema id={id}, subject {SUBJECT} v1");
}
