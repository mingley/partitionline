#![cfg(feature = "registry")]

use std::{env, process::Command, time::Duration};

use partitionline_schema::registry::{
    RegistryCacheConfig, RegistryClient, RegistryClientConfig, RegistryError, SchemaReference,
    SchemaVersion,
};
use serde_json::{Value, json};

const ADDRESS_SUBJECT: &str = "pl-live-address";
const ORDER_SUBJECT: &str = "pl-live-order";

fn registry_url() -> String {
    let url = env::var("PARTITIONLINE_REGISTRY_LIVE_URL").expect("set the loopback registry URL");
    let port = url
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .expect("use http://127.0.0.1:<port> for the isolated registry");
    assert!(port > 0);
    format!("http://127.0.0.1:{port}")
}

fn curl_json(url: &str, body: Option<Value>) -> Value {
    let mut curl = Command::new("curl");
    curl.args([
        "--disable",
        "--silent",
        "--show-error",
        "--fail-with-body",
        "--noproxy",
        "*",
        "--proto",
        "=http",
        "--connect-timeout",
        "2",
        "--max-time",
        "5",
        "--max-filesize",
        "65536",
        "--header",
        "Accept: application/vnd.schemaregistry.v1+json",
    ]);
    if let Some(body) = body {
        curl.args([
            "--header",
            "Content-Type: application/vnd.schemaregistry.v1+json",
            "--data-binary",
            &body.to_string(),
        ]);
    }
    let response = curl.arg(url).output().expect("run curl against the fixture");
    assert!(
        response.status.success(),
        "fixture request {url} failed ({})\n{}\n{}",
        response.status,
        String::from_utf8_lossy(&response.stderr),
        String::from_utf8_lossy(&response.stdout)
    );
    serde_json::from_slice(&response.stdout).expect("JSON response from the fixture")
}

fn register(base: &str, subject: &str, schema: &Value, references: &[Value]) -> u32 {
    let response = curl_json(
        &format!("{base}/subjects/{subject}/versions"),
        Some(json!({
            "schema": schema.to_string(),
            "schemaType": "AVRO",
            "references": references,
        })),
    );
    let id = response["id"].as_u64().expect("registration returned an ID");
    let id = u32::try_from(id).expect("schema ID fits in u32");
    assert!(id > 0);
    id
}

fn assert_avro(schema: &str, schema_type: Option<&str>, expected: &Value) {
    assert_eq!(schema_type.unwrap_or("AVRO"), "AVRO");
    assert_eq!(
        &serde_json::from_str::<Value>(schema).expect("Avro schema JSON"),
        expected
    );
}

#[tokio::test]
#[ignore = "requires an empty loopback Schema Registry and curl"]
async fn lookup_and_cache_against_redpanda() {
    let base = registry_url();
    assert_eq!(curl_json(&format!("{base}/subjects"), None), json!([]));

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

    let address_id = register(&base, ADDRESS_SUBJECT, &address, &[]);
    let order_v1_id = register(
        &base,
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
    let client = RegistryClient::new(
        RegistryClientConfig::new(&base)
            .request_timeout(Duration::from_secs(5))
            .max_attempts(1)
            .cache(cache),
    )
    .unwrap();
    let latest_v1 = client
        .get_schema_by_subject(ORDER_SUBJECT, SchemaVersion::Latest)
        .await
        .unwrap();
    assert_eq!(latest_v1.subject, ORDER_SUBJECT);
    assert_eq!(latest_v1.version, 1);
    assert_eq!(latest_v1.id, order_v1_id);
    assert_eq!(latest_v1.references, [reference]);
    assert_avro(&latest_v1.schema, latest_v1.schema_type.as_deref(), &order_v1);
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

    let order_v2_id = register(
        &base,
        ORDER_SUBJECT,
        &order_v2,
        std::slice::from_ref(&reference_json),
    );
    assert_ne!(order_v2_id, order_v1_id);
    let live = curl_json(&format!("{base}/subjects/{ORDER_SUBJECT}/versions/latest"), None);
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
    assert_avro(&latest_v2.schema, latest_v2.schema_type.as_deref(), &order_v2);
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
