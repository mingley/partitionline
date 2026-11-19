use std::{env, path::PathBuf, process::Command, time::Duration};

use partitionline_schema::registry::RegistryClientConfig;
use serde_json::{json, Value};

pub(super) struct RegistryFixture {
    base: String,
    port: u16,
    ca_file: Option<PathBuf>,
}

impl RegistryFixture {
    pub(super) fn from_env() -> Self {
        let url = env::var("PARTITIONLINE_REGISTRY_LIVE_URL").expect("set the loopback registry URL");
        let (prefix, suffix, ca_file) = if let Some(port) = url.strip_prefix("http://127.0.0.1:") {
            ("http://127.0.0.1:", port, None)
        } else {
            let port = url
                .strip_prefix("https://localhost:")
                .expect("use http://127.0.0.1:<port> or https://localhost:<port>");
            let ca_file = env::var_os("PARTITIONLINE_REGISTRY_LIVE_CA_FILE")
                .expect("set the generated fixture CA file");
            ("https://localhost:", port, Some(PathBuf::from(ca_file)))
        };
        let port = suffix.parse::<u16>().expect("use a numeric registry port");
        assert!(port > 0);
        Self {
            base: format!("{prefix}{port}"),
            port,
            ca_file,
        }
    }

    pub(super) fn assert_https(&self) {
        assert!(self.ca_file.is_some(), "use the HTTPS registry listener");
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "CA file I/O runs on Tokio's blocking pool"
    )]
    pub(super) async fn client_config(&self) -> RegistryClientConfig {
        let config = RegistryClientConfig::new(&self.base)
            .request_timeout(Duration::from_secs(5))
            .max_attempts(1);
        if let Some(ca_file) = self.ca_file.clone() {
            let pem = tokio::task::spawn_blocking(move || std::fs::read(ca_file))
                .await
                .expect("join the CA file reader")
                .expect("read the generated fixture CA");
            config.ca_pem(pem)
        } else {
            config
        }
    }

    pub(super) fn request_json(&self, path: &str, body: Option<Value>) -> Value {
        let mut curl = Command::new("curl");
        curl.args([
            "--disable",
            "--silent",
            "--show-error",
            "--fail-with-body",
            "--noproxy",
            "*",
            "--connect-timeout",
            "2",
            "--max-time",
            "5",
            "--max-filesize",
            "65536",
            "--header",
            "Accept: application/vnd.schemaregistry.v1+json",
        ]);
        if let Some(ca_file) = &self.ca_file {
            curl.args(["--proto", "=https", "--cacert"])
                .arg(ca_file)
                .arg("--resolve")
                .arg(format!("localhost:{}:127.0.0.1", self.port));
        } else {
            curl.args(["--proto", "=http"]);
        }
        if let Some(body) = body {
            curl.args([
                "--header",
                "Content-Type: application/vnd.schemaregistry.v1+json",
                "--data-binary",
                &body.to_string(),
            ]);
        }
        let url = format!("{}{path}", self.base);
        let response = curl.arg(&url).output().expect("run curl against the fixture");
        assert!(
            response.status.success(),
            "fixture request {url} failed ({})\n{}\n{}",
            response.status,
            String::from_utf8_lossy(&response.stderr),
            String::from_utf8_lossy(&response.stdout)
        );
        serde_json::from_slice(&response.stdout).expect("JSON response from the fixture")
    }

    pub(super) fn register(&self, subject: &str, schema: &Value, references: &[Value]) -> u32 {
        let response = self.request_json(
            &format!("/subjects/{subject}/versions"),
            Some(json!({
                "schema": schema.to_string(),
                "schemaType": "AVRO",
                "references": references,
            })),
        );
        let id = response["id"]
            .as_u64()
            .expect("registration returned an ID");
        let id = u32::try_from(id).expect("schema ID fits in u32");
        assert!(id > 0);
        id
    }
}

pub(super) fn assert_avro(schema: &str, schema_type: Option<&str>, expected: &Value) {
    assert_eq!(schema_type.unwrap_or("AVRO"), "AVRO");
    assert_eq!(
        &serde_json::from_str::<Value>(schema).expect("Avro schema JSON"),
        expected
    );
}
