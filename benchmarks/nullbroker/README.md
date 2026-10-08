# Null broker

This fixture validates Produce batches and serves a deterministic Fetch stream.
It does not persist produced records, replicate data, or implement consumer groups.
Use a Kafka cluster for storage, group, transaction, and durability tests.

Build and test with the latest stable Rust:

```sh
cargo +stable test --locked --manifest-path benchmarks/nullbroker/Cargo.toml
cargo +stable build --locked --release --manifest-path benchmarks/nullbroker/Cargo.toml
```

The advertised versions are Produce 9–12, Metadata 12–13, Fetch 15–17,
ListOffsets 7–10, InitProducerId 5, FindCoordinator 1–6, and ApiVersions 0–4.
Requests outside those ranges close the connection. Unsupported ApiVersions
requests receive the standard v0 error response so newer clients can negotiate down. All clients use the same
handlers and batch validation.

`--trace-api-versions true` records request keys, versions, and counts in the
broker artifact. Leave tracing disabled for timing runs. Fetch seed, record count,
payload size, batch size, compression, and fault modes are explicit CLI options.

`peers/` contains functional checks using Java Kafka clients 4.3.1,
librdkafka 2.15.0, and franz-go 1.22.0. Each check acknowledges 512 records and
validates every key, payload byte, timestamp, header count, offset, and partition in the independent seeded
Fetch stream. The produced payloads match that seed. Successful Fetch validation
does not establish storage or produced-record readback.

`peers/run.py` runs one compiled peer, joins the peer and broker, checks their
artifacts, and verifies that the listener port is released. Its command argument
is a JSON array with `{bootstrap}` as the address placeholder. A source binding
must include the broker binary's `binary_sha256`. The runtime's Linux parent-bound
execution helper must be present at `benchmarks/runtime/tools/parent-bound-exec.py`.
The retained qualification records exact build commands and SDK/source hashes.
These checks do not measure comparative performance.
