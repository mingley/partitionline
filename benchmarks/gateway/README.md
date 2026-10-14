# Gateway produce cells (KL10-02)

Exploratory local cells from `docs/plan/gateway-adoption.md` section 0:

- `lb-gateway-produce-gzip`
- `lb-gateway-produce-lz4`
- `lb-gateway-produce-ack`

Each cell runs at an open-loop 25k records/s and unpaced, for partitionline and
the isolated rust-rdkafka peer. The driver refuses to compare runs whose codec,
level, acks, max in flight, linger, batch size or TLS settings differ. Fire-and-forget
is never compared with per-record completion.

This crate depends only on `partitionline`. librdkafka stays in
`benchmarks/peers/rust` and is spawned as a subprocess.

```sh
cargo test --locked --manifest-path benchmarks/gateway/Cargo.toml
cargo run --manifest-path benchmarks/gateway/Cargo.toml -- emit-config --cell lb-gateway-produce-gzip
KAFKA_BOOTSTRAP=127.0.0.1:9092 GATEWAY_RUST_PEER_BIN=/path/to/rust-peer \
  cargo run --release --manifest-path benchmarks/gateway/Cargo.toml -- campaign --out /tmp/gateway
python3 scripts/benchmark-report.py /tmp/gateway/lb-gateway-produce-gzip-open-loop-25k-partitionline-rep1.json
```

Differences from gateway-adoption.md section 2 (host, optional PLAINTEXT, HWM
reconciliation, gzip backend, shared deadline) are recorded on every result and
are not comparison gates.
