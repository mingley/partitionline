# Gateway produce and fetch cells (KL10-02, KL10-14)

Exploratory local cells from `docs/plan/gateway-adoption.md` section 0:

- `lb-gateway-produce-gzip`
- `lb-gateway-produce-lz4`
- `lb-gateway-produce-ack`
- `lb-gateway-fetch-gzip`

Produce cells run at an open-loop 25k records/s and unpaced, for partitionline and
the isolated rust-rdkafka peer. The produce driver refuses to compare runs whose
codec, level, acks, max in flight, linger, batch size or TLS settings differ.

The fetch cell consumes a gzip json1k backlog written once by the pinned peer.
Both clients use manual assignment from those explicit start offsets and matched
fetch sizes, wait times and TLS. Every consumed ID/hash is checked against the
KL04-07 produce history.

This crate depends only on `partitionline`. librdkafka stays in
`benchmarks/peers/rust` and is spawned as a subprocess.

```sh
cargo test --locked --manifest-path benchmarks/gateway/Cargo.toml
cargo run --manifest-path benchmarks/gateway/Cargo.toml --bin gateway-produce -- emit-config --cell lb-gateway-produce-gzip
cargo run --manifest-path benchmarks/gateway/Cargo.toml --bin gateway-fetch -- emit-config --cell lb-gateway-fetch-gzip
KAFKA_BOOTSTRAP=127.0.0.1:9092 GATEWAY_RUST_PEER_BIN=/path/to/rust-peer \
  cargo run --release --manifest-path benchmarks/gateway/Cargo.toml --bin gateway-fetch -- campaign --out /tmp/gateway-fetch
python3 scripts/benchmark-report.py /tmp/gateway-fetch/lb-gateway-fetch-gzip-backlog-partitionline-rep1.json
```

Differences from gateway-adoption.md section 2 are recorded on every result and
are not comparison gates.
