# InitProducerId v6 (KL05-35)

Typed request/response forms preserve both two-phase flags and both ongoing-transaction identity fields. Compatibility wrappers supply false flags and -1 ongoing sentinels. Ordinary producers negotiate v0–v6 and use only the regular returned identity. Each coordinator connection retains its negotiated version; startup and epoch recovery keep one original deadline.

Apache Java serializers/parsers and public request factories from Kafka 4.1.2, 4.2.1 and 4.3.1 check 39 request/response pairs in both directions. Tests cover all flag combinations, separate identities, terminal errors, unknown tags, truncation, complete input, older negotiation, coordinator movement and epoch exhaustion. True flags on older versions return Unsupported before an application frame. A deliberately changed Rust flag fails the independent check.

A fresh Kafka 4.3.1 broker with unstable APIs explicitly enabled accepted ordinary Rust initialization at v6. Captured bytes show both flags false. Java independently checked the initialization fields and the combined public Rust/Java transaction history: five committed records visible and two aborted records absent. The broker and observation proxy were joined and their ports released. Full prepared transaction lifecycle remains a separate feature.

```sh
PL_INIT_PRODUCER_ID_V6_OUTPUT=work/init-v6-output cargo test --locked --test init_producer_id_v6 --test producer_initialization
python3 tests/conformance/run-init-producer-id-v6.py --peer-cache work/kafka-peers --rust-output work/init-v6-output --report work/init-v6-peer.json
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Use fresh output/report paths. The peer runner checks loaded jar hashes, generator/runner source and fixture hashes, with bounded downloads and Java process deadlines. Native execution requires an isolated broker and fresh topic; the opt-in test is ignored in ordinary suites. `summary.json` records final checks, source provenance and limitations; `validation/` retains draft failures and native/independent receipts. No production or speed rank is claimed.
