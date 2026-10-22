# Adoption path

partitionline is meant to replace librdkafka-backed clients for services that
need an asynchronous Kafka client with typed configuration and bounded queues.

Read the [support matrix](support.md) before choosing a version. It lists the
tested Kafka versions, Rust toolchains, platforms, and qualification limits.

Version 0.1.0 is published. Check the package with
`bash scripts/check-installable.sh`. Maintainers planning another release should
use the [release guide](RELEASE.md); earlier release records are in
[STATUS.md](STATUS.md).

## Install

`partitionline` **0.1.0** is on [crates.io](https://crates.io/crates/partitionline):

```toml
[dependencies]
partitionline = "0.1"
```

Git tags remain available for bisects; new adopters should prefer crates.io.


## Pilot checklist

1. Produce + fetch against your Kafka 3.9 / 4.x cluster (`examples/roundtrip`).
2. Classic, cooperative, or KIP-848 groups (`examples/group`, `examples/cooperative`, `examples/kip848` on Kafka 4.x).
3. TLS (`rustls`, optional mTLS via `TLS_CLIENT_CERT_PEM` /
   `TLS_CLIENT_KEY_PEM` on `examples/tls`) and SCRAM-SHA-256/512 or OIDC as
   required (`examples/sasl` with `TLS_CA_PEM` for SASL_SSL, `examples/oauth`).
   Real-broker check: `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh`
   (PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS).
4. Transactions / EOS if you need them (`examples/txn`, `examples/eos`).
5. Share groups on Kafka 4.1+ with `share.version=1` (`examples/share`).
6. Scrape `Producer` / `Consumer` / `Admin` / `ShareGroup` metrics; optional
   `tracing` feature for spans (`docs/guide.md`).
7. Read defaults that differ from Java (`auto.offset.reset=Earliest`, etc.).

For KL-08 **24h then 7d** adopter evidence (two independent workloads), copy
[adopter-exercise.md](adopter-exercise.md). The template ships **UNFILLED** and
does not close KL-08 or lift Suite HOLD until real records are filed.

## Remaining work

| Gap | Status |
|---|---|
| crates.io release | **0.1.0 published.** Depend on `partitionline = "0.1"`. |
| zstd | Optional feature, off by default. Encoding and decoding exist. Qualification is finite. See [zstd](zstd-spike.md). |
| Kerberos / GSSAPI | Not implemented. The [provider proposal](gssapi-boundary.md) is waiting for approval. |
| Schema Registry | Separate unpublished [companion](../partitionline-schema/README.md). The application picks the serializer. |
| Signed Suite HOLD benches | Still unsigned. See [STATUS.md](STATUS.md). |

Tell us what blocks a pilot: issue
[#85](https://github.com/mingley/partitionline/issues/85) or the adoption
issue template.

## Verify locally

```bash
bash scripts/ci-civilization-check.sh
# Docker-less agent / nested VM:
bash scripts/ci-native-kafka.sh start
SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh
bash scripts/ci-native-kafka.sh stop
# TLS + SCRAM (isolated ports; needs local Kafka/Java/openssl):
REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh   # PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS
```

See [STATUS.md](STATUS.md) for earlier runs and [benchmark instructions](benchmark.md)
for current measurement procedures.

## Dependency bumps

New dependency bumps land on `main`. The post-cut parks for flate2, the SCRAM
crates (hmac, pbkdf2, sha2), `lz4_flex`, and `actions/checkout` have already
landed. The overlapping Dependabot pull requests were #87 through #92. Do not
reopen those parks.

```bash
bash scripts/check-dependabot-parks-coverage.sh
```

An open Dependabot cargo or Actions bump that the script does not know about
fails the check.
