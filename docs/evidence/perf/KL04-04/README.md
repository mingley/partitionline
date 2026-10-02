# KL04-04 native C peer correctness evidence

Validation used native **librdkafka v2.15.0** at
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, built with configure/GNU make and
a C compiler in `/workspace/work/c-peer/`. The source and shared library never
enter Cargo. Each run's build manifest records the compiler, source/binary/library
hashes and host OpenSSL/zlib/zstd versions. Kafka was a separately downloaded
**Apache Kafka 3.9.1** native KRaft broker on `127.0.0.1:19094`, with controller
on `127.0.0.1:19095`, isolated scratch logs and fresh synthetic topics. Its startup
log identifies Kafka commit `f745dfdcee2b9851`. Client-side operations used CPUs
0–2 and 4 during other agents' measurement windows. These are correctness
checks on a cloud VM, with no performance comparison or qualification claim.

| Retained attempt | Expected / actual | Verdict |
|---|---|---|
| `initial-broker-loss.json` | First detached broker exited; 10000 warmup callbacks timed out. Timed 256 were later acknowledged, but no valid initial durability/HW audit or receipt verification was established. | Failed run retained, shared validator rejects; JSON Schema rejects unknown RF/minISR represented as 0. |
| `success.json` | Fresh six-partition topic, RF1/minISR1; 10000 warmup acknowledged and excluded; 256 offered/accepted/acknowledged/HW delta/consumed; zero callback failures, missing/duplicate/bad records. | Native exit 0, JSON Schema and shared validator pass. |
| `min-isr-probe.json` | Attempted failure injection with RF1/minISR2 and acks all; broker actually acknowledged all 32 and independent consumption confirmed all 32. | **Failed failure-injection experiment**; exact successful delivery measurement retained. This is not evidence of rejected delivery or a quorum of 2. |
| `delivery-failure.json` | Topic `max.message.bytes=128`; 32 records with 1000-byte values; native callbacks all report broker `MSG_SIZE_TOO_LARGE` (code 10). 32 accepted, 0 acknowledged/consumed, HW delta 0, 32 callback failures. | Native exit 1 and scenario disposition failed; JSON Schema structurally valid, shared validator rejects integrity failure. |

Non-timeout callback failures conservatively remain `unknown` in seven-way
accounting; the exact callback error code/count is retained separately. Enqueue
acceptance stays 32 and is never counted as acknowledgment. An unexpected
successful probe does not replace either failed run or the deliberate callback
failure evidence. Every artifact uses exclusive creation and a new path per
attempt. Companion paths were renamed together with their retained result to
label the first attempt/probe accurately; no delivery counts were changed.

`validation.json` records schema/shared-validator verdicts and checksums of every
result companion. All companions are checked in: raw C delivery/configuration/HW
counts, CSV latency samples, effective settings, stderr and build manifests.
Source provenance reports the then-current main HEAD and `clean=false` because
parallel task implementations were pending. Native library and driver source
bytes are separately hashed in each build manifest.

Six adapter guardrail tests and all 17 shared validator tests pass. Shell syntax
and `git diff --check` pass. A native-backend harness smoke produced 32 records and
independently confirmed HW delta 32; its seeded execution order, resolved Rust
reference settings and HW audit are in `harness/`. A failed harness run confirms
that delivery errors return nonzero and retain the independent HW audit and logs.
The first harness attempt exposed the historical helper's Docker preference for
custom endpoints; the new backend selection sends native tools and clients to
the same endpoint. Only the local container started by that attempt was stopped.

Reproduce the driver checks after building the pin, with a running broker and
fresh isolated topics:

```bash
python3 -m unittest discover -s benchmarks/peers/librdkafka -p test_adapter.py -v
python3 -m unittest discover -s benchmarks/tests -p test_report.py -v
# kafka-topics.sh: create six partitions, RF1, minISR1
KAFKA_BOOTSTRAP=127.0.0.1:19094 KAFKA_TOPIC=new-c-peer-success \
  COUNT=256 WARMUP=10000 ACKS=-1 IDEMPOTENT=1 \
  BROKER_IMAGE=native:apache-kafka-3.9.1 BROKER_VERSION=3.9.1 \
  C_PEER_BUILD_DIR=/workspace/work/c-peer \
  python3 benchmarks/peers/librdkafka/run.py roundtrip --result /tmp/c-success-new.json
# kafka-topics.sh: create six partitions, RF1, minISR1, max.message.bytes=128
KAFKA_BOOTSTRAP=127.0.0.1:19094 KAFKA_TOPIC=new-c-peer-oversize \
  COUNT=32 WARMUP=0 PAYLOAD_BYTES=1000 ACKS=-1 IDEMPOTENT=1 \
  DELIVERY_TIMEOUT_MS=1000 FLUSH_TIMEOUT_MS=3000 \
  BROKER_IMAGE=native:apache-kafka-3.9.1 BROKER_VERSION=3.9.1 \
  C_PEER_BUILD_DIR=/workspace/work/c-peer \
  python3 benchmarks/peers/librdkafka/run.py roundtrip --result /tmp/c-failure-new.json
# The deliberate failure must exit 1; the shared report validator must reject it.
python3 scripts/benchmark-report.py /tmp/c-success-new.json
python3 scripts/benchmark-report.py /tmp/c-failure-new.json
```

Suite HOLD stays active. The small sample counts and single repetitions do not
meet throughput/latency campaign requirements. Secure/compression campaigns,
open-loop latency, transactions, idempotent sequence audits, fsync validation and
controlled-host signoff were not executed. Broker resources, allocations and
average RSS are explicitly unmeasured; C CPU/peak RSS include the audit stage.
