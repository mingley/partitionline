# Closed-loop producer acknowledgment diagnostic

This is an exploratory diagnostic, not a qualified client comparison. Its
settings and driver costs differ, the run is short, and it cannot establish a
performance ranking or satisfy the strict benchmark matrix.

## Run and results

[Run 42444229431](https://github.com/mingley/partitionline/actions/runs/42444229431)
used a shared x86_64 Ubuntu 24.04 GitHub runner and a single Kafka 3.9.1 broker
on loopback. The client was pinned to CPU 0 and the broker to CPUs 1–3. Each
arm used a fresh, single-partition topic with replication factor 1 and
`min.insync.replicas=1`. The workload was nontransactional, idempotent,
plaintext Produce with acks=all, a 16-byte key and a seeded 100-byte value.
Each arm completed 16 warmup acknowledgments before 64 measured
acknowledgments, with at most one application record outstanding.

The numbers are recorded to make the run inspectable. They are measured-phase
wall times in execution order and must not be compared as a speed claim.

| Pair | Driver | Measured phase |
| --- | --- | ---: |
| 1 | Partitionline Rust | 84.936745 ms |
| 1 | librdkafka C | 144.555946 ms |
| 2 | librdkafka C | 81.124441 ms |
| 2 | Partitionline Rust | 53.445116 ms |

A Kafka 3.9.1 Java consumer independently read all 80 records in each arm
using `read_uncommitted`; the verifier checked the keys, values, headers,
partitions, offsets, and independently queried end offsets. Each per-record
send or callback completed; the C peer reported no queue-full retries or
failed, rejected, timed-out, or unknown outcomes. The independent verifier's
eight tests and the Rust generator's one test passed. Warmup was
complete before measurement; flush was not the sole delivery barrier.

[Readout run 42444809381](https://github.com/mingley/partitionline/actions/runs/42444809381)
downloaded the retained artifact, checked its SHA-256 and source hashes, and
reran the receipt verifier for all four arms. It did not rerun the benchmark.
The artifact is `ack-paired-42444229431-1`, ID `13602847198`, SHA-256
`541836b5b0afd56c7f9dc1eb8c87de89dac7f0c0b125167c62862fc8b2a52f8c`;
GitHub reports expiry on 2026-12-08. The run logs remain the source for full
settings and results.

[Earlier run 42444129226](https://github.com/mingley/partitionline/actions/runs/42444129226)
failed its stable rustfmt check on the new Rust diagnostic before the Rust
test, native build, broker startup, or measurements. The formatting was fixed
before the successful run; the failed attempt is not a timing sample.

## Pins

The isolated diagnostic source is commit
[`c8a6c6fb84a27f224b37a2fce9cc8f4e443c9333`](https://github.com/mingley/partitionline/tree/c8a6c6fb84a27f224b37a2fce9cc8f4e443c9333),
based on `264322ff430b0594f4c3864e53dcd832de2a930b`. The job checked that
`src/` and `Cargo.lock` were unchanged from that base. The readout source is
[`6d61aa227217278dc71f875ec7869063d9cf89f3`](https://github.com/mingley/partitionline/tree/6d61aa227217278dc71f875ec7869063d9cf89f3).
The diagnostic is kept on its qualification branch and is not part of main.

Source SHA-256 values recorded by the run and rechecked by the readout:

| File | SHA-256 |
| --- | --- |
| `Cargo.lock` | `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc` |
| `src/producer.rs` | `e21d03f495f504f167bc56139dc03798b0f323ca51370c7dceade23a2dad066e` |
| `examples/bench_ack_closed_loop.rs` | `e170149243946a6e8f7a3caf9c9160fa35f17430aeb694fc09969dca0aeb557e` |
| `benchmarks/peers/librdkafka/peer.c` | `90bfefe3c8eb112b3ede5c0fd161fd9e49730f7815726e45386a347ca535fcb9` |
| `benchmarks/peers/librdkafka/source-pin.json` | `084404914c692e0b11758634a7bc194b91898eb26f2f05c8f67ffb386f5774ba` |
| `benchmarks/peers/partitionline/closed_loop_peer.c` | `9e990f2bb78776b4184c63a085a331f00e5ce399863d547edda0af20b2e16d1e` |
| `benchmarks/peers/partitionline/ReceiptAudit.java` | `d4605d116a0acd17bbacb5367316933560db42c1303fdbfe8efa385793fdbed5` |
| `benchmarks/peers/partitionline/verify_ack_pair.py` | `cf616e568fe999a39df45daafcd9dcd8f66eed9288053ba4412e8de4185798fc` |
| `benchmarks/peers/partitionline/fences.py` | `e06deb79d0907f58c3aa9d15be2a48d5dc31355df7a6b7c74cfe52ddc497ba0d` |

The original job recorded these binary SHA-256 values. The binary files were
not retained, so the readout checked the recorded hashes for consistency but
could not rehash the binaries.

| Binary | SHA-256 recorded by the job |
| --- | --- |
| Rust diagnostic | `61f3c91b6dd9e5a603e3eacdafb934b7ae1cda157f1693739840506f5d466905` |
| C diagnostic | `a75e1eb42853601862c07396ce405f5c912114920b797ea5a29ab39f3b875b99` |
| `librdkafka.so.1` | `76273cabc8e70047f97a08af9ce68d47ee1b2c466a3ed26b476717fee8542d83` |
| `kafka-clients-3.9.1.jar` | `25c5e4eb059c35766f645c0e0bd2fe623a1ebdc18250957506b1edbf476d1272` |
| `ReceiptAudit.class` | `21433aa5fde04456905ce16ff70c242d8fa335bf4603fd78ad17a356e31dd62a` |

The build used stable `rustc 1.99.1 (58cf8f9c3 2026-10-13)`,
LLVM 23.1.1 and a locked release Rust build. The C peer used librdkafka
2.15.0 at commit `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`,
tree `429a83d1ba5350c0846aeaee4efe821b463f6b85`. The broker image
digest was
`apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed`.

## Limits

Both drivers used one application record at a time, configured a one-record
batch, zero linger, acks=all, idempotence, a 30-second delivery timeout and
one maximum in-flight request. The requested one-connection setting was not
independently measured. The C queue's global message and byte limits are not
equivalent to Partitionline's per-connection channel and visible-byte limits,
even though this small workload had no C queue-full event. The C socket
timeout was 60 seconds and its idle timeout was 0; the Rust driver used a
30-second request timeout and the default idle timeout. Driver work differs:
the C phase includes an empty flush after all callbacks, while the Rust phase
includes receipt writing; per-record timing boundaries also differ.

Two short pairs on a shared host do not meet the sample, duration,
open-loop latency, or independent reproduction requirements of the benchmark
contract. The strict matrix's rejection rules remain unchanged. This run
does not qualify production readiness, throughput superiority, or a fastest
Kafka client claim.
