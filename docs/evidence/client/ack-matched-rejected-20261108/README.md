# Exploratory Kafka producer comparison: rejected

The connection-equality gate failed. On both x86 runs, the sampler observed
two established broker sockets in each verified Partitionline arm and one
in each librdkafka arm. These raw closed-loop observations are **unmatched**
and cannot support a performance ranking or the benchmark contract.

The two runs tested a single-broker Kafka 3.9.1 instance and fresh
single-partition topics with replication factor 1 and `min.insync.replicas=1`.
Both producers used plaintext, idempotence, acks=all, zero linger, one
record per batch, no compression, a 16-byte key, a seeded 100-byte value,
and at most one application record and one Produce request in flight. Each
arm sent 10,000 warmup records followed by 10,000 measured records. These are
not replicated production durability settings.

[First run 42458016805](https://github.com/mingley/partitionline/actions/runs/42458016805)
completed all 10 arms and independently verified 200,000 records. The final
gate failed because all five Rust arms had a sampled maximum of two sockets,
while all five C arms had a maximum of one.

[Second run 42458425769](https://github.com/mingley/partitionline/actions/runs/42458425769)
reproduced the socket mismatch in its four verified Rust arms; all five C
arms had a sampled maximum of one. It verified 180,000 records across nine
arms. In pair 3 Rust, the sampler failed with `PermissionError` while reading
`/proc/<pid>/fd/0`; the driver exited 0 and the end offset was 20,000, but
the harness did not run the independent Java reader. That arm remains
unverified and is omitted from the table. Neither that arm nor its individual
sends were retried. Both runs failed overall; no aggregate ratio was emitted.

The sampler inspects process-owned established sockets at 50 ms intervals
across the driver lifetime, including startup and shutdown. It does not
identify their roles, correlate samples to the timed phase, or rule out
short-lived extra sockets. In the first run, Rust had two sockets in 106 of
108 samples for pair 1 and in all 52–58 samples of each other arm. C had one
socket in all but the first sample of each arm. In the second run, each of
the four verified Rust arms had two sockets in 61–123 samples, with at most
two other samples; the five C arms had one socket in all but the first
sample. The sampler failure produced no usable socket report for pair 3
Rust.

## Raw observations

These numbers are included for inspection, not comparison. Records/s uses
the measured phase's wall time, including record generation and receipt
handling. Each latency sample spans the producer call to completion; the
Rust future and C delivery callback have different completion boundaries.
The Rust driver allocates its key before the per-call clock, while
librdkafka copies key and value inside the call. Queue limits and retry,
socket and timeout semantics also differ despite matched nominal settings.
There was no fault or recovery test. Measured phases lasted 1.33–2.35
seconds, below the throughput contract's 60-second or one-million-record
minimum. These closed-loop samples do not measure scheduled-arrival latency.

The first run:

| Pair | Client | Records/s | p50 µs | p99 µs | p99.9 µs |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Partitionline | 6008.8 | 148.500 | 393.954 | 2001.551 |
| 1 | librdkafka | 6469.1 | 149.945 | 233.372 | 1107.870 |
| 2 | librdkafka | 6838.2 | 136.030 | 291.321 | 1207.640 |
| 2 | Partitionline | 6868.0 | 129.709 | 355.811 | 2588.425 |
| 3 | Partitionline | 7319.0 | 125.260 | 300.688 | 1987.971 |
| 3 | librdkafka | 6980.9 | 135.179 | 262.520 | 1229.948 |
| 4 | librdkafka | 7156.4 | 133.878 | 234.482 | 737.902 |
| 4 | Partitionline | 6635.5 | 131.617 | 413.310 | 3005.319 |
| 5 | Partitionline | 7512.0 | 126.897 | 231.388 | 1065.957 |
| 5 | librdkafka | 6721.2 | 136.384 | 367.621 | 1272.095 |

The second run:

| Pair | Client | Records/s | p50 µs | p99 µs | p99.9 µs |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Partitionline | 4259.3 | 219.443 | 651.149 | 2587.245 |
| 1 | librdkafka | 5227.6 | 178.892 | 453.104 | 1986.099 |
| 2 | librdkafka | 6085.3 | 159.630 | 253.330 | 516.656 |
| 2 | Partitionline | 6150.0 | 157.414 | 256.241 | 608.442 |
| 3 | librdkafka | 5708.0 | 169.040 | 260.659 | 831.948 |
| 4 | librdkafka | 5791.6 | 167.057 | 255.039 | 394.738 |
| 4 | Partitionline | 6335.1 | 154.747 | 231.113 | 334.182 |
| 5 | Partitionline | 6277.2 | 153.423 | 250.216 | 534.277 |
| 5 | librdkafka | 6145.3 | 156.979 | 246.890 | 723.746 |

For every table row, the producer completed each acknowledgement before
sending the next record. An independent Kafka 3.9.1 Java reader used direct
partition assignment and `read_uncommitted`; a Python validator checked all
keys, values, offsets and header counts against the acknowledgements and the
independently queried offset fences. No repository consumer or group
implementation was used for readback.

## Pins and retained evidence

| Run | Source | Artifact | Archive SHA-256 |
| --- | --- | --- | --- |
| 42458016805 | [c96f02e](https://github.com/mingley/partitionline/tree/c96f02ef5e915462cdfbe82b4d6da52fa4dd9923) | [13609190311](https://github.com/mingley/partitionline/actions/runs/42458016805/artifacts/13609190311) | `f5703e26f17dff1d150d95fd444dec924311cf6fb3ec34e435310e908c1dd770` |
| 42458425769 | [5452ee0](https://github.com/mingley/partitionline/tree/5452ee0b560c3431563d147886af3fedcd2e27f5) | [13609230874](https://github.com/mingley/partitionline/actions/runs/42458425769/artifacts/13609230874) | `279efe3c9718090839f1de63c0186effbc7c4a7c9db6a4de4c36277a77806558` |

Both source trees are based on
[6298f6b](https://github.com/mingley/partitionline/commit/6298f6bfbed2ab98189d291ebebd2190a2ee75b6);
the jobs checked that `src/`, `Cargo.toml`, `Cargo.lock`, and the
librdkafka build files matched that base. The jobs used stable rustc 1.99.1,
LLVM 23.1.1, GCC 13.3.0 and Temurin 21.0.13. The native library was
librdkafka v2.15.0 at commit
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, tree
`429a83d1ba5350c0846aeaee4efe821b463f6b85`. The broker image was pinned to
`apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed`.
The Java reader checked its Kafka client version and commit.

The first shared Ubuntu 24.04 runner reported four logical CPUs on an Intel
Xeon Platinum 8573C; the second reported four on an Intel Xeon Platinum
8370C. Both pinned the client to CPU 0, sampler to CPU 2, and broker to CPUs
1 and 3. The captured `cpu.max` files were empty, so a CPU quota is not
established. These runs were not independent reproductions.

[Readout 42458956257](https://github.com/mingley/partitionline/actions/runs/42458956257)
checked both artifact identities and archive digests, verified recorded
source hashes against each source tree, and independently reran the
receipt verifier on 10 and 9 arms respectively. It did not rerun Kafka
or reclassify the unverified arm. The artifacts retain the full samples,
delivery traces, readback records, settings, host details and logs, with
an expiry on 2026-12-08. Binary hashes are recorded but the binaries
themselves were not retained for rehashing.

The second fixture explicitly captured and checked librdkafka's effective
`request.timeout.ms=30000`; the first explicitly set it but did not include
it in the effective-config allowlist. Both set nominal 30-second delivery
and socket/request timeouts and a 540-second idle timeout. The first
fixture also left ten sleeping watchdog children; the GitHub runner cleaned
them up after the job. The second fixture changed watchdog cleanup.

## Why one configured connection produced two observed sockets

Source inspection supports, but does not independently prove, the socket
roles. The producer retains its bootstrap connection as
[`Shared.meta`](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs#L1159)
and
[stores it after bootstrap](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs#L2368).
The configured count applies to per-node produce slots; each
[slot opens its own connection](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs#L6722).
The metadata connection is mutex-owned for
[round trips](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs#L6286);
a produce worker separately owns a connection, writes frames, and matches
[its FIFO responses](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs#L8532).
A second reader or an uncoordinated mutex around the existing connection
would not safely share the response stream. A bootstrap broker need not be
the partition leader, and leader or address changes require a fallback.

The existing
[`BrokerPipeline`](https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/net.rs#L3151)
provides a single request dispatcher and explicit SASL quiesce and
reauthentication, but the producer data worker does not use it. It is not
a drop-in replacement for the worker's no-response `acks=0` writes,
batch deadlines, idempotent retry ordering or reconnection logic. Metadata
round-trip cancellation retires its connection; sharing must decide the
fate of all in-flight Produce requests. The current producer data path uses
`write_all_timeout`, while metadata uses `send_deadline`, which checks
SASL session expiry. Producer session-expiry behavior was not tested here.

## Outstanding work

- Keep the physical connection gate failed. Design and qualify any shared
  broker dispatcher for response correlation, request limits, no-response
  writes, metadata fallback and leader changes.
- Prove cancellation, partial writes, deadlines, disconnects, shutdown,
  idempotent retries and ordering, and SASL session expiry/reauthentication
  with mixed metadata and Produce traffic before changing the socket
  ownership model. Measure any added channel, allocation and wakeup cost.
- Preserve partial sampler data on errors, report inaccessible descriptors
  as incomplete rather than silently ignoring them, and perform independent
  readback after a successful driver exit even if sampling failed. A future
  attempt must still reject incomplete socket evidence.
- Run a genuinely matched workload and the repository's open-loop,
  duration, sample-size, durability and independent-reproduction gates before
  making a client ranking or production-readiness claim.
