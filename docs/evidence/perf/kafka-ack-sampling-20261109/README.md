# Kafka producer comparison sampling: isolated repair

The [isolated candidate](https://github.com/mingley/partitionline/tree/dda24520173b7724851a92d3f3fb176ccb7600ae)
copies the earlier acknowledged-produce fixture onto source
`99bf320cfaca7c5bdada83ad7e6d675b0d04c234` and changes its sampler and
orchestration. **The fixture is not applied to main.** This is a diagnostic
repair, not a matched Kafka result or a performance claim.

The earlier fixture stopped before independent readback whenever its socket
sampler failed, even when the producer exited successfully. It also discarded
samples collected before an OS error. The candidate records those samples,
marks the report incomplete and returns a failure. It does not retry or skip
a denied descriptor. After a successful producer exit and successful offset-fence checks, the
candidate attempts independent readback before checking socket evidence. The
arm and aggregate still fail if sampling fails, the report is incomplete or inconsistent, fewer
than two samples exist, or the sampled maximum differs from one connection.
A sampler reading at 50 ms intervals cannot rule out unsampled connections.

## Controls

[Run 42666500827](https://github.com/mingley/partitionline/actions/runs/42666500827)
uses the exact candidate commit. Its Python and shell job passed 13 controls:
eight synthetic sampler, aggregate and shell-orchestration tests and five
existing receipt-verifier tests. They cover synthetic permission errors,
preserved partial samples, no retry of the denied operation, readback after
a successful driver and failed sampler, failed-driver handling, malformed
reports and refusal to emit ratios for an incomplete or mismatched arm.
The shell controls use fake Docker, taskset, Python, Java and producer commands;
they do not contact Kafka or inspect `/proc`. `bash -n` passed.

The separate example job in the same run passed `cargo check --locked --example
bench_kafka_paired` on x86_64 Linux with stable rustc 1.99.1. This type-checks
the copied Rust example against the candidate source; it does not execute it.

These controls did not compile or run the C driver or Java reader, execute the
Rust driver, or run the repaired fixture against Kafka. The fake readback
control checks control flow, not Kafka delivery. No new peer comparison was
run and no previously denied process access was retried.

## Earlier live evidence

[Run 42458016805](https://github.com/mingley/partitionline/actions/runs/42458016805)
built and ran the earlier Rust and C drivers and a Java Kafka 3.9.1 reader,
and verified all 200,000 records across ten arms. It failed its comparison gate:
every Partitionline arm sampled a maximum of two broker sockets, and every C
arm sampled one.

[Run 42458425769](https://github.com/mingley/partitionline/actions/runs/42458425769)
verified 180,000 records across nine arms and failed the same gate. In the
remaining Rust arm, the sampler encountered a `PermissionError`; despite
the producer's successful exit, the earlier harness did not perform independent
readback. That arm remains unverified. Neither earlier failure is corrected or
reclassified by this candidate.

The earlier fixture revisions were
[`c96f02e`](https://github.com/mingley/partitionline/tree/c96f02ef5e915462cdfbe82b4d6da52fa4dd9923)
and
[`5452ee0`](https://github.com/mingley/partitionline/tree/5452ee0b560c3431563d147886af3fedcd2e27f5),
both based on `6298f6bfbed2ab98189d291ebebd2190a2ee75b6`. They used
librdkafka 2.15.0 at `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab` and
`apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed`.
[The retained evidence](../../client/ack-matched-rejected-20261108/README.md)
records source, binary and archive hashes, compiler versions, raw results,
and the single-broker RF1 scope.

Before applying this copied fixture, review and test all its components
together against the current source, including actual C and Java builds and
independent readback. Any future comparison must retain errors, reject
incomplete socket evidence, independently verify records, and meet the
[benchmark measurement rules](../../../benchmark-contract.md). These synthetic
controls do not establish equal physical connections, production readiness,
or a fastest-client ranking.
