# Closed-loop acknowledgment diagnostic

This isolated diagnostic runs one Partitionline send or one librdkafka delivery
callback at a time. It uses nontransactional, idempotent Produce with acks=all,
one explicit partition, 100-byte seeded values and 16-byte ID keys.
Every warmup delivery completes before measurement starts. A missing, failed or
ambiguous delivery fails the attempt; flush is not a completion barrier.

The C peer is a diagnostic copy of the pinned librdkafka adapter with an added
per-record callback barrier and receipt trace. Its complete native configuration,
queue-full count, and maximum application outstanding count are checked. The
Rust peer writes every individual delivery result and its returned offset. A
separate pinned Kafka 3.9.1 Java consumer reads every warmup and measured
record under read-uncommitted isolation, and an independent verifier checks the
actual bytes, partition, offset, headers and broker end offsets.

This is a shared-host, closed-loop diagnostic. The C and Rust drivers have
different queue accounting and driver overhead. Partitionline's per-connection
channel bound and visible-byte memory limit are not librdkafka's global message
and byte queue limits. The peers request one broker connection and are kept at one outstanding record;
the native peer must report zero queue-full events. Actual connection counts
are not independently measured. The effective queue settings are still different. This diagnostic therefore does not produce an eligible
matrix result or a comparative speed claim. The strict matrix and Suite HOLD
are unchanged. Output includes raw times so failed and successful attempts can
be investigated; it is not an open-loop latency measurement.

The qualification workflow uses two balanced paired repetitions on an isolated
single-broker Kafka fixture. It retains the source, binary, native-library and
broker-image identifiers, every acknowledgement and Java receipt, and failures.
A local Kafka image tag is inspected and its digest recorded, rather than assumed
to be immutable.
