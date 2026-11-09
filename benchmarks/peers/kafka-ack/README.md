# Exploratory acknowledged-produce comparison

This isolated fixture compares two producer APIs on a shared x86 runner. It
does not satisfy the benchmark contract or establish a fastest-client claim.

Both producers send a single outstanding record to a fresh one-partition
Kafka topic. Every send is individually acknowledged before the next begins.
The warmup and measured phases each contain 10,000 records. Each record has a
unique 16-byte key, a seeded 100-byte value and no headers. The clients use
acks=all, idempotence, no compression, zero linger, a one-record batch,
one maximum in-flight request and plaintext. The broker uses replication
factor 1 and min.insync.replicas 1.

Record generation precedes each per-record clock. Both drivers stop the
phase clock before writing receipt files; Rust measures from send to future
completion and librdkafka measures from produce call to the delivery
callback. The Rust driver allocates the record key before starting its
per-call clock; librdkafka copies the key and value inside its call. Runtime
scheduling and the callback boundary also differ. Phase throughput includes
record generation and delivery handling. Percentiles are closed-loop call-to-completion samples and omit
scheduled arrivals, so they cannot establish open-loop tail latency or
saturation throughput.

A pinned Apache Kafka Java client uses direct partition assignment to read
every record between independently queried fences. The Python verifier checks
each acknowledged offset, key, value and header count. The fixture also
samples each driver's established broker sockets every 50 ms; sampling cannot
exclude short-lived extra connections. Queue limits and retry, socket and
timeout behavior are not identical, despite matching nominal settings. No failure or recovery performance is
measured.

An external watchdog stops a stuck attempt. Ambiguous deliveries are never
retried; failed attempts and partial evidence are retained. No repository
consumer implementation or consumer group is exercised by the verifier.

If socket sampling encounters an OS error, it records the samples already taken,
marks them incomplete and exits with an error. It does not retry or skip the
denied descriptor. A successful producer still receives independent readback;
the arm and comparison remain failed when socket evidence is incomplete or
unmatched. The socket sample is discrete and never proves that an unsampled
connection did not exist.
