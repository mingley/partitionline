# Exploratory Apache Java client against the RF1 listener

This run used the retained `operator.elf` from an uncommitted, single-node RF1
candidate in `/workspace/work/partitionline-rf1`, based on commit
`64da3d2db9ffa80fb680806dc377e52f4652b38e`. The build receipt lists hashes
for broker source files, manifests, test and fixture inputs; the driver checked
those hashes and the executable hash before and after the run. The actual
binary and source files named in that receipt are included. This is not a
qualification of the main branch or a production cluster.

The Apache Kafka 4.1.2 Java SDK was extracted from the digest-pinned Apache
Kafka image recorded in the included SDK extraction receipt. Its jar, logging
API jar, peer source and compiled class are included. Java Admin created a
two-partition topic on the plaintext listener. A Java Producer configured with
`acks=1`, `enable.idempotence=false`, and no compression sent six records. A
manually assigned Java Consumer checked their exact partition, offset, key,
value and headers, including a null key, null value, UTF-8 text, duplicate
header names, null header value, and empty header value. The listener then
shut down on SIGTERM and restarted from the same data directory. The Java
Consumer read all six records, the Producer wrote two more, and the Consumer
read and checked all eight. Both shutdowns reported six accepted and six
joined connections and zero worker failures.

The first attempt failed before server startup because the `javac` executable
was unavailable; its error and result are retained. The second attempt used
`java --add-modules jdk.compiler com.sun.tools.javac.Main` and passed. Its Java
stderr contains the SDK's missing SLF4J binder warning; the client used the
no-operation logging backend. Raw Java/server outputs, commands, state-file
hashes and final journal bytes are retained.

This covers one small, ordinary, uncompressed, graceful-restart history. It
does not qualify idempotence, transactions, groups, security, replication,
crash recovery, sustained load, performance, or production readiness. It must
be repeated against a source-bound, committed integrated candidate.

Verify stored and reconstructed files with `python3
benchmarks/runtime/verify-evidence-archive.py <this directory>`.
