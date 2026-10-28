# Kafka producer sequence boundary oracle

The pinned Kafka Java 4.3.1 client jar's
`org.apache.kafka.common.record.internal.DefaultRecordBatch` arithmetic wraps
nonnegative producer sequences modulo 2^31. `SequenceOracle2.java` and
`SequenceOracle3.java` call its `incrementSequence` and `decrementSequence`
methods directly and compare five boundary cases each with independent `long`
modulo calculations. Both successful invocations exited 0; the exact output,
source/jar/launcher hashes before each run and matching post-run hash checks are
in `run-02.log`, `run-03.log`, `input-02-before.sha256`,
`input-03-before.sha256`, and `input-*-after-check.log`.

The client jar SHA-256 is
`52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36`.
The OpenJDK version is recorded in `java-version.log`; the Java launcher was
hashed before and after each run. The JDK `lib/modules` SHA-256 recorded after
the runs was `f82ec56fbf7c7b69da09a04d4fb103c74c44e6443c8a1b2744e59c5539e7f781`;
it was not captured before them. These inputs do not fully pin the OS or all
runtime dependencies.

Commands (the jar path is in the before-hash files):

```sh
java --class-path /workspace/work/goal-qualification-20261028/narrow-profile/java/kafka-clients-4.3.1.jar /workspace/work/producer-sequence-java/SequenceOracle2.java
java --class-path /workspace/work/goal-qualification-20261028/narrow-profile/java/kafka-clients-4.3.1.jar /workspace/work/producer-sequence-java/SequenceOracle3.java
```

The first attempted source, `SequenceOracle.java`, imported the pre-4.3 class
path and failed to compile; its output is retained as `run.log`. Its process
exit status was not separately recorded. These checks establish the Java helper
results for the listed cases only. They do not establish producer wire-level
interop, concurrent sequence reservation, cancellation safety, or broker
behavior.
