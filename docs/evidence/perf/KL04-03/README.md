# Apache Java peer evidence

The pinned Apache4.3.1 peer uses JDK21.0.12.1 and asynchronous send callbacks.
Two separately verified Maven caches produce identical class archives. Ten
actual offline SDK/config/vector tests and20 reporter tests pass. The build
freezes source inputs before checking toolchains, dependencies and compilation.

The final isolated Kafka4.3.1 run covers six acknowledged roundtrips and all
five codecs. It verifies768 timed records and excludes96 warmup records. Each
record's full key, value, partition and uniqueness is checked. Separate CLI
offset checks and stored segment codec/CRC/count audits agree. All successful
and failed result documents pass full Draft7 structural validation. The broker
rejection retains16 failures, with zero acknowledgments and16 unknown outcomes;
acks0 also records zero acknowledgments. Reusing a result path fails before
sending and preserves every prior artifact.

The parent waits for every JVM and the broker. All Kafka client threads close;
both broker listeners close and can be rebound. Source and class archive hashes
match before and after execution. The earlier missing-error-field failure and
all compiler/cache/checker failures remain in validation. Dependency jars are
retained once in retained-build; older attempt logs and archives remain.

Frozen workload settings, counts and dispositions are unchanged. The driver
has byte-based batching and seeded ID keys, so18 frozen cells are explicitly
unsupported by this driver. This is a driver limitation, not an Apache SDK
capability statement. Receipt consumption is not a timed fetch benchmark.
No controlled performance campaign or production qualification is claimed.
Suite HOLD remains active. summary.json lists the exact scope and limits.
