# KL07-08 diagnostics

`ConsumerMetrics.reconnects` counts failed leader connects. `ConsumerGroup::outcomes` counts rejoins after the initial join and OffsetCommit successes and failures. `diagnose_produce`, `diagnose_fetch`, and `diagnose_group` return static sentences. They do not copy topic names, group ids, keys, values, or credentials.

Queue bytes stay `ProducerMetrics.bytes_buffered`. In-flight produce retries stay `Producer::retries_in_flight`. Completed queue-to-ack time stays `ack_latency`. There is no separate in-flight age counter. Lag stays `current_lag`, one ListOffsets call per partition the caller names.

Five interleaved `nb-fetch-bulk` runs compared a release runtime with the `tracing` feature off and on. No tracing subscriber was installed. Allocations were 135451 on every run. CPU-ns/record paired mean was -3.0% with 95% CI [-14.9%, +8.8%]. That interval is wider than the 2% budget, so this host does not prove the budget. Tracing stays off by default. The new counters are not on the per-record path.
