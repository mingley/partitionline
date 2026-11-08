# Producer create-time observations

A controlled public producer history against the repository mock observed that
an implicit record timestamp can change on retry, and that a successful receipt
can report a different timestamp from the original record stored by the mock.
This is not a real Kafka result. The producer source was unchanged for the
history; the mock was instrumented to record decoded Produce timestamps.

## Setup and results

The baseline is [`7dfbe4735d1c4268a1b8a7185bb34f1121f0db13`](https://github.com/mingley/partitionline/commit/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13).
Its `src/producer.rs` blob is `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`
(SHA-256 `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`).
The [test-only fixture](https://github.com/mingley/partitionline/commit/253b7b0ead74dd967d17205383d3a5a7b8c9b9ac)
adds mock observation of each decoded batch's producer ID, epoch, sequence,
batch timestamp and first record timestamp. It does not change mock append or
response decisions, although the extra observation can affect timing.

Each profile used one configured connection slot, one partition, one
idempotent producer and one record per batch. The mock delayed responses by
200 ms; the producer's configured retry backoff was 500 ms. For the retry
case, the mock appended the record and then dropped the first response. It
accepted the later same-identity, same-sequence, same-count request as a
duplicate, returned the original offset, and kept the original log record.
The test compared decoded request timestamps, receipt metadata and mock log.

In [run 42483933332](https://github.com/mingley/partitionline/actions/runs/42483933332),
both default and all-feature profiles passed the observational assertions:

| Profile | Ordinary first attempt: request / receipt / log | Lost response: original / retry / receipt / log |
| --- | --- | --- |
| Default | `1794143710325 / 1794143710526 / 1794143710325` | `1794143710526 / 1794143711230 / 1794143711432 / 1794143710526` |
| All features | `1794143733521 / 1794143733723 / 1794143733521` | `1794143733723 / 1794143734425 / 1794143734628 / 1794143733723` |

Values are milliseconds. Each profile also sent an explicit timestamp of
`1234567` through a dropped-response retry; its original request, retry,
receipt and mock log all had that value. The ordinary delayed-ack case shows
that the receipt difference does not require a retry. The retry case shows a
separate change to the timestamp encoded on the second request.

The mock's successful Produce response in this fixture sets
`log_append_time_ms` to `RecordBatch::NO_TIMESTAMP` (`-1`). On that
response, the client samples a fresh clock in
[acknowledgement handling](https://github.com/mingley/partitionline/blob/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13/src/producer.rs#L8874),
and [`record_metadata`](https://github.com/mingley/partitionline/blob/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13/src/producer.rs#L11683)
uses it when the record has no explicit timestamp. The reported receipt is
therefore client-side reconstruction in this fixture. If a response supplies
a nonnegative LogAppendTime, that function instead uses the broker-supplied
value. This history did not exercise a real broker or establish its
timestamp-mode behavior.

The qualification commit is
[`d5b9fa1c5a2f3181461e45969b88ae402f375d1c`](https://github.com/mingley/partitionline/commit/d5b9fa1c5a2f3181461e45969b88ae402f375d1c).
The mock fixture SHA-256 is
`71498d85af00f7fd579e809daa2979a7b596e5553cc08ad40dd417e1bebe230c`,
the history test SHA-256 is
`8575683691d6117627469cb8444173080bf446809b6351a22a27780f004e1033`,
and the lockfile SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
Both jobs used stable Rust 1.99.1 and LLVM 23.1.1 on hosted x86-64. Formatting
was diagnostic only and reported differences in the test-only fixture and test;
strict formatting did not pass.

The preceding [run 42483537737](https://github.com/mingley/partitionline/actions/runs/42483537737)
passed the lost-response and explicit-timestamp observations in both profiles
but did not include the ordinary delayed-ack control. Its diagnostic
formatting check also reported differences.

A repair must preserve explicit timestamps and honor a nonnegative
broker-provided LogAppendTime. It must also account for cancellation during
the first encode and retain the original effective create time across retry.
The separate [batch ownership evidence](../producer-retry-flush-split-20261108/README.md)
records unresolved cancellation and retry boundaries. No integrated ownership
repair is established by these observations.
