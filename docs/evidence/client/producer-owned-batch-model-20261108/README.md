# Producer owned-batch model

This is an isolated design model, not a producer fix. It does not qualify
production readiness or performance. The reviewed main source is
[`342e7b90ee63d6824be2652b65d71be483ea6d16`](https://github.com/mingley/partitionline/commit/342e7b90ee63d6824be2652b65d71be483ea6d16),
`src/producer.rs` blob `dcd012b3f86a8b9d4254f88d7f48946ce8f9e922`,
SHA-256 `60a3fa0985358198e81757e6080af8b58ab52cb00edc1f34e2c97ed033e90b78`.
The landed timestamp correction does not fix the retained
[public retry-split witness](../producer-retry-flush-split-20261108/README.md),
[batch-ownership controls](../producer-batch-ownership-controls-20261108/README.md)
or [sequence rollover witness](../producer-sequence-rollover-20261108/README.md).

## Proposed ownership

The model serializes each transition. A pre-admission reservation acquires a
record credit and logical payload bytes together; close can revoke it before
commit. Committed records live in a root independent of worker descriptors.
A single selector seals each partition batch with a nonreused key, member
tickets, identity generation, modulo-`2^31` sequence range and frozen
timestamps. A revoked worker must stop before its batch is reclaimed. Normal
dispatch, retry, flush and close would need to use the same owner and selector.

An offered write is treated as possibly written. The model never clears an
unknown-write fence by itself and does not retry that batch or switch identity
without an external proof it does not supply. Individual timeout outcomes do
not detach members of an unknown batch. This sacrifices liveness: retained
records can exhaust shared credits and block other partitions indefinitely.

Flush captures an admission cutoff and retains errors for overlapping waiters
even when one cancels. Completion intervals compress around unfinished tickets.
Close seals admission and captures a cutoff atomically; it waits on unresolved
writes. A modeled timeout prevents new *claims*, marks unfinished work closed,
and retains uncertain batches. It does not prove that an offer granted earlier
cannot begin or finish actual socket I/O after the observer returns. The model
uses one saved close result, so a later observer replays an earlier timeout
even if cleanup subsequently drains; independent rejoining is not modeled.

Constructor limits use zero as unlimited. A bounded owner mode needs finite
record, logical-byte, partition-state, cohort and waiter budgets. This is not a
total-memory bound: header objects, topic storage, allocation capacity,
assembled request envelopes, compression scratch, workers, sockets and
callback jobs require accounting too. Current `send_all` eagerly collects its
input and output outside per-record admission. A finite owner also cannot bound
arbitrary caller or interceptor allocations. Sequence history cannot be
discarded merely because its numeric value is zero; even a proven-unwritten
rollback can conservatively retain a partition credit in this model.

`ProducerConfig` and `Error` are exhaustive public types, so adding fields
or variants can break downstream source. A separate limits constructor and
outcome-aware receipt surface are possible additive routes. They must retain
the documented zero-as-unbounded `buffer_memory` and `max_request_size`
behavior of existing APIs. Neither is implemented here.

## Observable behavior

The delivery-timeout builder describes time from queue until acknowledgement
or `Timeout`; the [guide](../../../guide.md#produce-cancellation-and-shutdown)
says a blocking interceptor can delay cleanup and close. Interceptor docs
promise insertion order within the interceptor chain, not an explicit order
between records. Current success and failure paths nevertheless invoke the
interceptor before sending the corresponding receipt.

The model conservatively orders callbacks per partition and publishes a
caller-held receipt cell after callback completion; the owner keeps a weak
reference. A control shows a later record's timeout can be recorded internally
while its receipt waits behind an earlier callback. This is not a measured
public-client history or a strict wall-clock guarantee. Publishing receipts
first or failing earlier members early would be observable policy changes.
The weak-cell model does not implement Rust wakers, user cycles, callback
panic handling or callback-worker recovery.

The modeled close seals before waiting and reports an included non-timeout
error. Current `close` and `close_timeout` first flush, then seal, and
convert non-timeout flush errors to success. Adopting the model changes that
behavior and must be documented and tested.

## Review and controls

Earlier green runs did not establish model correctness:

| Model and run | Result | Subsequent source-review counterexamples |
| --- | --- | --- |
| [`1ba03e0`](https://github.com/mingley/partitionline/commit/1ba03e085a17e8970565ab38ec8ba01066731720), [42491616468](https://github.com/mingley/partitionline/actions/runs/42491616468) | 13 controls passed; 4 mutants rejected | A completed lease could revive an old batch and clear a newer partition fence. Interleaved/cross-partition cohorts could stall; cohort and partition metadata accumulated. Close or callbacks could exhaust shared tokens; canceling close lost its cutoff. Review also found unfenced canceled-cohort admission, timestamp overwrite on regrouping, stale-event exceptions, missing deadline-vs-ack ordering, a later timeout blocked by callback order, ambiguous soft/hard size handling, and a pre-commit credit leak for invalid model inputs. |
| [`fd2bc30`](https://github.com/mingley/partitionline/commit/fd2bc308295a408ecd569b9b923ff9a22cdeab7b), [42492475442](https://github.com/mingley/partitionline/actions/runs/42492475442) | 28 controls passed; 12 mutants rejected | Non-sticky selection could exceed aggregate hard limits; failing one queued sticky member stranded its cohort; an arbitrary queued result could claim success without an acknowledgement. A timed-out close could retain its internal cutoff after cleanup, an empty pre-admission cohort needed revocable ownership, and live-record credits alone did not bound old partition metadata. |
| [`345ea45`](https://github.com/mingley/partitionline/commit/345ea45e89760b63e0bd05f99f1bdd7ad342dedf), [42494024195](https://github.com/mingley/partitionline/actions/runs/42494024195) | 35 controls passed; 15 mutants rejected | Callback completion followed by a later worker stop could strand close and cycle an expired unwritten batch until another expiry. Empty close needed progress without an observer. The model allowed a new claim after timed close; waiter accounting and one globally cached close timeout also needed explicit policy. |
| [`b0d069f`](https://github.com/mingley/partitionline/commit/b0d069fa615fe6a648dd64d102608bb43d5d68de), [42494649300](https://github.com/mingley/partitionline/actions/runs/42494649300) | 44 controls passed; 15 mutants rejected | New model claims stop after timeout, but a previously granted offer may still start real I/O before its worker stops. The model cannot establish OS or broker quiescence. |

The final comment-only clarification is
[`d1f817f853be30853a76a0459abd1a8011ff00a8`](https://github.com/mingley/partitionline/commit/d1f817f853be30853a76a0459abd1a8011ff00a8).
[Run 42494824528](https://github.com/mingley/partitionline/actions/runs/42494824528)
passed 44 controls and rejected 15 mutants on Python 3.12.3. The
[model](https://github.com/mingley/partitionline/blob/d1f817f853be30853a76a0459abd1a8011ff00a8/tests/models/producer_owned_batch_model.py)
is blob `22fb14c8bd3f7f501e747b6d1f190b191d809850`, SHA-256
`9f1e742734ea2d088d0bd41fcc9ff8b3b1912df2b81d20467f2cab0a3ae20caa`;
the [mutant runner](https://github.com/mingley/partitionline/blob/d1f817f853be30853a76a0459abd1a8011ff00a8/scripts/check_owned_batch_model_mutants.py)
is blob `0e745842d60dc2cdb7167e87814cdb8aaea204b8`, SHA-256
`b5280cea01b2af4f4a7c7378e63606a1bdac8edcc4e7f701a8c759112ee49254`.
[Artifact 13623266554](https://github.com/mingley/partitionline/actions/runs/42494824528/artifacts/13623266554)
has SHA-256 `458bac254eb43ed91d40a859c4b07df053061dd3e4ec6b5fddca4f023020e382`.
The reviewer inspected the same model behavior at `b0d069f`; the final
commit changes a comment only. These are model histories and source-derived
counterexamples, not proof that the public producer reached those schedules.

To reproduce at the model commit from the repository root:

```sh
python3 -m py_compile tests/models/producer_owned_batch_model.py scripts/check_owned_batch_model_mutants.py
python3 tests/models/producer_owned_batch_model.py
python3 scripts/check_owned_batch_model_mutants.py
```

The model and runner live in the linked model commit; they are not part of
the production producer.

## Smallest safe integration route

Keep an isolated Rust branch until admission, batch selection, retries, terminal
publication, flush and close all share a rooted owner and generation-checked
sequence/write journal. Add finite limits through an additive API without
silently changing legacy zero settings. Start with conservative one-batch
partition ordering and fail closed on uncertain writes; a production retry or
identity transition needs a verified Kafka fencing/settlement rule.

The model's full scans, sorting and atomic Python calls are not a hot-path
algorithm. The Rust design needs rooted resumable cursors at every yield and
bounded deadline, completion, partition and callback indexes. Transaction
admission/EndTxn fences, topic-incarnation mapping, multi-partition request
sizing, partial TLS writes, shutdown, callback panic/blocking and broker
settlement remain unmodeled. The retained public split and worker-cancellation
histories, both feature profiles, real-broker compatibility and paired
performance tests must pass before applying an integrated repair.
