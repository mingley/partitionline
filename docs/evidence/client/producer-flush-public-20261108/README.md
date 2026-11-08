# Public producer flush histories

These local mock-broker controls reproduce limitations already described by
`Producer::flush`. They are not fixes, Kafka broker qualification, or
performance measurements. Both use source commit
[`6298f6b`](https://github.com/mingley/partitionline/commit/6298f6bfbed2ab98189d291ebebd2190a2ee75b6)
and stable Rust 1.99.1 on an x86_64 GitHub runner.

| History | Observation |
| --- | --- |
| Accepted record expires | The target produces one `Timeout` callback and leaves no buffered bytes, but `flush_timeout` returns `Ok`. The `send` variant also returns its own `Timeout`. |
| Retired worker before retry handoff | With the old worker paused at the retry handoff, flush returns `Ok` while the target receipt is pending. After release, the retry is acknowledged. |

## Accepted record expires

[Run 42457483274](https://github.com/mingley/partitionline/actions/runs/42457483274)
passed two controls with default features and the same two with all features.
A successful warmup creates the worker. Each control then admits a target
through public `try_send` or `send`, holds the current-thread test runtime
for 2.25 seconds, and calls flush with a separate longer deadline. The producer
uses a two-second delivery timeout and one-second request timeout.

After flush, each control records two accepted records, one acknowledgement,
one Timeout callback, one produce error, zero buffered bytes, and only the
warmup in the mock log. Flush returns `Ok`; in the `send` control the target's
own future returns `Err(Timeout)`. The hold is deliberate test scheduling;
this result does not establish behavior for every timeout or broker.

The [witness source](https://github.com/mingley/partitionline/blob/7cefc580817f51a76e6e38beea04a003101429d2/tests/public_flush_history.rs)
is isolated from main. Its SHA-256 is
`e9ecbc1bc70cac14ae54dff5a2f088277fd5ef547c04f5c67f443e077a17b0b2`.
The retained artifact is `13607384581`, SHA-256
`20480259b7b57b461123f4daca10dde2511756fdc7bf393738badd17624667f0`.

## Retired worker before retry handoff

[Run 42458476249](https://github.com/mingley/partitionline/actions/runs/42458476249)
passed one control with default features and the same control with all
features. The mock appends an idempotent target and closes the Produce socket
without replying. The real retriable read path retires the worker. A
qualification-only timing gate then pauses it after the existing cooperative
retry split and before the retry counter increments.

While paused, the test observes no published worker, zero counted retries,
a pending target receipt and the target's bytes still reserved. A single poll
of public `flush_timeout` returns `Ok`. The mock's decoded log contains the
exact warmup and target values at offsets 0 and 1. After the gate is released,
the retry keeps the same producer identity and sequence, the target receipt
returns offset 1, its acknowledgement callback occurs once, and the mock log
still contains exactly those two records.

The gate adds a scheduling pause and an existing hidden worker-inspection
helper is used for observation; the run is not an uninstrumented reproduction.
The mock is a separate server-side observer using this repository's codec, not
an independent Kafka implementation. Its delayed ApiVersions reply keeps a
replacement worker from obscuring the observed window. Delivery timeout is
30 seconds; the flush timeout is eight seconds.

The isolated [witness source](https://github.com/mingley/partitionline/blob/8371f09924118dd8c3a8098d4b0f7ee8b6c66e6c/tests/public_flush_retirement.rs)
has SHA-256
`58a92133da4f4a5ae30c6cd6c3676a15cc74624e629450841f12e2bf255cfcdf`.
The instrumented producer blob is
`8e08f33e18d7aca953a09c2df988e170f6e17817`; the cfg-gated timing patch
SHA-256 is `3cd5c7a749d590c5f472a1c6509b056dec7d9b5944eeaab42618ceeca82c9c6f`.
The read-only mock-log accessor patch SHA-256 is
`8161c84fae487b7b6f366537424ad48d07cf1a8778505e3cd2c094a45173c58b`.
The retained artifact is `13609150113`, SHA-256
`095b3780c687ced00392308c99eb0f5e3c90274177151f886ad061dd9f52a336`.

Both runs pinned the baseline producer blob
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2` and `Cargo.lock` SHA-256
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
Artifacts have a 30-day retention setting.

Earlier setup attempts are retained. [42457421283](https://github.com/mingley/partitionline/actions/runs/42457421283),
[42457956519](https://github.com/mingley/partitionline/actions/runs/42457956519),
and [42458236663](https://github.com/mingley/partitionline/actions/runs/42458236663)
stopped at formatting before any tests. The intermediate retirement runs
[42458028731](https://github.com/mingley/partitionline/actions/runs/42458028731)
and [42458341206](https://github.com/mingley/partitionline/actions/runs/42458341206)
passed earlier versions of the control; the final run adds callback identity.
