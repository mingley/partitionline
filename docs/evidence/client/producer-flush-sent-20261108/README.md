# Flush error sent before observation

[Run 42467199386][run] exercises a producer error that is successfully sent
to a flush response channel but never observed by that flush's caller. This is
a gated observation of existing behavior, not a producer fix or a complete
flush-error contract.

The test uses a public `Producer` with one mock broker and one partition. A
warmup record succeeds. The public test hook injects a pre-send encoding
failure for the next record; its `send` receipt reports `Error::Protocol`,
the error callback runs once, the target does not appear in the mock log, and
`bytes_buffered` is zero.

The first `flush_timeout` is polled once to Pending. A cfg-only marker in the
worker reports that `tx.send` succeeded with the recorded error while that
oneshot receiver was still live. The test does not poll the first flush again
before selecting one of two histories:

- In the control, polling the first flush returns the protocol error, and a
  following flush returns Ok.
- In the other history, a second overlapping flush returns Ok while the first
  result remains unpolled. Dropping the first flush discards its result; a
  subsequent flush also returns Ok. The error was therefore published to a
  channel but not observed by that caller or either later flush in this
  history.

Both histories ran once in each of the default and all-feature profiles on
hosted x86_64 with stable Rust 1.99.1 and LLVM 23.1.1. The jobs passed because
their assertions observed these outcomes; that does not qualify the producer.
The marker and explicit pre-send fault injection affect the fixture's timing
and failure source. The mock uses this repository's codec and is not a real
Kafka peer. This does not establish how every concurrent error should be
reported, nor does it prove a general fix for overlapping or canceled flushes.

The [witness commit][source] is based on main
`60caa746c3654c89d01557e35e1100cdc2a84b74`. The normal producer source Git
blob is `1e7733c136e54b532cdc8bddabd1e0c828e50ed2` (SHA-256
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`).
The instrumented producer blob is
`2bef0d05d181356259f8a5bec251ab819fd474b6` (SHA-256
`82c3bc8449f2fe1941ff0df87d2c6295841b7b9f85ff2e2a7ce3fda4751a0f1d`).
The witness test blob is `5d9474a6c31c0cfed129c509e0a1084723cae0f1`
(SHA-256 `6297926316a997a229f83553a72c26490238d46dfe99ebfa9553c26b85ab53fe`);
the lockfile SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.

An implementation needs an explicit acknowledgment point after the caller
actually obtains the error, with bounded retention and correct handling of
overlapping waiters and cancellation. A successful channel send alone cannot
serve as that acknowledgment.

[run]: https://github.com/mingley/partitionline/actions/runs/42467199386
[source]: https://github.com/mingley/partitionline/commit/0e75c55321fad2781fb79cd8799a66323464d597
