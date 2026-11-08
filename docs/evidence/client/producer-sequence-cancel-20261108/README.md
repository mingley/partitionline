# Sequence reservation during producer cancellation

An isolated unit fixture shows what happens when an async owner of a producer
batch is dropped after the real `assign_sequences` helper reserves a Kafka
sequence range but before the helper finishes stamping that range.

The fixture creates 8,193 `Pending` records with individual receipt channels,
manually polls an async owner holding the batch, and stops when the shared
sequence map advances. At that point `stamp_sequences` has yielded after
stamping its first 8,192 entries. The test then drops the owning future. All
8,193 receipt channels close, the map remains at 8,193, and the next call to
`sequence_in` reserves a range starting at 8,193. A normal-completion control
assigns 0 through 8,192 and confirms that all receipts remain open while the
batch is retained.

The [source][sequence-source] advances the shared partition counter before
awaiting `stamp_sequences`. The producer's `fire` path calls this helper
while it owns grouped records, before the socket write. A cancellation there
therefore needs ownership and sequence-range recovery, not only a completion
counter. This last connection to `fire` is source analysis; the fixture does
not execute that path.

[Run 42459449898][run] passed both controls with default features and with all
features: 2 passed, 0 failed and 2,244 filtered out in each run. The job used
x86_64 Linux, stable Rust 1.99.1 (LLVM 23.1.1), Tokio 1.53.3, and the locked
dependency set. It checked out the test-only branch at
`27322903bc4ad05c2560e4a6d64b186f42bb4e79`, based on main commit
`1036a393eb85f23032b935bf86862b7e98e36e14`.

- Producer Git blob: `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`; SHA-256
  `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- [Test source][test-source] SHA-256:
  `6ecfed8a57c1180658ab0f0be7937de32415ab9cbb2681e65da45b304d53090b`.
- Producer file after the workflow appended the test-module declaration,
  SHA-256: `d2feb7014ff6a8edc1ee1f2e541d4b7c12e9624cfd2dc835886baeccb72058b3`.
- `Cargo.lock` SHA-256:
  `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.

The fixture uses the private helper and an artificial owner, not the public
producer, a worker, a socket, or a Kafka broker. It does not reserve or check
producer byte accounting, run interceptors, measure performance, or demonstrate
an observed broker-side sequence error. A production repair still needs a
rooted batch journal, a safe decision for cancellation before and after a
possibly partial write, and public producer histories verifying receipts,
accounting, idempotent ordering, retries, shutdown, and transaction behavior.

[run]: https://github.com/mingley/partitionline/actions/runs/42459449898
[sequence-source]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L10402
[test-source]: https://github.com/mingley/partitionline/blob/27322903bc4ad05c2560e4a6d64b186f42bb4e79/src/producer/sequence_cancel_witness.rs
