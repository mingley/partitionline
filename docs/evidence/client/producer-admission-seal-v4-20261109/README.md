# Producer admission across orderly close

9 November 2026

This is an isolated building block on top of the [bounded ownership attempt](../producer-bounded-ownership-attempt-20261109/README.md). It is not applied to main and does not qualify producer delivery, Kafka interoperability or performance.

The earlier bounded candidate sealed admission before flushing. Its [mock control](../producer-bounded-ownership-attempt-20261109/README.md) appended a target, dropped its delayed response, and observed both the admitted send and orderly close time out without a retry. Source review found that the retry re-entered an admission method that rejected all slots after the seal; that review alone did not prove the cause.

The new candidate distinguishes an already-admitted slot from a prepared slot. An orderly seal permits the former to re-enter admission while rejecting the latter; a fatal closed state still rejects both. It does not add an authoritative batch journal, per-partition write fence, identity-transition fence or sequence-rollover repair.

## Checks

[Run 42695579497](https://github.com/mingley/partitionline/actions/runs/42695579497) used hosted x86_64 Linux and stable Rust. In both default and all-feature profiles:

- A seeded unit control permitted an admitted slot after sealing, rejected a prepared slot, and rejected an admitted slot after fatal closure or completion.
- The prior public close/retry mock control passed. The mock appended the target once and dropped its delayed response; the retry used the same recorded sequence, the receipt returned offset 1, close succeeded, the mock log retained one target and buffered bytes returned to zero.
- Four selected bounded-mode controls and the held-callback close control passed.

The selected default legacy buffer-ownership suite passed 12 tests, the all-feature close-error suite passed six, and all-target/all-feature Clippy passed. `cargo fmt --all -- --check` failed, so the workflow failed overall. No full client suite, independent Kafka broker test or performance comparison was run. The seeded state and mock schedule are not proof that all admission, retry or shutdown histories are safe.

## Sources and remaining work

The candidate is [commit e718d134](https://github.com/mingley/partitionline/commit/e718d134f7fa16f1027a8a9a0fc6b027bd764b4c), based on bounded candidate `f71e1a55c87476a5a4f1e72e66e1ce1bd27c6708`. The [qualification workflow commit](https://github.com/mingley/partitionline/commit/f20ef18cd4befb38ab58413764d9c0ad7e722f3f) pins producer source blob `f2a0384c0da06860f24d26c78745c9100757e513`, owner source blob `268c05d44c782647977ad82dafa7745c66f34786`, and close/retry test blob `472c4b4e67c4fa3e988563003b27a15b57aad798`.

The [owner source snapshot](owned.rs.txt) and [workflow snapshot](workflow.yml.txt) retain this building block for review. The earlier evidence bundle contains the base patch and [close/retry fixture](../producer-bounded-ownership-attempt-20261109/fixtures/producer_owner_close_retry.rs.txt).

A worker may still offer pending writes after a fatal close, and identity publication is not serialized with write offers or their outcomes. A partial or dropped write remains ambiguous. Whole-batch retry, partition ordering, sequence rollover, callback and shutdown behavior therefore remain open. Do not merge this candidate as a production safety fix.
