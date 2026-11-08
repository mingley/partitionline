# HashSet order during large send_all scans

This note concerns a proposed large-homogeneous-input optimization. It does not describe a failure in the current shortcut, and it does not show a Kafka-client or connect-queue failure.

The existing nudge scan holds the cluster lock for each chunk of up to 8,192 records, inserts each observed `(leader, slot)` into a `HashSet`, yields after every full chunk, and sends the set's entries to the connect queue after scanning. A proposal would insert one representative per homogeneous chunk while keeping the chunk yields and union of observed slots.

If metadata changes between yields, different chunks can observe different leaders. In Rust 1.99.1, duplicate `HashSet` inserts can affect capacity: the standard library uses hashbrown 0.17.1, whose [insert lookup reserves before checking for a duplicate](https://github.com/rust-lang/hashbrown/blob/v0.17.1/src/raw.rs#L1120). Capacity can affect iteration order. Equal unions therefore do not by themselves establish the same nudge order.

## Standalone witness

[Run 42475049099](https://github.com/mingley/partitionline/actions/runs/42475049099) compiled [witness.rs](witness.rs) with stable Rust 1.99.1, LLVM 23.1.1, on x86_64. For three successive leader keys, it compares 8,192 insert attempts per key with one insert attempt per key, using the same hasher in each pair. Both sets contain the same keys.

| Hasher | Leaders | Repeated-insert capacity and order | One-per-chunk capacity and order |
| --- | --- | --- | --- |
| Fixed fixture hasher | 1, 2, 4 | 7; 1, 2, 4 | 3; 4, 1, 2 |
| Cloned `RandomState` | 1, 2, 3 | 7; 3, 1, 2 | 3; 1, 2, 3 |

A modeled queue with one free slot would take the first key in each order. This is an in-memory model, not an actual Tokio channel. The fixed hasher is a valid deterministic fixture; the `RandomState` example is one sampled state, cloned so both arms share its keys. The run printed its selected hash values. A one-key control compared 8,191 inserts with one and found the same capacity and order. The current shortcut is limited to fewer than 8,192 homogeneous records and one locked chunk, so this three-leader witness does not establish a defect in it.

The source blob was `e93cdbc72075d4ea69aa69d6348887d4dbb190a8`, SHA-256 `9d816603470d2d3a984328606a0614f9cec08324dce011e88c6b1cb4f4f5735c`. The binary SHA-256 was `7d3907236f2b3b98653bab0a1a86b8eb39984d4c983c9bd433577221f3743d1a`; the binary was not uploaded. Logs and compiler details are in [artifact 13615470363](https://github.com/mingley/partitionline/actions/runs/42475049099/artifacts/13615470363).

## Design limits and tests

A same-route representative gives the same set for the same locked metadata snapshots when the freshness clock is monotonic: a fresh first record inserts that chunk's route, while a stale first record stays stale within the locked chunk. That set-level result does not preserve HashSet capacity, iteration order or all real schedules. A shorter scan acquires and releases the cluster lock earlier, allowing metadata updates, cancellation, queue draining, delivery deadlines and per-record `max_block` waits to interleave differently. `Instant` normally advances monotonically; clock anomalies remain a limit on the set argument.

A 16,386-record suffix could observe three leaders in two full chunks and a two-record tail. If only one of the connect queue's 16 places is free at emission, a changed iteration order could change which nudge is accepted. This is a source-derived scenario, not a reproduced public failure. A controlled test would need real mock broker leaders, gates around the actual scan yields and queue receiver, a nearly full queue, and paired seeded set construction. It should record per-chunk metadata, every attempted nudge, queue acceptance, deliveries and independent readback. Such hooks would still show one forced schedule.

Route-shape tracking during collection must continue to consume arbitrary iterators once, preserve the existing 8,192-item collection yields and defer validation and interceptor effects as before. It must use actual collected lengths rather than trusting size hints, keep sticky partitioning's early nudge return, and recalculate the pending suffix after metadata splits; the ready list is nudged separately. Large or invalid topic names must not make the additional shape check unbounded between yields.

Keeping a yield for every original full suffix chunk also retains quadratic chunk work for large batches. With no split or early error, for `N = q * 8192 + r`, the suffix scans yield `8192 * q * (q - 1) / 2 + q * (r + 1)` times. At one million records this is 60,535,546 yields and 61,535,424 representative checks, excluding collection yields. Reducing that work further would change the existing scheduling behavior and needs a separately stated contract and qualification.
