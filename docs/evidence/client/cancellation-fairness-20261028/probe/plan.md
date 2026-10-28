# Destructor diagnostic plan

Do not run timed samples while client or broker timing suites are active. This
standalone std-only Rust program models owned String/Vec destruction; it does
not invoke Tokio, the Kafka client, or claim to reproduce an end-to-end failure.
It constructs 2,000,000 positions, transfers 10% to partial output, and times
remaining `Vec::IntoIter` destruction and output destruction separately. It
also times item-at-a-time cleanup in chunks (1,024 and 8,192), each final
buffer deallocation, total cleanup, and an externally held owner whose full
final destructor time is reported separately. Thread yield is not a Tokio yield;
wall times include scheduler/allocator noise. Chunk sorting and printing happen
after the timed sections. The borrowed-reference check is not a future cancellation, and it does not
qualify final owner drop.

Build separately at opt-level 0 (matching unoptimized tests) and opt-level 3
(release-like), with current `rustc +stable`, source and binary SHA-256,
compiler/command/environment, CPU quota, timestamps, and all raw JSON lines.
Before actual samples, run `RECORDS=1`, `RECORDS=10` and `RECORDS=1000` as
controls at each optimization level; verify counts, empty-iterator behavior,
output syntax, and exit status. Then run at least five alternating-order rounds
for each chunk size. Each round allocates fresh values outside timed sections.
Retain all failures. System noise and allocator background work can affect
measurements. This measures neither actual async scheduler fairness nor any
verified end-to-end performance improvement.
