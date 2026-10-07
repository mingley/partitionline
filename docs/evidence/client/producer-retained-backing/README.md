# Accepted producer payload ownership (KL02-11)

Admission compacts payload backings and header capacity after reserving bytes. Shared and custom-owned buffers copy their visible bytes; exact-size unique buffers can reuse their allocation. A reservation guard releases cancelled submissions that have not entered the worker queue.

Eight controlled socket regressions cover ordinary/sticky admission, aliases, cancellation, retry, rejection and terminal release. Three private cases check spare capacity, hidden prefixes and pointer reuse. The old source fails the ownership and channel cancellation controls; logs and source are retained.

The native probe produces 45 records across all five codecs to a fresh Kafka 4.3.1 broker. Java and C consumers independently check every field. Custom payload owners release before acknowledgement. Commands, binaries, source hashes, stored batches and joined-process closure receipts are under `native/`.

`summary.json` records the exact checks and limits. Reproduce deterministic tests with `cargo test --test producer_retained_backing`; the native driver records its required peer paths. Use latest stable Rust. The budget covers accepted visible payload bytes. Allocator rounding, record objects, waiting inputs, caller aliases, encoding scratch and socket memory remain additional memory. Copying costs need separate measurements.
