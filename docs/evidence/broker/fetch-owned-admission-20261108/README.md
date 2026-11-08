# Actor-owned data admission

The change moves each broker data-request permit with the request to the actor and back with its reply. Canceling the caller does not free that permit while the queued or claimed actor command still owns the request. Waiting Fetch requests retain the permit between snapshots. Grouped Produce keeps buffers and results ahead of their permits in drop order, including during an unwind.

The change covers Produce, Fetch, ListOffsets and the configured DeleteRecords data path. Retention sweeps and partition compaction still use borrowed permits. This evidence covers the local broker actor; it does not qualify replication, the whole server, or performance against another implementation.

## Admission limits

The read profile checks that the number of admission slots times the configured maximum request plus response lengths fits `max_retained_bytes`. This sizes logical payloads; it is not a bound on all live bytes or process RSS.

A `Vec` can retain more capacity than its length. The real-actor cancellation control used a 4,096-byte request with 8,192 bytes of capacity under a 4,160-byte logical envelope. The public `Handler::exchange_request` also accepts a caller-supplied spare `Vec` without checking its length or capacity against this budget. The built-in transport clears its spare before reuse, but its capacity can still exceed the logical envelope. Actor-cached buffers, parser metadata, allocator overhead, other transport buffers and caller-owned memory are outside the budget. Once the oneshot reply is consumed, the caller or transport can briefly retain the transferred buffers after the actor permit is released.

The earlier [modeled overlap](../fetch-actor-overlap-20261108/README.md) showed 8,192 bytes of retained logical input under a 4,160-byte envelope in a controlled actor schedule. The new baseline control confirms that an actual claimed command can retain its request after its canceled caller releases the old borrowed permit. Neither result establishes the cause of the earlier uninstrumented QueueFull failure. See the [canceled-queue evidence](../fetch-canceled-queue-20261108/README.md).

## Qualification

The combined candidate [7dcef58](https://github.com/mingley/partitionline/commit/7dcef586a8c6205cca10b71222088d83a84689fd) is based on `80a687e`, which includes the independent journal, partition, Produce and segment repair. Its metadata, Fetch, Fetch-test and inventory Git blobs are `279348d6a0ad32914f81fb44e072d95a6436e9a2`, `e8846ef5233c7669ec7e1ad52889f782344eedba`, `fd19e5cafa2cd99dea61279b52a79b9877e4f0ca` and `8a837628025b22d1ff73dddf3cae7250d9bed828`. The registry changes only those three admission-related source pins and preserves the journal repair's other pins.

| Registry source pin | SHA-256 |
| --- | --- |
| Metadata source | `4b58da7de8daf51d12c658c35a9252b91907e18475e49cb04fac3de751cb9a5b` |
| Fetch source | `0a35525b57e7cc6d83bb5ca82c544070126f8f2d1932b36e38525cf12a823e89` |
| Fetch test | `0e5fd830937e85b8be40c6c4ab0a2797c118a3a44f47f681f04ff558dec53cd1` |
| Produce source, preserved | `fa3bff1c088ff391ed34fdb8ec211da5f757b08c807a2c21d0ba1cd648391a56` |
| Partition source, preserved | `e2006a15994329fecdb424f16af7d186cb2c5e769be8103fa32f9f21e37cd48e` |
| Segments source, preserved | `dc437576653833f172aa8363e9ad21adbb3ee39df142e6036c05560b5cd61dc5` |

The revised deadline fixture observes the actual Fetch handler pending, completes a FIFO metadata barrier, and then verifies the sole data permit is held before waiting for the transport deadline and EOF. It uses the same transport for the subsequent Produce and Fetch, sets its handler deadline to the existing 2 s read/write budget, and bounds the outer operation and shutdown. This removes the fixture's incidental 100 ms deadline for a durable Produce; it is still a finite timing assumption.

[Combined focused checks](https://github.com/mingley/partitionline/actions/runs/42450600781) passed formatting, strict all-target/all-feature Clippy, four focused Router/transport tests per profile, and two counterexamples per profile. A forged pre-dispatch observation failed at the permit check; forgetting the canceled permit failed on fresh Produce with a handler error. The [combined baseline and drop-order controls](https://github.com/mingley/partitionline/actions/runs/42450600748) passed in both profiles.

The [combined all-target suites and inventory](https://github.com/mingley/partitionline/actions/runs/42450600785) passed 754 default and 902 all-feature tests, with three ignored in each profile. Both separate doctest gates passed with zero doctests; the API inventory and all 55 controls passed. The three ignored descriptor diagnostics passed individually in both profiles in the [separate descriptor run](https://github.com/mingley/partitionline/actions/runs/42450883159), including reservation recovery under a 64-descriptor limit.

The normal [broker CI on main `80a687e`](https://github.com/mingley/partitionline/actions/runs/42450412672) separately passed 747 default and 895 all-feature tests with three ignored in each, inventory and fuzz. Those results predate the combined candidate.

An earlier clean candidate [0210c7c](https://github.com/mingley/partitionline/commit/0210c7cdf2ade9b6849c4beea019b3544911aaff) was based on `ba8d72f`, before the journal repair. Its metadata, Fetch and inventory blobs were `279348d6a0ad32914f81fb44e072d95a6436e9a2`, `e8846ef5233c7669ec7e1ad52889f782344eedba` and `64b4cad3c8cbd2269a1486007987986367876eb3`. Its production and test code matched `6d1b06c` apart from rustdoc.

On that earlier source, [focused checks, formatting and Clippy](https://github.com/mingley/partitionline/actions/runs/42449036645) passed in both feature profiles. The [plain broker suites](https://github.com/mingley/partitionline/actions/runs/42449036683) passed 746 default and 894 all-feature tests; the inventory checker and all 55 inventory controls passed. [Baseline and mutation controls](https://github.com/mingley/partitionline/actions/runs/42449036657) passed in both profiles.

The default [all-target run](https://github.com/mingley/partitionline/actions/runs/42449097771) failed in `transport_deadline_cancels_wait_and_releases_read_admission`: the fresh Produce received an unexpected EOF, and the transport reported two handler deadlines. The run stopped after 508 passes and one failure; the Fetch target had 25 passes and one failure, and later targets did not run. The all-feature all-target suite passed 894 tests. Both separate doctest gates passed with zero doctests. The original default failure remains unexplained; neither ownership nor timing has been ruled out.

A [controlled comparison](https://github.com/mingley/partitionline/actions/runs/42449931948) ran the original test once on baseline `7f59ef4` and candidate `0210c7c`, and both isolated runs passed. With a test-only forwarding handler, a FIFO barrier completed before the QueueFull probe, establishing that a pending real Fetch held the sole data permit. After the handler deadline and a second barrier, a fresh Router Fetch succeeded. Two separate gates then reproduced the same EOF and two handler-deadline counters: one held Produce before dispatch and left no records, while the other held the response after a complete durable Produce acknowledgement and verified exact records and watermark after reopening. These controls show that the archived counters permit different explanations; they do not establish where the original fresh Produce stalled or erase the original failure. The [earlier diagnostic](https://github.com/mingley/partitionline/actions/runs/42449783570) also passed, but its QueueFull probe preceded the barrier and could not distinguish queue occupancy from a held permit.

The earlier exact-code [focused run](https://github.com/mingley/partitionline/actions/runs/42448431941) passed formatting, strict all-target/all-feature Clippy and 16 selected tests in each feature profile. Its [plain broker suite](https://github.com/mingley/partitionline/actions/runs/42448433000) passed 746 default and 894 all-feature tests, with zero doctests; it also passed the inventory checker and 55 controls. Plain `cargo test` does not establish all-target coverage. The [baseline and mutation run](https://github.com/mingley/partitionline/actions/runs/42448470369) passed in both profiles: the old borrowed-permit implementation failed the intended claimed-actor assertion, and deliberately reversing each lease, completion and grouped-batch drop order triggered its corresponding probe.

The earlier fixture qualification had an independent default-suite `Journal(AlreadyOpen)` failure; its successful all-feature and focused checks do not erase that failure. It is retained in the [canceled-queue report](../fetch-canceled-queue-20261108/README.md).

## Performance

A [single-thread primitive probe](https://github.com/mingley/partitionline/actions/runs/42446032814) on hosted x86 measured a median 22.05 ns for borrowed semaphore acquire/drop and 31.01 ns for owned permit plus Arc clone, with a paired median increase of 8.73 ns across 21 interleaved samples. This is not an end-to-end broker measurement.

A [combined-tree paired benchmark](https://github.com/mingley/partitionline/actions/runs/42450565502) compared `80a687e` with `7dcef58` on one hosted AMD EPYC 9V45 machine, two CPUs and ext4 using stable Rust 1.99.1. Eight pairs alternated execution order; every run checked 2,600 unique acknowledged offsets, all 202,800 fetched record bytes, and the complete history after reopening. Median candidate/baseline elapsed ratios were 0.9561 for Produce (pair range 0.5731–1.4238) and 0.9867 for Fetch (0.9682–1.0058). Median per-run throughput was 20,552 versus 21,074 requests/s for Produce and 225,360 versus 228,501 requests/s for Fetch, baseline then candidate. Produce windows were 88–192 ms and Fetch windows 17–18 ms, so the result does not establish a stable speedup or reliable tail latency. Separate traced runs observed 607 and 606 partition-journal syncs for 2,600 acknowledgements, including setup and recovery; tracing changes scheduling and does not prove batching in timed runs. The [artifact](https://github.com/mingley/partitionline/actions/runs/42450565502/artifacts/13605631349) retains raw samples, source/binary hashes and traces; it was not independently inspected.

A [matched release-mode router benchmark](https://github.com/mingley/partitionline/actions/runs/42448800810) compared baseline `7f59ef4` with the candidate code at `6d1b06c`, before the final rustdoc-only changes. Eight pairs alternated execution order on two CPUs of one hosted AMD EPYC 7763 machine using ext4 and stable Rust 1.99.1. Each run checked 2,600 unique acknowledged offsets, all 202,800 fetched record bytes, and the complete history after reopening.

The median candidate/baseline elapsed ratio was 1.0189 for Produce, with paired ratios from 0.6520 to 1.5186, and 1.0037 for Fetch, with paired ratios from 0.9601 to 1.0155. Median per-run throughput was 20,010 versus 19,660 requests/s for Produce and 140,129 versus 139,759 requests/s for Fetch, baseline then candidate. The Produce windows lasted 104–202 ms and Fetch windows 27–29 ms; this is a noisy smoke comparison, not a reliable tail-latency or broad performance qualification.

Separate traced runs observed 624 baseline and 618 candidate partition-journal `fdatasync` calls for 2,600 acknowledged produces each; those counts include setup and reflect traced scheduling. They do not establish batching in the timed runs. [Raw samples, traces, source and binary hashes](https://github.com/mingley/partitionline/actions/runs/42448800810/artifacts/13604813381) were retained but not independently inspected. These results cannot support a fastest-in-the-world claim.

## Retained failed attempts

- [26452f9](https://github.com/mingley/partitionline/actions/runs/42446658734): selected tests and Clippy passed; formatting failed.
- [3da460e](https://github.com/mingley/partitionline/actions/runs/42447012668): the actor fixture reached its permit checkpoint, then its all-topics Metadata barrier hit the 64-byte response limit; the test expected success. Clippy also rejected an unnecessary test-helper unwrap. The fixed fixture confirmed the Metadata reply's planned length was 103 bytes and the subsequent valid Fetch reply was 12 bytes, without increasing the 4,160-byte envelope.
- [ee06afd](https://github.com/mingley/partitionline/actions/runs/42447593661): formatting and compilation failed because grouped-Produce code still referenced fields moved into the lease.
- [4d57001](https://github.com/mingley/partitionline/actions/runs/42447913780): targeted tests and Clippy passed; two test-only lines failed formatting.
- [4271be1](https://github.com/mingley/partitionline/actions/runs/42448227919): formatting and targeted tests passed; strict Clippy rejected the test-helper return type's complexity. Its [separate complete plain suites](https://github.com/mingley/partitionline/actions/runs/42448227914) passed.
- [First paired benchmark setup](https://github.com/mingley/partitionline/actions/runs/42448555870): no measurement ran. Cargo rejected nested checkouts as unlisted members of the outer workspace. The later run copies the verified crates outside that workspace without changing their manifests.

Actions artifacts expire. The linked logs and summaries are evidence for these scoped checks, not a production-readiness certification.
