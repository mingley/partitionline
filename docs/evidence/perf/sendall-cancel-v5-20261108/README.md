# send_all cancellation without the added yield

The test-only [witness commit](https://github.com/mingley/partitionline/commit/e7b49883311211ffdb81a3c6708499f4afc81621) compares baseline `47df27f9f3e04fc524849f3a9753c0b0469f8947` with candidate `a781fbf47e724d04d37426e2b1c6b13dc2692003`. It does not change production code. In the [hosted run](https://github.com/mingley/partitionline/actions/runs/42472001367), all six tests passed for each source with default features and again with all features, on x86_64 using Rust 1.99.1 and LLVM 23.1.1.

The ready-route history warms a mock partition, then submits 1,025 explicit-partition records with distinct eight-byte values, no interceptor and no sticky partitioner. It manually polls the public future once on a current-thread runtime, wrapped in `tokio::task::unconstrained`, and drops it before a worker can run. Both sources accepted IDs 0–1,024 during that poll. After cancellation and flush, the mock log contained exactly those IDs in order, every original custom byte owner had been destroyed once, the error counter remained zero and buffered bytes reached zero. A separate completed call returned all 1,025 ordered receipts. Once the canceled call is dropped, its individual receipt receivers are gone; this test does not claim the caller observed those outcomes.

The interceptor and sticky-partitioner fallback histories also admitted and acknowledged all 1,025 records in both sources. The interceptor history checked every acknowledgement callback, including the warmup callback, and observed no error callbacks. This witness checks the public behavior of a fixture matching the shortcut's input conditions; it does not instrument the internal shortcut. The unconstrained manual poll isolates a specific schedule and does not establish the usual Tokio cooperative scheduler's poll boundary. Removing the added yield does not establish cancellation-boundary equivalence under real schedules: faster admission can change when an existing channel or buffer wait becomes pending. The full-channel and full-buffer histories below explicitly create the same blocked boundary in both arms.

## Existing admission waits

The buffer history fills a 256-byte buffer with 32 eight-byte records, then drops a `send_all` blocked on capacity. In both sources the already accepted IDs reached the mock in order and buffered bytes returned to zero.

The channel history warms a route and fills its actual 4,096-record worker channel using `try_send`; a further `try_send` returns `QueueFull`. A second `send_all` contains 128 different records. Its first record is compacted and holds an eight-byte reservation while waiting for the full channel, but has not been accepted; its remaining 127 inputs are still unaccepted. In both sources, dropping that future released the extra reservation, leaving 32,768 bytes accounted for the 4,096 accepted records. After flush, all 4,096 accepted values appeared in order in the mock log and buffered bytes reached zero; none of the 128 unaccepted values appeared.

Each wait history includes a caller-supplied custom `Bytes` owner in the unaccepted inputs. Its destructor waits for an external release thread. Dropping the future held the current-thread runtime for 3,000 ms, and a pre-registered timer could not run before the release. Both baseline and candidate showed this at both existing waits. The three seconds are the test's chosen hold, not an inherent destructor latency.

## Source and build pins

- Baseline producer SHA-256: `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Candidate producer SHA-256: `e074dfdc9d91a64cf25102a747e82af4aaf9be4baf4280b148f42b49280464b3`.
- Identical copied witness SHA-256: `18a43fdcb2b81fa4c40b7d6157d4c681c31370196329f9ae84197f5714937e60`.
- Identical mock source SHA-256: `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`.
- Default-feature baseline and candidate test executable SHA-256: `e0cea0132d22d2e2e5f9739274f21f2bbe2335275462c05c7d5d0c525f380a7e`, `cccd1669d7da40ee406728a7260da88d0e37c74aa4816674addabfa8e6afa00d`.
- All-feature baseline and candidate test executable SHA-256: `a1271c657e7aeb5210cc133c5a890f65915b573dd3f56bdccbe8cad0da46eb63`, `f435f9760b8b82c4ebaf9a71d2d24332234bf19e9899a61c2904090312721b46`.

The arms used separate checkouts and target directories. The run used Cargo's test profile. Executable hashes were logged; the executables were not retained as artifacts.

These tests use the repository's mock and codec. They do not cover a real Kafka broker, failures and retry handoffs, transaction boundaries, route changes, cancellation under an ordinary cooperative schedule, or performance. They do not qualify production readiness. The earlier candidate with an added 512-record yield was tested separately in [run 42471492541](https://github.com/mingley/partitionline/actions/runs/42471492541); its results do not qualify this candidate.
