# send_all cancellation at the ready-route yield

The test-only [witness commit](https://github.com/mingley/partitionline/commit/7a53626b3d81df5d266e80b7faa54174f74f3a90) compares baseline `47df27f9f3e04fc524849f3a9753c0b0469f8947` with candidate `3fd30f13c07056dc0ac3d937b3f370d92880bd98`. It does not change production code. The [hosted run](https://github.com/mingley/partitionline/actions/runs/42471492541) passed both default and all-feature jobs using Rust 1.99.1, LLVM 23.1.1 and x86_64. Each arm reported six passing tests; the candidate-specific blocked-destructor test printed a not-applicable message in the baseline arm.

Each history warms a one-partition mock route, then submits 1,025 explicit-partition records with distinct eight-byte values. The test manually polls the public future once on a current-thread runtime, wrapped in `tokio::task::unconstrained`, and drops it before the runtime can schedule the worker. The wrapper deliberately removes cooperative-budget interruptions to isolate the candidate's explicit yield; these observations do not establish the usual Tokio scheduler's poll boundary.

| History | Baseline | Candidate |
|---|---:|---:|
| No interceptor or sticky partitioner: records accepted before cancellation | IDs 0–1,024 | IDs 0–511 |
| Inputs still unaccepted at cancellation | 0 | 513 |
| Mock records after cancellation and flush | IDs 0–1,024, ordered | IDs 0–511, ordered |
| Returned receipts when the call completes normally | 1,025, ordered | 1,025, ordered |
| Interceptor fallback: accepted records and acknowledgement callbacks | 1,025 | 1,025 |
| Sticky fallback: accepted records | 1,025 | 1,025 |

The accepted prefix was checked through the public queued and acknowledged counters, visible-byte accounting, per-ID owner destruction and the mock log. Before cancellation, the candidate had 512 accepted records, 4,096 buffered bytes and no unaccounted in-progress reservation. Its inputs met the candidate's homogeneous, fresh, explicit-route conditions, with no interceptor or sticky partitioner; no worker ran during the poll. The source reaches its new 512-admission yield at this point. The baseline had 1,025 accepted records and 8,200 buffered bytes. After cancellation and flush, the log contained exactly the accepted IDs in order, the error count remained zero, and buffered bytes returned to zero in all these histories. After a canceled `send_all`, its individual receipt receivers have been dropped, so the test cannot assert that the caller received those delivery results. The separate successful-call history checks all 1,025 returned receipts.

## Blocking input destructor

The witness uses a custom `Bytes` owner whose destructor waits on an external release thread. In the candidate arm, the blocking owner is the first input still unaccepted at the new yield. Dropping the future held the current-thread runtime for 3,000 ms while a timer registered before the cancellation passed its deadline; the timer task ran after the external release. That input was supplied by the caller and had not been accepted by the producer.

A separate matched history fills a 256-byte producer buffer after 32 admissions and stops at the existing buffer-capacity await. Both baseline and candidate showed the same 3,000 ms blocked destructor and delayed timer when the waiting future was dropped. Thus cancellation-time destruction of an unaccepted custom input was already reachable; the candidate adds a cancellation point in the tested ready-route history. The three seconds are the test's chosen external hold, not a measurement of inherent destructor latency.

## Source and build pins

- Baseline producer SHA-256: `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Candidate producer SHA-256: `10c84895af80e31c3c9e8a18bd84480caf248d228adece495c1539cdd4c902d9`.
- Identical copied witness SHA-256: `44441c5130954f8cb366dc402f3dc07ac6f95145668331ab14dedda547a9d119`.
- Identical mock source SHA-256: `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`.
- Default-feature baseline and candidate test executable SHA-256: `3b943e45602789404417c8266a80b82596b0831aae3f1356b45e97e68eeee5dd`, `a09600630b64d4379799682ecb873835f7269ff90e01ed1f428648ed2a5e687f`.
- All-feature baseline and candidate test executable SHA-256: `5d9fe168100737657832b7e7eee0eb8a9abb15cbf025e161f4ba7d895729ae2f`, `b953cbc31d2e592f276bfbfcec3aba51cd01213e4e541b838fda227c5c093c7d`.

Baseline and candidate used separate checkout directories and target directories. The run used Cargo's test profile. The executable hashes were logged; the executables were not retained as artifacts.

These tests use the repository's mock and codec, one connection and one explicit partition. They do not cover a real Kafka broker, failures and retry handoffs, transaction boundaries, route changes, cancellation under an ordinary cooperative schedule, or performance. They are not production-readiness evidence.
