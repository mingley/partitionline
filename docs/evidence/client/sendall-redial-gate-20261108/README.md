# Producer redials during metadata discovery

The original `send_all_redials_leaders_while_refreshing_metadata` test configured two idle leader connections, stale topic metadata, and 250 ms delays for both ApiVersions and Metadata responses. It asserted that the whole send completed in less than 450 ms.

In [run 42471756660](https://github.com/mingley/partitionline/actions/runs/42471756660), the assertion failed in 12 of 12 matched trials for each source: six default-feature and six all-feature trials. Baseline `47df27f9f3e04fc524849f3a9753c0b0469f8947` ranged from 756.787–757.823 ms with default features and 756.921–758.446 ms with all features. Yielding candidate `3fd30f13c07056dc0ac3d937b3f370d92880bd98` ranged from 756.473–757.957 ms and 756.979–760.700 ms, respectively. The no-yield candidate was tested separately in [run 42471922968](https://github.com/mingley/partitionline/actions/runs/42471922968), not in the paired 12-trial run. The yielding candidate's earlier grouped failure is [run 42471313301](https://github.com/mingley/partitionline/actions/runs/42471313301).

In the instrumented baseline [timeline run 42472391861](https://github.com/mingley/partitionline/actions/runs/42472391861), the metadata connection's ApiVersions request and both leaders' first ApiVersions requests appeared about 1 ms after the send began. The Metadata request appeared about 253 ms after the start, both leaders negotiated again about 506 ms after the start, and their Produce requests arrived about 759 ms after the start. The 40 ms idle limit is shorter than either 250 ms response delay. Early replacement sockets can therefore become idle again while metadata is outstanding; the trace alone does not prove the cause of every later teardown. Controls with one delay set to zero passed the old timing assertion. The timeline shows first-handshake overlap and does not support the old total-latency bound.

## Replacement test

The test-only candidate `f292980374e3db10d0b4c90f31b9c8cceb1b9d67`, based on main `e487f7294773f75112f15437992ed780b53c7307`, names the test `send_all_redials_leaders_during_metadata_discovery_handshake`. Production source is unchanged. It uses the original 30 ms metadata age and 40 ms idle settings, identifies the warm metadata and leader sockets, waits 60 ms with no subsequent request or response on those sockets, and confirms that the mock's completed metadata request count did not increase during that wait.

The mock then holds ApiVersions responses until it has observed requests on three distinct replacement sockets: the metadata rediscovery connection and both leader connections. Only then does the test release the gate and check the subsequent metadata reload, ordered delivery receipts, and each topic's mock log. Its five-second timeout limits a stalled test; it is not an end-to-end latency assertion. This proves concurrent discovery handshakes in this controlled history. The Metadata RPC itself is sent after its connection's handshake; the test does not claim the leader redials begin while that RPC is already in flight.

Replacement negotiation precedes retirement of the old socket. The test records whether each old socket closed after installation but does not assert physical cleanup. All three were observed closed in the isolated positive trials; broader socket-retirement coverage remains open. The original timing test did not assert physical closure.

## Qualification and controls

[Run 42473359326](https://github.com/mingley/partitionline/actions/runs/42473359326) passed the replacement three times with default features and three times with all features, plus stable rustfmt and strict all-target/all-feature Clippy. A separate mutation `0914c126f88128c7c9d6695cdfce63fd2fb322cb` moves the leader reconnect until after metadata refresh. With the same test and mock bytes, the test failed as intended in both feature profiles, reporting that the leader redials did not overlap the metadata discovery handshake. The production mutation exists only on its negative-control branch.

The first gate run, [42473007592](https://github.com/mingley/partitionline/actions/runs/42473007592), passed its positive histories and the mutation reached the intended failure in both profiles. The jobs still failed overall because the workflow exited immediately on the expected failure status and rustfmt found a test assertion layout difference; Clippy was skipped. The corrected run above fixed both issues and renamed the test to describe the handshake.

The identical two test blobs were applied to main `e487f7294773f75112f15437992ed780b53c7307` in the test-only commit `f292980374e3db10d0b4c90f31b9c8cceb1b9d67`. [Run 42473694151](https://github.com/mingley/partitionline/actions/runs/42473694151) passed three focused tests per feature profile, stable rustfmt, and strict all-target/all-feature Clippy on that tree. All three old sockets were observed closed after installation in these trials too; closure was not asserted.

Source pins:

- Positive production `src/producer.rs` Git blob: `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`; SHA-256: `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Test `tests/mock_produce.rs` Git blob: `4fd9a5a940086a97037bb45192ed84224a1542c6`; SHA-256: `98829e2b886d4eadb3d307adf41ab14535b17872ba81422be83e258f9231086f`.
- Mock `tests/common/mod.rs` Git blob: `30c2069ae40ff81c893acb202ee3d370ee75780f`; SHA-256: `446e536ea6112a9655a89901e240c38eabc78c38087a1afb8df3fee86f63eddd`.
- Negative-control production Git blob: `e8bc8aab0a4a2d790b0eed6350f52dcd018a41d9`; SHA-256: `ae6e15d9176270fddcbceefc8f7977e518c960180c5cce75aa4f410378cf4f28`.

The histories use the repository mock on x86_64 with Rust 1.99.1 and LLVM 23.1.1. They do not validate a real Kafka cluster, a production latency objective, or overall producer readiness.
