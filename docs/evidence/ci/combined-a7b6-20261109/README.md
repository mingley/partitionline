# Combined client and broker checks

[Run 42669678125](https://github.com/mingley/partitionline/actions/runs/42669678125)
checked combined commit `a7b6edc1645e3a4789cf699a3b9e7656c55a3d10`.
The workflow-only branch was at `5e770f542badb650da6698b8b350de2c6a0b2e34`;
each job checked out the combined commit itself and verified the working
files and source blobs. All jobs used x86_64 Linux and stable Rust 1.99.1.

All seven jobs passed. No requested step was skipped and the test suites
below reported no ignored tests.

| Job | Result |
| --- | --- |
| Format | Client and broker formatting passed. |
| Client strict | All-target/all-feature Clippy and warning-denied Rustdoc passed; 9/9 all-feature doctests passed. |
| Client behavior, default | Pagination 83/83; producer close errors 6/6; produce cancellation 23/23. |
| Client behavior, all features | Pagination 83/83; producer close errors 6/6; produce cancellation 23/23. |
| Broker transport, default | Unit controls 47/47; existing transport integration 16/16; backpressure integration 5/5. |
| Broker transport, all features | Unit controls 51/51; existing transport integration 16/16; backpressure integration 5/5. |
| Packed docs | 20 Markdown controls and 4 documentation-diagnostic controls passed. The extracted package passed checks for 30 documents, 229 local links, 61 pinned repository targets, and its license/exclusion inventory. |

The packed-crate consumer ran four stable feature cells: default, tracing,
zstd, and tracing plus zstd. Each compiled 24 documented examples and checked
58 local links; none ignored an example. The run retained its package reports
as the `combined-a7b6-package-docs` artifact.

The pinned source blobs include:

- `src/admin.rs`: `e6c9899f3c645023782a9a33531665e3ef4b741c`;
  `src/admin/pagination_tests.rs`: `d497709daffaa1cae3e6cd328aa0848a41c91567`.
- `src/producer.rs`: `61b084c065291f077aa6112fdba6dc15b33848bd`;
  `tests/producer_close_errors.rs`: `3244d743442dec21fcf3159e9855157bc813abc6`;
  `tests/produce_cancel.rs`: `65b44339a6fac2d7946298d5130e6bf8572ef69b`.
- `partitionline-broker/src/transport.rs`:
  `7d43efab6f3e929611ed2df1def5f2edb96f475a`;
  `partitionline-broker/tests/transport_backpressure.rs`:
  `2fc5613de332cd47396847f1778f69d33a5897a4`.
- Client `Cargo.lock`: `96e364d34121fe90183f87f4f49fd92b883bc447`;
  broker `Cargo.lock`: `19652acd96b929402b1e4a9394a33d12a7e69449`.

These focused checks do not include the broad Admin or producer suites, the
schema companion, live broker compatibility, or comparative performance.
Earlier broad-suite failures remain recorded in the respective change
evidence; this run does not establish production readiness.
