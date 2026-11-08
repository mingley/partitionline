# Producer redial test request timeline

The existing `send_all_redials_leaders_while_refreshing_metadata` test failed
its 450 ms limit on both the unchanged baseline and an isolated optimization.
In matched run [42471756660](https://github.com/mingley/partitionline/actions/runs/42471756660),
all six baseline and six candidate trials failed in each of the default and
all-feature profiles, at approximately 756–761 ms. Successful collection of
those failures did not make the test pass.

To investigate, run
[42472391861](https://github.com/mingley/partitionline/actions/runs/42472391861)
kept the baseline producer source unchanged and added request timestamps to the
test mock. The test configured `metadata_max_age=30 ms`,
`connections_max_idle=40 ms`, and a 250 ms mock delay for both ApiVersions and
Metadata. The original assertion still failed at 759.169 ms. From the test's
second `send_all` start, the mock recorded:

| Time after start | Request arrival at mock |
| ---: | --- |
| 1.021 ms | ApiVersions, node 1 |
| 1.052 ms | ApiVersions, node 3 |
| 1.069 ms | ApiVersions, node 2 |
| 253.566 ms | Metadata, node 1 |
| 505.764 ms | ApiVersions, node 2 |
| 505.801 ms | ApiVersions, node 3 |
| 758.828 ms | Produce, node 2 |
| 758.987 ms | Produce, node 3 |

The first leader ApiVersions requests and node 1's request began together
during discovery. The 40 ms idle setting is shorter than either mock delay,
which is consistent with the later leader ApiVersions requests. These are
request-arrival observations: the trace does not identify each physical socket,
prove each teardown cause, record response completion, or establish that a
leader handshake overlapped an already-issued Metadata request.

Two diagnostic variations of the same test set either the ApiVersions delay
or the Metadata delay to zero; both passed the existing timing assertion.
They do not replace a deterministic overlap test or prove the original
450 ms bound. The trace retains the original failing case.

The source was qualification commit
[`1ca030b756a1e93ebedd0db89e25d5b270338d72`](https://github.com/mingley/partitionline/commit/1ca030b756a1e93ebedd0db89e25d5b270338d72):
producer blob `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`, instrumented
mock blob `57edf391a5f476d9810ad9e6a9b4a6baac64f66d`, and test blob
`98525894f0f0f4d108a7849c559d62a73927b6e7`. It ran with default features,
Rust 1.99.1 and LLVM 23.1.1 on hosted x86_64. The executed test binary had
SHA-256 `daadb6f4fa5a140d4260935f458614044200db5760da81443bb7e84124558d1f`;
the binary was not retained. The
[logs and source pins](https://github.com/mingley/partitionline/actions/runs/42472391861/artifacts/13614505483)
were retained. Mock logging changes scheduling, and this is one trace per
variation, not a latency benchmark.
