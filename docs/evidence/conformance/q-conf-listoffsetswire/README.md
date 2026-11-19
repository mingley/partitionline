# ListOffsets v1–v11 wire fixtures

Each lane generates request and response bodies with a pinned Apache SDK. Rust
checks the decoded fields and writes fresh bodies. Apache parses the Rust bodies
and checks those fields again. Both parsers must consume the whole body.
Encodings match byte for byte. For the tagged cases, Rust discards unknown tags
at the body, topic, and partition levels and matches the corresponding
untagged encoding.

The fixtures cover empty arrays, multiple topics and partitions, timestamp
queries, offset selector values, throttle time, leader epochs, and mixed
success and error responses. The cases include the v10 timeout field, the v11
earliest pending upload selector, and the replica request error builder at
v5, v6, and v11, including its error codes and sentinel fields.

| Apache SDK | Declared versions | Tested versions | Pairs per build |
| --- | --- | --- | ---: |
| [3.9.1](https://github.com/apache/kafka/tree/f745dfdcee2b9851204ddbbcd423626ab87294bc) | v0–v9 | v1–v9 | 24 |
| [4.1.0](https://github.com/apache/kafka/tree/13f70256db3c994c590e5d262a7cc50b9e973204) | v1–v10 | v1–v10 | 27 |
| [4.1.2](https://github.com/apache/kafka/tree/c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c) | v1–v10 | v1–v10 | 27 |
| [4.2.1](https://github.com/apache/kafka/tree/18d5ecd939c8d510fdd72d0abb1f7099659dcd58) | v1–v11 | v1–v11 | 31 |
| [4.3.1](https://github.com/apache/kafka/tree/26b251a451ce941d3d7a55e6487bcb7f16b5ad48) | v1–v11 | v1–v11 | 31 |

The ten lanes checked 280 pairs in each direction across the default and
all-feature builds. Both checks rejected a v10 request with its timeout changed
from 1260 to 1261. The runner checks each SDK JAR's SHA-256, the loaded Java
class source, and the request and response version ranges.

The retained 31-pair corpus has SHA-256
`b675b5c40736e10a47c9e27c78c1b035906c94586b211f1dfb9d88db814db67b`. The runner compares it with
fresh 4.3.1 output and checks the matching rows against the older SDKs.
[Corpus](../../../../tests/fixtures/list_offsets_wire/apache-v1-v11.txt).

The first run passed the peer checks and strict lint. It found formatting
differences in the Rust test, which were corrected before the package run.
[Generation run](https://github.com/mingley/partitionline/actions/runs/44610208228).

The package run on `2e94fc3430edad5d0958749f8ca5256f0c9ea77d` used
Rust 1.100.0 and Temurin 21.0.13+9 on GitHub's x86_64 Ubuntu runner. The peer checks,
retained-byte comparison, Rust formatting, and strict lint passed. The offline
fixture test passed in both feature profiles in the checkout and after
extracting the crate built by `cargo package --locked --no-verify`.
[Workflow](../../../../.github/workflows/list-offsets-wire.yml) ·
[Package run](https://github.com/mingley/partitionline/actions/runs/44610943173).

KL01-21 remains open for complete registered coverage, live version
negotiation, and broker behavior including timeout enforcement and tiered
storage. All fixtures use isolation level zero. The documented body API
starts at v1.
