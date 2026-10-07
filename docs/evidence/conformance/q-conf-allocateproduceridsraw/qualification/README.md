# AllocateProducerIds qualification

Actual Apache4.1.2/4.2.1/4.3.1 builders and parsers checked fixed fields,
defaults, signed limits, error factories, truncated messages and opaque tags.
Rust retains the known fields and skips opaque tags; Java retains their payloads.
Each default/all-feature build independently parsed81 Rust wire bodies.

Six fresh native Kafka installations ran across the two feature builds.
Each build executed30 raw controller requests through Rust and Java:18
nonoverlapping1000-ID blocks and12 stale-epoch errors. SDKs parsed60 live
bodies per build. Ordinary broker listeners did not advertise this controller
API; Rust Admin refused it. All sockets/processes closed, brokers were waited
and both ports per installation were rebound. Native executed-file hashes
match the retained release archives. Latest-stable Rust passed2,082 default
and2,094 all-feature tests; strict Clippy and formatting passed.

The three current raw-extension ledger cells qualify. Historical exclusions
and original source/peer pins remain. Internal allocator state injection,
exhaustion, recovery, permissions and broader case applicability remain open.
No production or performance claim follows from this cohort.
summary.json records scope and the published source. validation/ and peers/
retain exact sources, process receipts, failures, messages and peer archives.
FILES.json and SHA256SUMS describe this snapshot. Do not regenerate it.
