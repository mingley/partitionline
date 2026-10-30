# Capability CI results

Both profiles pass at `209d0b068`: 93 required Rust tests passed, none failed
or were ignored. The suite counts are 13 marker, 13 Share, 14 partitioner,
29 sticky-partitioner and 24 Streams tests. The two SDK server probes remain
separate and unrun.

Pinned Kafka 4.1.2, 4.2.1 and 4.3.1 Java clients generated fresh marker and
Share fixtures. Java then completed 450 reverse checks over 225 Rust-exported
pairs. There are 85 null-field controls and 663 generated fixture files;
that file count includes tables and reports. Six Rust TSVs bring the final
member count to 669. Streams passes 24 Rust cases and its 234-file export gate.
Independent Streams Java parsing and SDK socket coverage remain unqualified.

Each report records 38 successful commands, no guard trigger, joined child
processes and unchanged source. The toolchain is Rust 1.99.1, LLVM 23.1.1,
x86_64. Independent review verified both compact ZIP hashes, inner validation
hashes, source pins and cross-profile consistency. It also matched all 114
original Streams fixture digests against Git. OOM status is unknown.

The original 37 MB archives exceed the download tool's limit, and the proxy
rejected direct transfer. They remain external GitHub artifacts 13017324430
and 13017540595. The new small reports contain the original validation JSON
and wrapper manifests, retained here unchanged or as lossless gzip. The full
archives, jar bytes and generated binary fixtures were not rehashed locally.

These are codec and focused API results. Broad client runtime failures,
release gates, production readiness and performance comparisons remain open.
The [fixture gate record](../capability-java-20261030/README.md) preserves the
earlier failure and its repair.

Source: [capability CI](https://github.com/mingley/partitionline/actions/runs/41137429080).
