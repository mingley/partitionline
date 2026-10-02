# KL05-11 — selected Produce v13 delta

Implemented and tested source: `7ee1a405fed66a6bc2f600ffd5485eb7fe5a01bb`.

Produce13 uses nonzero Metadata10+ topic UUIDs. Requests retain their sent UUID/name snapshot, responses validate those identities and partitions before acknowledgments, and UNKNOWN_TOPIC_ID refreshes metadata while retaining the original delivery deadline and sequence. Produce3–12 name layouts remain available; peers without usable UUID metadata use the older ceiling. TransactionV2 requires finalized `transaction.version >= 2` plus EndTxn5 support, otherwise the transaction path retains its Produce11/EndTxn4 ceiling and explicit partition registration.

The exact Apache schema and Java semantic sources were inspected before raising the cap. `upstream/` retains21 source files from checksum-pinned4.1.2/4.2.1/4.3.1 releases; `provenance/` retains official distribution identity. The immutable historical4.1.0 fixtures remain unchanged. `java-final/` retains independently compiled generators, commands, jar hashes and results for six request/response cells per current release. Final Rust tests additionally executed all three native Apache decoders over freshly generated Rust requests/responses, checking topic IDs, records/CRC, offsets and throttle.

## Final checks

| Toolchain | Default core | All-feature core | Clippy/format/docs | Doctests |
|---|---:|---:|---|---:|
| Stable1.99 |1801 passed,4 existing ignored |1802 passed,4 existing ignored |strict gates passed |4 passed |
| Rust1.85.0 |1801 passed,4 existing ignored |1802 passed,4 existing ignored |strict gates passed |4 passed |

Each full cell includes330 full-surface and33 protocol-oracle successes. Six new runtime cases cover UUID usage, stale identity retry/deadline, immutable response mapping after metadata change, malicious/missing identities, every3–12 fallback and finalized transaction feature selection. The new oracle cases cover six independent cells, four mapping failures and325 truncated prefixes. Explicit metrics tests passed10 and credential tests28default/29tracing per toolchain. Python protocol-coverage guards passed15.

`commands.json` retains exact commands/environment and their log hashes. `results.json` selects the authoritative passing cells: stable default/all-feature and MSRV all-feature from `final-gates-retry2/`, MSRV default from `final-gates-retry1/`. The archived4299 tracked source files were rehashed after the final checks and all matched `source-integrity.json`. CPU affinity was0–2,4; compilation used two jobs, incremental0 and final dev/test debug0.

## Attempts and limits

All failed and superseded attempts are retained. `precommit/` and `java-attempts/` preserve preparation failures and corrected checks. `final-gates/` failed because standalone example binaries were absent. `final-gates-retry1/` passed both default cells but its all-feature builds exhausted disk; those builds provide no behavioral result. Exact stable default executable identities were preserved in `retained-binaries-stable.json` before cleaning this worker compiler cache. The688MB compressed binary archive remains in scratch and is not included in Git. The source stayed unchanged during recovery.

This is selected wire/runtime support, not full production qualification. Current Java processes prove codec compatibility, not a live Kafka cluster. The exhaustive older-version independent case remains blocked; older fallbacks have runtime/local coverage and retained fixtures. Transaction feature selection uses a fixed initialization snapshot; recreate the producer after feature upgrades. Four existing tests requiring explicit live/manual harnesses remain ignored. No performance or distributed EOS claim is made.

## Reproduce

Start from a clean `git archive 7ee1a405fed66a6bc2f600ffd5485eb7fe5a01bb`. Set dedicated target directories, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_PROFILE_TEST_DEBUG=0`. For each toolchain run the retained command list, including `cargo build --locked --examples` before default all-target tests and the all-feature example build before that test cell. The retained runner under `precommit/` uses scratch paths and can be adapted to another host. Independent Java generation is provided by `tests/conformance/java/generate_produce_v13.py`; its `--help` documents checksum-pinned jar inputs and fixture verification. Set `PARTITIONLINE_PRODUCE_V13_JARS` and `PARTITIONLINE_PRODUCE_V13_CLASSES` as recorded to require the fresh three-release decoder test. Retained `.class` outputs are regenerated from the hashed source; they are not committed.

`SHA256SUMS` covers every retained evidence file except itself. The task-card evidence JSON links this bundle and the source-pinned implementation/fixture manifests.
