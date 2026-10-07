# Committed offset topic IDs

OffsetCommit and OffsetFetch v10 use UUIDs in requests and responses.
OffsetClient exposes complete bounded8-10 models. ConsumerGroup commits keep
assignment IDs, including queued commits; named Admin calls capture metadata
before committing or projecting fetched UUIDs. Batched fetches preserve group
order. Older actual coordinators refuse UUID intent before the offset RPC.

Latest-stable Rust passed2,062 default and2,074 all-feature tests, including24
socket and4 codec regressions. Both builds executed96 process profiles against
Apache4.1.2/4.2.1/4.3.1:81 supported profiles and15 expected refusals each.
Each build independently parsed180 Rust offset bodies and actual socket frames.
Public Java name-based calls used8/9 and refused10-only peers. Rust UUID calls
refused older peers. Metadata versions10-13 were checked with actual SDK parsers.
All peers/processes were waited; shutdown checked closed/reusable ports,
zero Rust tasks and zero Java Kafka threads. Four extracted package consumers
compiled24 documentation examples each; strict checks and53 checker tests passed.

This qualifies the client changes against scripted peers. UUID replacement in
metadata simulates recreation; live coordinator persistence and cluster recovery
remain open. Timeout/cancellation/lost replies do not prove a commit was absent.
No production readiness or comparative performance result is claimed.
The required conformance ledger remains51 independently qualified cases of182.

summary.json records limits and the published review source. validation/ keeps
exact source, binaries, SDK pins, commands, wire captures and first failures.
FILES.json and SHA256SUMS describe this snapshot. Do not regenerate it.
