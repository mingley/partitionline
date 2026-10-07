# Streams group descriptions

Admin::describe_streams_groups queries actual GROUP coordinators using API89v0.
It preserves all group, topology, task, member, offset and endpoint fields,
caller order and repeated IDs. Broker response errors remain typed; discovery,
capability and connection failures have separate provenance. Explicit opt-in,
per-connection negotiation and one deadline cover the complete operation.
Retained results and duplicate expansion are checked before deep clones.

Latest-stable Rust passed2,034 default and2,046 all-feature tests. Eleven
focused tests cover routing, limits, retries, partial errors and cancellation.
Both feature builds executed24 genuine Java Admin calls and24 public Rust
calls across Apache4.1.2/4.2.1/4.3.1. Full fields are compared with SDK parsers
and the actual public Java projection, including null/empty values, errors,
coordinator movement, disconnects and capability downgrades. Each final peer
joined workers/listeners and checked closed/reusable ports and zero tasks.
Every final process was waited; Java checked that its Admin thread stopped.

Strict Rust checks and53 coverage-checker tests passed. Four extracted package
consumers each compiled23 examples. Source is published at
https://github.com/mingley/partitionline/tree/ade80f6b289a187a881daa4090cf201057c5ec39.
The working branch and index are unchanged. This is component/public-call
qualification against scripted peers. Live Streams coordinator behavior,
framework execution, production readiness and performance ranking remain open.

summary.json records bounds and differences from Java's Map/future API.
validation/ retains exact source, commands, class/fixture/wire/binary hashes,
publication records and first failures. FILES.json records sizes/modes;
SHA256SUMS covers this snapshot. Do not regenerate this qualification.
