# Streams heartbeats

StreamsClient sends typed API88 v0 heartbeats to the GROUP coordinator. The
caller supplies member IDs, epochs, topology and task changes and receives the
full response. Coordinator errors retry under one deadline; terminal errors
remain typed. A successful call caches one socket, so subsequent heartbeats
use one RPC. Cancellation/error drop owned sockets. Close sends no leave.

Latest-stable Rust passed12 focused tests per feature build and2,023/2,035
full-suite tests. Default and all-feature builds each ran135 public Rust calls
using inputs from three actual Apache SDKs. Each SDK independently parsed54
heartbeat frames,54 GROUP lookups and45 returned responses. Actual builders
and error factories cover epochs0/7/-1/-2, null/empty and coordinator/member/
Streams errors. All final processes were waited. Each peer joined listeners
and workers and checked closed, reusable ports and zero runtime tasks.

Warm reuse, close, group change, downgrade, delayed replies, cancellation,
correlation errors and admission bounds have socket tests. Cancellation observes
EOF while the client remains alive, then reuses that client successfully.
Strict Rust checks and53 coverage-checker tests passed. Four package consumers
compiled22 examples each.

Exact source is published at commit16b7a14d42e77e4541135182d80f0038e1784c5c
on the codex/finish-open-cards-20261007 review branch. The checked-out branch and
index remain unchanged. Execution archives are retained locally. This
qualification does not establish live Streams coordinator behavior, framework
execution, production readiness or a speed ranking.

`summary.json` records scope and limits; `validation/` preserves source, command,
class, wire, binary, publication and earlier failure records. `FILES.json`
records file modes and sizes; `SHA256SUMS` covers this snapshot.
