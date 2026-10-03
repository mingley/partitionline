# ListTransactions all-broker regressions: source preparation

These WORK files are uncompiled and unexecuted. They are not conformance or
runtime acceptance evidence. No Cargo, JDK, compiler, SDK client or listener has
been launched for this preparation. The independently authored socket bodies
come from the pinned Apache schemas; the generator will separately execute the
official serializers and aggregation components after coordinator authorization.

`socket_peer.rs` owns three loopback listeners representing two broker IDs. The
third endpoint is a replacement address for broker 2. Normal discovery returns
exactly those two IDs and zero topic entries, after verifying the actual request
is Metadata1 with `Topics=[]`. The peer advertises required startup APIs19/20,
Metadata1, ApiVersions0 and the configured API66 range. Other Admin operations,
including FindCoordinator, are absent. Only the shared failing-first test permits
the actual old FindCoordinator6 transaction lookup for an empty ID and returns
broker 1; it then asserts the independently known four-row result. The corrected
implementation must make no coordinator lookup.

The same existing-public-API assertion lives in `all_brokers_regression.rs`.
For the old-main failing-first run, copy `before_existing_api.rs` to the isolated
`tests/list_transactions_routing.rs` target and install the same helper/shared
files. This avoids compiling newly added partial-result methods against old
main. For the corrected target, use the WORK top-level test file. Do not change
the common four-row assertion to obtain a pass.

There are 17 named Tokio tests. They cover conflicting duplicate IDs and per-node
origin; terminal errors15/16/29/53 and complete-only failure; mixed0/1 capability
and duration rejection; affected-broker-only loading retries capped at8; broker 2
movement with at most3 Metadata refreshes; stalled broker/delayed discovery/
post-deadline no-dispatch and cancellation; zero deadlines/invalid filters;
required-null fields, truncated counts, huge counts/trailing bodies; Metadata
topics/nullhost/duplicateIDs/truncated counts/byte bounds, including a nearly1MiB
unexpected-topic body; aggregate100000 listings and
aggregate16MiB response bytes. The large byte cases are synthetic bounded wire
resource controls, not authentic broker policy or heap-peak measurements.

Successful tests close the Admin and explicitly stop, drain and join every
listener and connection task before checking outcomes. A timeout/worker panic is
a failed run. Drop aborts only as emergency failure cleanup and never supplies a
successful shutdown verdict. Each actual request retains node/listener/version/
correlation/receive time and exact length-prefixed frame; each selected response
retains its exact frame plus whether the write completed.

Set `PARTITIONLINE_LIST_TRANSACTIONS_PROOF_DIR` to a fresh WORK directory during
each future run. After every task has joined, the helper writes exact request/
response frames, a bounded `frames.tsv` and `joined-ownership.txt` before outcome
assertions. Subcase directories are explicit and cannot overwrite an earlier
attempt. The old-source four-row assertion therefore retains its real frames
before failing. No proof directory has been generated in this preparation.

Peer limits:3 listeners,16 total accepted connections per listener,32 frames per
connection,192 capture rows,256KiB input frame,2MiB total captured request bytes,
17MiB response body,24MiB total captured response bytes,16 queued replies per
broker,24MiB total queued response bytes,2s client-operation/close budget and2s
per-listener join budget (at most6s across three listeners).
There is one large17MiB refusal frame; the cumulative-byte case uses two bodies
of about9MiB. The2MiB request-frame ceiling permits at most another2MiB of copied
request-body bytes. Captured payload Arcs are shared, not cloned across summaries.
Those explicit fixture envelopes do not establish production allocator peaks.
Prefer `--test-threads=1` for this finite resource suite. Request filters are
checked against actual captured bytes with an independent compact-field reader.

`ListTransactionsOracle.java` uses the actual
`MessageUtil.toByteBufferAccessor(message,version).buffer()` API, actual request
builders/parsers, AllBrokersStrategy, ListTransactionsHandler, AllBrokersFuture,
and public ListTransactionsResult all/byBrokerId/allByBrokerId. It generates16
full frame pairs, verifies that aggregation waits for both brokers, preserves
duplicate IDs, treats14 as a mapped retry and fails complete results on terminal
broker errors. The component fixture responses are policy-assembled inputs;
executing them is not evidence that an Apache broker produced those values.

Its separate `live` entry point invokes genuine public
`Admin.listTransactions(options)` and records byBrokerId plus complete outcomes,
with1s request,2s API/close and3s future waits. It requires a separately owned,
SDK-capable peer with appropriate Metadata/header negotiation; the Rust legacy
Metadata1/ApiVersions0 helper is not labeled as a qualified Java live server.
Authentication reconnect is not implemented by this plaintext fixture; that
acceptance needs an additional genuine bounded authentication history.

`prepare-and-run.py` is source-only future execution support. It pins exact
kafka-clients4.1.2/4.2.1/4.3.1 and SLF4J jar hashes; strict-compiles each SDK once
and performs two fresh component/generator runs per SDK. It preserves failed
commands, exits, logs, sources and class hashes in WORK, and compares all emitted
wire bytes across releases/replays. It caps JVM heap128MiB, subprocess45s,
logs512KiB,16 classes of at most1MiB and33 fixture files of at most512KiB per run.
JAR/class/ELF payloads never belong in Git. Generated outputs must be real actual
SDK artifacts; no placeholder vectors are installed during this preparation.
