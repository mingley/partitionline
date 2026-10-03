# Ordinary persisted Fetch/ListOffsets

The explicit `Router::open_with_read_store` profile serves Produce3–13,
Fetch4–6, ListOffsets1–3 and the existing four metadata/admin APIs. The
metadata-only constructor remains four entries and Produce-only remains five.
No incremental Fetch sessions, replica fetches, retention, transaction manager,
replication, consumer groups or production/performance qualification is implied.

One blocking actor owns Catalog and Store. UUID/name/partition resolution,
append, deletion and bounded reads are serialized. Unknown unmaterialized but
catalogued partitions return empty/HW0 without creating files. Deleting and
recreating a name resolves its new UUID; historical journals remain charged to
existing store/disk/index budgets and cannot reappear through that name.

Fetch returns complete batches, including the containing batch for an interior
offset. The first nonempty batch across the ordered request may exceed request
and partition byte limits, within the hard response ceiling. Later batches that
do not fit remain available at their original offsets. Recovered entries with
multiple batches are filtered at batch boundaries. ListOffsets searches in
record offset order; the nonmonotonic 1000,1007,1003 vector selects timestamp1007
at offset1 for query1003. Earliest/latest are 0/HW and have timestamp-1. Unsupported
negative timestamp selectors return35. Legacy Fetch4/5 storage errors map56 to6;
Fetch6 uses56. Per-partition error record arrays are empty. Timestamp and fetch
scan exhaustion close with an explicit resource error, preserving storage; they
cannot become a false timestamp miss or skip retained data.

Waiting happens outside the actor and retains the bounded data permit and
original request only. Every iteration captures the watch generation before
enqueueing its serialized snapshot. Committed append/catalog changes and known
handle poison transitions notify before sending receipts, including canceled or
lost receipts. Candidate output bytes are released before waiting. The original
admission instant fixes the request wait deadline; configured wait limits and
transport handler deadlines may shorten it. Normalized minBytes is clamped to
maxBytes, following pinned KafkaApis. Stop wakes waiters; an unexpected worker
exit closes the actor-owned watch sender. Cancellation drops queued receivers
and releases data admission. Active reads cannot mutate storage; active append
cancellation still has the existing ambiguous synchronized-write contract.
TCP peer disconnect itself is observed by the transport's bounded read/handler
lifecycle, not an extra per-read disconnect monitor.

`fetch::Limits` bounds each snapshot to default16MiB visited journal payloads,
65536 entries and60s admitted wait; positive ceilings are64MiB/65536/600s. Entry
ownership overhead is included in the lower journal call while logical scan
bytes count payloads. Cancellation is checked at partitions, batches and each
searched timestamp record. The existing response cap remains absolute even for
an oversized first batch. `with_retained_bytes` defines one combined admitted
request/output ceiling, default/max1GiB: startup enforces
`maxQueued * (maxRequestBytes + maxResponseBytes) <= maxRetainedBytes`.
Long-poll inputs and completed unconsumed input-plus-response replies retain
those permits. Existing request/output512MiB envelopes still apply separately.
Actor scan buffers, parser descriptors, allocator overhead, transport/caller
ownership, OS buffers and RSS are outside this retained byte envelope and have
separate existing limits. The single actor holds at most one visited entry.

ReadCommitted reports ordinary-only LSO=HW and no aborted transactions because
transactional, control and idempotent writes remain explicitly rejected. Tests
replay those rejection fixtures through the read/write constructor and compare
unchanged read-committed bytes. This does not implement transaction isolation for
a future transactional producer.

The committed KL11-68 fixture seed and provenance provide366 read outcomes
across Apache4.1.2/4.2.1/4.3.1:330 complete responses and36 structural rejections.
Their assembler uses actual Apache encoders/parsers; independently executed
Apache LogSegment probes support the batch and timestamp policies. They do not
represent a full Apache KafkaServer session. Complete local body parsing rejects trailing bytes even where the Apache
message parser can leave a tail; duplicate selectors and invalid client-only
replica/isolation/byte settings have an explicitly stricter local42 policy.
The direct Rust test compares every
response exactly and emits each actual response via
`PARTITIONLINE_FETCH_RESPONSE_DIR`. `PARTITIONLINE_FETCH_REPORT` contains
schema1, seven `read_write_api_versions`,366 `case_results`, and15 actual
`api_versions_cases` (versions0–4 for all three request fixture releases).

Nineteen integration tests additionally exercise restart and multi-batch entries,
current UUID after delete/recreate, lazy empty reads, scan/output boundary errors,
absolute waits/minimum clamp, an intentionally unpolled completed snapshot,
a deterministic append between snapshot and receipt polling, admission/cancel,
append/delete while waiting, shutdown wakeup, registered-handle corruption and
poison wakeup, whole-batch global limits/order, ordinary readCommitted, malformed
counts/prefixes/duplicates, actual TCP goldens/unsupported EOF, transport deadline
cancellation and joined transport shutdown. The live harness is inert normally;
`serve_live_probe` uses `PARTITIONLINE_FETCH_LIVE_PORT` and
`PARTITIONLINE_FETCH_LIVE_DIR`, writes `ready`, and exits on `stop` or180s.

`development/` honestly records precommit dirty-tree checks, including initial
failures. The first invocation was observed through PTY without a configured raw
log; its summary and first mismatching wire bytes are retained and explicitly
labeled. Later invocations retain full stdout/stderr. `run-pinned.py` archives the
complete exact pushed Git tree, including configuration and all fixtures and runs stable
and Rust1.85 default/all-feature all-target tests, format, strict Clippy, strict
rustdoc and both doctest configurations. Final immutable outcomes and actual
independent Java/native runtime receipts will be bound in FINAL.md; those are
not inferred from the development tests.

The first immutable attempt archived4387 selected files and omitted committed
root clippy.toml. Its strict Clippy run correctly failed83 catalog-test unwrap
assertions under that incomplete input set. `final/` preserves the failed logs
and explicitly incomplete receipt. The corrected `final-qualified/` uses all
17370 committed Git blobs/configuration from58810 and checks the complete file
set and Git identities before and after every command. This was an execution
harness prerequisite correction; no production or test source changed.
