# Client v14 mock-producer failures

The hosted v14 qualification reported the same 15 `mock_produce` failures in
the default and all-feature targets. It reported 32 passed, 15 failed and none
ignored in each profile. The decoded job log lists test identities but not the
panic bodies or per-test traces, so the precise v14 failure phase and elapsed
time for each test are unknown. The failed qualification reports artifact was
not accessible through the available supported downloader.

Fourteen failing tests contain handshake-count, reuse or latency expectations;
the fifteenth, `send_all_redials_leaders_while_refreshing_metadata`, asserts a
separate 450 ms end-to-end behavior. `cases.json` maps each identity to its
original source line and its distinct trigger or assertion. A shared root
cause for all 15 has not been established.

The v14 source explains why several expectations conflict with its current
ownership policy. `open_produce_socket` always opens a connection and performs
ApiVersions; `open_conn_with_known_versions` also ignores the saved ranges and
negotiates the new socket. Startup keeps the selected metadata socket, records
the other completed bootstrap handshakes' versions, and retires those other
sockets. The metadata, produce-worker and transaction-coordinator roles own
separate connections. See `src/producer.rs:1998–2087, 2135–2160, 4453–4504`.

For example, `produce_reuses_bootstrap_api_versions` expects no additional
ApiVersions after startup and then requires the first produce to finish in less
than 200 ms after a 250 ms mock negotiation delay. In the source, a first
worker opens a fresh, separately negotiated socket. Likewise,
`transactional_startup_skips_a_second_bootstrap_handshake` expects exactly one
ApiVersions response even though same-address transaction discovery opens a
dedicated, newly negotiated coordinator socket. These are deductions from the
source and test setup, not v14 panic locations. In particular, an earlier
assertion or another error may have stopped an individual test first.

The four offset-commit cases exercise a different connection lifetime:
`group_coord_roundtrip` discovers a group coordinator into a local connection
and returns without retaining it for the next call (`src/producer.rs:5519–5589,
5686–5745`). A second successful call therefore needs another live connection
unless ownership is changed. The independently reviewed supervised-cache draft
is not qualified: its v1 added eviction test contradicts its eight-lane cache
policy, and its ordering and cancellation limits remain open. Do not substitute
that draft for a validated fix.

Version lists cannot safely be treated as proof for a newly dialed socket: an
address may resolve to a replaced or upgraded broker, and SASL authentication
and its lifetime belong to the live connection. A feasible performance design
retains or transfers an actual negotiated, authenticated socket, or introduces
a bounded live-socket owner that serializes/multiplexes requests safely. It
must preserve per-socket versions, correlation, response ownership, idle and
authentication expiry, cancellation, physical retirement, deadline and close
semantics. Sharing a socket among metadata, producer and coordinator roles also
needs a design that keeps the existing concurrency and idle-close controls.

The 450 ms `send_all` case remains an end-to-end goal, not a handshake-count
assertion. In a separate earlier v7 run, its recorded full time was 756.313 ms;
an earlier v10 trace showed a 250 ms metadata-socket negotiation, a subsequent
250 ms Metadata response, and another leader negotiation after speculative
leaders exceeded the configured 40 ms idle interval. Those are historical
measurements, not v14 timing evidence. The v14 job reports the original test
failed; its exact v14 timing and trace remain unavailable. The added overlap
diagnostic is a separate test and does not replace the original 450 ms bound.

## Discriminating controls

Keep all existing identities, counts, deadlines and assertions unchanged. Add
separate diagnostics that reproduce the original settings and retain bounded
wire events for Accept, ApiVersions request/response, application request and
response, and Close, with node, physical socket, version and monotonic order.
Use explicit response barriers rather than timing alone. Before asserting,
record the operation result and sanitized bounded trace even on an early error.

1. For first produce and same-address transaction startup, compare the socket
   used by the application with the socket whose successful ApiVersions and
   authentication preceded it. Distinguish a reused live socket from a second
   socket carrying only an address-cached version list.
2. For repeated group offset commits, hold the first response until observed,
   then capture both commits' socket IDs. Repeat after coordinator movement,
   cancellation, idle expiry, changed advertised versions and SASL expiry;
   require the original deadline and verify old physical retirement.
3. For `send_all`, use the already present overlap diagnostic plus the original
   end-to-end test. Capture metadata negotiation/response, each leader socket,
   every actual Produce and both acknowledgements. An improvement must pass the
   original `<450 ms` assertion; overlap alone is not a substitute.

The current qualification runner stores per-target logs as artifacts and prints
only summary rows. To make a rerun actionable when artifacts cannot be
downloaded, add a bounded, explicitly sanitized failure-only event projection
to the job output, while preserving the complete original log, failure status
and all assertions. This is a proposed diagnostic, not a completed patch or a
measured improvement. No Cargo, native tests or benchmark ran locally.
