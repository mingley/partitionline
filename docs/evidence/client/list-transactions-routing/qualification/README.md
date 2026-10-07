# All-broker transaction listing

ListTransactions discovers every broker, negotiates each connection and keeps
broker IDs with partial results. Complete results fail if any broker fails.
Successful listings concatenate without hiding duplicate IDs or conflicting
states. Discovery, reconnects and retries share one caller deadline. Broker,
filter, response, retry and refresh limits are explicit.

The final source passed17 focused tests and72 Java/Rust process profiles in
each feature build. Actual Apache4.1.2/4.2.1/4.3.1 SDKs generated17 exact
request/response pairs each and exercised genuine public Admin calls. Tests
cover terminal errors, mixed0/1 negotiation, loading, stalls, discovery delays
and authenticated reconnects. All owned processes/tasks and ports close.
The old unchanged source returned two rows where the same assertion requires
four; its real empty-key coordinator lookup and missed broker are retained.

The default suite passed2,004 tests and all features passed2,016. Strict
Clippy, formatting and rustdoc pass. Raw frames, actual SDK classes, executable
hashes, original failures and source snapshots are retained in validation/.
summary.json records bounds and scope. This is client-component qualification;
live transaction-state recovery, API66 v2, production readiness and performance
ranking remain separate work.
