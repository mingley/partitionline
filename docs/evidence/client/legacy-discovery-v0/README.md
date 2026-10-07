# Legacy discovery

Metadata v0 uses a non-null topic array; an empty array means all topics.
GROUP coordinator discovery v0 has one key and no coordinator-type field.
The client negotiates these versions and preserves the fields they omit.
IPv6 metadata and coordinator addresses use brackets when connecting.

Named Metadata v0 queries require automatic topic creation to be enabled.
Admin can list all topics, but refuses named queries whose creation policy
cannot be represented. Controller and UUID operations refuse missing identity.
Transactions cannot use GROUP-only coordinator discovery v0.

Startup, metadata refresh and GROUP discovery keep their original timeout
across connection setup, fallback and retries. Legacy GROUP discovery admits sixteen
bootstrap addresses and eight attempts. Metadata v0 decoding checks body,
collection, string, decoded-storage and sparse partition-index limits before
allocating route tables. Coordinator discovery uses the returned address
when replacing a stale v0 route.

Apache SDK 4.1.2, 4.2.1 and 4.3.1 generate and parse selected complete bodies.
Actual Java and Rust callers run against owned scripted peers. The current
Java Metadata builder refuses v0; those refusals remain part of the results.
Java GROUP discovery v0 succeeds when Metadata is newer. These checks cover
client behavior; they do not qualify a live broker or comparative performance.

Fault profiles cover loading, unavailable coordinators, wrong coordinators,
disconnects, address changes, authorization failures and operation deadlines.
Java Admin treats NOT_COORDINATOR during discovery as terminal; Java Consumer
and Rust Admin retry it. When the node ID stays the same, Java Admin reuses a
live socket at the old address; the other callers follow the returned address.
A move to a different node is followed by all callers. At the original call
deadline, Java Admin cancels the in-flight request with DisconnectException;
Java Consumer reports TimeoutException. The checks record elapsed call time.
