# Static review

An independent reviewer examined the initial epoch-fence draft and the later
replica-limits candidates. The epoch draft introduced an extra journal record
for a higher epoch. A valid one-entry journal could then be consumed by an
empty or no-op epoch adoption, leaving no capacity for the first data record.
A durable epoch store needs a capacity and downgrade design; that draft was not
built or integrated. Its code also could not establish leader authority or a
committed-watermark truncation guard.

The review found that a follower could copy a source payload above its own
configured maximum, then reject the persisted journal on reopen. The encoded
record header actually occupies 25 bytes, while the earlier limit check used
21. The review also identified deterministic journal admission failures being
reported as poisoned storage, a fixed recovery batch size that exceeded small
journal index limits, and missing protection against a poisoned source log.

In v1, preflighting only the follower payload missed the combined journal space
needed for truncation and its first replacement. V2 checked those byte and
index limits, but had not reserved index capacity and did not account for a
process exit between the two durable writes. The review also identified that
valid earlier records could be blocked by validation of later records outside
the actual request or journal capacity.

V3 reserves the two index slots, computes an eligible bounded prefix, documents
that publication remains non-atomic, and includes targeted regression cases.
The independent review found no further blocking mismatch by source inspection;
it did not compile or run the tests. The separate v3b receipt records the test
and lint results. Fencing, committed-watermark safety, crash and network
qualification, unbounded legacy-history growth, and prefix-scan performance
remain open.
