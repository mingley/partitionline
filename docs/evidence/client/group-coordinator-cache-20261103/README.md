# Group coordinator cache prototype

This [source-only prototype](candidate.patch.gz) retains one negotiated group
coordinator connection to avoid repeated discovery and handshakes. Its ten
new controls have not been compiled or run, and no speedup has been measured.
The original v10 Mock Produce tests and limits remain unchanged.

An [independent review](independent-review.json) found four blockers: a
cached TCP descriptor can remain owned after shutdown; physical retirement is
not witnessed before redial; provisional discovery and fallback connections
can lose their owner on cancellation; and close can return success while a
parked request still owns the slot. The prototype must not be adopted until
those paths are repaired and qualified. Exact patch reconstruction passed;
no native tests, live Kafka, TLS/SASL or performance results exist.

A [partial follow-up](candidate-v2.patch.gz) attempts physical retirement and
adds Linux descriptor controls. Its [review](v2-independent-review.json) still
finds unresolved provisional and fallback ownership, and one control does
not prove retirement before a replacement is accepted. It is unrun and remains
unsuitable for adoption. The proposed [ownership design](ownership-design.md)
sets out the remaining work.

A later [supervised draft](supervised-v1-evidence.tar.gz) adds bounded
connection lanes and explicit socket ownership. Its [independent review](supervised-v1-review.json)
found a conflicting eviction test, an existing close-cancellation gap,
ordering limited to individual retry attempts, possible lane saturation by
one group, and no bound on request bytes waiting before admission. It has not
been compiled or run and must not be adopted in its current form.
