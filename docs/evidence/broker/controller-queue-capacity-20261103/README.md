# Controller queue capacity

`ControllerHandler` validates a queue budget using the request limit, but each
queued job owns both a request and a spare vector. The current admission check
uses request length and does not bound either retained capacity or their sum.
The configured product therefore does not establish the stated queue-byte bound.

The [source evidence](evidence.tar.gz), [receipt](receipt.json) and
[independent review](independent-review.json) distinguish queued jobs from the
active actor, completed replies and caller-owned futures. Their small examples
are arithmetic models and proposed tests; no actor, Rust test or network case
was run. A fix needs a combined request/spare capacity policy, checked budget
math and controls for buffer reuse and error ordering.

A proposed [queue-byte budget](budget-design.tar.gz) remains blocked. Its
[independent review](budget-design-review.json) found shutdown interleavings
where a reserved slot can prevent the actor from being woken, plus a separate
post-close send risk. These are source findings that need deterministic native
controls. The design also needs to document connection-close and fairness
effects before it can be implemented.
