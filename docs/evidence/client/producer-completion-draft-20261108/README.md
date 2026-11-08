# Experimental producer completion draft

This branch contains an incomplete experiment. Do not merge it as a producer
fix. The accepted-record ticket is not a rooted owner for its payload,
receipt, callback, byte reservation, sticky state, or Kafka sequence range.

The draft reserves up to 100,000 admission slots, attaches a non-cloneable
ticket when a worker accepts a record, carries the ticket with Pending through
retries, and has flush wait for its first-poll cutoff. A dropped unfinished
ticket causes a fail-closed flush error. Normal ticket completion occurs after
the current callback and receipt publication code. This does not repair
cancellation cleanup: an unexpected drop can still lose the receipt, retain
bytes, and run arbitrary drop or callback code on the runtime thread.

Known gaps include Kafka batch sequence reservation and ambiguous writes,
transaction-generation fencing, canceled flush error ownership, callback
panic and wake behavior, receiver publication unwind, close/shutdown ownership,
worker starvation, sticky cohort boundaries, and the compatibility impact
of the new hard record cap. The existing error-reporting path is retained.
The five-millisecond polling fallback changes latency and is not qualified for
production. Any successful focused test applies only to its stated fixture.
