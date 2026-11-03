# Producer pending queue

The [design](DESIGN.md) explores replacing repeated vector prefix shifts with
a worker-owned deque. It describes the required ordering, cancellation,
deadline, backpressure and memory behavior, as well as the comparisons needed
to measure the change. No benchmark result is claimed.

The [independent review](independent-review.json) records the unresolved
implementation details and verifies the source pins; the archived receipt
predates that review. The [evidence archive](design-evidence.tar.gz)
preserves the earlier design and the hosted v14 failure record. The v14 job
did not expose the failure body, so the cost of shifting has not been shown to
cause that failure. The design must be rebound and reviewed for later sources.

An isolated [prototype](prototype-v2-evidence.tar.gz) explores the deque and
preserves the earlier draft. Its [review](prototype-v2-review.json) says not
to adopt it: the code is uncompiled, and conditional allocation-unwind paths,
memory limits, scheduling and cancellation still need work. The
[addendum](prototype-v2-unwind-addendum.json) records those source findings;
no failure or speedup has been observed for this prototype.

A later [source analysis](segmented-obstruction-evidence.tar.gz) and its
[review](segmented-obstruction-review.json) show why retiring queue segments
separately can change retry backoff, metadata refresh and callback observations.
Preserving those semantics without allocating during cleanup needs a broader
retry or queue design; no v3 implementation was produced.
