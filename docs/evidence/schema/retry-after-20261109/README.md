# Schema Registry Retry-After

9 November 2026

The read-only Schema Registry client previously ignored `Retry-After` on
HTTP 429 responses. The parser stopped at the status line before reaching
headers. Separately, the chunked response path discarded the parsed value.

The client now reads the first `Retry-After` header for all three supported
response framings. Nonnegative decimal delay-seconds are capped by the
configured maximum backoff and remain subject to the overall request
deadline and attempt limit. HTTP-date, malformed and overflowing values
use exponential backoff. This changes the timing and outcome of some
lookups: a request can now time out where it previously retried sooner.
The schema companion has not been published.

## Controls

[Run 42665048946](https://github.com/mingley/partitionline/actions/runs/42665048946)
used Rust stable on an x86_64 hosted runner. The workflow formatted the
candidate before testing; the tested `partitionline-schema/src/registry.rs`
Git blob was `6dc764d026718509575854ecb3955d00c44e77a0`.

The public-client loopback tests returned a 429 with `Retry-After: 3600`,
followed by a successful response. With a 700 ms overall deadline, 2 s
maximum backoff and 5 ms initial backoff, each original-source control
(Content-Length, chunked and close-delimited) retried and succeeded in
about 10 ms instead of returning the expected timeout. The candidate
returned `Timeout` after one request for each framing. A parser-only
mutant passed Content-Length and still failed chunked, verifying the
separate response-header propagation fix.

Other controls cover zero, mixed-case names, multiple fields, malformed
values, overflow, HTTP-date fallback, capped delays, exhaustion, and
dropping a pending retry. The cancellation control observed one request
for longer than the capped delay after the future was dropped; it does not
prove behavior for an external registry.

Nine focused retry controls passed. Full schema default, all-feature and
no-default-feature test profiles passed, as did format, all-target
all-feature Clippy with warnings denied, and all-feature documentation
with warnings denied. These are mock and offline checks, not a live
Schema Registry or Kafka interoperability test.
