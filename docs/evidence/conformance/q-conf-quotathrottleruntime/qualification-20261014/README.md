# KL01-25 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 40 Produce v9 / Fetch v12
quota-throttle bodies (8 per peer, 0 unsupported) for topic
`pl-quota`. Two independent generations per peer matched
byte-for-byte. Rust decoded every body leftover-empty and rejected
every truncated prefix.

Named cells follow hashed `ProduceResponse.shouldClientThrottle`
(v6+), `FetchResponse.shouldClientThrottle` (v8+), and
`FetchSessionHandler.handleResponse`: Produce throttle 0 / 250 / -1,
Fetch throttle 0 / 80 with records, and empty throttled full (80 ms,
session 91, no topics) reset to INITIAL.

A loopback broker served the Apache response bytes and advertised
PRODUCE max v9 and FETCH max v12. Public Producer replay on the 4.1.0
and 4.3.1 peers observed 250 ms quota metrics, waited before the next
send, and treated zero/negative `ThrottleTimeMs` as non-unmuting.
Public Consumer replay observed 80 ms Fetch quota metrics, waited
before the next fetch, and sent INITIAL (not incremental, not LEGACY)
again after an empty throttled full response.

When the same jars are present, Apache parsers reverse-read four named
cells per peer (20 parses).

The registered `feature-produce-fetch-quota-throttle-runtime` row
stays `not_run` at `cb7e97d3b92a8555aea34d59266a2990c206395f`. No
`cases.json` rows were added. `records.rs` and `buf.rs` were not
edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_quota_throttle_runtime.py` into a new
directory if the generator or pins change.
