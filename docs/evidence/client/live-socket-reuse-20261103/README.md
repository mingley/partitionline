# Live socket reuse

This [design](DESIGN.md) and its [control plan](CONTROLS.md) explore reusing an
actual negotiated connection to address repeated handshakes in the v16 mock
failures. The [review](independent-review.json) checks source pins, ownership
and the distinction between the v16 output and earlier wire traces.

The producer can have a partial write or a worker-owned response in flight
even when the connection's own in-flight counter is zero. Safe handoff must
also account for session expiry, cancellation, correlation state and physical
socket retirement. These are unrun control specifications, not an implemented
fix or a measured speedup. The original 450 ms test remains failing.
