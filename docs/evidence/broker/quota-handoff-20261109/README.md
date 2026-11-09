# Produce quota handoff: implementation plan and controls

Status: design and isolated controls. The broker does not enforce Produce
bandwidth quotas or send nonzero Produce throttle times. These notes propose a
limited first policy; they are not a wire-compatibility or production claim.

The [earlier design](../quota-produce-design-20261108/README.md) records the
Kafka 4.1 source comparison and why `quotas::Window` cannot be attached
directly to the listener. This note specifies a smaller policy and identifies
a transport constraint that applies *before* a mute result is available.

## Read-ahead before the result

At the pinned baseline, during a handler call, the transport's disconnect
watcher consumed and validated pipelined input. Its buffer was bounded to the
configured maximum request size plus the four-byte prefix. After a complete
frame, the watcher continued reading to detect disconnects. Two individually
valid frames could exceed that combined bound and close the connection as an
invalid length before the handler returned. See
[`watch_disconnect`](https://github.com/mingley/partitionline/blob/133d2413217cee4130bca2267e4e36b7225ff86f/partitionline-broker/src/transport.rs#L1377-L1432).

Consequently, turning prefetch off only after receiving a quota deadline is
insufficient at that baseline. Sleeping in the handler also delays the response
and retains a handler permit. A quota-aware exchange needs bounded backpressure
both while the handler runs and after it returns a mute deadline. A separate [transport candidate](https://github.com/mingley/partitionline/commit/05d6d6faae819f4fb7c23baba73892d9067ea473)
addresses the pre-result boundary; it does not implement quota enforcement.

The watcher can read only the remaining capacity, validate what it has read,
and stop reading when that capacity is full. It then waits for the handler
rather than closing on more queued socket data. It can still
detect an EOF while capacity remains. If unread data fills the buffer, a
later EOF cannot be observed until the handler ends or its existing deadline
expires; shutdown must still cancel and join the connection. This changes disconnect-detection latency and does not promise immediate
detection behind unread bytes. Keep existing handler and
authentication deadlines. After a quota result, response writing and the
mute wait must not prefetch.

## Small first policy

Introduce a separate, validated policy and a distinct quota-enabled router
type. The initial policy can be a single bandwidth bucket per router actor:
every Produce request uses it regardless of client ID, principal, connection,
or reauthentication. This requires no identity allocation, preserves debt
across reconnects while the actor is alive, and avoids accidentally treating a
peer address or client-supplied ID as an authenticated identity. It is not a
Kafka per-user or per-client quota and does not isolate tenants. Independent
routers have independent buckets, and restarting the actor resets its
in-memory debt. These limitations must be explicit in its public docs.

Use a positive byte rate, burst capacity, and maximum displayed mute. Keep
the rate meter separate from the existing data semaphore and leases. A
possible exact-integer token bucket stores credit or debt in byte-nanosecond
units, where one byte costs 1,000,000,000 units. On each charge:

1. Read the trusted monotonic clock. Refill by elapsed nanoseconds times the
   byte rate, clearing debt first and limiting positive credit to the burst.
2. Charge the original header-plus-body length, without the four-byte frame
   prefix. Keep negative balance as debt.
3. Convert debt to milliseconds by rounding up, then clamp the reported delay
   to the configured maximum and Kafka's signed 32-bit field. Keep all excess
   debt in the bucket.
4. Form an absolute resume deadline from the same clock and the reported
   delay with checked arithmetic.

In production, the actor can use `std::time::Instant` and the transport can
convert that same deadline to `tokio::time::Instant`. Tests need an explicitly
shared clock/timer abstraction: pausing Tokio time does not automatically
advance `std::time::Instant` in the blocking actor.

Use checked wide arithmetic. A refill multiplication that exceeds the
representation can safely fill a finite bucket; a charge or debt overflow
must instead enter a documented fail-closed state, never reset debt to zero.
An unrepresentable deadline closes that connection while retaining debt.
This token bucket has different numerical behavior from Kafka's sampled rate
and must not be described as equivalent.

Charge at an actor-owned completion point, after Produce processing, even if
the response receiver has gone away. One precise initial rule is to charge
every Produce job that the actor starts, including parse/processing failures
and cancellations after start; jobs canceled before the actor accepts them,
or rejected before Produce processing starts, are not charged. This charges
some failed requests and is a documented policy choice. A crash or process
restart can lose in-memory debt.

The grouped path must retain each original byte count independently of the
receiver and must charge each started request once, in actor order, including
`acks=0` and dropped replies. Do not attach quota release to a handler future.
Preserve the existing lease and grouped-buffer ownership. See the single and
grouped paths in
[`metadata.rs`](https://github.com/mingley/partitionline/blob/133d2413217cee4130bca2267e4e36b7225ff86f/partitionline-broker/src/metadata.rs#L1155-L1289).

## Handoff and API compatibility

A provided handler trait method can return a new opaque controlled outcome
containing the existing `ExchangeOutcome` plus an optional absolute resume
deadline. Its default delegates to the existing `exchange_request` and has
no deadline. This avoids adding
fields to public `ExchangeOutcome` and router configuration structs, so
existing struct literals remain valid. Adding trait methods still requires
checking downstream source compatibility, including method-name collisions.

The quota-enabled router must not expose a path that silently discards its
deadline. A separate type can keep the underlying router private, implement
the controlled exchange, and reject its legacy handler entry points with a
specific error. Any additional direct-dispatch API must return the control
alongside the response and require the caller to enforce it.

The Produce writer already reserves an `i32` throttle field for supported
versions 3 through 13. Capture the offset while writing instead of reparsing
the response or assuming tagged fields will always be empty. After processing,
the actor writes the computed delay there and returns the same deadline with
the outcome. A successful `acks=0` has no response bytes but still carries
the deadline. Errors that close the connection still retain the charge.

The transport writes a response promptly, then waits only until the absolute
deadline before processing the next frame. A slow write consumes part or all
of the wait; it must not restart the full delay. Release the handler permit
before waiting. Keep already buffered bytes, but perform no reads while muted.
A no-response outcome takes the same wait path. Do not apply the ordinary
handler timeout to the mute. Keep shutdown and authentication expiry or
revocation effective during the wait; if the authenticated session expires
first, close rather than processing another request under expired authority.
The read deadline for the next request starts after the mute, not before it.

## Controls and remaining qualification

The [baseline controls](controls.rs) exercise a small valid pipeline and two
individually valid maximum-size frames while the first handler is held. At
`133d2413217cee4130bca2267e4e36b7225ff86f`, both passed under default and
all features in [run 42663188859](https://github.com/mingley/partitionline/actions/runs/42663188859).
They reproduce the old close behavior and are not regression expectations for
the backpressure change. To rerun them, copy the file to
`partitionline-broker/tests/quota_transport_boundaries.rs` in a clean checkout
of that baseline and run:

```sh
cargo test --locked --manifest-path partitionline-broker/Cargo.toml --test quota_transport_boundaries
```

The existing grouped Produce test
[`grouped_produce_replies_keep_their_own_admission_permits`](https://github.com/mingley/partitionline/blob/133d2413217cee4130bca2267e4e36b7225ff86f/partitionline-broker/src/metadata.rs#L4179-L4245)
also drops an unconsumed reply after the actor passes a barrier, then checks
the next append's offset. Accounting in the waiting handler would miss
completed actor work in that case.

A proposed policy is not a completed implementation. Before enabling it,
add deterministic meter tests for fractional refill, burst, cap with
persistent debt, overflow, multiple connections and reconnects; socket tests
for immediate responses, `acks=0`, v3/v5/v6/v8/v9/v13 fields, pipelining
and a slow response; grouped and canceled work tests; shutdown and
authentication-expiry tests; and live Kafka client and pinned reference-broker
comparisons. Run default and all-feature broker checks and report the exact
benchmark environment and results. These controls do not establish production
readiness or a performance ranking.
