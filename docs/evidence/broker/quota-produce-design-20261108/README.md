# Produce bandwidth quotas: integration design

> Update: The transport now applies backpressure when read-ahead is full.
> A close behind the full buffer may be observed only after the active handler
> finishes; the handler deadline and shutdown remain effective. The behavior
> described below refers to the pinned source baseline.

Status: design and local controls only. The broker does not yet apply
`quotas::Window` to listener traffic or send nonzero Kafka throttle times.
No wire implementation or real-client interoperability result is claimed here.

## Current boundaries

The Produce request header can contain a null, empty, or long client ID.
[`RequestHeader::parse`](https://github.com/mingley/partitionline/blob/0c9ab1e712a043a2acc5fa56963cff2f3e5ccc91/partitionline-broker/src/protocol.rs#L100)
accepts those values, while `Window` requires a nonempty client name of at
most 249 bytes without control characters. A direct conversion would reject
requests that the header parser accepts. A client ID is supplied by the
client and can change between requests on a connection. The peer address is
not an authenticated principal; verified SASL identity is optional.

`Window::admit` combines bandwidth charging with in-flight and identity
capacity limits. It refuses a charge when accumulated delay exceeds
`max_throttle_ms`, rather than recording the excess debt and returning a
wire delay. The local error cannot be translated generically to Kafka error
89. This can happen even when no work remains in flight: with a rate of one
byte per second and a 1,000 ms limit, three one-byte charges at the same time
consume the initial token, add one second of debt, then refuse the third
charge. Independent connections using the same ID can create that sequence.

The router admits data work with an owned semaphore permit, then transfers it
with the queued request and unconsumed response. Produce batches split those
leases into parallel request and sender arrays. The actor owns the actual
processing and response encoding, but the transport currently receives only
response bytes and reusable buffers. The [Produce writer](https://github.com/mingley/partitionline/blob/0c9ab1e712a043a2acc5fa56963cff2f3e5ccc91/partitionline-broker/src/produce.rs#L2125)
writes zero for the throttle time. A wrapper around the handler cannot
safely release actor-owned work merely because the caller disconnected or
timed out.

The listener immediately reads another request after a no-response outcome.
It also reads ahead while a handler runs and while a response is written.
Extending the existing disconnect watcher into a long mute could consume
pipelined data and close the connection at the read-ahead bound; Kafka channel
muting instead stops processing subsequent requests and applies socket
backpressure. Sleeping inside the handler is also incorrect: the normal
handler timeout is 30 seconds, while the window permits a 60-second delay.

## Reference behavior

The following is a source review of Apache Kafka 4.1.0 at
[`13f70256db3c994c590e5d262a7cc50b9e973204`](https://github.com/apache/kafka/commit/13f70256db3c994c590e5d262a7cc50b9e973204),
not a live comparison:

- The [Produce handler](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/core/src/main/scala/kafka/server/KafkaApis.scala#L479-L520)
  records quota after processing, mutes the channel, and sends the response
  without waiting for the mute to expire. Successful `acks=0` has no
  response but can still incur a bandwidth mute.
- The measured [request size](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/core/src/main/scala/kafka/network/RequestChannel.scala#L95-L99)
  is the parsed header plus body, excluding the four-byte network frame
  prefix. The request-quota path is separate and is not enforced for
  `acks=0` in that handler.
- [Produce clients](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java#L294)
  use client-side throttling from version 6; older clients rely on the broker
  withholding processing of the next request. See also
  [KIP-219](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/74685726/KIP-219+-+Improve+quota+communication).
- Kafka's [quota manager](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/core/src/main/scala/kafka/server/ClientQuotaManager.scala#L206-L245)
  records a sampled rate. The local window is a token bucket, so the two
  algorithms should not be presented as numerically equivalent.

## Required integrated change

A safe opt-in path needs the following pieces together:

1. **Explicit policy and identity.** Resolve a typed key from the verified
   principal and request client ID under a documented policy, including
   plaintext, null and empty IDs, reauthentication, changing IDs, and
   oversized keys. Do not use the socket address as proof of identity or
   silently truncate a key. Bound both entry count and key bytes. If a
   shared overflow bucket is used, retain its debt and do not move a
   previously untracked identity into a fresh full bucket before that debt
   clears; otherwise reconnecting can escape the limit. Document any
   collateral throttling of identities sharing the overflow bucket.
2. **Rate accounting separate from work ownership.** Keep the existing
   data-lease budget through queueing, execution, cancellation and reply
   handoff. Record the actual Produce charge at a defined actor-owned
   completion point, including grouped batches and successful `acks=0`.
   Specify what is charged when parsing fails, cancellation races a durable
   append, a response is dropped, or shutdown drains queued work. A
   bandwidth meter must preserve debt when a displayed delay is capped; it
   must not turn routine quota pressure into a fabricated Kafka error.
   Decide whether the policy implements Kafka's sampled-window calculation
   or a separately documented token-bucket approximation.
3. **One trusted clock.** Derive times inside the broker from a monotonic
   clock shared by actor and listener; do not accept caller-supplied future
   milliseconds. Carry an absolute resume deadline with checked arithmetic,
   and define how a delay larger than the configured limit or wire
   `i32` can represent is handled without erasing debt.
4. **Versioned response and listener handoff.** Carry the per-request delay
   through the ordinary and grouped actor paths into the Produce encoder,
   and return the mute deadline to the transport even when there is no
   response body. Send the response promptly, then defer the next request
   until the absolute deadline. Preserve already-buffered data, avoid further
   prefetch while muted, and keep shutdown and authentication invalidation
   responsive. Define the bounded handling of peer close and idle timeout
   during the mute. A long or slow response write must not restart the full
   delay.
5. **Compatible entrypoints.** `ExchangeOutcome` and the router
   `Config` have public fields, so adding fields breaks struct literals.
   One possible additive design is a new defaulted handler method returning
   an opaque controlled outcome; existing handlers would default to no mute.
   An opt-in router constructor can accept a separate quota policy.
   Direct router dispatch must either return the control outcome or reject
   the quota-enabled path, rather than discarding its mute instruction.
   Confirm downstream source compatibility and document the new API.

The quota state should be shared by requests from every listener using the
same router, survive a connection closing, and retain debt until safely
reclaimed. Its restart semantics must be documented. Avoid a full scan of
all identities in the request hot path: bounded count does not by itself
bound latency. Any expiry index must also be bounded under repeated updates.

## Verification needed before enabling

Use deterministic clock and socket tests for the default-off behavior,
Produce versions 3, 5, 6, 8, 9 and 13, null and changing IDs, multiple
connections, nonzero throttle time, immediate responses, pipelining and
`acks=0`. Cover the single and batched actor paths, queue rejection,
disconnect before and after processing, timeouts, authentication expiry,
shutdown, capacity exhaustion, arithmetic limits, and long response writes.
Check that no work or debt is prematurely released or silently reset.

Then test real pinned Kafka clients against the broker and compare the
observable response and mute behavior with a pinned reference broker. Record
the client and broker versions, settings, results, and failures. These tests
are not present yet. Performance measurements require a separate matched
comparison; these controls do not establish a speed ranking.

## Local controls

The two controls in [`controls.rs`](controls.rs) use the public parser and
window APIs. They do not create sockets or issue Kafka requests. On x86-64,
[run 42499121431](https://github.com/mingley/partitionline/actions/runs/42499121431)
passed both tests with default and all features using stable Rust 1.99.1 and
LLVM 23.1.1. The job copied the pinned fixture to a temporary Cargo integration
test target; it did not change broker source.

The examined repository revision is
`0c9ab1e712a043a2acc5fa56963cff2f3e5ccc91`. Git blob pins:

| File | Blob |
| --- | --- |
| `partitionline-broker/src/quotas.rs` | `fe6ed7dabd863ec9e1b3f54101d57ea4d2abc6a0` |
| `partitionline-broker/src/protocol.rs` | `ef4b58ac44dd7b93b91b61f64654002898c72b48` |
| `partitionline-broker/src/transport.rs` | `913ebb4caef03382bf4dfd0a02f249c2b6c5c1ef` |
| `partitionline-broker/src/metadata.rs` | `279348d6a0ad32914f81fb44e072d95a6436e9a2` |
| `partitionline-broker/src/produce.rs` | `28ac0a2eb00c4994cc3ebc8d3fe4fa035e6bdc4a` |
| `controls.rs` | `2ff8fe41c662ba250d517001e4ecaada65d9b4c4` |
