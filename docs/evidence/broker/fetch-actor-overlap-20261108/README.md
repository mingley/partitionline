# Canceled Fetch input overlap

The configured Fetch input/output admission ceiling does not cover every
cancellation overlap. A caller can drop its dispatch future and release its
permit while the actor still owns the request. Another request can then
acquire that permit.

A [modeled ownership witness](https://github.com/mingley/partitionline/actions/runs/42445149916)
tested commit `e869addeb1c6723ec8b2edbc78f17f3e622bda40` on x86_64 with
stable Rust 1.99.1. It first sent a valid 4,096-byte empty Fetch through a real
router configured with a 4,160-byte input/output admission ceiling. It then
used the same dispatch path with a test worker gated after taking the first
command. After the first caller canceled, the worker still held that input
while a second 4,096-byte input queued under the reused permit: 8,192 bytes
of owned input versus the configured 4,160-byte ceiling.

The witness asserted the first buffer before and after cancellation. It failed
when a test-only mutation dropped that buffer early, and a separate assertion
that the two inputs fit the ceiling also failed. The modeled schedule passed
ten times in each feature profile. All-target, all-feature Clippy, the broker
API inventory and its 55 controls passed. Job logs were inspected; the
[uploaded artifact](https://github.com/mingley/partitionline/actions/runs/42445149916/artifacts/13603322178)
was not independently downloaded or inspected.

The gated worker models a possible scheduling point after actor dequeue; it
does not run the complete production actor in that phase. The witness counts
owned input bytes, not allocator overhead, RSS or total process memory. An
owned-permit design and its effects on cancellation, long-poll retries,
batched Produce and shutdown require separate implementation and testing.
