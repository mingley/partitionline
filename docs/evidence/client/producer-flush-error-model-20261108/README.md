# Bounded flush error-state model

This isolated model explores retaining a worker's first flush error until a
waiter reports it. It is not wired into `Producer`, is not a repair, and
changes error coalescing semantics. The [public flush witness][public] separately
shows a successful response-channel send whose error was never observed.

## Model and results

The model keeps a current error epoch, one later exact error and an overflow
flag. Already registered waiters retain their epoch; only an acknowledgment of
the current epoch advances it. An empty epoch stays open. Further errors
beyond the later slot become a synthetic `Protocol` summary. This is an
explicit experimental coalescing rule, not a claim of equivalence to the
current producer.

[Run 42469919468][run] exercised [model commit f7aacc9][model] on hosted
x86_64 with stable Rust 1.99.1 and LLVM 23.1.1. In each of the default and
all-feature profiles, 12 controls passed and one negative control was ignored
in the positive suite. The controls cover empty epochs, a dropped unpolled
waiter, record-before-ack and ack-before-record ordering, reverse old
acknowledgments, repeated summary epochs, and a 256-iteration thread race.
These are finite model controls, not an exhaustive concurrency proof.

Private `Error::Io` payloads with blocking and panicking destructors showed
that discarded errors can block or unwind the recording thread even after the
model releases its state lock. The panic control checked the intended panic
and then verified that the later error and summary remained reachable. These
fixtures do not establish that such custom error payloads are reachable through
ordinary public producer transports.

The ignored control was then run separately and **failed as intended** in
both profiles: after A, B and a worker `Timeout`, the model returned its
generic `Protocol` summary instead of preserving the timeout category. The
workflow's success records that expected failure; it does not qualify the
model. Current transaction abort handling ignores non-Timeout/non-Closed
delivery errors, so treating this generic summary as an ordinary protocol
error could allow abort processing to continue without the required conservative
distinction.

## Limits and next work

The two error slots and flag bound only the current per-worker model state.
Unpolled caller-held old epochs can remain alive, and an error payload can
contain arbitrarily many bytes. Exact retention of arbitrarily many distinct,
sent-but-unobserved errors needs explicit backpressure or a documented
coalescing policy. If error credits are exhausted, a flush that waits for all
queued work before reporting the oldest error can deadlock with admission
backpressure.

The model does not exercise oneshot publication, a public future returning
`Poll::Ready`, multiple workers, worker retirement, retries, close, runtime
shutdown, or transaction generations. A correct implementation must
acknowledge only at an appropriate public boundary, handle canceled and
swallowed errors, and avoid allowing error cloning or final destruction to
panic after acknowledgment but before the result is returned. A typed
internal overflow outcome or conservative failure policy needs its own
transaction and concurrency qualification. No hot-path cost was measured.

The model Git blob is `47fd74c08a66117771aa4fcd0f02fae5f3241b7c` (SHA-256
`cc2a83f321b392a7fba3fa4ae02dfd0ec6c441fc9bf0c1064a70a0a0632051c9`);
the producer file contains only a cfg(test) module declaration on top of base
`60caa746c3654c89d01557e35e1100cdc2a84b74`. The lockfile SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
Earlier [run 42469436464][first] passed the first six controls but did not
test severity. [Run 42469692095][earlier] included the expected negative and
private destructor controls, but its blocking-lock regression path could
hang; the final fixture uses a bounded competing thread and an RAII release.

[public]: ../producer-flush-sent-20261108/README.md
[run]: https://github.com/mingley/partitionline/actions/runs/42469919468
[model]: https://github.com/mingley/partitionline/commit/f7aacc92f613a44b042a64afa2e024cd3fd39853
[first]: https://github.com/mingley/partitionline/actions/runs/42469436464
[earlier]: https://github.com/mingley/partitionline/actions/runs/42469692095
