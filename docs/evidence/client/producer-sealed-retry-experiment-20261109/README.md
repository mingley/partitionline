# Experimental sealed producer retry handoff

This is an isolated, incomplete experiment based on the bounded record-owner
candidate `bb334d1b17c3529337de7fe94b77d85da35e5639`, itself based on main
`731eb97a08445a18da0b043986ae192397ba176a`. It is not applied to main and
does not establish production readiness, safe delivery, Kafka interoperability
or performance.

The experiment sends an already-sequenced, validated, rooted partition group
as one message through a separate retry actor and one message into a worker's
sealed-batch channel. The worker activates it as a single item, selects the
whole active group, and expires an active group together. An incomplete,
mixed or sticky retry is conservatively rejected in this experimental path.
Existing non-rooted producers keep the prior per-record path. These mechanics
are not a complete batch journal or a partition fence.

## Sources and first run

The initial source-only commit was
[`2ccad099c5fb8f94b2a4350606c2e8b58358d602`](https://github.com/mingley/partitionline/commit/2ccad099c5fb8f94b2a4350606c2e8b58358d602),
producer blob `203f2dfb87520c0ef15d9c14b6c9a70bbaf3df2e`. The
[instrumented workflow commit](https://github.com/mingley/partitionline/commit/f18516a45210fcd7b11070ddea95d661ec460387)
used producer blob `cecec14d5609ddf12258e6633e0f632e840b0cf6` and
test blob `a61600da3ba385b90eda1f2f60848ea5537c0410`.

[Run 42687112161](https://github.com/mingley/partitionline/actions/runs/42687112161)
compiled both profiles. Each released-gate mock history observed a whole
retry `(1000, 0, 1, 8193)`, one retry attempt and unchanged mock log length.
Both held-flush histories failed because flush finished during the supposed
hold. The original gate could consume a stale `Notify` permit from the
preceding released history, so this failure is not a valid observation that
flush returned while the handoff was actually held. The default legacy
`buffer_ownership` suite passed 12 tests. Clippy and formatting failed; in
addition to inherited owner-candidate diagnostics, Clippy found this
experiment's deprecated `fetch_update` and unit-valued binding.

## Corrected gate and results

The corrected source-only candidate is
[`9f454b41adf2517ada593dd13f9148a5030f5b0e`](https://github.com/mingley/partitionline/commit/9f454b41adf2517ada593dd13f9148a5030f5b0e),
producer blob `27df35a47d6d44c1ab12f6d50828e610d8045d5d`. It replaces those
two retry-code lint issues. The corrected
[instrumented workflow commit](https://github.com/mingley/partitionline/commit/90f5f6fca15b003aa36428f3f4b0f89cb99a0d0f)
uses a held-state loop that cannot consume an earlier release as permission
to proceed. Its exact tested blobs are:

| File | Git blob |
| --- | --- |
| `src/producer.rs`, including the cfg-gated pause | `7f661d3c7c5ba9a550be227220ad3722bef0db81` |
| `src/producer/owned.rs` | `f3489802f0b9ee1afe61c34d54a2041e447773c3` |
| `tests/producer_sealed_retry_public.rs` | `39110c6048eebba35308fabbcae0862d0b4c9f59` |
| `tests/common/mod.rs` | `4afd104e0b5e10227167d0d38328fe03dda07a0a` |
| `Cargo.lock` | `96e364d34121fe90183f87f4f49fd92b883bc447` |

[Run 42687672000](https://github.com/mingley/partitionline/actions/runs/42687672000)
used hosted x86_64 Linux and stable rustc 1.99.1. In both default and
all-feature profiles the released and held histories each observed one whole
retry `(1000, 0, 1, 8193)`, first and last receipt offsets 1 and 8193,
8,193 target acknowledgements and unchanged mock log length. The held
history paused the actor before worker handoff, observed for 300 ms that
flush had not returned and no retry had been written, then released it.
The selected default legacy `buffer_ownership` suite passed 12 tests.

The workflow still failed: all-feature Clippy stopped on three inherited
`bb334d1` diagnostics (unused mutable binding, large enum variant and large
error result), and `cargo fmt --all -- --check` reported differences in
the producer, owner module and test files. No full suite, warning-denied
documentation, real Kafka control or performance benchmark ran.

The mock control uses a dropped response after the original append and a
qualification-only scheduling gate. It covers one non-sticky partition and
a finite observation window. It checks aggregate callback count and mock log
length, not every value or exactly one callback for each record. It cannot
rule out a later retry outside the observation.

## Unresolved correctness and integration

- The validator uses numeric base, generation, length and member sequence.
  It does not assign a nonreused reservation identity. Two distinct batches
  whose numeric metadata recur can be confused.
- No authoritative partition fence prevents a later accepted batch from
  writing before the retained one reaches a replacement worker. A proposed
  public control uses the existing pre-write `WriteRetriable` fault, holds
  the retained group, and then sends a later record: the sequence counter is
  ahead while the mock has not appended the retained group. That control
  was prepared but not run; the overtaking outcome is source-derived, not
  an observation.
- Identity publication is not serialized with actual write admission or
  in-flight resolution. A request may start or finish across an identity
  change, and ambiguous outcomes cannot safely be reassigned.
- The root supervises orphaned records individually; it has no immutable
  batch entry or broker-settlement journal. If a worker stops while a sealed
  group and deferred fresh records coexist, its drop path can combine them
  and fail them rather than restore the group. Shutdown aborts the retry
  actor before all workers. A whole channel message alone does not solve
  cancellation, callbacks, receipts or flush/close barriers.
- Sticky retries, mixed or incomplete sequenced groups and rollover are not
  implemented by this path; rejection sacrifices progress and is not proof
  of safe later writes. Original sequence reservation still saturates.
  Validation and activation scan entire batches synchronously, and the new
  retry actor serializes groups; no scheduler or throughput measurements
  were made.

The source-only retry patch and the later owner candidate
`f71e1a55c87476a5a4f1e72e66e1ce1bd27c6708` share only overlapping context
in `Inner::drop`; their changed statements differ. They were not combined.
The later owner separates admission sealing from transport closure, whereas
this retry draft relies on `Owner::seal()` setting `Shared.closed` in its
expiry path. Even in the earlier owner, that flag does not fence a write
already in progress or one that races the check. Existing per-record `admit` behavior during
orderly close also differs from this draft's already-admitted batch handoff.
A mechanical merge would be unsafe. An authoritative batch journal,
per-partition fence and explicit identity/write and shutdown policy are
required before further integration.

## Retained source and reproduction

This directory preserves [the owner base patch](owner-base.patch), the
[sealed retry patch](sealed-retry.patch), the [test-only gate patch](witness.patch),
the [public retry fixture](fixtures/producer_sealed_retry_public.rs), the
[bounded-owner fixture](fixtures/producer_owner_bounded_controls.rs), and
the [original workflow](fixtures/producer-sealed-retry-workflow.yml.txt).
The patches are evidence artifacts; they are not applied to main.

From a clone containing this evidence directory, reproduce in a separate
worktree with the current stable toolchain:

```sh
evidence_dir="$(pwd)/docs/evidence/client/producer-sealed-retry-experiment-20261109"
repro_dir="$(mktemp -d)"
git worktree add --detach "$repro_dir" 731eb97a08445a18da0b043986ae192397ba176a
cd "$repro_dir"
git apply --check "$evidence_dir/owner-base.patch"
git apply "$evidence_dir/owner-base.patch"
git hash-object src/producer.rs src/producer/owned.rs
git apply --check "$evidence_dir/sealed-retry.patch"
git apply "$evidence_dir/sealed-retry.patch"
git hash-object src/producer.rs
git apply --check "$evidence_dir/witness.patch"
git apply "$evidence_dir/witness.patch"
cp "$evidence_dir/fixtures/producer_sealed_retry_public.rs" tests/
cp "$evidence_dir/fixtures/producer_owner_bounded_controls.rs" tests/
git hash-object src/producer.rs src/producer/owned.rs tests/producer_sealed_retry_public.rs tests/common/mod.rs Cargo.lock
RUSTFLAGS='--cfg sealed_retry_witness --check-cfg=cfg(sealed_retry_witness)' cargo test --locked --test producer_sealed_retry_public -- --nocapture --test-threads=1
RUSTFLAGS='--cfg sealed_retry_witness --check-cfg=cfg(sealed_retry_witness)' cargo test --locked --all-features --test producer_sealed_retry_public -- --nocapture --test-threads=1
```

After the owner patch, the two printed source blobs should be
`251eeece67abc29867aa9212f6ce005503aed4ad` and
`f3489802f0b9ee1afe61c34d54a2041e447773c3`. After the sealed retry patch,
the producer blob should be `27df35a47d6d44c1ab12f6d50828e610d8045d5d`.
After the gate patch and fixture copies, the five printed blobs should match
the corrected-run table above. The workflow file is retained for inspection;
the commands above reproduce only its focused mock tests, not every strict
check. Remove the temporary worktree when finished.

A previous version of this note truncated the source-only producer blob by
two characters. The full blob above is the one recreated by applying the
retained source patches.

[Patch verification run 42690188165](https://github.com/mingley/partitionline/actions/runs/42690188165)
passed on hosted x86_64 Linux. It checked and applied all three retained
patches to `731eb97`, copied the two fixtures, and verified the source,
fixture, mock and lockfile blob hashes. It did not run Cargo or producer
tests; its workflow exists only in the isolated qualification commit
`ae3c3a1511ad0810936b8ddf52399c5e3028918d`.
