# Requested topic validation with cooperative row conversion

The aggregate `describe_topics` can return a topic or next cursor that
was not requested. The [initial candidate][prior] rejected these responses
but added substantial overhead to the 32- and 256-name error-row fixtures.
This note tracks a combined candidate with an ordered-name fast path. It is
under review; final qualification results are recorded below as they
complete.

The candidate compares each returned name to the current and next requested
names. It compares the strings, so the hint cannot accept an unrelated
name. A different order, an omitted name, or duplicates may cause a
fallback. For more than eight inputs, the first mismatch builds one
randomized `HashSet<&str>` from caller names; smaller lists use direct
membership checks. The fallback yields after each 8,192 inputs and reuses
the index. This does not assume Kafka or another peer returns names in the
request order. The empty aggregate remains a no-op, Metadata fallback
keeps its existing projection, and the raw one-page API is unchanged.

For a response page, an immediately repeated cursor is checked first.
Subject to a timeout, any null name wins over an unrequested named row;
that row wins over an unrequested next cursor; those checks precede
same-page topic-ID conflict detection. An unrequested name or cursor
returns a fixed-text client-side `Error::Protocol`, not a broker error.
Checking cursor membership is a client consistency policy; the protocol
schema does not promise this behavior for every peer.

The [candidate][candidate] is based on main
`464d33d5afe668d46274972cd3baa4e870159bf8`, which contains the accepted
cooperative topic-row checkpoint. The candidate preserves that checkpoint:
the counter carries across pages and yields after 8,192 successfully
converted rows. Deferred name indexing can yield before the current row is
counted. The candidate source Git blob is
`3125c234259c87d1bc4a89d0cd0734d124529bea` (SHA-256
`ebeaea8620424abe1f05278ae1af7a97c902a4e20bc705a1a4dbcaf0dfec4984`)
and its tests blob is `f18e87db6a6eab6d33a05288fe1d39817b9c2388`
(SHA-256
`8343a51719ba69207d431aae29f41f1d231be0667517c0341cc51a91c99d1e8c`).
The main baseline source blob is
`23cb280eee5e91a64a9a6f58347f707524163d07`.

## Checks

The [combined gate][gate] has passed rustfmt, strict all-target/all-feature
Clippy, all-feature Rustdoc with warnings denied, and nine client doctests.
Kafka 4.1.2 smoke tests passed in each feature profile: the broker CLI
confirmed topics with four and two partitions, and the aggregate fixture
checked the topics, duplicate requests and seven missing-name errors.
The smoke uses image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`
and fixture Git blob `5163fe81c4cbfd2462ffa86ba406851ff4a92fec`.
It does not reproduce unsolicited or null response rows.

The final candidate's 66 focused pagination controls passed in each
profile. Both full Admin suites **failed** on 80 ms wall-time assertions:

| Profile | Passed / failed | Failed test (measured time) |
| --- | --- | --- |
| Default | 670 / 3 | `decoding_many_described_group_members_does_not_hold_the_caller` (83.491501 ms); `grouping_many_named_results_does_not_hold_the_caller` (84.293590 ms); `indexing_many_coordinators_does_not_hold_the_caller` (127.861731 ms) |
| All features | 672 / 1 | `indexing_many_coordinators_does_not_hold_the_caller` (110.190508 ms) |

The 20 independently restored mutants per profile and matched release
timing are still running. The exact-suite failures are not passing
results, and their causes have not been established.

A separate [instrumented row-checkpoint control][observer] passed in both
profiles: two public cross-page cancellation/timeout checks and one
mismatch-scan cancellation check passed in each. Removing the row yield
made the targeted test fail with
`row checkpoint resumed before observation: 2` in each profile, as
intended. The observer-only source
blob `c6042c72b8067f9c49f2dca6f31f0e907aaabc98` adds test-only
before/after markers at the row yield; its tests blob is
`8038f78dd5672ea7db82cb0f3b73d4f98df9691b`. The markers add work at the
test checkpoint. These current-thread mock checks exercise that
suspension and socket behavior; they are not an exact-source run or a
wall-clock bound.

The [earlier hint-only gate][hint-gate] passed 60 focused tests in each
profile, strict checks and six restored mutants per profile. The earlier
[combined draft][draft-gate] passed 66 focused tests in each profile and
strict checks using the candidate production blob but an earlier test blob
`a715d649c26bc99d5407d9a1fc29e9229aa0a471`. These do not replace the
final-source gate. A [preceding unformatted draft][draft-fmt] failed
rustfmt on three test-line wraps; a hint draft also failed rustfmt before
its corrected check. An isolated branch update was rejected as
non-fast-forward and then updated with a lease. The [four-arm prototype
timing][prototype-perf] is separate from the combined-source benchmark.

## Matched Admin suites

The [matched suite comparison][broad] ran both source versions on a
shared AMD EPYC 9V45 x86_64 runner with Rust 1.99.1 and LLVM 23.1.1.
It used the same compatibility test fixture in both arms, Git blob
`33d917cba15f1c0c5ed5646c277b0890a725938c` (SHA-256
`f11ee5fa7fa095e684e1ba0f7971b4fa2ea37c5ad5618251b93756fa3fbd2827`).
This fixture does not contain the new name-validation or row-helper
controls; those ran separately at the candidate test pin above. The
matched workflow **failed**. In execution order:

| Order | Profile and arm | Passed / failed | Failed test (measured time) |
| --- | --- | --- | --- |
| 1 | Default baseline | 633 / 2 | `grouping_many_named_results_does_not_hold_the_caller` (100.990434 ms); `indexing_many_coordinators_does_not_hold_the_caller` (81.886451 ms) |
| 2 | Default candidate | 632 / 3 | `decoding_many_consumer_group_members_does_not_hold_the_caller` (99.864319 ms); `grouping_many_named_results_does_not_hold_the_caller` (96.591971 ms); `indexing_many_coordinators_does_not_hold_the_caller` (85.627277 ms) |
| 3 | All features candidate | 632 / 3 | `flattening_many_list_reassignments_does_not_hold_the_caller` (86.812287 ms); `grouping_many_named_results_does_not_hold_the_caller` (89.779487 ms); `indexing_many_coordinators_does_not_hold_the_caller` (82.633298 ms) |
| 4 | All features baseline | 631 / 4 | `decoding_many_listed_groups_does_not_hold_the_caller` (85.454642 ms); `flattening_many_list_reassignments_does_not_hold_the_caller` (91.471043 ms); `grouping_many_named_results_does_not_hold_the_caller` (82.687971 ms); `indexing_many_coordinators_does_not_hold_the_caller` (93.198846 ms) |

The same runner saw wall-time failures in both source arms. This does not
establish their cause or change the exact candidate-suite failures.

## Prototype timing

The [four-arm comparison][prototype-perf] used one pinned CPU on a shared
AMD EPYC 9V74 runner with Rust 1.99.1 and LLVM 23.1.1. It compared baseline
source `4d4212057984e47201b2bb1fe383ac2ae5a55562`, initial eager validation
`eb5a967b89e94a8a3d1a7598919accdfde96f20f`, capped eager validation
`31373ec93f6e22365383b58dc458b4f3ffc0bf19`, and the pre-merge hint
prototype `ee7ebc1c097780d3c39bd49a81c89a1af254d65a`. All arms used the
same benchmark fixture `5d7b8522b58257ffd6fcedacef93e5be6d71b3c5`
(SHA-256 `e6521ed66d8da77c580d0fcba3ee5224a84f3720933aa7778ce7bfb30a96eac8`).
The [CSV](prototype-timings.csv) retains all 576 elapsed-time samples in
measurement order. There were 12 alternating pairs for every shape and
variant. Each cell is median paired candidate/baseline ratio, range, and
number of slower pairs:

| Shape | Initial | Capped | Hint prototype |
| --- | --- | --- | --- |
| Single successful page | 1.0049 (0.9997–1.0081; 11/12) | 1.0006 (0.9896–1.0095; 7/12) | 1.0033 (0.9954–1.0140; 9/12) |
| 16 successful pages | 1.0018 (0.9952–1.0108; 7/12) | 1.0031 (0.9974–1.0115; 8/12) | 1.0018 (0.9883–1.0060; 9/12) |
| 4 ordered error rows | 1.0039 (0.9587–1.0114; 10/12) | 1.0006 (0.9953–1.0090; 7/12) | 1.0033 (0.9962–1.0117; 9/12) |
| 32 ordered error rows | 1.0821 (1.0766–1.0863; 12/12) | 1.0547 (1.0462–1.0592; 12/12) | 1.0059 (1.0004–1.0125; 12/12) |
| 256 ordered error rows | 1.1817 (1.1737–1.2009; 12/12) | 1.1702 (1.1572–1.1813; 12/12) | 1.0199 (1.0114–1.0566; 12/12) |
| 32 reversed error rows | 1.0912 (1.0847–1.0961; 12/12) | 1.0519 (1.0416–1.0617; 12/12) | 1.0401 (1.0327–1.0508; 12/12) |
| 256 reversed error rows | 1.1767 (1.1665–1.1866; 12/12) | 1.1671 (1.1524–1.1836; 12/12) | 1.1854 (1.1216–1.1916; 12/12) |
| 32 names, three successful pages | 1.0387 (1.0334–1.0551; 12/12) | 1.0252 (0.9712–1.0316; 11/12) | 1.0095 (0.9978–1.0218; 11/12) |

The single-page case ran 4,000 reads; the 16-page case ran 250 reads; the
4-, 32- and 256-name cases ran 2,000, 1,000 and 200 reads respectively;
the three-page case ran 250 reads. Error-row shapes contain
`UNKNOWN_TOPIC_OR_PARTITION`; the page shapes include successful
partitions. The hint reduced cost on the ordered fixtures, but the reversed
256-name case remained materially slower on this host. These are
loopback fixtures, not comparisons with other Kafka clients. They do not
qualify the merged source; [its matched timing][perf] is separate and still
running.

## Limits

The initial fallback reservation is based on at most 32 input entries even
when there are fewer distinct names; the table's actual capacity depends on
the standard-library implementation. Subsequent growth depends on distinct
caller names. The set borrows caller strings and never stores peer names.
This is a bounded initial reservation, not an absolute memory bound: input
collection, response decoding, result accumulation and the fallback table
still consume memory.

The row checkpoint, index build and unsolicited-row remainder scan have
separate counters. They do not bound the work of hashing long names, table
growth, allocation, nested partition cloning or synchronous destruction in
one poll. Cleanup uses per-vector and per-topic chunks, not a global
partition-drop or wall-time bound. The decoded response is still owned
while validation and cleanup run.

For tests, before/after thread-local counters are updated only at each
8,192-input index yield. The page and cursor cancellation controls wait
until the former exceeds the latter, then cancel and reuse the same
socket. They verify that suspension point, not an elapsed-time guarantee.
The temporary row observer and its overhead are reported separately.

[prior]: ../admin-requested-topic-validation-20261108/README.md
[candidate]: https://github.com/mingley/partitionline/commit/542602573ceb8712360cbab6b8381e856129b481
[gate]: https://github.com/mingley/partitionline/actions/runs/42485196572
[observer]: https://github.com/mingley/partitionline/actions/runs/42485346758
[hint-gate]: https://github.com/mingley/partitionline/actions/runs/42484132205
[draft-gate]: https://github.com/mingley/partitionline/actions/runs/42484745286
[draft-fmt]: https://github.com/mingley/partitionline/actions/runs/42484469639
[prototype-perf]: https://github.com/mingley/partitionline/actions/runs/42483323372
[broad]: https://github.com/mingley/partitionline/actions/runs/42485301649
[perf]: https://github.com/mingley/partitionline/actions/runs/42484971478
