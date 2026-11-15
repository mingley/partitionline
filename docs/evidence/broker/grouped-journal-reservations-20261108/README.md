# Grouped journal reservation

Status: applied on main. The qualified standalone source is in
[0ee9fbb](https://github.com/mingley/partitionline/commit/0ee9fbbae7fa00be576dbad67c81da76bc6b3499);
[bae47cc](https://github.com/mingley/partitionline/commit/bae47cc07522ced04d547d5c6fe0a6a84dc556ed)
combines the same journal, Produce documentation and registry bytes with the
record-parser change on `e02c1b8`. The one-off workflows are not part of the
applied change.

## Finding and change

A grouped write reserves an index slot before staging a record. The old
reservation considered only the committed index length. On an empty index, the
first and second staged records left index capacity at one even though
publishing both required two slots. Publishing the already-synchronized second
entry could therefore grow the index via infallible `Vec::push`. The staging
`VecDeque` could likewise grow via `push_back` after the record write. A
[baseline witness](https://github.com/mingley/partitionline/commit/1b6d49cc3ad989bd57311e3950d327e01ddc02ae)
reproduced the index-capacity mismatch in both profiles. It did not reproduce
a real out-of-memory event, data loss, or a process abort.

The candidate reserves space for the committed index and every staged entry,
including the new one, before writing. It also calls fallible staging-queue
reservation before writing. Capacity arithmetic is checked against the
configured index limit. An index or queue reservation error leaves the new
record unwritten and earlier staged entries available. A queue can retain its
capacity after use; the Produce configuration documentation now states that
staging queues and allocator over-allocation are outside the configured
committed-index charge. The corresponding pinned Produce source checksum was
updated; only its documentation changed.

These reservations do not make the entire path allocation-free. Reserving a
later entry can allocate while earlier entries remain staged; cloning its sync
descriptor still occurs after the write and may fail, using the existing
poison/reopen behavior. The tests inject reservation errors; they do not force a
real allocator to fail. Rust's global allocator or operating system can still
abort or terminate a process. The configured index charge is not an RSS bound.

## Controls

[Standalone qualification](https://github.com/mingley/partitionline/actions/runs/42455879697)
tested the exact final journal source on x86_64, Rust 1.99.1. Both focused
profiles reproduced the baseline index witness, then passed the candidate
controls. A test-only observation taken immediately before each write checked
that the index has room for the committed and all staged entries and that the
queue has an available slot; neither capacity changed while the relevant entry
was pushed, and the index capacity did not change during publication after
sync. Separate injected errors at index-growth and actual queue-growth
boundaries preserved file bytes and prior staged entries. Other controls
covered index limit and arithmetic overflow rejection before a write, and a
real-file sequence with partial publication, unstage, replacement, sync,
reopen, and exact payload/order verification.

The standalone default and all-feature `--all-targets` suites passed 759 and
907 tests respectively, with three ignored in each. Formatting, strict
all-target/all-feature Clippy, strict Rustdoc, inventory and all 55 inventory
negative controls passed. Both doctest commands found zero tests. The ignored
descriptor tests require separate runs; the suite counts do not count them as
passing.

[Three temporary mutants](https://github.com/mingley/partitionline/actions/runs/42455936966)
removed queue reservation, reserved only one index slot, or moved the synthetic
queue failure past the file write. The controls detected each mutant in both
feature profiles. These are test controls, not real allocator failures.

The [combined-tree gate](https://github.com/mingley/partitionline/actions/runs/42455998248)
tested the journal with the landed record parser: 761 default and 909
all-feature tests passed, with three ignored in each suite. Both focused
profiles, formatting, strict all-target/all-feature Clippy, strict Rustdoc,
inventory and all 55 inventory negative controls passed. Both doctest commands
found zero tests.

[Separate combined-source descriptor controls](https://github.com/mingley/partitionline/actions/runs/42456150975)
ran the three ignored diagnostics individually in fresh single-threaded
processes under both profiles. All six passed; the exhaustion check used a
64-descriptor limit. In each profile the local journal fixture reported six
baseline descriptors, eight with a journal, nine with one staged sync, eight
with only the ticket, and six after dropping it. The grouped Produce fixture
reported eight baseline, 12 with two idle stores, 28 with eight groups staged,
and 12 after completion. These are scoped Linux process observations, not a
process-wide descriptor ceiling.

The combined `records.rs`, `produce.rs` and registry SHA-256 values were
`5accc57fff730a286754c0247fe4b72b7d9f16ab53581f20eb2f489e4bc28741`,
`ace0982025aac1d2f66846865fe4e9083b578a2dfccb6fba16af1f1d5183ff11`
and
`52c20b9199faf702d3374119c8033fcea947ef39f92b999e67b00344a7eda9e4`.

A separate [main run at `e02c1b8`](https://github.com/mingley/partitionline/actions/runs/42455943182)
passed 757 default tests with three ignored. Its all-feature suite stopped at
641 passed, one failed and three ignored when a before-failover CreateTopics v3
returned error 56 in the three-process metadata quorum test. The log did not
record the operation token, message, target, earlier error-41 attempts or the
mutation outcome. The later combined pass does not explain that failure. It is
distinct from the earlier before-failover Delete error 56 and remains open.

## Local release diagnostic

The [paired diagnostic](https://github.com/mingley/partitionline/actions/runs/42455891787)
compared the baseline witness source with the final standalone journal source,
compiled in release mode with a temporary benchmark function and example.
Each sample staged 192 groups of one or eight distinct 8-byte records into a
local `Journal`, called `sync_data` once per group, and published in order.
After the timed section it reopened the file and checked every offset, record
count and payload. All 48 samples passed that readback.

Rust 1.99.1 ran on one pinned CPU of a shared x86_64 AMD EPYC 9V45 host. The
ext4 mount reported `journal_async_commit,nobarrier,data=writeback`.
The 12 paired candidate/baseline median elapsed ratios were 0.969 for width
one (range 0.604–1.329) and 0.931 for width eight (0.674–1.463). Trials
lasted about 36–67 ms and varied substantially. These are noisy local
measurements, not a Kafka throughput comparison or a reliable speedup claim.
The filesystem configuration does not qualify durable-storage performance or
power-loss behavior.

Baseline journal source SHA-256:
`9d846c5997896df16537c0e35de5d811eeb65027dc5ab271adf9d2c740a0d057`.
Candidate journal source SHA-256:
`480d55ee2a8b3f4a7924c5b5d18f30b814d59e36a69d8cb563ce6bb6b5b7d6c2`.
Baseline/candidate binary SHA-256:
`7afba14cc48429b9872b1f048bf3d3d62f708414bb74c44d8032428034662efc` /
`75609bd14d718f63ea48420d8075f6b15962c2253d97e4e8cfba7010fbcf8291`.
The temporary benchmark and example SHA-256 values were
`f9ed0eac6a6a139f555c48e5d7855db7a05fba672875b9c327bb33583e10e89a` and
`766e7d63a7fa15a93dfb56dfbebda7028876fa1d81bdb96f4b76258a01b64914`.
The raw samples are below; GitHub reported SHA-256
`9bcc2b25f139fcae35c35cee76bb97093ddbd0bd913ee61e642f704a0acfcbb8`
for the [sample artifact](https://github.com/mingley/partitionline/actions/runs/42455891787/artifacts/13607686811).
The archive was not independently downloaded and the binaries were not retained.

| Pair | Width 1 baseline ns | Width 1 candidate ns | Width 8 baseline ns | Width 8 candidate ns |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 39908847 | 36118813 | 45530954 | 57657305 |
| 2 | 59824016 | 36110931 | 44856858 | 46856291 |
| 3 | 36988370 | 36639570 | 43710789 | 63969150 |
| 4 | 55995545 | 36001037 | 50355191 | 44446116 |
| 5 | 38784019 | 39870139 | 64411040 | 46326941 |
| 6 | 40152200 | 39454419 | 44024116 | 46142196 |
| 7 | 58227925 | 39536792 | 66706444 | 45758533 |
| 8 | 37133477 | 38307157 | 46321363 | 46490575 |
| 9 | 60714725 | 43133788 | 60033349 | 49159347 |
| 10 | 39043937 | 37942253 | 47862412 | 46881509 |
| 11 | 42112344 | 55971810 | 64419252 | 43395519 |
| 12 | 39810630 | 38470481 | 60621098 | 44792192 |

## Earlier attempts

- [42454961008](https://github.com/mingley/partitionline/actions/runs/42454961008)
  and [42454990832](https://github.com/mingley/partitionline/actions/runs/42454990832)
  passed the baseline and three candidate controls in both profiles but failed
  formatting before Clippy.
- [42455234485](https://github.com/mingley/partitionline/actions/runs/42455234485)
  passed the all-feature suite (907 passed, three ignored) but the default run
  stopped with 682 passed, one failed and three ignored when a five-peer Raft
  observer addition returned `Deadline`; later test binaries did not run.
  Static checks found two Clippy warnings in the new tests. Inventory failed
  because the Produce documentation edit had not yet been repinned.
- [42455478480](https://github.com/mingley/partitionline/actions/runs/42455478480)
  passed the default suite (759 passed, three ignored); the all-feature library
  stopped at 514 passed, one failed and three ignored when a three-node Raft
  proposal returned `NotLeader` on a node which subsequently reported itself a
  follower while another reported leader. Its static job retained the same two
  test-only Clippy warnings. The corrected inventory and negative controls
  passed. These Raft failures are unresolved; the logs do not establish their
  cause or attribute them to this change.
- [42455587283](https://github.com/mingley/partitionline/actions/runs/42455587283)
  passed both full profiles (759/907, with three ignored each), focused
  controls, static checks and inventory after the lint and checksum corrections.
  [42455701167](https://github.com/mingley/partitionline/actions/runs/42455701167)
  passed both full and focused profiles and inventory but failed formatting of
  the later pre-write observation.
- [42455338005](https://github.com/mingley/partitionline/actions/runs/42455338005)
  failed before a benchmark sample because its temporary example lacked crate
  documentation. [42455607183](https://github.com/mingley/partitionline/actions/runs/42455607183)
  and [42455746185](https://github.com/mingley/partitionline/actions/runs/42455746185)
  are successful earlier-source diagnostics; their timing does not replace the
  final-source diagnostic above.
