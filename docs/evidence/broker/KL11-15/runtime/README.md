# KL11-15 snapshot runtime evidence

The bounded fixed-voter snapshot component qualifies at pushed source
`d147bcf1c0164778bdbad625842363f3721bc10e`. A complete image stays inactive until
a synchronized Install receipt selects its generation, group, base and checksum.
Install and replay preserve every previously committed record byte, retain the
verified suffix, and preserve the receiver's local term and vote.

[Final evidence](final-evidence.json) records the source, 141 actual command
receipts, results, earlier failures and limits. The coordinator owns whole-card
closure. The implementation keeps the canonical opaque record prefix; it does
not perform application-state folding or physical log compaction. Snapshot
exchange remains caller-driven with trusted configured fixed voters. It adds no
Kafka API advertisement or native FetchSnapshot codec.

## Final validation

| Evidence | Result |
| --- | --- |
| [Shared complete-source matrix](../../KL11-10/storage/final-d147bcf1/validation.json) | 19 commands pass; stable/MSRV default/all features: 256/352/256/352 tests, 1,216 total, no failures or ignored tests; fmt and all four strict Clippy/docs/doctest cells pass |
| [Independent snapshot histories](../oracle/history/final-d147bcf1/primary/validation.json) | Actual 3- and 5-voter histories in all four cells: 8 histories, 684 events, 168 raw checkpoints; 124 deliberate proof controls rejected |
| [Independent publication/install cuts](../oracle/history/final-d147bcf1/cuts/validation.json) | 24 publication cuts/48 states and 8 authoritative Install cuts/16 states pass |
| [Inherited replication regression](../oracle/history/final-d147bcf1/legacy-regression/validation.json) | 8 histories, 776 events, 104 raw data/election journal pairs pass |
| [Compiled guards](compiled-mutants-070598ee/validation.json) | Three positive controls pass; all three compiled unsafe guard removals fail at their intended runtime assertions |
| [Apache image foundation](../foundation-final/45582234/validation.json) | 120 actual component assertions across Apache 4.1.2/4.2.1/4.3.1 and three deliberately wrong assertions reject |

The 1,216 count covers the complete broker crate. Each cell includes 29
replication and 10 image-suite functions plus library owner/cut tests. The raw
state counts above overlap where a second independent checker checks the same
state; they are not additive independent scenario totals.

The shared matrix verified all 44,019 Git file identities before and after each
command. [Retained final captures](final-d147bcf1/capture-retention.json) copy
1,156 snapshot/image/journal/trace files and the four exact Rust source files.
Original capture bytes and modes remain unchanged. The shared matrix retains
compressed proof binaries and their raw/gzip hashes; the six guard binaries are
separately retained with hashes in their receipt.

The [source bridge](source-bridge-d147bcf1.json) proves all four relevant Rust
blobs and modes unchanged from the exact `070598ee` compiled-guard run to
`d147bcf1`. It also proves 153 Apache oracle/fixture Git entries unchanged from
`45582234`. The image Rust source changed after that foundation run through
mutable load, an idle getter and test-only publication cuts; [the exact patch](foundation-source-to-d147bcf1.patch)
is retained. Foundation Rust outcomes remain attributed to their original pin;
the final matrix reran the current image suite. The Apache component evidence is
separate from custom local image/recovery qualification.

## Recovery and bounds

The owner verifies complete images, exact group identity and every old committed
record before appending an authoritative Install receipt. Replay reloads the
selected image and recomputes the outcome against the preceding WAL; declared
receipt fields alone cannot authorize content. A missing or corrupt selected
image poisons the owner until verified reopen. Images never import a vote or
current election term.

Actual process cuts cover image file sync, rename and directory sync, then
synced Install before election reconciliation and reconciled state before ACK.
Unselected replacement files remain inert; old selected images and later
committed suffixes recover. Independent checks include incomplete/foreign
images, checksum-valid forged receipts, committed-byte changes, descriptor
changes, term/sequence fencing and lost/stale ACKs.

Live content is bounded to 4,096 records/64 MiB. Old and replacement state, one
image decode, recovery scratch and retained actor request/receipt envelopes share
the 512 MiB ceiling. Transfer chunks have a 4 MiB ceiling. Default image storage
reserves eight complete generations plus one staged image; Store uses at most
four file descriptors in addition to the two existing journals. Mutable Store
ownership serializes loading and transfer. No mutable storage handle escapes.

## Preserved failures

All earlier attempts remain under this directory. The final JSON names the
failing-first floor-recovery and selected-image poison tests and their original
source/raw files. It also retains the initial Node test's wrong term expectation,
the child/parent clock regression, development compile/lint failures and the
exact `4ffd7557` and `070598ee` incomplete full-gate attempts. The former passed
240 tests before the test artifact writer lint failure; the latter passed 1,152
behavior tests before an external private OIDC helper's MSRV lint failure. The
fourth documentation cells and fmt were not run in that incomplete attempt.

Legacy traces were sealed only on [separate copies](legacy-replay-d147bcf1-fixed/validation.json).
The first copy wrapper incorrectly required the annotated trace to remain
byte-identical; its actual failure is preserved in
[wrapper-failure.txt](legacy-replay-d147bcf1/wrapper-failure.txt). The first sealer
itself succeeded and never changed the original captures. A fresh copy preserves
`trace-raw.json` bytes/modes and every original JSON field while allowing the
sealer's documented annotations; the root's independent regression checks all
copied histories.

Finite process/IO and mutation cases do not prove physical power-loss or hardware
behavior, autonomous peer transport, dynamic quorum changes, native Kafka
snapshot interoperability or production qualification.
