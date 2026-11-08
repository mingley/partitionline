# Observer image failure diagnostics

In the default broker library of
[run 42457826681](https://github.com/mingley/partitionline/actions/runs/42457826681),
`actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join`
failed its assertion that a checkpoint image exceed 64 KiB. The test had
proposed two distinct 40 KiB payloads and waited for the returned positions
to reach the committed prefix on every genesis voter. The checkpoint builds a
full-prefix image with uncompressed record payloads, so an image containing
both expected records must exceed 80 KiB.

The old test retained only the assertion message. It did not log the returned
image size or base, the proposal terms and positions, or the image's payload
identities. Its commit helper checked that voter prefixes agreed at each
returned index but did not compare the records to those particular proposals.
The cause of the small-image assertion remains unknown; the log does not
establish whether the records were absent, whether a sampled state was stale,
or whether another problem occurred. The same run had three other Raft test
failures and stopped at 471 passed, four failed and three ignored; later test
targets did not run. See the [deadline-test record](../frame-read-deadline-test-20261108/README.md).

## Diagnostic change

The test retains each proposal's returned term and index and its expected
repeated-byte payload. If either the image-size or base-index assertion would
fail, it reads the selected leader's state-file sample and prints the
descriptor's exact byte length and base term/index along with the sampled role,
term, last index and committed index. A bounded summary describes sampled records
by term, index, kind, payload length and CRC32C. Separate flags compare each
expected 40 KiB payload byte-for-byte at its returned term and index. CRC32C
is a non-unique fingerprint, not proof of an arbitrary payload's identity.

The sampled prefix comes from `fetch_committed(1, 64, 256 KiB)` and can be
truncated; the state file is not atomically sampled with the checkpoint. The
diagnostic labels it non-atomic and reports whether its sampled entries form a
structurally contiguous sequence through the image base. A false expected-match
flag may reflect a missing or clipped sample; a true flag describes the
sample, not the checkpoint image. Only nonempty barriers are marked invalid;
the diagnostic does not reject an empty non-barrier entry.

Entry output is capped at 64 entries and 7 KiB of detail. Role and error text
use a bounded snippet; a synthetic large-role control checks the combined line
stays below 12 KiB. No raw payload is printed. The original image-size and
base-index assertions, network operations and proposal behavior are unchanged.
No failed proposal is retried by this diagnostic.

## Qualification

The final test-only source was developed on main
`1036a393eb85f23032b935bf86862b7e98e36e14`. The standalone
[run 42459184540](https://github.com/mingley/partitionline/actions/runs/42459184540)
passed 764 default and 912 all-feature broker all-target tests, with three
ignored in each profile. Both focused profiles, formatting, strict
all-target/all-feature Clippy, inventory and all 55 inventory negative controls
passed.

For each feature profile, a synthetic test checked exact expected matches,
missing or replaced payloads, a term mismatch, an empty data entry, malformed
hex and bounded output. A temporary mutant that ignored the expected term
failed the identity assertion. Another that removed the entry and output
bounds failed the length assertion. These are deliberate test failures and
were checked as assertion failures, not accepted build or setup failures.

A separate qualification-only control used a fresh three-peer cluster and a
real checkpoint. It forced diagnostic emission and a deliberate test failure
after the checkpoint while leaving the returned image size intact. In both
profiles of [run 42459236870](https://github.com/mingley/partitionline/actions/runs/42459236870),
the diagnostic printed an actual 82,315-byte descriptor at base index 3 and
two 40 KiB exact expected matches in the sampled prefix. The mutation changed
the local test assertion; it did not alter the checkpoint. These two fresh
histories validate the diagnostic wiring and do not explain the earlier
small-image failure.

The [combined tree ecbb6c3](https://github.com/mingley/partitionline/commit/ecbb6c396ceb0520d9f61600d7bef25877a6a145)
overlaid the diagnostic on main
`4409633fa989effe1083e623f6e59fcc0d179eee`, which already includes the
frame-deadline test change. [Run 42459236870](https://github.com/mingley/partitionline/actions/runs/42459236870)
passed 766 default and 914 all-feature broker all-target tests, with three
ignored per profile. Both observer and deadline focused/mutation profiles,
formatting, strict Clippy, inventory and all 55 inventory controls passed.
The ignored descriptor diagnostics were not run in this gate. These passing
histories do not explain the earlier failures.

Earlier [42458559631](https://github.com/mingley/partitionline/actions/runs/42458559631)
and [42458743828](https://github.com/mingley/partitionline/actions/runs/42458743828)
qualified an earlier diagnostic source. The first forced the local reported
size to zero to test output wiring; the second preserved the real descriptor
size. [42458840811](https://github.com/mingley/partitionline/actions/runs/42458840811)
added the conservative empty-data check. The final source also bounds the
role field. No setup failure was reported in these runs.

The final `raft_runtime.rs` Git blob is
`5a9610523130bf3d381b00cf04ebd36bd539fb05`, SHA-256
`f874a60a8c8fe2b9c52ebaa872383ab94d1e21abc72c65d6800095e96ff3286b`.
[Default focused logs](https://github.com/mingley/partitionline/actions/runs/42459236870/artifacts/13609112138)
and [all-feature focused logs](https://github.com/mingley/partitionline/actions/runs/42459236870/artifacts/13608962870)
are retained for 30 days. GitHub reported SHA-256 values
`093ccc134af0fca440feff1eb6d42e57c5c105bad7a297aa5fae1f5ac66dc6f0`
and `0bce2ba57b3ec72a0d87b1ca487c63154f08e079b660c71f4a3ce55fc5c79480`.
The archives were not independently downloaded.
