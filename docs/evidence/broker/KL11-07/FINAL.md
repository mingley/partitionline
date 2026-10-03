# Frozen Fetch/ListOffsets validation

The production source is pushed main `58810f2ed90ac9643d59a60098a29f5a2f87a8d0`.
The explicit ordinary read/write constructor serves Fetch4–6/ListOffsets1–3;
metadata4 and Produce5 constructors preserve their prior profiles. Ordinary-only
readCommitted has LSO=HW/no aborts because transaction/control/idempotent writes
remain rejected. Implementation and resource/cancellation limits are in README.md.

| Frozen source | SHA256 |
| --- | --- |
| partitionline-broker/src/fetch.rs | `5252f9a682fcf1f03fb11cfa2949636007140d227b1a860543fb80910aa67701` |
| partitionline-broker/src/metadata.rs | `90e82f56d4fd3891faa3a89eb4b701d65086f5885c1ba8b418eada48c006fafe` |
| partitionline-broker/src/produce.rs | `dc664197faf139a64af4f0e7b1a589662907c8aaa126d96b25bb2338d687ab7f` |
| partitionline-broker/tests/fetch.rs | `9df92db9a23f24fd1139b8e14231b606282f57169c9e125af850f258997947c0` |

A complete archive of all17370 committed Git blobs/configuration was checked
before and after every QA command. The manifest aggregate is
`301f24f45b30331a9a0c4f6fe1fb953c430865b440b96d5d628be250629512c0`.
Full Git blob identities and file hashes are retained in
`final-qualified/source-integrity.json`; the exact executed evidence harness is
retained separately and does not pretend it was committed in58810.

| Toolchain | Default tests | All-feature tests | Format/strict Clippy/rustdoc | Doctests |
| --- | ---: | ---: | --- | ---: |
| stable1.99.0 | 151 | 225 | pass(default/all) | 0(default/all; strict compile pass) |
| Rust1.85.0 | 151 | 225 | pass(default/all) | 0(default/all; strict compile pass) |

No test failures or ignored tests occurred in those four final cells. Counts
include normally inert live harness tests; actual independent sessions are bound
separately below. The four report files are byte-identical across toolchains and
features:99 ApiVersions/header cases,555 metadata/admin cases,708 Produce cases,
and366 Fetch/ListOffsets cases. Read cases are330 exact authentic Apache fixture
responses and36 complete structural closes, with15 actual seven-profile API18
serializations covering versions0–4 and all three request fixture releases.
All330 raw read response files are retained per cell. These assembler fixtures
plus actual Apache LogSegment probes have the scope described in KL11-68; no
full KafkaServer session compatibility is inferred from direct fixture replay.

Nineteen Fetch integration tests cover the named actor/store/scan/wait/cancel,
restart/identity/ordering and actual TCP controls in README.md.
`validate-final.py` verifies the whole-source QA receipts, exact outcomes/raw
bytes and four matching-count/wrong-range counterexamples. The independent
c_peer static actor review found the combined read retention omission; the frozen
implementation now enforces one explicit default/max1GiB combined envelope and
tests boundary-minus1/unpolled completed admission/cancel release. The review
hash is `763f526826ad1b80976430c8f094a08b59cd223fdea3796fd5c9be62944f8369`.

The first dirty-tree development test run caught the Fetch error abort-array
encoding and three tests' expected leader-epoch mismatch (Produce correctly
assigns0; lower direct Partition seeds retain-1). A queue-barrier test also needed
bounded admission retry. Full later logs and the explicitly partial first PTY
summary are preserved in `development/`. The first immutable execution omitted
root clippy.toml from a4387-file selected archive, causing83 test-unwrap lint
failures. That failed prerequisite is preserved in `final/`; corrected final
QA uses the complete tree with its committed test lint policy. Production source
was not changed to resolve the harness omission.

Independent Apache4.1.2/4.2.1/4.3.1 and pinned native2.15 accepted live sessions
ran from the complete source58810/default-feature binaries on stable and Rust1.85.
Initial/restart runs total66 peer executions/79590 assertions,1464 live read cases
(1320 complete responses/144 actual EOFs),11328 exact record hash comparisons,
48 native deliveries and408 native consume receipts. All44 allocated topic UUIDs
survived reopening. Both restart comparisons retain identical catalog/partition
bytes. Native/Java reads cover earlier actual c34 ordinary/normalized histories
and fresh native writes consumed by each Java SDK. Cross-language receipts and
all raw peer/server failures, source/class/jar/binary hashes and commands remain
in KL11-68. The aggregate hash is
`255771ca141350ccc4e61d5d02929178aed76d2005c82a00f0a3929080be5c3d`.
Actual full-frame API18seven-profile checks include60 independent exchanges.
Live expected seed batches use official Apache setPartitionLeaderEpoch(0),
preserving direct-Partition fixture epoch-1 outside protected CRC.

`independent-runtime-bindings.json` verifies each accepted raw receipt hash,
zero peer/server exits, native/API aggregate identities and both unchanged
restart snapshots. The additive shared gate source is pushed
`2db175c70d7f3e0c15390a1e6c97903958cfee09`; its four17383-file full-snapshot gate
cells,48 mutation guards and all60 actual API18exchanges pass. Frozen production
and controller blobs remain identical to their existing pushed pins. Gate receipt
hash: `8b0733c9c87c99baf1c4170b537a4746356970fc323005ad20421f0fcef446d7`.
The separate immutable matrix and these independent receipts make07 eligible
for closure; no pending implementation or acceptance gate remains.

No incremental sessions, replica reads, retention, transactions, group
coordination, replication, cross-process storage locking, full production or
comparative performance qualification is claimed.
