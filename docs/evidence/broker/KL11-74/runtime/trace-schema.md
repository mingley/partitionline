# Development dynamic membership journal/trace schema

This is an implementation schema for KL11-74, not a qualification result.
The corrected developmental source is frozen in `source-freeze.json`; final
pushed-source qualification is still pending. Fixed PLREPL01/PLELECT1/PLSNAP01
bytes remain distinct.
All multibyte integers below are big-endian. Outer Journal framing, CRC32C,
record_count=1 and operation offsets are the existing Journal contract.

## Canonical configuration

PLVOTR01 is defined in membership.rs. Header40: magic8, configuration epoch8,
position term8/index8, finalized feature i16=1, count u16, zero4. Voter28: ID4,
directory16, feature min/max i16, endpoint count u16, zero2. Endpoint6 followed
by strings: listener length u16, host length u16, port u16, listener bytes, host
bytes. Voters and endpoints sort canonically; UTF8/reserved/count/size checks
precede allocation. Max64 voters, four endpoints each, 249-byte strings,
135168 encoded bytes. Genesis epoch0/position(0,0); each successor adds/removes
exactly one ID, preserves every other descriptor and increments epoch by one.

## Dynamic content journal

Init op1: PLREPL02 + op1 + zero7, then the unchanged fixed Init identity body:
local ID4, partition4, count u16, cluster length u16, topic length u16, zero2,
sorted genesis IDs4 each, cluster/topic UTF8 bytes. Append local directory16,
genesis byte length4 and full PLVOTR01 genesis. Both local ID and local directory are absent from the
transferable group identity used in Install receipts. Its exact bytes are the
old Init body after local ID (partition/count/string lengths/reserved/IDs/strings),
followed by genesis length4 and canonical genesis; this order differs from the
election canonical group described below.

Ops2–5: PLREPL02 + operation byte + zero7, resulting configuration length4 + zero4,
canonical resulting configuration, an 80-byte authority envelope, then the
unchanged PLREPL01 operation body. Authority offsets relative to its start:

| Offset | Field |
| --- | --- |
| 0 | authority kind1 (0 local, 1 received) + zero7 |
| 8 | authorizing current term8 |
| 16 | leader ID4, intended receiver ID4 |
| 24 | sequence8 (zero only for local operations) |
| 32 | source leader_commit8 (prior local commit for local authority) |
| 40 | source leader directory16 |
| 56 | receiver directory16 |
| 72 | sender configuration epoch8 |

Exact receipt/body term/IDs/sequence/commit agree. Receiver directory must be the
local immutable key. The source must be a known full voter key; a pending removed
leader is eligible only in that configuration's same term. Local checkpoint
selection can preserve an observer's already committed prefix, with no majority
or leader lease invented. Append replay checks the prior configuration against
the recorded source commit floor, allowing committed historical catch-up while
rejecting a local double-change before the prior configuration commits.

The exact configuration is independently recomputed from actual records/images
and suffix after every replayed operation; a checksum-valid forged result fails.
Record kind byte2 is a Voters entry; its payload position must exactly equal the
record term/index. Kind0 is opaque Data and kind1 is an empty Barrier.

The latest operation that actually changes the configuration is its authoritative
source ordinal. An election state binds the full canonical configuration and
that exact content-Journal ordinal. Bounded random fetch validates historical
references; operation0 refers only to the immutable matching Init genesis.
For a positive source ordinal, the referenced append/truncate/install result
must differ from the preceding operation's canonical result, or genesis for
predecessor0. An equal-view Data or Commit receipt cannot redefine the origin,
even when a later genuine configuration change restores the final context.
Truncation cannot cross durable commit. Installing an image preserves exact old
committed overlap and reconstructs the active configuration from image+retained
suffix. Physical records remain bounded and retained; there is no compaction.

## Directory-qualified election journal

PLDINIT2: magic8, local ID4, local directory16, genesis length4, zero8, genesis,
group length4, canonical group. Canonical group is cluster length u16+bytes,
topic length u16+bytes, partition u32 (max506 bytes). Full exact initialization
must match the supplied local/group/genesis before any state is recovered.

PLELECT2 / PLRECON2 / PLELCFG2 header96:

| Offset | Field |
| --- | --- |
| 0 | magic8 |
| 8 | local ID4 |
| 12 | local directory16 |
| 28 | current term8 |
| 36 | voted-for ID4 (zero when absent) |
| 40 | vote present1 |
| 41 | zero3 |
| 44 | voted-for directory16 (zero when absent) |
| 60 | durable summary term8/index8 |
| 76 | content configuration-source ordinal8 |
| 84 | canonical configuration length4 |
| 88 | zero8 |
| 96 | canonical configuration bytes |

PLRECON2 appends prior summary term8/index8 + prior verified commit floor8.
PLELCFG2 appends latest committed configuration index8 and preserves exact term,
numeric vote and vote directory. Rollback cannot erase a committed configuration.
Historical votes remain tied to their original directory even after removal or
same-ID re-addition; they are never reinterpreted through the current voter map.

## Dynamic images

PLSNAP02/version2 uses the old fixed header fields and completion seal, adds
genesis length4+canonical genesis before header CRC, and permits entry flag2 for
canonical Voters records. Both create and streaming decode validate exact config
successors from that immutable genesis. Foreign genesis/directory histories and
incompatible typed flags fail. Current term/vote remain receiver-local. The
inactive publication and authoritative Install receipt rules are unchanged.

## Runtime causal capture format

The developmental stable run already retains3/5 histories with132/201 events
and28/42 paired checkpoints. These are runtime observations, not final independent
qualification. Each event has ordinal, monotonic caller now_ms, kind, acting `key`,
actual args/result and post-operation states for all owners. Context contains
source leader/candidate Key, intended peer Key and sender configuration epoch;
requests additionally contain term, sequence, previous/target, leader_commit and
all typed record bytes. Actual emitted vote/feature/append/image messages are
recorded before delivery. Deliberately changed inputs carry input_origin with
source ordinal and changed_fields, and never qualify as majority receipts.

Each checkpoint copies the actual content/election Journals and image directory,
records path/length/digest in the external seal, and binds the event ordinal,
local Key, immutable group/genesis, confirmed operation/state counts, current
configuration, committed prefix and selected descriptor. Child-process exit
captures precede actual exit; the parent records the observed exit code and
carries the child clock epoch before reopening and making fresh checkpoints.
The independent oracle reconstructs configurations and commits from raw bytes,
then checks correlations, distinct NEW-majority matches/current-term commits,
committed prefixes, saved directory votes and process/recovery causality.

## Explicit profile limits/policies

A receiver must already know a leader's full directory/configuration. It rejects
an unknown newly added leader before term/content mutation; self-supplied voter
history grants no authority. Trusted discovery/continuous autonomous catch-up
remain KL11-76/70. Known candidate directories may request votes across local
configuration epochs; fresh-log, single-vote and exact source-correlation checks
still apply, allowing completion of an uncommitted change.

Committed removal always fences a removed leader, even after caller timeout.
All three authentic Apache bare-component late-removal probes leave their resign
flag false after the pending RPC expires; this stronger local fence is explicit.
Positive recent prior catch-up follows the authentic helper rather than requiring
equality to current LEO. Raw ACKs separately require exact emitted context/target,
monotonic progress and local durable bounds. No native modern wire or early
semantic configuration ACK is claimed. An appended ChangeReceipt with
committed=false is only an uncommitted position.

The current driver saves `trace.json`, `events-progress.jsonl`,
`checkpoints-progress.jsonl`, `last-states.jsonl` and `clock.txt`. Top-level
`group.genesis` and `locals` carry full canonical descriptions. Post-state `voters`
contains canonical_hex/epoch/position/feature/descriptors; `voted_for` is a full
Key or null. Content-source ordinals are reconstructed independently from the
paired raw content/election files; no emitted safety verdict substitutes for that
reconstruction. Checkpoints contain relative wal_path/election_path/images_dir
and confirmed counts. External manifests bind actual bytes, modes and source
overlay/command identity without modifying these originals.

A child actually exits88 after NEW(n+1) remains uncommitted with only the old
minimum majority. Its last caller clock16 is persisted; the parent records the
observed exit at17 before reopening owners. The source-bound driver uses exact
actual vote/feature/append/image structures, including barrier and Voters kinds.
A declared directory mutant and duplicate actual ACK both fail without content
mutation. New-majority commitment and committed removal are derived from these
causal receipts and actual journals by the independent checker.
