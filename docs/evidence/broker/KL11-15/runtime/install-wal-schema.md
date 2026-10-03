The snapshot-enabled owner extends the existing PLREPL01 operation journal with
opcode5. This is a custom local receipt format and caller-driven typed exchange;
it adds no Kafka API advertisement, snapshot wire codec, physical compaction or
application-state fold. All integer fields below are big endian. Journal framing,
CRC32C, offset/count checks and fsync behavior remain the existing Journal contract.

| Offset | Width | Field |
| --- | --- | --- |
| 0 | 8 | `PLREPL01` |
| 8 | 1 | opcode5 |
| 9 | 7 | zero |
| 16 | 1 | authority:0 local checkpoint,1 remote install |
| 17 | 7 | zero |
| 24 | 8 | receiving current normalized term |
| 32 | 4 | configured leader ID |
| 36 | 4 | receiving local peer ID |
| 40 | 8 | sequence:0 local,positive remote |
| 48 | 8 | leader committed end |
| 56 | 16 | nonzero image generation |
| 72 | 8 | image base normalized term |
| 80 | 8 | image base inclusive one-based index |
| 88 | 4 | canonical full-prefix record count |
| 92 | 4 | zero |
| 96 | 8 | exact total opaque payload bytes |
| 104 | 8 | exact encoded image bytes including completion seal |
| 112 | 4 | full-image CRC32C including completion seal |
| 116 | 4 | zero |
| 120 | 8 | prior local tail term |
| 128 | 8 | prior local tail index |
| 136 | 8 | prior committed end |
| 144 | 8 | retained result tail term |
| 152 | 8 | retained result tail index |
| 160 | 8 | resulting committed end |
| 168 | 1 | prior selection present:0 or1 |
| 169 | 7 | zero |
| 176 | 16 | prior selected generation;zero when absent |
| 192 | 8 | prior selected base term;zero when absent |
| 200 | 8 | prior selected base index;zero when absent |
| 208 | 4 | prior selected checksum;zero when absent |
| 212 | 4 | zero |
| 216 | variable | exact Init transferable group bytes |

The group bytes equal Init's payload starting at offset20, after its local ID:
partition i32, voter count u16, cluster length u16, topic length u16, zero2,
sorted u32 voters, UTF8 cluster and topic. They must equal the configured Init
group and image Identity exactly; the source replica's local ID is excluded.

Replay requires the prior tail, commit and selected image to equal the actual
preceding WAL state. It loads and independently validates the referenced complete
image, checks every old committed record byte for byte, and recomputes the
retained suffix and resulting tail. Same-term changed bytes are rejected even
above commit. Conflicting uncommitted content can be discarded only under a term
strictly above the old tail term. The base/commit cannot regress. Local receipts
require leader=peer=local, sequence0 and base=old commit; remote receipts require
a different configured leader, peer=local, positive sequence and leader commit
at least the base. Receiving term must cover the image terms and preceding WAL
authority. Local commit is unchanged; remote commit becomes the verified base.

Only a synchronized receipt selects an image. A published image without a receipt
remains inert and consumes a finite generation slot. The owner preserves all
canonical prefix records in memory and retains an exactly corresponding later
suffix; later Append/Truncate/Commit operations replay normally. No current term
or vote is read from an image. Recovery reconciles the receiver's old election
tail using that exact tail's prior WAL-confirmed commit floor, allowing an Install
to advance commit beyond the old tail without weakening committed overlap checks.

Development failure artifacts under `development/floor-first/` retain the actual
pre-fix source, logs, WAL/election/image bytes and successful same-file recovery
after the floor fix. Final qualification must bind a pushed immutable source SHA.
