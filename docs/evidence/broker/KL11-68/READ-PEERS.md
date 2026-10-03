# Ordinary persisted read peers

These are the independent read peers and completed runtime qualification for
KL11-68. Strict compile identities are separate from actual wire, restart and
record evidence. The accepted reports are summarized in
`live-read/validation.json`; every failed numbered attempt is retained.

`ReadControlPeer.java` creates a topic through actual Apache Admin, appends the
two frozen 104/78-byte batches with Produce13/acks1, and runs all 122 frozen
Fetch4–6/ListOffsets1–3 cases for its release. Only `alpha` is renamed and the
correlation is changed, using the official serializers. The live Produce path
assigns partition leader epoch0; the official batch setter adapts that
unprotected metadata from the direct-Partition fixture's epoch-1. The actual full response
must equal the pinned response; the 12 malformed cases must produce clean EOF.
It also captures API18 versions0–4 and requires exactly the seven implemented
ordinary API ranges. Its restart phase uses the allocated topic UUID and the
same durable bytes without reseeding.

`CodecReadPeer.java` reads the accepted c34 Produce histories through actual
Fetch4–6 in both isolation modes. It checks all 120 normalized rich records and
the marker, or the default-codec rejection marker alone, against the independent
KL11-67 plain fixture. It checks exact offsets, timestamps, null/empty/binary
keys and values, ordered duplicate headers, record hashes, batch checksums,
ordinary batch flags, watermarks and retained UUIDs. It makes 12 exchanges and
732 record comparisons per release/run. This uses the stored uncompressed
normalization; no codec capability is inferred from a Fetch response.

The accepted `OrdinaryPeer.java` remains unchanged. Its read phases use actual
manual Apache consumers, forced Fetch4–6/ListOffsets1–3, and the actual Producer
receipts retained under `live-produce/`. The new `ordinary-peer.c` uses actual
librdkafka2.15.0 ordinary producers and manual consumers, including the unchanged
all-topics metadata call and real watermark/timestamp queries. Native runtime
must wait until real Produce>=3 and Fetch>=4 are both advertised.
The optional native read-only topic/count argument also consumes the actual
JavaSDK acks1 histories. `NativeReadPeer.java` independently consumes all 24
native-written records with each real Apache manual consumer, retaining exact
record hashes, headers, offsets, watermarks, timestamps and topic UUIDs before
and after restart.

Strict Java preparation is reproducible with:

```sh
python3 docs/evidence/broker/KL11-68/prepare-read-peers.py --attempt 6
```

Use a new attempt number. Attempts1/2 compiled the read controls; attempt3 also
compiled the codec reader; attempt4 also compiled the native-history readers.
Namespace adaptation only changes
`common.record.internal` to `common.record` for Apache4.1/4.2. Attempt5 also
includes the explicit live leader-epoch adaptation. The current proof
is `read-control-build/validation-attempt-5.json`. Native strict build/source
identities and the retained initial compile failure are under
`native-peer-build/`.

`run-read-live.py` starts `tests/fetch.rs::serve_live_probe` from an immutable
source archive with `PARTITIONLINE_FETCH_LIVE_PORT/DIR`. Its trusted job JSON is
a list of objects with `peer` (`ordinary`, `read_control`, `codec_read`, `native`, `native_read`),
`phase`, and, for Java peers, `release` and `state`. `native_read` selects the
Apache manual consumer of the native history; optional native `topic`/`count`
select the read-only JavaSDK history. It retains the exact commands,
compiler, source/SDK/class/native hashes, peer logs, client state and the complete
bounded journal snapshot. Supply `--read-build` with the current strict-build
proof. Toolchain targets use debug0, incremental0, jobs1 and CPUs0–2,4.

Upgrade-read attempts copy the accepted c34 catalog and partition journals into
a fresh read attempt. Original accepted Produce histories are immutable. Fresh
native/control topics then exercise the composed router, followed by a separate
process restart. Local acks-1 still means the declared single-node ISR and local
fsync; these peers establish no replicated or transactional durability claim.

Accepted full-source runs are stable `attempt-4`/`attempt-5-restart` and
MSRV `attempt-1`/`attempt-2-restart`. Stable attempts1–3 remain development
history: an expected epoch mismatch, an external native configuration spelling,
and a native message/fetch-size configuration constraint. Their initial Rust
binary was built from a partial archive that excluded a lint configuration;
its product/test/fixture blobs match the frozen source. Accepted runs use the
full 17,370-file archive and separate verified full-source binaries. The sibling
build's commands, compilers, before/after Git identities and test receipts are
copied under `live-read/prebuilt/`; no second build is claimed.
