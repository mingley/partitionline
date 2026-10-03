This standalone test-only adopter exercises the public `partitionline` Producer, manually assigned Consumer, and Admin against the persisted single-node broker. It adds no dependency or feature to the published client or broker defaults.

`src/main.rs` creates and reads ordinary uncompressed records under acks0/1/-1, preserves null/empty keys and values plus ordered duplicate/null headers, checks exact offsets/timestamps, calls real ListOffsets, checks precise unavailable operations, and deletes/recreates a topic by UUID. Restart retains every prior UUID and seeded ID, rejects the deleted UUID, and appends fresh checkpoints into the new namespace. A read-only recovery phase checks all seed receipts before the checkpoint writes. All client operations use explicit resource/deadline bounds; the process has a 90-second deadline and a 1MiB report bound.

`CrossReadPeer.java` uses each official pinned Apache SDK's actual Admin/manual Consumer to read Rust/native Producer histories. The frozen KL11-68 `OrdinaryPeer.java` supplies fresh Java Produce3–13, acks0 channel/EOF, CRC-corrupt input and manual Fetch/ListOffsets histories; `ordinary-peer.c` supplies genuine librdkafka Producer/manual Consumer calls. `JournalPeer.java` independently decodes each actual persisted Kafka payload, including the retired identity's separate journal. `pins.json` names the immutable upstream inputs; existing evidence is used only as pinned source/build provenance, never as new runtime results.

After this source is pushed, create a full immutable Git archive and a per-file Git blob/SHA256 integrity receipt. Use one bounded target directory with debug information and incremental compilation disabled. Prepare all peers and default-feature real server binaries:

```sh
python3 scripts/broker-interop.py prepare --source /absolute/source --integrity /absolute/integrity.json --scratch /absolute/work/peers --target /absolute/work/target --evidence /absolute/evidence/build-attempt-1
```

For each `stable`/`1.85.0` × `default`/`all-features` lane, use fresh state/client/evidence paths and the pinned preparation receipt:

```sh
python3 scripts/broker-interop.py run --source /absolute/source --integrity /absolute/integrity.json --scratch /absolute/work/peers --target /absolute/work/target --evidence /absolute/evidence/seed-attempt-1 --preparation /absolute/evidence/build-attempt-1/preparation.json --toolchain stable --features default --port 19135 --state /absolute/work/lane/state --clients /absolute/work/lane/clients
```

Repeat with the same state/clients, a fresh restart evidence directory and `--restart`. Each process failure/timeout remains in its attempt directory; retries use fresh numbered attempts rather than rewriting histories. Seed executes27 genuine peer processes and restart24, bounded to32 per attempt. Restart checks every complete journal hash at startup and after the read-only public Consumer phase, before any checkpoint append.

Write a trusted local `runs.json` with four `lanes` entries containing `toolchain`, `features`, and absolute accepted `seed`/`restart` evidence directories. `seal.py` validates all actual processes, full source/binary/jar/library pins, exact independently reconstructed record receipts, fresh on-wire seven-API profiles, malformed CRC/error/EOF results, retained IDs and journal hashes. It reverse-decodes all25 partition journals (including the deleted namespace) with all three Apache releases and compares every persisted receipt:

```sh
python3 tests/conformance/broker/interop/seal.py --source /absolute/source --integrity /absolute/integrity.json --preparation /absolute/evidence/build-attempt-1/preparation.json --runs /absolute/runs.json --output /absolute/evidence/sealed-attempt-1
```

The profile is Produce3–13, Fetch4–6, ListOffsets1–3, Metadata0–13, ApiVersions0–4, CreateTopics2–4 and DeleteTopics1–6. These local ordinary histories do not qualify replication, group coordination, transactions, idempotence, retention, optional server codecs or production readiness. Positive acks sync locally; acks-1 refers to the declared single-node ISR. acks0 has no durable client receipt and is checked through subsequent genuine reads. The deliberately corrupt input is an actual wire CRC fault; physical journal corruption has separate storage-card qualification.
