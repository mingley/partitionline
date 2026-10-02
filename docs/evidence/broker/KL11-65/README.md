# Assigned ordinary batch storage

The storage adapter validates an entire ordinary, uncompressed magic2 input,
assigns contiguous signed Kafka offsets, then writes the complete input as one
checksummed, synchronized journal entry. It changes only each batch's first
eight bytes. A bad later batch cannot append an earlier prefix. Recovery checks
inner record CRCs, exact counts and assigned offset ranges even when outer
journal checksums are valid. Fetch returns whole journal entries and applies
explicit entry and output budgets.

Seven real-file tests cover multi-batch byte preservation and restart, invalid
and unsupported input, signed offset overflow, entry/index/output/disk limits,
four forged payload variants, all 106 incomplete final-entry prefixes and live
corruption that poisons the handle without returning a partial vector. Both
toolchains pass these tests with defaults and all features. The final full
broker matrix at `4e70bbd1cfab39c59e975989b0d480b032dbe0de` also passes strict
format, all-target Clippy and rustdoc. Module and test bytes match the initial
storage implementation and the earlier tested `6b4d330` source.

Actual output is a 350-byte journal containing four records at offsets 7–10
and a fifth at 11, with batch bases 7, 8 and 11 and exclusive end 12. Pinned
Apache Kafka 4.3.1 decodes both original and assigned records and verifies their
CRCs. The independent bitwise checker verifies journal header/payload CRCs,
batch CRCs, unchanged protected bytes and exact offsets, timestamps, keys and
values against Apache's original history. All four final toolchain/feature
proofs are byte-identical. Eleven independent commands pass.

Reproduce the proof using retained `final-results-retry1` payloads:

```sh
python3 -B docs/evidence/broker/KL11-65/run-proof.py \
  --proof-root /absolute/repo/docs/evidence/broker/KL11-65/final-results-retry1 \
  --fixtures /absolute/repo/partitionline-broker/tests/fixtures/records \
  --jar /absolute/kafka-clients-4.3.1.jar \
  --slf4j /absolute/slf4j-api-1.7.36.jar \
  --output /absolute/new/proof-output
```

`results.json` records source pins, commands, counts, limitations and referenced
full-matrix logs. `artifact-checksums.json` covers this evidence. Failed ENOSPC,
superseded Metadata helper lint and sparse developmental archive attempts remain
retained. The first proof producer mixed SLF4J stderr with JSON stdout and failed
parsing; the corrected producer retains the two streams separately.

This is a synchronous storage API: callers bound scheduling, retained outputs
and cross-process path ownership. Failed I/O has an ambiguous durability outcome
and poisons the handle. Codecs, idempotence, transactions, replication, wire
behavior, physical power-loss qualification and comparative performance are
separate work.
