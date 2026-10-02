# Independent Metadata and topic-admin evidence

`MetadataOracle.java` loads only the official Apache Kafka client classes. It emits Kafka request/response headers plus bodies (without the four-byte transport length), then uses the official Apache request/response parsers to consume and reproduce every byte. The generator does not load, import, or read partitionline production code. `generate.py` verifies the pinned input jars, compiles with `-Xlint:all -Werror`, runs each release twice, checks every retained checksum, and requires every advertised API/version pair. The 175 cases per release are identical across Apache 4.1.2, 4.2.1 and 4.3.1: 525 declared cases, comprising 519 response goldens and six deliberate local selector rejections. Rejected cases retain request frames and set `response_hex` to null; their historical malformed response frames remain under `schema-only/`, excluded from successful router comparisons.

The fixture seed is node 0 at 127.0.0.1:19095, cluster `partitionline-fixture`, `alpha` with UUID 2 and two partitions, and `__consumer_offsets` with UUID 3 and one partition. Each case starts from its declared independent `fixture` or `empty` seed. Correlation ID is 7 and client ID is `metadata-oracle`. Create defaults are one partition and replication factor one. Fixtures cover the composed registry (Metadata 0–13, ApiVersions 0–4, CreateTopics 2–4, DeleteTopics 1–6), named/all/empty/missing/invalid requests, authorized operations, nullable names, topic IDs, duplicate IDs/names/aliases, flexible unknown tags, validate-only, manual assignment, and v4 defaults.

## What was actually executed

The official Apache serializers and parsers executed for every golden. Metadata v10/v11 semantic failures also execute the actual Apache `MetadataRequest.getErrorResponse` method to pin its broker-free error envelope. Selecting that error for nonzero IDs or null names follows the exact pinned `KafkaApis` source guard. Ordinary success and local validation responses are assembled from the declared single-node fixture policy and encoded by Apache; those values are not responses from an executed Apache broker/controller. Each case's `basis` records that distinction, and each manifest retains the actual Apache parsed request and response.

`upstream-topic-policy.json` additionally retains the actually executed Apache topic-name validation, internal-name classification, and collision-method outcomes. `upstream-pins.json` pins all three official distribution and jar identities plus 45 retained exact schema/request/controller snapshots. `pin-upstream.py` can fetch them again by full upstream commit. Those source files retain their ASF license notices. Controller inspection verifies duplicate and name/ID alias rules, the reserved internal-topic name, manual-assignment field requirements, and the pre-v12 Metadata semantic guard. Local replication-factor/configuration/resource policies and their messages are deliberately declared local behavior. The v12/v13 null-name/zero-ID selector is deliberately rejected locally before response encoding. Official schema serialization/parsing accepts the field combination, while actual upstream `Topic.validate(null)` throws `NullPointerException` in all three jars. There is no upstream broker/controller runtime claim. The prior source b2937766 contained a policy-assembled error3 response with neither identity populated; those exact diagnostic frames and parser outcomes are retained under `schema-only/`, and the prior 525-response comparison is superseded by 519 valid response comparisons plus six explicit local rejections. No authorizer is configured; topic operations 3576 and cluster operations 8096 follow the pinned source. Apache topic-result ordering is unspecified (Delete shuffles); these byte goldens use the router's declared deterministic ordering. Live peers compare topic identities/sets rather than claiming upstream ordering.

`MetadataPeer.java` is an independent forced-version TCP peer plus the actual official `AdminClient`. `metadata-peer.c` uses actual librdkafka Admin/Metadata APIs. The live runner binds the real committed Rust router on a fresh configurable loopback port, creates topics, restarts the process with the same durable catalog, and checks retained IDs before deleting/recreating them. Live runtime outcomes are recorded separately from serializer goldens. This evidence does not qualify Produce, replication, authorization, arbitrary topic configs, or higher CreateTopics versions.

## Reproduction

Run from the repository root, with the previously verified official Kafka jars and SLF4J jar available:

```sh
python3 docs/evidence/broker/KL11-63/generate.py \
  --jars /workspace/work/broker-wire/jars \
  --scratch /workspace/work/broker-metadata \
  --output /workspace/partitionline/partitionline-broker/tests/fixtures/metadata
python3 docs/evidence/broker/KL11-63/pin-upstream.py
```

`oracle-generation.json` and `logs/` retain commands, actual outcomes, repeat checks, cross-release checks and per-file hashes. `failures/` retains preparation failures; no failed runtime or generator attempt is substituted by a successful result.
