# Ordinary Produce / Fetch / ListOffsets oracle

This first source batch contains independently generated Apache Kafka 4.1.2,
4.2.1 and 4.3.1 wire fixtures for ordinary Produce 3–13, Fetch 4–6 and
ListOffsets 1–3. It contains 1,074 cases: 708 Produce and 366 Fetch/ListOffsets.
All three generators compile with `-Xlint:all -Werror`, execute twice with
identical complete output hashes, and produce identical cross-release wire
bytes. The only release adaptation changes record-class imports to the package
actually exposed by each pinned jar.

There is no executed Rust handler or full Apache broker qualification in this
batch. Success outcomes are explicitly assembled for the declared ordinary
single-node log policy and encoded and parsed by independent Apache classes.
Error manifests separately record actual `getErrorResponse`, record-helper,
CRC validation and parser outcomes. Authentic Apache storage component methods
execute in `LogStorageProbe.java`; their outcomes are not labeled broker
runtime outcomes.

`upstream-pins.json` retains the exact official distribution SHA-512/SHA-256,
client jar SHA-256 and full Apache source commit for each release. Eighty-one
source/schema files retain their upstream URLs, byte lengths and SHA-256s.
The pinned librdkafka 2.15.0 feature source shows that magic-2 depends on both
Produce >=3 and Fetch >=4. Native ordinary Produce qualification therefore
requires a real Fetch handler and truthful API advertisement.

## Fixture contract

Binary frames contain the Kafka request/response header and body without the
four-byte length prefix. Each regular binary is at most 128 KiB. Correlation ID
is 7; client ID is `produce-oracle` or `fetch-oracle`. The five TSV columns are
case name, API key, API version, seed and expected outcome. Outcomes are
`response`, `no_response_keep_open`, `close` or `structural_reject`.

The `fixture` seed is the canonical catalog: node 0, loopback port 19095,
cluster `partitionline-fixture`, alpha UUID 2/two partitions and internal
consumer-offsets UUID 3/one partition. Each case starts with fresh empty
partition journals. Produce success assigns base offset 0. `acks=1` and `-1`
require local sync before success; `-1` covers only the declared ISR `[0]` and
does not establish replication. `acks=0` success keeps the connection open
without a response; a partition error closes it, as pinned KafkaApis source
specifies. The initial batch has 192 response, 11 keep-open, 11 close and 22
structural cases per release.

The `fixture_small_entry` seed sets entry bytes 80, journal file bytes 4096,
index entries 16, fetch bytes 4096 and maximum stores 2; aggregate disk/index
budgets are 8192/2048. Its otherwise valid ordinary Apache batch exceeds the
entry limit and selects local MESSAGE_TOO_LARGE (10), with the actual Apache
error serializer executed.

The `log` seed additionally populates alpha partition 0 with Apache-built
uncompressed batches: offset 0/count 3/timestamps 1000,1007,1003 (104 bytes),
then offset 3/count 1/timestamp 1010 (78 bytes). Partition 1 is empty. Log start
is 0 and high-watermark/last-stable-offset are 4. Both seed batch files match
the independently executed storage probe bytes. Read-committed fixtures are
valid only under the ordinary-only profile that rejects every transactional
write; their aborted-transaction list is empty. Fetch/ListOffsets contribute
110 responses and 12 structural rejects per release.

## Semantic limits and observed differences

Produce requires exactly one magic-2 batch per partition. Actual Apache
`ProduceRequest.validateRecords` rejects zero/multiple batches and magic 1 with
INVALID_RECORD (87). The helper does not verify CRC: actual batch
`ensureValid` separately detects protected payload damage as CORRUPT_MESSAGE
(2). Null record data passes that helper, but the actual error method can throw
while deriving partition sizes; the explicit local null-record rejection 87
is therefore labeled stricter policy, with that upstream exception retained.

Non-null transaction IDs, idempotent/transactional/control batches and
LogAppendTime batches select the explicit local ordinary/CreateTime-only
restriction UNSUPPORTED_FOR_MESSAGE_FORMAT (43). Apache can encode and parse
these features; code 43 is not claimed to be its semantic rejection.
Transactions, coordinators, control-marker execution and producer sequencing
are excluded. The first batch is uncompressed; compression qualification is
pending its separately pinned codec lane. Higher Fetch/ListOffsets versions,
incremental sessions, replicas, flexible Fetch, UUID Fetch, epochs and tiered
storage remain outside this wire profile.

Actual Apache LogSegment probes execute 36 reads and 51 timestamp searches
across the three releases. Offset 1/2 reads return the containing batch 0 plus
the following batch. `maxBytes=1,minOneMessage=true` returns the whole first
available batch (104 or 78 bytes), establishing the KIP-74 exception. The
configured local output ceiling still applies. A 105-byte component read can
include one byte of the next batch; the wire corpus deliberately does not
claim that partial trailing-batch behavior equals a complete-batch local
output policy. Segment reads alone accept negative offsets; pinned LocalLog
checks separately establish broker offset-range rejection.

Timestamp search returns the first offset whose timestamp meets the query,
not the lowest matching timestamp: query 1003 returns timestamp 1007/offset 1.
No matching timestamp returns success offset/timestamp -1. Actual storage
timestamp searches accept negative numbers, but pinned ListOffsets dispatcher
admits only earliest -2/latest -1 for versions 1–3 and rejects all other
negative sentinels as UNSUPPORTED_VERSION (35). Special sentinel results use
timestamp -1 and offset 0/4, separately from raw timestamp search.

## Reproduction and retained development failures

Run from the repository with the verified distribution archives and client
jars in scratch storage:

```sh
taskset -c 0-2,4 python3 docs/evidence/broker/KL11-68/pin-upstream.py
taskset -c 0-2,4 python3 docs/evidence/broker/KL11-68/probe-storage.py --jars /workspace/work/broker-wire/jars --distributions /workspace/work/broker-wire/releases --scratch /workspace/work/broker-log-oracle/storage-probes
taskset -c 0-2,4 python3 docs/evidence/broker/KL11-68/generate-produce.py --jars /workspace/work/broker-wire/jars --scratch /workspace/work/broker-log-oracle/produce --output /workspace/partitionline/partitionline-broker/tests/fixtures/produce
taskset -c 0-2,4 python3 docs/evidence/broker/KL11-68/generate-fetch.py --jars /workspace/work/broker-wire/jars --scratch /workspace/work/broker-log-oracle/fetch --output /workspace/partitionline/partitionline-broker/tests/fixtures/fetch
```

Two Produce oracle replay failures remain under `failures/produce-attempt-1`
and `produce-attempt-2`: convenience Apache control/LogAppendTime builders use
wallclock timestamps, so their output changed between replays. Explicit
bounded builders with timestamp 1000 corrected both. The initial storage
runtime failure `logs/storage-runtime-4.3.1-attempt-1.txt` retains the missing
Yammer metrics dependency; the reproduction runner extracts and hashes the
official metrics jar from the pinned distribution. These are oracle setup
failures, not Rust product failures. All final strict compilation, generation
and replay logs remain retained.
