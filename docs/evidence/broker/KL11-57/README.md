# KL11-57 upstream API inventory evidence

The Apache Kafka 4.1.2, 4.2.1, and 4.3.1 inventories each contain all 93 assigned
API keys and complete request/response pairs. Each release has 89 active keys
and four removed keys (4–7), with 48 broker-only, 12 controller-only, and 29
both-listener APIs. No handler, implemented API version, or qualification is
claimed. All active feature statuses remain missing and not run; removed keys
have an explicit upstream not-applicable reason.

`tests/conformance/broker/api-matrix.json` records source commits, original and
retained archive checksums, per-file hashes, both directions' exact ranges,
listeners, unstable versions, ApiKeys flags, and valid-version header mappings.
The retained source archives preserve LICENSE and NOTICE, protocol JSON, and
relevant generators. The source subset is 207, 207, and 208 files respectively,
in three approximately 72 KiB archives outside the client package. Rebuilding
from all three checksum-pinned full originals reproduced the committed subset
archives and matrix byte for byte. Python 3.12.14 and zlib 1.3.2 were used.

[The static verification](matrix-verification.json) passed. The verifier reads
bounded tar members in memory without filesystem extraction, independently
anchors reviewed archive digests, and rejects corrupt hashes, missing or
duplicate keys/pairs, mismatched schema/header/flag classifications, and false
implementation claims. The 24 [guard tests](mutation-tests.log) passed, including
comment scanning with URLs and escaped quotes inside strings, unsafe archive
members, size bounds, changed schemas, and deliberately false classifications.
Six [negative CLI executions](negative-checks.json) retain failed reports and
exit 1 exactly as required. Those reports remain failed; a passing expectation
means the invalid input was rejected.

[The independent Java oracle](upstream-java-oracle.json) compiles against the
official distribution's kafka-clients jar for each target release. The peer
agent verified the Apache distribution SHA512; this runner independently checks
the local jar SHA256 and its embedded version and commit prefix before use. It
asserted all 279 API rows and 894 valid request/response header version pairs
against Apache's compiled ApiKeys and generated ApiMessageType. All three runs
passed, including 24 request/response header rejections for removed keys. Three
deliberate variants changed ApiVersions v4's expected response header from 0 to
1; each failed Apache's independent assertion with exit 1. Exact TSV inputs,
outputs, Java source, commands, toolchain, and binary hashes are retained.

Reproduce the Python checks from the repository root:

```sh
python3 -B scripts/check-broker-api-matrix.py
python3 -B -m unittest discover -s tests/ci -p test_broker_api_matrix.py -v
```

To reproduce the independent compiled assertions after obtaining the exact
distribution jars and matching provenance described in the oracle manifest:

```sh
python3 -B docs/evidence/broker/KL11-57/run-upstream-oracle.py \
  --jars /path/to/verified-jars --provenance-dir /path/to/provenance \
  --work-dir /path/to/scratch
```

The exact upstream facts include ApiVersions response header 0 at all body
versions, classic nullable `ClientId` encoding in flexible request headers,
and the latest-version instability changing from five APIs in 4.1.2 to only
InitProducerId in the newer two releases. ShareFetch and ShareAcknowledge v2
already occur in the 4.2.1 pin. Produce's schema minimum remains 3 while Apache
advertises 0 on the broker listener as a librdkafka workaround; that exception
is recorded separately and does not authorize partitionline to advertise an
unimplemented version.

The initial development guard run reported two incorrect spot-check assumptions:
Vote was expected on both listeners, and ShareFetch v2 was expected only in
4.3.1. The retained exact source showed Vote is controller-only in all three
releases and ShareFetch v2 exists in 4.2.1. Those assertions were corrected;
the source-derived inventory did not change. The initial run had 22 passing
tests and two failures; its raw terminal log was not saved. One intermediate
scratch manifest-generation command also had a Python syntax error before
execution; the retained executable runner replaced it and all three runs
passed. These development failures are recorded in the task evidence.

This is static schema and compiled metadata evidence. It does not exercise
network handlers, topic/storage behavior, replication, durability, security,
client semantics, or production/performance qualification. Byte-level header
and ApiVersions interop evidence belongs to KL11-04. No completion or benchmark
hold is lifted by this inventory.
