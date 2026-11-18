# Produce v4, v6, v7 and v12 wire fixtures

The [committed corpus](../../../../tests/fixtures/produce_wire/apache-v4-v6-v7-v12.txt)
adds four request and response pairs to the existing Produce body coverage.
Each request contains two uncompressed magic-v2 records per partition, with
headers and a tombstone. Rust checks decoded batches and response fields,
constructs fresh bodies, and encodes them. Apache parses the Rust bodies
and checks the fields and record CRCs. The runner also compares the Rust and
Apache encodings.

| Version | Response fields exercised |
| --- | --- |
| v4 | Success, storage error, log append time and throttle |
| v6 and v7 | Success, log start offset, leader error and throttle |
| v12 | Leader and endpoint tags, record errors and nullable error messages |

The runner tests default and all-feature Rust builds. Apache SDKs 4.1.0,
4.1.2, 4.2.1 and 4.3.1 each run four pairs per build. SDK 3.9.1 runs
v4, v6 and v7. Its generated request and response types declare a maximum
of v11, matching the [pinned request schema](https://github.com/apache/kafka/blob/f745dfdcee2b9851204ddbbcd423626ab87294bc/clients/src/main/resources/common/message/ProduceRequest.json)
and [response schema](https://github.com/apache/kafka/blob/f745dfdcee2b9851204ddbbcd423626ab87294bc/clients/src/main/resources/common/message/ProduceResponse.json).
Across the ten lanes, Java generated 38 pairs that Rust checked
and Java checked 38 Rust pairs. All corresponding bodies matched byte for byte.

The first run on `1e3019a036f3223381a7985bdaf4f99186943cf4` failed on a
v4 fixture mismatch. [Run log](https://github.com/mingley/partitionline/actions/runs/44411773090). Apache's generated response type defaulted
the error partition's base offset to zero. That run stopped before printing the generated corpus. The fixture now sets
the intended error sentinel of `-1` explicitly. That run also caught Rust formatting
differences. The [corrected generation run](https://github.com/mingley/partitionline/actions/runs/44412308423)
passed the peer comparison, formatting and strict test lint. Both parsers
rejected a deliberately changed v4 throttle value. Runtime and startup
timeout controls confirmed cleanup of the owned process groups.

The corpus SHA-256 is
`250cf4b1ea87d209d170d6a17a175db880ddc2934306d72c4ca9a1764de5d399`.
The [workflow](../../../../.github/workflows/produce-wire.yml) records artifact
hashes and the commands. The offline test consumes the retained corpus without
a JVM or network.

The [package run](https://github.com/mingley/partitionline/actions/runs/44412909379)
on `731952fd354b019a76f800bdc86601358638b0ca` passed the same ten lanes,
the retained-byte comparison, formatting and strict test lint. It used Rust
1.100.0 and Temurin 21.0.13. The offline test passed in both feature profiles
in the checkout and again after extracting a package built with
`cargo package --locked --no-verify`.

These authored cases check selected message bodies. Version negotiation,
compression including v7 zstd, transaction V2 execution, retries, live
brokers and other record and response shapes remain open. The case registry
and KL01-18 acceptance criteria are unchanged.
