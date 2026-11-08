# Header-bearing record validation experiments

The ordinary record validator first tries a fast walk for batches without
headers. A positive header count makes that walk decline and the generic
validator starts from the beginning. Three isolated candidates tried to
reduce that repeated work. None is applied: each regressed at least one
measured control. The benchmark checks memory-resident uncompressed batches;
it does not measure Kafka throughput or rank clients or servers.

## Paired measurements

The following runs compared main `ba8d72f51c23b4bfbd1057e61bbd483da831aa91`
with separate candidate sources on one pinned logical x86-64 CPU of a shared
GitHub-hosted runner. Each case used eight alternating AB/BA pairs and seven
timed samples per arm and process. Rust was 1.99.1 with LLVM 23.1.1; builds
used separate release target directories. The fixture checked batch, record and
header counts, every record's key, value, offset and timestamp, and a checksum
rejection. Malformed-case fixtures also checked the same exact error on every
iteration. Per-arm input lengths and CRCs matched.

| Case | Direct header walk ratio | Preserve plain walk ratio | Discriminator ratio |
| --- | ---: | ---: | ---: |
| One record, no headers | 1.097 | 0.998 | 0.979 |
| One record, one header | — | — | 0.895 |
| 1,024 records, no headers | 1.001 | 1.018 | 1.096 |
| 1,024 records, one header each | 0.623 | 0.805 | 0.782 |
| 1,024 records, four headers each | 0.681 | 0.819 | 0.839 |
| 1,024 records, header only on last | 0.424 | 0.851 | 0.835 |
| 256 records, one header and a 1,024-byte value each | 0.896 | 0.993 | 1.042 |
| Invalid attributes on first record | — | 1.137 | 1.139 |
| Invalid attributes on last record, no headers | — | 1.361 | 1.059 |
| Invalid attributes on last record, earlier headers | — | — | 1.567 |

Ratios are candidate median nanoseconds per validation divided by baseline
median, using the pooled samples. Lower is faster. Pair medians and individual
samples are in the linked artifacts. The one-record no-header direct-walk
result was noisy: paired ratios ranged from 0.553 to 1.150. These short,
shared-host experiments are not production latency estimates.

- The [direct header walk](https://github.com/mingley/partitionline/actions/runs/42450039443)
  used source `99108fb47ddd234f6b9e8a18c89bbf2446d88672`. It improved
  header-heavy cases but increased the one-record no-header median by 9.7%.
  [Artifact](https://github.com/mingley/partitionline/actions/runs/42450039443/artifacts/13605247835):
  GitHub reported SHA-256 `643e2577ed79a93342e10bad25c18db0ca5787413c08aa3c312f05187afc3595`.
- The [two-walk variant](https://github.com/mingley/partitionline/actions/runs/42450441736)
  used source `65af9e881452e69215c5505c40c75b615f8132c1`. It kept
  the original no-header walk, then tried a header-aware walk after any
  decline. It increased the measured late-invalid median by 36.1%.
  [Artifact](https://github.com/mingley/partitionline/actions/runs/42450441736/artifacts/13605297252):
  GitHub reported SHA-256 `9a9382e6987544bfa5b6504d92051f036adbeb4e992880add4f40d4ab13af2cb`.
- The [discriminator variant](https://github.com/mingley/partitionline/actions/runs/42450854784)
  used source `8cfcbe0a9d7353fc1da432790662297b0601db1d`. It tried the
  header-aware walk only after a positive header count. Its 1,024-record
  no-header median increased by 9.6%, and the late-invalid case after earlier
  headers increased by 56.7%.
  [Artifact](https://github.com/mingley/partitionline/actions/runs/42450854784/artifacts/13606125999):
  GitHub reported SHA-256 `18dfd7e652d9f5cdba2785a2a41a3c1cea59e0ea8f3f8e9e557b6f482a1e6bb0`.

The baseline `records.rs` SHA-256 was
`408a814bf89826961e4170b8822d67b0bc2efeb59510ec614a4653afb0824d8e`.
Candidate source SHA-256 values, in table order, were
`dec7e8088f0cea49c00b380008b3b4be25c58430bc8e624b9959d6aac22b6502`,
`143613f05aa28458803fe45c30eb519418aa3f554243705d1905b25941125088` and
`676ccf9c3317b6b403144af2fefb81581e29d474e52505c6719d9e6fedeccf55`.
Each run recorded distinct binary hashes; the binaries themselves were not
retained. The artifacts were not independently downloaded.

## Correctness and failed setup

The differential controls compare exact errors and partial counts against the
generic walk, with both admission and read modes. They cover null and empty
fields, UTF-8 keys, multibyte lengths, sparse offsets, per-record and aggregate
budgets, byte mutations, truncations, and an exhausted header budget across
batches. Focused library and 14 record integration tests passed in both feature
profiles for the [direct walk](https://github.com/mingley/partitionline/actions/runs/42450022332)
and [discriminator](https://github.com/mingley/partitionline/actions/runs/42450829565);
formatting and strict all-target/all-feature Clippy also passed. These are
focused controls, not full broker qualification.

Earlier [42449225558](https://github.com/mingley/partitionline/actions/runs/42449225558)
passed those test selections but failed formatting and Clippy.
[42449827055](https://github.com/mingley/partitionline/actions/runs/42449827055)
and [42450382717](https://github.com/mingley/partitionline/actions/runs/42450382717)
passed tests and Clippy but failed formatting. The first benchmark attempt
[42449325881](https://github.com/mingley/partitionline/actions/runs/42449325881)
failed to compile its example because it lacked crate-level documentation. The
second [42449654498](https://github.com/mingley/partitionline/actions/runs/42449654498)
built both examples but looked for them in the wrong target directory. Neither
attempt produced measurements.

A possible next design could retain the boundary and validated state of the
header-aware walk and resume the generic parser at the first unvalidated record.
It would need to preserve exact error positions and the partial record and
header counts, including budget failures. It is not implemented or qualified.
