# Header-bearing record validation experiments

The ordinary record validator first tries a fast walk for batches without
headers. A positive header count makes that walk decline and the generic
validator starts from the beginning. Nine isolated candidates tried to
reduce that repeated work. None is applied. Earlier versions regressed at
least one measured control; the latest needs broader qualification. The benchmark checks memory-resident uncompressed batches;
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

## Resumable header walk

A fourth [isolated candidate](https://github.com/mingley/partitionline/commit/f446fe349ec2f49ab0c2d0e0400d1d70c59e6adf)
lets the header-aware walk publish only complete records, then resumes the
generic walk at the first unvalidated record. It carries the cursor, previous
offset, maximum timestamp and completed header count. This avoids restarting
the generic walk at the beginning after a late malformed record. It also keeps
a boolean no-header scan and records whether that scan saw a positive header
count.

[Run 42451631245](https://github.com/mingley/partitionline/actions/runs/42451631245)
used the same paired procedure and workloads above. Candidate/baseline median
ratios were 0.951 for one header-free record, 1.070 for 1,024 header-free
records, 0.830 for one record with a header, 0.740 for 1,024 records with
one header each, 0.743 for four headers each, 0.735 for a header only on the
last record, and 0.934 for the larger-value case. The early-invalid ratio was
0.999, the late-invalid no-header ratio was 1.045, and the late-invalid ratio
after earlier headers was 0.738. All eight paired ratios for the 1,024-record
header-free case were between 1.058 and 1.082. The increased no-header and
late-invalid no-header medians leave this candidate unapplied.

The candidate `records.rs` SHA-256 was
`2edefb30d2f6dce7815308d29e64ab4f7f82df089bb1877628b0ae3bedb35c58`.
The fixture SHA-256 was
`5b77706970c10ca45a9cdf461dcf5871cdc5891ea8a0128f2c6cd1209fb96cda`.
Baseline and candidate release binary SHA-256 values were
`ed445ed68ef7238d70ec884e7cb52dcf596dce158d6fd1f9d39ce912b0691c28`
and `26f85c742adf8bc6562dbfbffdf1529c4e934df52fbdcef902e321d6c2a9ac1c`.
The [artifact](https://github.com/mingley/partitionline/actions/runs/42451631245/artifacts/13605773823)
has GitHub-reported SHA-256
`3457169da9862c320731162099793c67826e217ff16c94127ba05fadad27acd8`;
it has not been independently downloaded, and the binaries were not retained.

The [focused run](https://github.com/mingley/partitionline/actions/runs/42451624981)
passed library controls and 14 record integration tests in both feature
profiles, plus strict all-target/all-feature Clippy, but failed its formatting
gate. Its controls compare generic errors, partial counts and visit counts for
a nonzero byte origin and a failure inside a later header. They also check an
exhausted aggregate header budget across batches, a maximum timestamp mismatch
and trailing bytes after the last record. The earlier
[42451384857](https://github.com/mingley/partitionline/actions/runs/42451384857)
also passed focused tests and Clippy but failed formatting. No full broker suite
or current-main qualification was run for this candidate.

## First-record probe

A [fifth isolated candidate](https://github.com/mingley/partitionline/commit/545578e483ed490c73d14a9a9854b28bb602310b)
kept the original no-header validator, then used a bounded first-record probe
to decide whether to enter the resumable header walk. A header first appearing
later in the batch went to the existing generic walk.

In [run 42452199862](https://github.com/mingley/partitionline/actions/runs/42452199862),
candidate/baseline median ratios were 0.888 for one record without headers,
1.048 for one record with one header, 1.003 for 1,024 records without headers,
0.672 for 1,024 with one header each, 0.752 with four headers each, 1.005
with a header only on the last record, and 0.882 for larger values with
headers. Early-invalid and late-invalid without headers were 0.999 and 1.006;
late-invalid after earlier headers was 0.673. All eight one-header single
record pairs were above baseline (1.030–1.061). This short shared-runner
measurement does not establish production performance.

The candidate source SHA-256 was
`f93e95135d20b3e8bc42005452064905acc986dbc652dfab11a208632786d777`;
fixture SHA-256 was
`5b77706970c10ca45a9cdf461dcf5871cdc5891ea8a0128f2c6cd1209fb96cda`.
Baseline and candidate binary hashes were
`ed445ed68ef7238d70ec884e7cb52dcf596dce158d6fd1f9d39ce912b0691c28`
and `7ee289e66124dcd702818d47e76ca44095758fb6fb741ce90c713dac17f0eddb`.
GitHub reported SHA-256
`e3009890bbf7769f7bd238634a371fbba22e10a4778b7d684aebaceb316b0b49`
for the [artifact](https://github.com/mingley/partitionline/actions/runs/42452199862/artifacts/13606113500).
The binaries were not retained, and the artifact has not been independently
downloaded.

[Run 42452193624](https://github.com/mingley/partitionline/actions/runs/42452193624)
passed the focused library and 14 record integration tests in both profiles
and strict all-target/all-feature Clippy, but failed formatting on four test
call sites. This version remains unapplied. A further isolated variant skips the probe
for single-record batches; its results are below.

## Single-record gate

[Run 42452507038](https://github.com/mingley/partitionline/actions/runs/42452507038)
tested isolated source
[`626e6208`](https://github.com/mingley/partitionline/commit/626e6208099392513e076ddb80a8d17e6310aa7e),
which skips the probe when a batch has one record. The same paired procedure
also measured two-, eight- and 32-record header batches. Candidate/baseline
median ratios were 0.959 for one header-free record, 0.881 for one header
record, 0.908 for two, 0.782 for eight, and 0.768 for 32. For 1,024 records,
the header-free ratio was 1.153, one header each was 0.756, four headers each
was 0.731, and a header only on the last record was 1.025. The larger-value
header case was 0.990. Early-invalid, late-invalid without headers, and
late-invalid after earlier headers were 1.076, 1.020, and 0.754.

All eight paired ratios for 1,024 header-free records were between 1.146 and
1.155. Although the original boolean no-header function was preserved in the
source, this run does not establish why the compiled candidate was slower. The
regression and the invalid controls keep the candidate unapplied. These
shared-host microbenchmarks do not establish a Kafka throughput ranking.

The candidate `records.rs` SHA-256 was
`f5d62a4762b0477a17cee038e1952e92979d30971542d821f523551f3ebd71b2`,
fixture SHA-256 was
`d268ab717b4447c560eba6b009ca58eb91e62d9cbcb776ecaf7ba258a3dbcc18`,
and baseline and candidate binary SHA-256 values were
`e4698019bfb9f7d9bd9da9dd975ae89b85c1d25f48ec27a07a2d7a738be7ebe7`
and `cc47803dc94514c108abc85f1ff7f4055f822c5d2f3999a5565b4b311e155e12`.
GitHub reported SHA-256
`1f0b90728ae73859d3c4472689606caaddebae648089d222191f8ebcd679f384`
for the [artifact](https://github.com/mingley/partitionline/actions/runs/42452507038/artifacts/13606362211).
The binaries were not retained, and the artifact has not been independently
downloaded.

[Run 42452481310](https://github.com/mingley/partitionline/actions/runs/42452481310)
passed formatting, focused library and 14 record integration tests in both
profiles, and strict all-target/all-feature Clippy. It did not run the full
broker suites on current main.

## Code-generation diagnostic

[Run 42452972248](https://github.com/mingley/partitionline/actions/runs/42452972248)
rebuilt the same fixture and sources for the gated candidate. Its baseline and
candidate executable hashes matched the two hashes above. Eight paired
candidate/baseline medians for the 1,024-record no-header case ranged from
1.133 to 1.180, reproducing the regression on those binaries.

The recorded x86 disassembly shows that the baseline
`try_accept_plain` had 285 disassembly lines and did not call a separate
`read_canonical_i64` symbol; the candidate had 339 lines and called a
separate `read_canonical_i64` symbol. These are observations about this
compiler and pair of builds, not proof that inlining or any one instruction
caused the measured difference. The [artifact](https://github.com/mingley/partitionline/actions/runs/42452972248/artifacts/13607045289)
has GitHub-reported SHA-256
`d9b1f273edcfade2c1d6a3fa0def3eecfe67e6e26397319a246679ef206ec4da`.
It contains the selected disassembly and measurements; it has not been
independently downloaded, and the binaries were not retained.

## Timestamp wrapper inline follow-up

The [wrapper-only candidate](https://github.com/mingley/partitionline/commit/981729e409cd994eebbea8df92c82627a968e70d)
added `#[inline(always)]` to the canonical timestamp decoder.
[Run 42453294784](https://github.com/mingley/partitionline/actions/runs/42453294784)
measured candidate/baseline median ratios of 1.076 for 1,024 header-free
records, 1.069 for early-invalid, and 1.043 for late-invalid without headers.
The one-header single-record ratio was 1.002; the 1,024 one-header-per-record
ratio was 0.745. This candidate also remains unapplied.

In the separate [code-generation run](https://github.com/mingley/partitionline/actions/runs/42453306515),
the two executable SHA-256 values matched the paired benchmark. The candidate
no longer had a separate `read_canonical_i64` symbol, but
`try_accept_plain` called the lower-level `read_canonical_u64` function,
which the baseline did not call. Its eight paired no-header ratios ranged
from 1.070 to 1.152. The code change and benchmark do not isolate a single
cause for the remaining regression.

The candidate source SHA-256 was
`05f4e2a0af73228bd60d9dce388a083cfb087eecb7f630721f6928cfced46493`;
fixture SHA-256 was
`d268ab717b4447c560eba6b009ca58eb91e62d9cbcb776ecaf7ba258a3dbcc18`.
Baseline and candidate binary SHA-256 values were
`e4698019bfb9f7d9bd9da9dd975ae89b85c1d25f48ec27a07a2d7a738be7ebe7`
and `182803178448a27eedbc0075a13e7c748804cad0814fa4feae0e77665038359c`.
GitHub reported SHA-256
`e728636692bab296839c2132638f178b0e48d430f9d39540ad022ee2f74b4b57`
for the [measurement artifact](https://github.com/mingley/partitionline/actions/runs/42453294784/artifacts/13606516440)
and `138da599ce81fd943f7dca330e12471bbef81a80bc168b01cedf62bd30ee204e`
for the [code-generation artifact](https://github.com/mingley/partitionline/actions/runs/42453306515/artifacts/13607215077).
Neither archive has been independently downloaded; binaries were not retained.
[Run 42453286145](https://github.com/mingley/partitionline/actions/runs/42453286145)
passed formatting, the focused tests in both profiles and strict Clippy.

A subsequent isolated variant also inlines the lower-level decoder; its paired
and disassembly results are below.

## Nested decoder inline follow-up

The [nested-inline candidate](https://github.com/mingley/partitionline/commit/00ebf674cafd594746ac3e25cfb8f648acfdb9cd)
also marks the lower-level canonical decoder `#[inline(always)]`.
[Run 42453611803](https://github.com/mingley/partitionline/actions/runs/42453611803)
measured candidate/baseline median ratios of 1.013 for 1,024 header-free
records, 1.021 for a header only on the last record and 1.022 for late-invalid
without headers. All eight paired ratios for each of those cases were above
baseline. Early-invalid was 0.999. Header-heavy valid cases ranged from 0.618
to 0.902. The candidate remains unapplied because the common and fallback
regressions remain.

The candidate source SHA-256 was
`7bdc7b868ee762ec34a92412bc65fe833679bedd05b0c5d45e2a7ca9e82415d5`;
fixture SHA-256 was
`d268ab717b4447c560eba6b009ca58eb91e62d9cbcb776ecaf7ba258a3dbcc18`.
Baseline and candidate binary SHA-256 values were
`e4698019bfb9f7d9bd9da9dd975ae89b85c1d25f48ec27a07a2d7a738be7ebe7`
and `6c943416e9a8e45baa50bb0e3847fcb008aa3754d46a9ac1ecf166f5b284fabc`.
GitHub reported SHA-256
`196f2cb3e0f3e0281c0b6796df9adcd7a7ab27bae3e7d8bed0c49b99b21d97f2`
for the [measurement artifact](https://github.com/mingley/partitionline/actions/runs/42453611803/artifacts/13607161014).
The archive has not been independently downloaded and the binaries were not
retained. [Run 42453600001](https://github.com/mingley/partitionline/actions/runs/42453600001)
passed formatting, the focused tests in both profiles and strict Clippy.

The separate [disassembly run](https://github.com/mingley/partitionline/actions/runs/42453612237)
rebuilt the same baseline and nested-inline binary hashes. Its eight paired
no-header ratios ranged from 0.999 to 1.034, with a median of 1.017. Both
`try_accept_plain` functions had no separate call to `read_canonical_i64`
or `read_canonical_u64`; the disassembly contained 285 lines for baseline
and 288 for candidate. The header scanner and first-record probe grew from
297 and 185 disassembly lines in the earlier gated candidate to 403 and 285
in the nested-inline candidate. These counts and the paired timing are
observations, not a complete instruction-cost or cache analysis.

The [code-generation artifact](https://github.com/mingley/partitionline/actions/runs/42453612237/artifacts/13607355168)
has GitHub-reported SHA-256
`814f685a86f8fd790702e0cb47f02af75611a5c91d0d18ab4f23718a5320b588`.
It has not been independently downloaded, and the binaries were not retained.

## Cold-outlined fallback

The [cold-outlined candidate](https://github.com/mingley/partitionline/commit/fdfd97f5c82dc2efb294fc671d394a6ddb0603de)
keeps single-record batches on the existing path and places the extra
first-record probe and resumable header scan behind a `#[cold]`,
`#[inline(never)]` helper. Marking this path cold is an optimization hint,
not a claim that headers are rare in production.

[Run 42453967798](https://github.com/mingley/partitionline/actions/runs/42453967798)
used the same paired fixture. Candidate/baseline median ratios and the ranges
of the eight paired medians were:

| Case | Pooled median ratio | Paired range |
| --- | ---: | ---: |
| One record, no headers | 0.972 | 0.939–1.648 |
| One record, one header | 0.965 | 0.954–0.981 |
| Two records, one header each | 0.810 | 0.798–0.833 |
| Eight records, one header each | 0.755 | 0.745–0.768 |
| 32 records, one header each | 0.663 | 0.656–0.672 |
| 1,024 records, no headers | 0.953 | 0.930–0.969 |
| 1,024 records, one header each | 0.672 | 0.661–0.678 |
| 1,024 records, four headers each | 0.698 | 0.687–0.708 |
| 1,024 records, header only on last | 0.990 | 0.974–1.004 |
| 256 records, one header and a 1,024-byte value each | 0.916 | 0.901–0.932 |
| Invalid attributes on first record | 0.999 | 0.990–1.010 |
| Invalid attributes on last record, no headers | 0.987 | 0.978–1.004 |
| Invalid attributes on last record, earlier headers | 0.666 | 0.659–0.673 |

The one-record no-header case included a large 1.648 pair outlier. A
[separate run](https://github.com/mingley/partitionline/actions/runs/42453969163)
rebuilt identical binary hashes and repeated only the 1,024-record no-header
case; its paired ratios ranged from 0.997 to 1.016. These shared-host results
are not proof of a general performance gain or a Kafka throughput comparison.

The candidate `records.rs` SHA-256 was
`f126d5a952cf8329a423ead7f3e295ed210ed8046d4115ed072e1f07c42eb71e`;
fixture SHA-256 was
`d268ab717b4447c560eba6b009ca58eb91e62d9cbcb776ecaf7ba258a3dbcc18`.
Baseline and candidate binary SHA-256 values were
`e4698019bfb9f7d9bd9da9dd975ae89b85c1d25f48ec27a07a2d7a738be7ebe7`
and `89951c22e7a68e863196a4520a0a0841c249076d370e12c595ba36b3337d5258`.
GitHub reported SHA-256
`6e4e7d551c9e6819eef2a9a3fdc47d6f29547d94da16b53ad30db3b4644e39a0`
for the [measurement artifact](https://github.com/mingley/partitionline/actions/runs/42453967798/artifacts/13606727029)
and `b6e33e8ca87aa214ba7c8f620317889230153071cc00687a1c85955668c3314c`
for the [code-generation artifact](https://github.com/mingley/partitionline/actions/runs/42453969163/artifacts/13606932812).
Neither archive was independently downloaded, and binaries were not retained.

[Run 42453957702](https://github.com/mingley/partitionline/actions/runs/42453957702)
passed formatting, focused record tests in both profiles and strict all-target,
all-feature Clippy. A later combined-source run is recorded below; it passed
the default broker suite but failed its all-feature suite. Real broker traffic,
longer fault campaigns, other processors and broader workloads remain unmeasured.

## Combined source and differential corpus

Clean candidate [`a0fd448`](https://github.com/mingley/partitionline/commit/a0fd44856104acf5449d2f1941d5747834c73ced)
changes only `partitionline-broker/src/records.rs` on main commit
`de152e78ca6d85e6cd4ea5915058b2bb6141cf42`. Its source SHA-256 is
`5accc57fff730a286754c0247fe4b72b7d9f16ab53581f20eb2f489e4bc28741`.
The production portion before the test module is byte-identical to the
cold-outlined candidate above; the added corpus and its formatting are test-only.

The corpus uses 256 deterministic seeds in both admission and read modes,
with original, truncated, byte-mutated, noncanonical-varint and
overflow-varint variants. It varies first and later headers, sparse offsets,
field sizes and nullability, invalid UTF-8, timestamp edges, source origins,
and byte and count budgets. It compares the exact error, partial record and
header counts, and test-only record visits with the generic walk; mismatch
messages retain the case ID. The initial
[run 42454605981](https://github.com/mingley/partitionline/actions/runs/42454605981)
passed the focused tests in both profiles and strict Clippy but failed
formatting; no seed failed. The clean candidate uses the formatted test source.

[Test-only counters](https://github.com/mingley/partitionline/actions/runs/42455027939)
confirmed 38 completed header fast scans and 170 partial-prefix resumes per
profile while executing the corpus. These counts are path executions, not
distinct seeds. The temporary counters are not part of the candidate.

The [combined broker qualification](https://github.com/mingley/partitionline/actions/runs/42454937724)
passed formatting, strict all-target/all-feature Clippy, inventory and its
controls. The default all-target command passed 757 tests with three ignored.
A test-only mutation that prevented cursor advancement caused the expected
assertion failure in both profiles. The all-feature command failed in the
library: 512 passed, one failed and three were ignored; later targets did not
run. The failed five-peer observer test sent a proposal to node2 and received
`Node(NotLeader)`, while its diagnostics showed node0 as leader. No cause or
relationship to the records change has been established. The candidate remains
unapplied.

The [default](https://github.com/mingley/partitionline/actions/runs/42454937724/artifacts/13607536446)
and [all-feature](https://github.com/mingley/partitionline/actions/runs/42454937724/artifacts/13606839289)
archives have GitHub-reported SHA-256 values
`f03616f248b00000a49084b6e6407b568c6499fdb33ce02a32bd2e5a2aab97e2`
and `316855f62bcb48e2cbd068ba1805555574b6e3af79714bc4f0a623957871780a`.
The [mutation archive](https://github.com/mingley/partitionline/actions/runs/42454937724/artifacts/13607107229)
has reported SHA-256
`a01ea16e994f782bbe9a33c83fd66b9934ca0abb9c838a38aa98fed5b2598066`.
These archives have not been independently downloaded.

## Fresh paired repeat on combined source

[Run 42454937680](https://github.com/mingley/partitionline/actions/runs/42454937680)
compared `de152e7` and `a0fd448` on a fresh shared x86-64 runner with the
same 13-case release fixture, one pinned logical CPU and eight alternating
AB/BA pairs. Candidate/baseline pooled median ratios and paired ranges were:

| Case | Pooled median ratio | Paired range |
| --- | ---: | ---: |
| One record, no headers | 0.975 | 0.966–0.984 |
| One record, one header | 1.000 | 0.976–1.018 |
| Two records, one header each | 0.961 | 0.936–0.971 |
| Eight records, one header each | 0.754 | 0.749–0.761 |
| 32 records, one header each | 0.620 | 0.617–0.760 |
| 1,024 records, no headers | 1.003 | 1.001–1.007 |
| 1,024 records, one header each | 0.600 | 0.588–0.613 |
| 1,024 records, four headers each | 0.671 | 0.657–0.678 |
| 1,024 records, header only on last | 0.942 | 0.927–0.948 |
| 256 records, one header and a 1,024-byte value each | 0.891 | 0.889–0.893 |
| Invalid attributes on first record | 1.001 | 0.999–1.001 |
| Invalid attributes on last record, no headers | 0.944 | 0.941–0.946 |
| Invalid attributes on last record, earlier headers | 0.594 | 0.587–0.608 |

The 1,024-record header-free median was 0.3% higher, and all eight paired
ratios were above baseline. This short shared-host measurement does not
establish the cause or a general performance change. It is not a Kafka
throughput comparison.

Fixture SHA-256 was
`d268ab717b4447c560eba6b009ca58eb91e62d9cbcb776ecaf7ba258a3dbcc18`.
Baseline and candidate binary SHA-256 values were
`19fe60ccc7679d4ad454223ffde57629e4b1888d19ad8c44e52806bcda9bcb3d`
and `c8b2b60e6d36721e52f2538093a92a7c1e8d6daa2b3377cf5054d40572aa4de8`.
GitHub reported SHA-256
`37185966ce3d142ee9429fb8532dce4ddcbf80ccec3d396674e2f5e2c2984539`
for the [artifact](https://github.com/mingley/partitionline/actions/runs/42454937680/artifacts/13606889831).
It has not been independently downloaded, and binaries were not retained.

A single same-runner, unchanged all-feature baseline/candidate comparison
[42455389333](https://github.com/mingley/partitionline/actions/runs/42455389333)
is in progress; it does not erase the failure above.
