# Schema Registry incremental HTTP responses

9 November 2026

The Registry reader rejected a valid chunked response when its encoded
framing, rather than its decoded body, pushed the accumulated wire buffer
over the body limit plus 64 KiB. It also repeatedly reparsed and recopied
previous chunks after each read. Header detection searched from the start
after each read and accepted a delimiter arriving just beyond the 64 KiB
header limit.

The candidate reads chunked responses incrementally. It caps decoded body
bytes at the configured body limit; response headers, each chunk-size line
and the trailer section at 64 KiB each; and cumulative chunk framing
overhead at five times the body limit plus 64 KiB. The last count excludes
payload bytes and the initial response headers. The limit admits one-byte
chunks within the body limit when their extra extensions and trailers fit.
It can reject size lines or trailers that older source accepted. Headers
ending beyond 64 KiB are also now rejected. Header scanning resumes at the
last three previously read bytes rather than rescanning the whole buffer.

A continuously ready reader yields to the executor after reaching 1 MiB
for headers, Content-Length or close-delimited reads, and 64 KiB for chunked
reads, or after 256 reads. The first header read can include chunked data,
and an individual read can cross a byte threshold. A yield does not
guarantee another task runs. Neither this work nor its tests bound the time
of one read, synchronous JSON parsing, allocator behavior or process RSS.
The cumulative framing limit is finite but does not establish a wall-time
bound.

## Correctness and prior failures

The public [baseline control](https://github.com/mingley/partitionline/actions/runs/42679578680)
returned `TooLarge { limit: 32768 }` for a valid 30,000-byte decoded
body encoded as one-byte chunks. A separate deterministic
[header baseline](https://github.com/mingley/partitionline/actions/runs/42680252004)
accepted a header ending one byte past 64 KiB.

The final [qualification](https://github.com/mingley/partitionline/actions/runs/42685194918)
passed split headers, chunk-size lines and terminators, extensions,
trailers, malformed and overflowing lengths, EOF, decoded-body limits,
exact framing-budget boundaries, header limits, Retry-After, timeout and
cancellation. Its always-ready controls directly observed first-poll
`Pending` and bytes read, without assuming another task was scheduled.
Ten cancellation repeats passed in each of default and all-feature
profiles. Full default, all-feature and no-default-feature schema profiles,
format, all-target/all-feature Clippy and warning-denied rustdoc passed.
The default and all-feature library runs each passed 62 tests; the public
chunked integration target passed five tests in each profile.

The original [candidate run](https://github.com/mingley/partitionline/actions/runs/42680644149)
failed an all-feature cancellation assertion that recorded only a boolean,
so its precise I/O outcome is unknown. In a later matched
[diagnostic](https://github.com/mingley/partitionline/actions/runs/42681311917),
an EOF-only cancellation assertion failed on baseline in 7/15 default and
7/15 all-feature trials and on candidate in 7/15 and 5/15 trials. Those
failures reported `ConnectionReset`, OS error 104; other recorded reads
were EOF. The [observations](cancellation-observations.txt) retain the
trials. The revised cancellation assertion accepts only EOF or
`ConnectionReset`, and fails for bytes, timeouts or other errors.

An earlier [256 KiB qualification](https://github.com/mingley/partitionline/actions/runs/42682215837)
failed a biased-select fairness test. Its failure did not identify the
framing or first-poll state. A separate
[diagnostic](https://github.com/mingley/partitionline/actions/runs/42682755913)
observed first-poll `Pending` for all three always-ready framings; five
biased-select trials selected the response for chunked despite that
boundary. The final test directly checks the first poll and makes no
scheduler-order assertion. The original failures remain results of their
own runs.

The exact formatted final blobs tested by run 42685194918 are Registry
source `d97c106775976bac06808bdcb113488ccfe71148`, public tests
`ea976950d2ae3dabc8266afcfde5d1806a59263e` and README
`f1ad99fb7fc71b6716bac1653a586e8085f884c6`.

## Measurements

These tests use prepared byte buffers and an always-ready in-process reader
on hosted x86_64 runners. They do not include TLS, TCP, a live Schema
Registry or Kafka. Each reported distribution has 12 alternating or rotated
pairs pinned to one CPU; candidate/baseline changes are paired medians,
with the smallest and largest paired changes shown. Results on separate
runners must not be treated as a single matched comparison.

The initial 32 KiB-budget [comparison](https://github.com/mingley/partitionline/actions/runs/42680920803)
measured large Content-Length at +20.21% (range −8.39% to +65.27%) and
large chunked at −88.44% (−90.24% to −87.67%). The
[initial raw samples](initial-32k-samples.csv) retain all 144 timings.
A second [32 KiB scaling run](https://github.com/mingley/partitionline/actions/runs/42681086761)
measured large Content-Length at +25.38% (+10.69% to +35.79%). Its
[240 raw samples](scaling-32k-samples.csv) include identical chunked and
header bytes read in 1-, 64- and 8,192-byte pieces.

The [256 KiB-budget run](https://github.com/mingley/partitionline/actions/runs/42681844971)
measured large Content-Length at +11.80% (+8.57% to +17.51%) and
close-delimited at +6.55% (+2.81% to +14.27%).
[Raw samples](budget-256k-samples.csv) retain all 288 timings.
The [uniform 1 MiB run](https://github.com/mingley/partitionline/actions/runs/42683392473)
measured large Content-Length at +4.61% (−7.51% to +14.21%) and
close-delimited at +0.33% (−11.46% to +9.41%).
Its [288 raw samples](budget-1m-samples.csv) and
[first-poll samples](poll-1m.csv) are retained separately. The tiny-chunk
first poll consumed 1,056,768 encoded bytes in each of 12 trials; median
elapsed time was 5.855 ms and range 5.836–5.910 ms.

The final [three-arm comparison](https://github.com/mingley/partitionline/actions/runs/42685117131)
compared the baseline, uniform 1 MiB budget and split budget on the same
runner. The [432 paired samples](budget-split-samples.csv) include all
shapes and raw elapsed times.

| Case | Uniform vs baseline | Split vs baseline | Split vs uniform |
| --- | ---: | ---: | ---: |
| 256-byte Content-Length | −1.13% (−7.08% to +36.36%) | −0.87% (−4.28% to +26.26%) | +0.38% (−28.27% to +35.76%) |
| 256-byte close-delimited | +0.22% (−3.76% to +30.80%) | +0.39% (−2.81% to +42.75%) | +0.69% (−23.26% to +45.27%) |
| 256 KiB Content-Length | +1.80% (−2.85% to +45.82%) | +0.78% (−5.40% to +61.27%) | −0.98% (−32.65% to +57.74%) |
| 256 KiB close-delimited | +0.90% (−3.13% to +35.44%) | +2.12% (−4.55% to +59.21%) | −1.27% (−27.45% to +58.04%) |
| 256 KiB chunked | −87.54% (−88.53% to −83.76%) | −85.75% (−86.70% to −73.23%) | +13.01% (−10.40% to +123.67%) |
| 8 KiB chunked, 1-byte reads | −81.31% (−81.89% to −80.48%) | −81.23% (−81.89% to −77.58%) | −0.03% (−4.42% to +19.02%) |
| 8 KiB chunked, 8 KiB reads | −1.63% (−5.24% to +2.59%) | −1.27% (−3.88% to +49.41%) | +1.41% (−5.20% to +45.64%) |

For the final comparison's 12 always-ready tiny-chunk first polls, the
uniform budget consumed 1,056,768 encoded bytes with median 3.062 ms and
maximum 3.096 ms. The split budget consumed 73,728 bytes with median
0.210 ms and maximum 0.325 ms. For a single large chunk, the respective
medians were 48.7 µs and 3.67 µs; maxima were 455.9 µs and 13.43 µs.
The [48 first-poll samples](poll-split.csv) include both candidates and
both workloads. These observed times do not establish a worst-case
latency bound.

## Benchmark source and executable pins

Each arm used a separate release target directory. Source columns are Git
blob IDs for the qualification source. These test builds include benchmark
instrumentation and omit the new candidate-only unit controls. Executable
values are SHA-256.

| Run and arm | Source blob | Executable SHA-256 |
| --- | --- | --- |
| 42680920803 baseline | `564024a75b86fc75380e1bbe327d5817e99c444e` | `77731549fec8a9a01281966c9c10cd1b09293cf7c089c3739e2a06550d19a069` |
| 42680920803 candidate | `2b54d68321ba5e8bf6f7507e5bad9db6708b23a9` | `1aeea8425aac014d865512c415375031e4be255db4537dc547d22ca7924d8b12` |
| 42681086761 baseline | `ec22e65cb00008b445c513eb08cf9c0f6771042e` | `2b7adb430d60d072c5090184663a57e7d79d0b10593ec138538248b9bca3b450` |
| 42681086761 candidate | `8c1155e77a7b9958ff73b2862a755bbf13bf9cf5` | `09e935d974d83f70efed2c0d66410c410295e43808cc118ca87066fe4cc8e4a9` |
| 42681844971 baseline | `27e4315a8639c0b9c97e7ef801a2e45afd3cf5ed` | `7ee4a8620b7a83edd871d1debd6b9dc3484c68770026119c339b43c995f1a60c` |
| 42681844971 candidate | `5e516b4522fe6332738114285cd0cdaf49d7e530` | `d32c8e0929ef7f31dc1d74c229d712d9bcc414dbb668a5afc027c3e5e30d34a4` |
| 42683392473 baseline | `ece3c8ff16276eb8f598f898b86fbe128bc9a119` | `4a53d30a77d5e01683cce30b8363ab2413c5de8a2067b0d648f1e4d172a0a7bd` |
| 42683392473 candidate | `58e1babe0889df933dd3978f772319a035f0d0c8` | `a03faf0fe523e84a1616ec945c8e5b29d4cdd6abd794d174c1383797b730a1f8` |
| 42685117131 baseline | `5eceb05506cc63a053181d7d34f17f97f33c57f8` | `ce855ba23a9d7e922ebfa487863b417985aba69c64fd96db5961a6767abb86a7` |
| 42685117131 uniform | `d84d79e8f62ce164ae4d2071ca2afe3ff82ce767` | `7b3e46d4599e5a4cabdfc7db0e8004412446c348c7c4b198ffb8e06d3aa4d68f` |
| 42685117131 split | `2d4a2b0e8bd34f0fbed87c51bbb0864a4b758cc2` | `c04cc5ca282cda707eb642a15715a1410409e85eadf6896d003f42511666c76d` |

For the split candidate, the benchmark source
`2d4a2b0e8bd34f0fbed87c51bbb0864a4b758cc2` matches the unformatted
candidate `35e679844e4b45c803a81bf591c63375253b2598` byte-for-byte
before its `#[cfg(test)] mod tests` module. The qualified formatted
production prefix `d97c106775976bac06808bdcb113488ccfe71148` differs
only by rustfmt wrapping one `if` condition. The benchmark's test module
uses the shared instrumentation in place of the new unit controls; the
separate qualification ran those controls on the formatted source.
