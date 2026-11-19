# Plaintext Produce pacing

`operator serve` and `Transport::bind_with_produce_quota` can pace ordinary
Produce on a plaintext listener. Each configured client ID has one token
bucket shared by its connections. All unlisted IDs share a fallback bucket.
A listener configures at most 256 labels. Requests cannot add buckets.

The listener charges the request header and body after it acquires a handler
slot. It keeps that charge if the handler fails or the connection closes.
The pause deadline starts at the charge. Time in the handler and writing
the response consumes part of the pause. The connection reports the
remaining pause in the Produce response and waits before
dispatching the next request. Successful `acks=0` requests wait without
sending a response. The pause holds the connection slot and releases the
handler slot. An idle EOF releases the connection early. A close behind
buffered input can wait until the pause ends. After handling the request and
writing any response, the paused connection reads at most one new byte and
only when the read-ahead buffer is empty.

The exact candidate was `b957d5b5f0817c187d7537cbbce843f80fe9c213`, broker tree
`4b1d1725bcd241999cbf2bac7a11d849a5b70310`.
The baseline was `2f466be99d0013ac94feac0634d2ebecdab1f3be`.
[Candidate](https://github.com/mingley/partitionline/commit/b957d5b5f0817c187d7537cbbce843f80fe9c213)

## Wire checks

Stable Rust 1.100.0 passed 826 default and 974 all-feature broker tests, with
four ignored in each profile. Formatting, strict Clippy, Rustdoc, doctests and
all 55 inventory controls passed on x86-64 Linux. Dedicated tests checked the
pause cap, retained debt, separate labels, idle EOF, a split frame prefix and
`acks=0` followed by Metadata. The configured-label test used one handler
slot for both paused and active connections.
[Qualification](https://github.com/mingley/partitionline/actions/runs/44635263910)

Produce v3–8 responses end with the throttle field. The current v9–13 writer
adds an empty final tag block after that field. Twenty-two fixture exchanges
cover every version 3–13 at `acks=1` and `acks=-1`. Each response matches its Apache golden after
clearing the four throttle bytes.

One hosted runner used the same socket probe with Apache Kafka 4.3.1 at digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
and the candidate. Kafka was set to 20 bytes/s per label. The candidate used
20 bytes/s, a 200-byte burst and an 8,000 ms pause cap. The first Bravo
response was unthrottled and the second Bravo connection shared its debt.
Both brokers sent the throttled Produce response promptly and returned
Alpha's next Metadata response after the pause. The probe checked success
and increasing offsets. The candidate joined seven accepted connections.
[Wire job](https://github.com/mingley/partitionline/actions/runs/44635263910/job/135952027640)
[Probe](../produce-client-quota-isolation-20261110/live-probe.py.txt)
[Raw samples](wire-samples.jsonl)

| Broker | Produce version | Alpha throttle (ms) | Alpha Produce RTT (ms) | Alpha next Metadata RTT (ms) | First Bravo throttle (ms) | Second Bravo throttle (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kafka 4.3.1 | 3 | 4,694 | 4.002 | 4,698.805 | 0 | 4,698 |
| Kafka 4.3.1 | 6 | 4,698 | 2.150 | 4,699.119 | 0 | 4,698 |
| Partitionline | 3 | 4,698 | 0.581 | 4,700.295 | 0 | 4,699 |
| Partitionline | 6 | 4,699 | 0.911 | 4,699.317 | 0 | 4,699 |

Kafka uses sampled rates. The candidate uses a token bucket and a capped
per-connection pause. Multiple connections can exceed the configured rate
and the bucket retains excess debt until refill. Restart clears the debt.
Authenticated quotas, Kafka quota administration, Fetch and replication
pacing and sustained overload qualification remain open.

## Default path with pacing disabled

The paired Produce probe compared the release binaries with pacing disabled on one hosted Ubuntu 24.04 x86-64 runner
using Rust 1.100.0. The scratch filesystem reported `ext2/ext3`. Each workload
ran eight alternating pairs. Each client sent 24 warm-up and 240 timed requests
using Produce v3, `acks=1` and one uncompressed 128-byte value per partition.
Both brokers used two Tokio worker threads and four sync slots. The probe
checked each acknowledgment and read back 122,496 record batches
after graceful restarts with Fetch v4.
[Paired job](https://github.com/mingley/partitionline/actions/runs/44635382413/job/135952422359)
[Probe](../reusable-sync-20261119/produce-pairs.py)
[All paired samples](disabled-pairs.log)

| Workload | Baseline records/s | Candidate records/s | Paired change median (range) | Median p99 µs | Lower candidate pairs | Verified records |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1c-1p | 6,123 | 6,193 | +1.10% (−11.39% to +21.97%) | 260.6 → 254.7 | 3/8 | 4,224 |
| 1c-4p | 18,415 | 17,848 | −2.72% (−6.86% to +5.17%) | 371.2 → 389.4 | 6/8 | 16,896 |
| 1c-8p | 23,987 | 23,769 | −1.42% (−2.13% to +12.21%) | 671.1 → 678.8 | 5/8 | 33,792 |
| 4c-4p | 38,513 | 38,071 | −1.26% (−2.61% to +0.17%) | 568.2 → 575.3 | 7/8 | 67,584 |

The candidate had lower median throughput for three workloads. Each paired
range includes zero. The default-path throughput effect remains uncertain
on this host. Other broker comparisons and sustained workloads remain open.

Release binary SHA-256 hashes

- Baseline `ca93c1be632f64aa9de5cae37403fe8c172195888cabfb309c83b78b585fd08f`
- Candidate `35b271c71a4e2a7f377eab23a2341cd0a9067bfab4079120569b39408934b601`

The first branch run passed both behavioral suites and its wire observations.
It failed formatting and strict Clippy rejected its standard-library mutex.
The candidate above applies the format changes and uses a Tokio mutex for each
bucket. [Initial run](https://github.com/mingley/partitionline/actions/runs/44634655573)
