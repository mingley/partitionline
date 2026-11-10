# Client-label Produce quota candidate

This is an isolated prototype based on the unapplied
[shared-bucket quota draft](../produce-quota-candidate-20261109/README.md).
It is not applied to main or qualified for release. It preconfigures at most
256 exact client-ID labels of at most 256 UTF-8 bytes each. Each configured
label has an actor-owned bucket; unknown, null, empty or malformed labels use
one shared fallback bucket. Requests cannot create new buckets. Client IDs are
caller-supplied, not authenticated identities. These logical limits are not an
RSS bound. The token-bucket calculation, capped connection mutes, restart-reset
debt and static configuration differ from Kafka's sampled per-user/client
quotas and do not establish fairness or strict rate enforcement.

## Wire observation

[Run 42867063051](https://github.com/mingley/partitionline/actions/runs/42867063051)
built a debug candidate broker on a hosted x86_64 runner and used the same
[probe](probe.py.txt) as the separate
[Kafka 4.3.1 observation](https://github.com/mingley/partitionline/blob/d1eab574468ffc023db0f683a052c4da761c491e/docs/evidence/broker/produce-client-quota-isolation-20261110/README.md).
The candidate was configured with 20 bytes/s, a 200-byte burst and an 8,000 ms
cap, selected so the first fixture fit and the second incurred debt. Its
[server](candidate-server.rs.txt), [workflow](wire-workflow.yml.txt) and
[raw samples](candidate-wire-samples.jsonl) are retained.

For one v3 and one v6 sequence, the first Alpha and first Bravo responses had
zero throttle; the second request on a separate Bravo connection with the same
ID reported 4,700 ms. Alpha's following Metadata responses took 4,701.304 ms
and 4,701.337 ms respectively. The probe checked success and increasing
offsets but did not independently read back records. The Kafka and candidate
runs used different hosted executions and different quota algorithms. The
timings are not a performance comparison or proof of sampled-quota parity,
TLS/SASL compatibility, or a timing bound.

The wire run used commit
[`0f3205e`](https://github.com/mingley/partitionline/commit/0f3205e590e9076d59890a42194b6d84189b0473).
Its production metadata and bandwidth blobs were identical to candidate
`b4c78673ddbdcf28ac46fcb1c35e628592ec1a64`. A
[later run](https://github.com/mingley/partitionline/actions/runs/42869711275) repeated the
same scenarios using commit
[`4419b1f`](https://github.com/mingley/partitionline/commit/4419b1f9c99056f4a636c8d044c4cedad6cf364d),
whose two production blobs match the later
`e993aabcaef316380e7c13ff31c0a81ab9b5be64` candidate. Fresh Bravo again had
zero throttle and its second connection reported 4,700 ms for v3 and 4,697 ms
for v6. Alpha's following Metadata responses took 4,701.601 ms and 4,699.527 ms.
The [later raw samples](candidate-wire-v5-samples.jsonl) retain all six entries.

## In-process selector cost

[Run 42867965009](https://github.com/mingley/partitionline/actions/runs/42867965009)
executed [this selector diagnostic](selector-benchmark.rs.txt) on one hosted
x86_64 runner with Rust 1.99.1. Eight alternating-order samples each performed
one million repeated selections on a short, cache-hot flexible request with
no header tags. There was no separate warmup. The shared mode takes the
no-header-parse fast path; the named modes parse the header and search a
preconfigured sorted table. The [raw samples](selector-samples.txt) and
[workflow](selector-workflow.yml.txt) are retained.

| Configured named labels | Median ns per selection | Sample range ns |
| ---: | ---: | ---: |
| 0, shared policy | 2.53 | 2.51–2.56 |
| 1 | 21.34 | 21.23–22.94 |
| 64 | 71.00 | 70.46–71.65 |
| 256 | 92.39 | 92.14–92.69 |

These are narrow in-process measurements on a shared runner. They do not
measure allocations, variable headers, contention, socket or broker throughput,
or a controlled performance regression bound. The diagnostic source was
[`5d6ea42`](https://github.com/mingley/partitionline/commit/5d6ea421f69c6edbca07595b914f13f7de46a171).

## Controls and limits

At `b7e73462964f7e0aebdba7946f84d5d78b5f85c4`,
[run 42866758015](https://github.com/mingley/partitionline/actions/runs/42866758015)
passed six meter, three actor-accounting and eight public quota controls in
each feature profile, one existing authenticated shared-quota control,
strict all-target/all-feature Clippy and rustdoc. Formatting failed, and the
dependent broad broker suite was skipped. An earlier candidate
[run](https://github.com/mingley/partitionline/actions/runs/42865079914) had the same
formatting failure. The
[formatted revision](https://github.com/mingley/partitionline/actions/runs/42868226917)
passed the same focused controls, formatting, Clippy and rustdoc. Its broader
broker job passed 836 default and 986 all-feature tests, with four ignored in
each profile. That run predates the failure-domain change and does not explain
the earlier unrelated broad-suite failure.

Two additional controls inject a corrupt response-control offset or a missing
internal charge length. On the prior candidate they each observed a different
named meter still return a ready quota mark and failed in
[run 42868573395](https://github.com/mingley/partitionline/actions/runs/42868573395).
These are injected internal states; they do not show such corruption occurs
naturally. Revision `c59ec2e65e8689800b13ee3da3b27dfa498b8fad` fails every
bucket closed on these structural failures while leaving an individual
numeric meter failure isolated. Its
[run](https://github.com/mingley/partitionline/actions/runs/42868617253) passed six meter,
five actor and eight public controls in each feature profile, the existing
authenticated shared-quota control, Clippy and rustdoc; formatting failed and
the broad suite was skipped. The formatting-only successor
`e993aabcaef316380e7c13ff31c0a81ab9b5be64` passed
[its focused run](https://github.com/mingley/partitionline/actions/runs/42869682907):
six meter, five actor and eight public controls in each feature profile, the
existing authenticated shared-quota control, formatting, strict Clippy and
rustdoc. Its dependent broad broker suite was still running when this note
was written.

The [candidate patch](candidate.patch) applies to
`50b5de969a14f11f52df6da36bf2037d4f61de60`. Text application of its hunks
reproduced the three successor source files exactly; native `git apply` was
not run here. The [candidate workflow](candidate-workflow.yml.txt) and
[negative-control workflow](invariant-baseline-workflow.yml.txt) are retained.

The probe and focused controls do not test actual Kafka Java clients against
the candidate, named quotas under TLS or SASL, dynamic quota changes, durable
debt, client-label authorization, long overload, or cross-node behavior.
The built-in broker binary does not expose this policy.
