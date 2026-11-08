# Producer send_all route controls

These are untimed tests of two isolated `send_all` candidates. They do not establish production readiness or a performance ranking. The test broker uses this repository's protocol codec, and the added counters and gates change scheduling. There is no real Kafka broker in these runs.

- The candidate with an additional 512-admission yield is [`3fd30f13c07056dc0ac3d937b3f370d92880bd98`](https://github.com/mingley/partitionline/commit/3fd30f13c07056dc0ac3d937b3f370d92880bd98), producer blob `d634fa4752e33d77e042404f199f31ee2bcb1c48`.
- The candidate without that additional yield is [`a781fbf47e724d04d37426e2b1c6b13dc2692003`](https://github.com/mingley/partitionline/commit/a781fbf47e724d04d37426e2b1c6b13dc2692003), producer blob `650e34cca0cfa0bee9825ffc7d7da86514e28a31`.

Both sets run public producer operations against the mock, with qualification-only counters inside the nudge decision. Each job ran `cargo +stable test --locked --test send_all_route_qualification` with default or all features, `--test-threads=1`, and `RUSTFLAGS="--cfg send_all_qualification --check-cfg=cfg(send_all_qualification)"`. The hosted x86_64 jobs used stable Rust 1.99.1. These are debug test binaries, not benchmark binaries.

## Results

| Candidate | Tested qualification source | Run | Default | All features |
| --- | --- | --- | --- | --- |
| With yield | [`7d87019347284bf40c4867cd6d37fc16924c1a6b`](https://github.com/mingley/partitionline/commit/7d87019347284bf40c4867cd6d37fc16924c1a6b) | [42471911610](https://github.com/mingley/partitionline/actions/runs/42471911610) | 10 passed | 10 passed |
| Without yield | [`e990368c7ba95eb431355c7a2893ee54eed5038e`](https://github.com/mingley/partitionline/commit/e990368c7ba95eb431355c7a2893ee54eed5038e) | [42472219751](https://github.com/mingley/partitionline/actions/runs/42472219751) | 13 passed | 13 passed |

For each profile, the size control checked acknowledgements, offsets and mock-log values for every record. The nudge counters recorded 31 fallbacks at size 31, 32 shortcuts at size 32, 8,191 shortcuts at size 8,191, and 8,192 fallbacks at size 8,192. They also checked a registered 249-byte topic and rejection of an unregistered 250-byte invalid topic. The 250-byte case uses zero metadata age and observes one fallback, a protocol error and no queued record.

Both suites checked fallback with an interceptor that rewrites the partition, the sticky policy, mixed partitions, an implicit route, and zero metadata age. The second suite also checked equal-length mixed topic names and rejection of a negative partition. The mock log and delivery offsets were checked for accepted records.

A gate before the first nudge held a 64-record send while the mock leader changed and the public metadata refresh completed. The old cached worker was still considered fresh and all 64 decisions selected the shortcut. Every eventually accepted target batch reached the new leader with the expected records and offsets. The test does not assert that a post-change Produce request reached or was rejected by the old leader. This is one forced schedule, not a claim that metadata and the fast cache update atomically. In a separate gated history, metadata aged past 250 ms before the nudge: the counters recorded one fallback and 63 shortcuts, and a metadata refresh occurred.

A worker-retirement history aborted the old warmed worker while the first target send was gated. It observed one fallback, a replacement worker, 63 shortcuts and the expected records. Another gate held the operation after 512 admissions while a synthetic hook filled the 16-entry connect queue with invalid node IDs. Before the gate released, the counter recorded 512 full-queue outcomes. In the subsequent interval the global counter recorded 16 accepted nudges, which can include background activity; all 1,024 records were acknowledged and read back. This establishes only that forced full-to-draining history.

The 512-admission gate corresponds to the additional yield in the first candidate. It is an **artificial suspension** in the second candidate, which has no such added yield. Dropping the gated send left exactly the first 512 records accepted and later present in the mock log. The second suite also ran an ungated first poll with Tokio's cooperative budget disabled: all 1,024 target records were admitted before the poll returned pending, and those records were later accounted for and read back. That controlled poll is not an ordinary-scheduler fairness test. The unused `yields` field printed by the second suite is a leftover counter that is never incremented; it is not evidence of runtime yield behavior.

A separate uninstrumented, matched public cancellation suite compares the second candidate with its baseline: [run 42472001367](https://github.com/mingley/partitionline/actions/runs/42472001367). The instrumented histories above do not replace that comparison or demonstrate all possible metadata, queue, cancellation, interceptor or worker schedules.

## Source and artifact pins

| Run | Instrumented producer blob / SHA-256 | Test blob / SHA-256 | Default test binary SHA-256 / artifact | All-feature test binary SHA-256 / artifact |
| --- | --- | --- | --- | --- |
| 42471911610 | `da6bde348a24e0d28879fc2ad5d2b8b3fa99af74` / `6efe1bb72be29f72e9f8c65cfaaec2077ec351c827ccdceb643e4927390a31d3` | `234eccde608c594cca82080f86940eae1a66c96b` / `7b9ef87b016654e1fe97e14e27b299493c430d5c6398c7a3b9ea9083faee16ec` | `6f2887d5082f113b67842c7b5ed980fc1577ea9cf0b2bdedcfda47f8abf29beb` / [13614380240](https://github.com/mingley/partitionline/actions/runs/42471911610/artifacts/13614380240) | `14aa718e1220422f1dd3564f6cd44663e232adc5ec8fb3b2a18183d4be29f787` / [13614086030](https://github.com/mingley/partitionline/actions/runs/42471911610/artifacts/13614086030) |
| 42472219751 | `9161723433cb7674c1db51e51ecb88852e43c3c5` / `faf9fb877b847b029bc595ea51c0f7ee580b87b5dc2d60dd782611d0c522649f` | `7d084c43395da0b264aa7719cc7317189936d925` / `ce9dbd623b0a06913c8140774cc75fcd5ee3e787adc1351c540a52b9a69b3432` | `90cd9e1e7b3cdccf8a178df308afdd077b4137a5db1cba0bcb29d59943ee3d13` / [13614046793](https://github.com/mingley/partitionline/actions/runs/42472219751/artifacts/13614046793) | `7e5a4cdd40cfb4ec34544e532ff295702cf63418fe15b40a6bf73e3de9a0e38d` / [13614325133](https://github.com/mingley/partitionline/actions/runs/42472219751/artifacts/13614325133) |

The common mock blob was `4917e00407f79e764909ddd37816e096303e5900`, SHA-256 `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`. The lockfile SHA-256 was `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`. The uploaded artifacts contain logs and provenance; the binary hashes above identify the executed test binaries, which were not uploaded.

## Earlier attempts

[Run 42471448705](https://github.com/mingley/partitionline/actions/runs/42471448705) failed before tests in both profiles on six unused results in the test-only counters. [Run 42471488205](https://github.com/mingley/partitionline/actions/runs/42471488205) then failed before tests on one unused result in the test partitioner. The counters were corrected.

[Runs 42471495855](https://github.com/mingley/partitionline/actions/runs/42471495855) and [42471551212](https://github.com/mingley/partitionline/actions/runs/42471551212) each had eight passes and one failure per profile: the maximum-length-topic test failed its warmup with `UnknownTopic` because the fixture had not created that topic. The subsequent suites register the topic explicitly; they do not alter the production candidate to fix the fixture.

## Leader-attempt follow-up

A separate trace adds the broker ID, topic, partition, payload values and constructed response error for each decoded mock Produce partition. [Run 42472940666](https://github.com/mingley/partitionline/actions/runs/42472940666) used the candidate without the additional yield and repeated the gated metadata-change test three times per profile; all six repetitions passed.

In every gated repetition, after the public metadata refresh, all 64 nudge decisions used the shortcut and none used the fallback. The mock received one batch for topic `t`, partition 0, with decoded IDs 0–63 on node 2 and constructed error code 6 (`NOT_LEADER_OR_FOLLOWER`). It then received the same IDs on node 1 and constructed error code 0. Per-record receipts, offsets and the mock log separately matched all 64 target values. These observations apply to this one forced schedule with a test-only trace and this repository's mock codec. They do not establish behavior under other schedules or against Kafka.

The source is [`acfeb4f06e4fd82a5e5ff3e6bb47dfea4242daf7`](https://github.com/mingley/partitionline/commit/acfeb4f06e4fd82a5e5ff3e6bb47dfea4242daf7), with instrumented producer blob `9161723433cb7674c1db51e51ecb88852e43c3c5`, mock blob `a42fabc941f9086d894b9e4831116a8137796389` (SHA-256 `ef7b56d2bbffa9b6fb46c5b5a282e49a662d880a2e96d6854fa9e2001efd6431`), and test blob `4365889bde22c6051d02c9123448acb24f096ab1` (SHA-256 `1284892fdd6ba05960892a211b0d3777ad447bb4d292b23f823393d70d7199b8`). The default test binary SHA-256 was `6fc78edeccefd2ebe6c179cf8c8d43b8a58442d2f0d264354fc4ff44b33e5628`, with logs in [artifact 13613334497](https://github.com/mingley/partitionline/actions/runs/42472940666/artifacts/13613334497). The all-feature test binary SHA-256 was `1861b23e43ed19aaa4accf9547e8d208495445b4133f2eff55e7aaf4333f2e14`, with logs in [artifact 13614591177](https://github.com/mingley/partitionline/actions/runs/42472940666/artifacts/13614591177). Binaries were not uploaded.

An earlier trace, [run 42472671526](https://github.com/mingley/partitionline/actions/runs/42472671526) at `4a7e2ff00e133a3975e291488986977b215f073f`, also passed three repetitions per profile. It checked that the rejected IDs were a nonempty subset of the accepted IDs but did not print them individually; the later run printed the sorted sets.
