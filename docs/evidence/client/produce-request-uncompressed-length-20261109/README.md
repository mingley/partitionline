# Produce request uncompressed-length refinement

Status: not adopted. This experiment measures the public Produce request
encoder, which is separate from the ordinary `Producer` encoder. It does not
measure client throughput or broker performance.

The [first candidate](../produce-request-first-length-20261109/README.md)
cached the first partition's planned length and showed compressed-workload
regressions. The refinement caches the first **uncompressed** partition length
instead. Compressed partitions continue to use the already-staged bytes'
length. If compressed partitions come first, the first later uncompressed
partition consumes the cached length. This adds no heap allocation for the
cache; it does add plan bookkeeping and a branch on the uncompressed write
path. Assembly was not captured, so the measurements do not identify a
specific instruction or code-generation cause.

The refinement retains most of the uncompressed gain, but in this run the
64-empty-partition case had a 1.0192 median paired time ratio versus baseline
and was slower in 11 of 12 pairs. LZ4's median was 1.0083 and 9 of 12 pairs
were slower, but its range, 0.9834–1.1000, crosses parity. Snappy's median
was 1.0051 with 8 of 12 pairs slower. One shared virtual machine and 12 pairs
cannot establish a universal effect; these regressions are sufficient to
defer both candidates.

## Inputs and method

- Baseline [`731eb97a08445a18da0b043986ae192397ba176a`](https://github.com/mingley/partitionline/commit/731eb97a08445a18da0b043986ae192397ba176a), API Git blob `49f4a12be47636db290a8973a2716c33fe7fab9c`, SHA-256 `07474d56731fdb9acf9c1b382ea29253fdf569548721119a60f233745e163ca9`.
- First candidate [`8c37f28b13f0131f146c1f57398dd8d593c1af3d`](https://github.com/mingley/partitionline/commit/8c37f28b13f0131f146c1f57398dd8d593c1af3d), API Git blob `dce17686443b44b2858133ac778bcf9df6940288`, SHA-256 `849a7cca7ef2d32c762648d42c37f59cc36d533886489229fe71ecc874015313`. Apply [first.patch](first.patch) to the baseline.
- Refined candidate [`72abcd0425c19e333a3c83cd211fa2912b7ae4f9`](https://github.com/mingley/partitionline/commit/72abcd0425c19e333a3c83cd211fa2912b7ae4f9), API Git blob `2fda005e814e95e0617e1abd10c19131d8e10892`, SHA-256 `84239de102750fb42493ad2e994f82abe6eee47f9cc91a1c324d62e801724e0a`. Apply [refined.patch](refined.patch) after the first patch.
- `Cargo.lock` SHA-256 `a6702ab5ac47b734ddda21c83502ce4810423662731494365b8e1432a486814a`; [fixture](../produce-request-first-length-20261109/bench_produce_plan.rs) SHA-256 `1f5e1a2cd209dbc9590c653a3a63deba0d1a22bd08e72a976b34cc117d89c0a3`; [measurement script](measure_three.py) SHA-256 `548fb259f97b0a10c1883f2974c710807270ae01dc051d1b5482a9f9e65551eb`; [workflow](workflow.yml) SHA-256 `ec1aab43bd50800abea7a583fb99b5d37a7f5755bd2f1859c8b44b448a860061`.
- Release binary SHA-256: baseline `8c7173770be6fd0ae625ebfd85e60bb4214b1c4f4bf9813ccba6afbffb7292ae`, first `5d12cf083da10b010226e6464b966b777a53b218c2a829b6cffffdcc730126ff`, refined `1f0a5530ddfb94cd06ddb9c7336efe1f435ef5141d72ac39ff91c95ca8c9e085`.

[Run 42683158768](https://github.com/mingley/partitionline/actions/runs/42683158768)
built all three source trees independently with separate Cargo target
directories, the same fixture, lockfile and release profile. It used rustc
1.99.1, LLVM 23.1.1 and a GitHub-hosted x86_64 AMD EPYC 9V45 virtual machine.
Measurements were pinned to one CPU. Each shape used 12 triplets in balanced,
alternating three-arm order, with shuffled shape order. [Raw samples](samples.json)
retain all 204 triplets and all three elapsed times and ratios. The
[run artifact](https://github.com/mingley/partitionline/actions/runs/42683158768/artifacts/13702509619)
contains corpus bytes, build/test logs and the original measurement JSON;
GitHub artifact retention was set to 30 days.

## Results

Times are median nanoseconds per complete request in each arm. Ratios and
ranges come from the 12 paired candidate/baseline times in each shape; below 1
is faster.

| Shape | Base ns | First ns | Refined ns | First/base median (range) | Refined/base median (range) |
| --- | ---: | ---: | ---: | ---: | ---: |
| one_empty | 69.5 | 69.9 | 70.2 | 1.0023 (0.9611–1.0776) | 1.0182 (0.8615–1.0437) |
| one_record | 124.8 | 112.0 | 113.0 | 0.8991 (0.6566–0.9483) | 0.9067 (0.6264–0.9315) |
| cold_one | 138.1 | 126.7 | 126.3 | 0.9126 (0.8244–0.9475) | 0.9145 (0.8419–1.0703) |
| eight_records | 488.8 | 432.0 | 430.7 | 0.8763 (0.8499–0.8910) | 0.8771 (0.8540–0.8914) |
| five_hundred | 25313.7 | 21768.4 | 21616.4 | 0.8595 (0.8000–0.8738) | 0.8521 (0.8132–0.8662) |
| header_heavy | 20047.7 | 17064.2 | 17064.7 | 0.8561 (0.8420–1.0127) | 0.8571 (0.8169–0.8787) |
| legacy | 6552.6 | 5656.3 | 5625.4 | 0.8625 (0.7293–0.8872) | 0.8578 (0.6619–0.9418) |
| topic_id | 6632.0 | 5658.7 | 5620.7 | 0.8532 (0.7785–0.8671) | 0.8506 (0.7733–0.8790) |
| multi | 3805.3 | 3783.3 | 3773.1 | 0.9892 (0.9687–1.0122) | 0.9914 (0.9610–1.0274) |
| many_empty | 2313.7 | 2285.5 | 2360.7 | 0.9949 (0.9389–1.1763) | 1.0192 (0.9963–1.3842) |
| async_eight | 630.9 | 573.6 | 574.2 | 0.9111 (0.7920–1.2359) | 0.9151 (0.7881–0.9255) |
| async_five_hundred | 25700.8 | 21830.7 | 21899.5 | 0.8527 (0.7560–0.8793) | 0.8505 (0.7697–0.9941) |
| async_multi | 3996.1 | 3936.5 | 3968.3 | 0.9851 (0.9465–1.0178) | 0.9901 (0.9551–1.0171) |
| gzip | 50489.2 | 50288.4 | 50951.9 | 0.9971 (0.7620–1.0055) | 1.0039 (0.7786–1.0216) |
| snappy | 7516.5 | 7543.1 | 7522.1 | 1.0057 (0.9614–1.0198) | 1.0051 (0.9580–1.0097) |
| lz4 | 15453.7 | 15559.3 | 15507.7 | 1.0068 (0.9917–1.0386) | 1.0083 (0.9834–1.1000) |
| mixed_compression | 115867.6 | 115532.7 | 114344.2 | 0.9956 (0.9606–1.0272) | 0.9854 (0.9371–1.0289) |

The first candidate's LZ4 case was slower in 12/12 pairs on the earlier
EPYC 7763 run and 9/12 here on the EPYC 9V45. The refined candidate was
slower in 9/12 here. These small samples and variable pair ranges suggest a
possible compressed-path cost; they do not prove it persists on other hosts or
workloads.

The deterministic fixture uses compressible payloads. It does not measure
zstd, a network, a real broker, independent clients, sustained throughput or
production traffic. There is no basis here for a global performance ranking.

## Checks

The three original 185-case corpora matched exactly, SHA-256
`68e6849ccc18bf583e1f6b808a4ff29a43de38aa5d997f760b27ca6559c27ecc`.
The corpus compares complete Produce v3–v13 requests, decoding, failure
outputs and cooperative cancellation snapshots, but uses this crate's own
decoder rather than an independent Kafka implementation.

The baseline and refined source trees each passed six focused
`cargo test --locked --lib produce_request` tests under default features
and all features, `rustfmt --check`, and
`cargo clippy --locked --all-features --lib -- -D warnings`. The first
candidate passed the same scoped checks in the earlier run. These checks are
not a production-readiness qualification.

The separate [ordered-controls run](https://github.com/mingley/partitionline/actions/runs/42684542086)
built all three source trees independently with an
[extended fixture](ordered-controls.rs) and compared 251 complete corpora
per arm. All three outputs matched, SHA-256
`d012367efe3d63042613e430ba29876dd2049be05ef8c3e51e6998eaf52852bc`.
The 66 added cases cover six arrangements across Produce v3–v13: compressed
before uncompressed and the reverse, with different record counts and value
lengths; empty compressed or uncompressed batches before later batches; and
gzip, Snappy and LZ4 mixtures. Every arrangement starts with an empty topic.
The fixture also compares sync and cooperative output for v3–v12 and decodes
each successful request. These are bounded controls, not an exhaustive proof.

The extended fixture SHA-256 is
`c147b01e6df181bfaefd033766c3c385ab67797d4eb9123417e69cba90ce8c3f`;
the [controls workflow](ordered-controls-workflow.yml) SHA-256 is
`650518f4380bd7887a0e8d9c0f70b550b8bb065357eca431f78f012c6ba6b834`.
The control binaries have SHA-256 `152bfc4f91393f67bd8ee3ed73e151479561065ee55befa8961f40fbd6058b6e`
(baseline), `9dfb9c4afa100410a18441b0af2d5a92a9b59b1cb468c4303930a77a195228c9`
(first), and `3e313677b5711d0dfaa6081b3fecec3b5452e8ba25faef5ef101f618004afb93`
(refined). They differ from the timing binaries because the fixture changed.
The [control artifact](https://github.com/mingley/partitionline/actions/runs/42684542086/artifacts/13704985534)
retains the original corpora and logs for 30 days.
