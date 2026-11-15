# Public producer flush histories

These isolated tests examine a proposed producer completion barrier. They use
the public producer API and a local mock broker with test-only scheduling gates.
They do not qualify the producer for production or measure performance.

## Results

| Source | Run | Default features | All features |
| --- | --- | --- | --- |
| Baseline `1036a393eb85f23032b935bf86862b7e98e36e14` | [42460119753][baseline-run] | 4 controls passed; 2 expected negative controls reproduced | 4 passed; 2 expected negatives reproduced |
| Completion draft `bc45ba4f65f126c406ff88a57fcfe08007d35b29` | [42460321562][candidate-run] | 5 controls passed; 1 expected negative control reproduced | 5 passed; 1 expected negative control reproduced |

Each listed control ran separately and reported exactly one test. Both jobs
passed their formatting checks. Their success includes the expected negative
results; it does not mean that all the tested properties pass.

- In the retirement history, a public send was accepted and the mock appended
  the idempotent record before dropping its Produce response. The real worker
  was removed and paused after the existing retry split, before the retry was
  counted. On the baseline, flush returned successfully while the record's
  receipt was still pending. With the draft, the same flush stayed pending;
  after release, the retry succeeded, its callback and receipt completed once,
  and flush returned successfully. The mock log had one target record before
  and after retry, and the retry retained the original producer sequence.
- Both sources passed the overlapping successful flushes, later-record,
  flush-deadline, and blocked-callback controls. The later record was deliberately
  held after the fixture marker; both sources kept the earlier flush pending
  during the 200 ms observation and completed both records after release. This
  observation does not require later records to finish or remain pending in
  every execution.
- Both sources still lost a recorded broker error when the first of two
  overlapping flushes was canceled. The canceled waiter consumed the worker
  error, so the surviving waiter returned success. The rejected record's
  receipt and single error callback reported broker error 10; the test also
  checked the record counters, released bytes and mock log before reporting the
  expected failure.

In a separate [focused draft run][focused-run], the existing
`cancelled_channel_admission_releases_only_the_unenqueued_reservation` test
failed in both feature modes. The default run returned `Timeout`; the
all-feature run observed 4096 buffered bytes where the test expected 4097.
Each arm reported 7 passed, 1 failed and 1 ignored in that suite; subsequent
mock-retry and formatting steps were skipped. These failures are not explained
by the public retirement result, and require a matched baseline comparison
before assigning cause.

## Test boundaries

The retirement gate is enabled only for the qualification build and extends
an existing scheduling boundary in the real retry path. It marks an actual
worker removal and pauses the worker without changing producer state. The mock
intentionally drops one reply. The baseline's cutoff counter is a fixture
marker after worker flush controls are queued, not a general linearization
point. The draft uses a separate observer immediately after its completion
cutoff is captured. Both observers are absent from ordinary builds.

The mock's response gates deliberately hold individual Produce replies, and
the mock uses the repository's own Kafka codec. The tests do not use an
independent broker or exercise all possible interleavings. The callback deadline
control uses a two-worker Tokio runtime; it does not establish a deadline bound
for a current-thread runtime blocked in user code.

## Provenance

Both runs used stable Rust 1.99.1 on x86_64 Linux with `Cargo.lock` blob
`d1cb698aaaf7554fe904ba02d671c4a8ab4975b3`. They used separate hosted jobs;
the candidate also used separate default and all-feature target directories
and recorded hashes for both test binaries.

| Item | Baseline | Completion draft |
| --- | --- | --- |
| Qualification commit | `9b6d000abff072c0eb7d1ca3450c5dd7873184a0` | `0742242aec0ba193505986985fc6b5d28fc3439f` |
| Uninstrumented `src/producer.rs` blob | `1e7733c136e54b532cdc8bddabd1e0c828e50ed2` | `8c0e6754f7bdd4500e8e9c5c4af8009cad86a671` |
| Instrumented `src/producer.rs` blob | `f0dfd6f1834092aa89ad152274e7a39991cbda4e` | `4b87fd5745de51418e00a7beab290aa5f74467b5` |
| Public control test blob | `5ebebae3ec6168bb2739a841ae0ad6b99da36273` | `8afe8f68297335f2708e5a2e83a4c139b1055f6a` |
| Retirement test blob, both sources | `98bf92a75f599eb3c1285f960fba8a341ccb553b` | `98bf92a75f599eb3c1285f960fba8a341ccb553b` |
| Mock fixture blob, both sources | `9d14334cf587b774701692547fdbdfd64bd399a1` | `9d14334cf587b774701692547fdbdfd64bd399a1` |

The candidate's completion-domain source blob is
`2fd3dbe9f8069e1e97526ed10d54118d0dac68a3`. The retained baseline artifact
[13609198506][baseline-artifact] has SHA-256
`199c0fb6fda6e2bae0014e0fdfb235dce078969cbe300f1ae42f8b56d22eef6f`;
candidate artifact [13608894641][candidate-artifact] has SHA-256
`d06797eebb155e5f9063de7824d6b6e4b368b8edf16a085b27120d2a5cee5b06`.
They retain the source and mock patches, hashes, test files and individual
logs. Git branches are `qualification/public-flush-barrier-controls-20261108`
and `qualification/public-flush-barrier-bc45-20261108`.

The first combined baseline attempt [42459824163][format-failure] completed
its semantic tests but failed rustfmt; its artifact was retained. The next
attempt [42459993219][earlier-pass] passed, including the callback control,
before the final baseline added the extra error, counter and buffer assertions.
The earlier independent retirement-witness setup and formatting attempts are
recorded in the [public flush history evidence](../producer-flush-public-20261108/README.md).

[baseline-run]: https://github.com/mingley/partitionline/actions/runs/42460119753
[candidate-run]: https://github.com/mingley/partitionline/actions/runs/42460321562
[focused-run]: https://github.com/mingley/partitionline/actions/runs/42460005132
[baseline-artifact]: https://github.com/mingley/partitionline/actions/runs/42460119753/artifacts/13609198506
[candidate-artifact]: https://github.com/mingley/partitionline/actions/runs/42460321562/artifacts/13608894641
[format-failure]: https://github.com/mingley/partitionline/actions/runs/42459824163
[earlier-pass]: https://github.com/mingley/partitionline/actions/runs/42459993219
