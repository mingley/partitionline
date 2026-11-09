# Producer close error reporting

These controls cover a narrow change to the client close result. They do not
establish delivery, cancellation, retry, ownership or ordering correctness.

## Sources and behavior

- Baseline: `dfe179d7066e2c1afecb88df5ba76d2920308c46`.
- First candidate: `51a53f8104789c25358984e621edc19dae506581`.
- Expanded candidate: `7c979b117054e8015719f33fd6bcf9f34bdb6672`.
- Formatted candidate and changelog: `8925eb187a739754420111c284641ecc3d493a2a`.
- The primary controls are `tests/producer_close_errors.rs` in the respective
  candidate commits. The broker-error controls use the public producer API and
  the repository mock; the dead-worker control also uses a hidden test hook.
- The source change returns the initial flush result after the existing
  shutdown and interceptor-close sequence. Earlier Git revisions returned only
  a flush timeout, suppressing other errors. Existing callers that expected
  `Ok(())` after a failed send can now receive the retained broker error.
  A previous flush can consume that error; errors arising only during shutdown
  are not returned. A successful close does not establish delivery.

The broker-error fixture warms a connection, then makes the mock return
`TOPIC_AUTHORIZATION_FAILED`. The public `send` and a separate `flush`
control see that same broker error. Without the earlier flush, both `close`
and `close_timeout` are expected to return it after cleanup. The tests check
one acknowledgement, one error callback, interceptor close, released bytes,
and closed admission on a retained clone. The timeout suite also checks a
stalled broker.

The `Closed` and double-close control uses the existing hidden worker-task
hook to abort an idle worker after a successful send. This is a deliberately
injected worker failure, not an observed broker failure. It checks each close
variant, a subsequent close and admission on another clone. A separate
concurrent-close control accepts either `Ok(())` or `Error::Closed` and
checks interceptor-close calls and admission.

## First run

[Run 42663676523](https://github.com/mingley/partitionline/actions/runs/42663676523)
used Rust 1.99.1 on x86_64 Linux, default and all features. In each profile,
the baseline flush/consumption and successful-close controls passed; the two
new close controls failed as expected because baseline close returned `Ok(())`.
All four first-candidate controls passed. The existing `produce_cancel`
suite then had 22 passes and one failure: its failed-send test unwrapped close,
which now returned broker error 87. The run stopped before its remaining
candidate suites or formatting and lint checks.

## Expanded run

[Run 42665367653](https://github.com/mingley/partitionline/actions/runs/42665367653)
used the expanded candidate on the same Rust profile and platform. Both
baseline profiles passed the flush and success controls, and reproduced all
three expected baseline failures, including the injected dead-worker case.

Both candidate profiles passed all six focused close controls, all 23
`produce_cancel` tests, all 29 sticky-partitioner tests, all 12 buffer
ownership tests, nine retained-backing tests (one ignored), the timestamp
control, eight initialization tests, twelve InitProducerId tests (one
ignored), and the selected transaction/fencing tests. The existing timed-close
control was included in `produce_cancel`.

The broader `mock_produce` suite had 28 passes and 17 failures in each profile.
The producer library suite had 119 passes and three failures in each profile.
Both failed an idle metadata socket assertion and a produce-topic assembly
latency assertion; the third failure differed between profiles. The run did
not establish their cause. All-feature targeted Clippy passed; formatting
failed in four test files. The formatted candidate applies the displayed
rustfmt changes and adds the compatibility note; the next run checks it.

## Matched baseline and formatted candidate

[Run 42667069515](https://github.com/mingley/partitionline/actions/runs/42667069515)
compared the baseline and formatted candidate on the same x86_64 runner per
profile, using separate Cargo target directories. It checked the producer
source blob, mock and fixture blobs, close-test blob and lockfile before each
build, and recorded SHA-256 hashes of the sources and executables. The
baseline used the candidate's close-test file to demonstrate the old behavior;
`mock_produce.rs`, its shared mock and the other production source files were
identical for the two broader checks.

| Profile | Suite | Baseline | Candidate |
|---|---|---:|---:|
| Default | `mock_produce` | 28 passed, 17 failed | 28 passed, 17 failed |
| Default | producer library | 119 passed, 3 failed | 119 passed, 3 failed |
| All features | `mock_produce` | 28 passed, 17 failed | 28 passed, 17 failed |
| All features | producer library | 121 passed, 1 failed | 120 passed, 2 failed |

The same 17 `mock_produce` test names failed on both sides in each profile.
In the default producer library comparison, both sides failed the idle metadata
socket and produce-topic assembly assertions; the third timing failure differed.
In the all-feature comparison, both sides failed the idle metadata socket
assertion, and the candidate also failed the produce-topic assembly timing
assertion. These are individual observations, not proof of a cause or a clean
producer qualification.

Both candidate profiles passed the six focused close tests and the selected
producer integration suites listed above. The formatted candidate passed
`cargo fmt --all -- --check` and targeted all-feature Clippy. The run as a
whole failed because the broader suites failed.

The producer source SHA-256 was
`60a3fa0985358198e81757e6080af8b58ab52cb00edc1f34e2c97ed033e90b78`
for baseline and
`6641b7a107b2bb2b74976d1d72cdf4a9a6c09bc02f98dfd33947c4c977485345`
for candidate. The test executable hashes, in baseline/candidate order, were:

| Profile and executable | Baseline SHA-256 | Candidate SHA-256 |
|---|---|---|
| Default close controls | `cade1118c729116cb596c5fa56f954ffdabe1cae03fe4ec6e8c144f1e5466205` | `088c1934b563f5648ed79de13cb06b4af2f11b13372f7aabe541a56550920ca3` |
| Default mock produce | `a74de3d31faf5df58c25351325977b1dcfbff74b345737b8ca2cd4e0cf7dd7c2` | `f996224268f82d8da254b88a8e1cefd44985fea28fe5284df70a966f1734c260` |
| Default producer library | `15512809daf5de292ae10187220219870698620849aed53776b0b5f70e349ff3` | `82cbfad6541aac3d45936a4042992468c3dbefa7a7a3b73e6b02825817540ecc` |
| All-feature close controls | `b31fe50b6cee87fa3184619ec5d927b122ef78807d0972010c838fccab803a33` | `99fcd4860a1d936cc3e894a7c14eae9f4ea369936074d20204a826c4f30d5c71` |
| All-feature mock produce | `cfd7eae40323bb875951acda86eec3494c9b8efbd9265ffd7a55dce2184aacb5` | `e4dd97f7e4c68e8946e72179f5ab006be6ec00f92809817563ce991fd6b4fc98` |
| All-feature producer library | `f9bdd5d3b6a0bf94a3b0c426303aa2d9015ff613fc3808874761757a77dc7654` | `f2f4b65ac9c174618e89a21835990a785357e3b857c095b7077b6876ceb356a1` |

Run 42666467018 also compared the broader checks, but reused one Cargo target
directory; its comparison is provisional and is not used in this conclusion.

