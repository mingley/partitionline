# Frame-read deadline test

The revised test is applied on main. Production transport logic is unchanged.

The [main broker CI run 42456887194](https://github.com/mingley/partitionline/actions/runs/42456887194/job/128657767650)
failed `transport::frame_read_deadline_tests::read_deadline_stops_a_ready_frame_body`
in the all-feature library. Its fake reader was continuously ready and
busy-waited for 120 microseconds per 1 KiB body read. The test gave the read a
30 ms deadline and required both `ReadDeadline` and wall elapsed below 80 ms.
The log reported `ReadDeadline` at 82.681715 ms. It did not record enough
scheduler or per-poll timing to establish why that invocation exceeded 80 ms.
The default suite had passed 761 tests with three ignored; all-feature stopped
at 516 passed, one failed and three ignored, so later broker targets and that
job's Clippy, Rustdoc and doctests did not run. Inventory and fuzz supervision
passed separately.

The production body reader checks the deadline between reads, limits each read
to 8 KiB and yields after 64 KiB of progress. It cannot preempt a synchronous
`poll_read` or an operating-system scheduling delay. The test change retains
the 30 ms deadline and checks that the result is `ReadDeadline` with an
incomplete body; it records elapsed time but no longer imposes an 80 ms wall
limit. Two independent controls retain the behavior coverage:

- In a paused Tokio clock, the fake reader advances time through the deadline
  inside its first body poll. The control verifies that the advance actually
  occurred there, that the result is `ReadDeadline`, and that no second body
  poll starts.
- A manually polled, continuously ready 256 KiB frame must return `Pending`
  before the body is complete. This checks the voluntary yield without relying
  on a scheduler to run another task.

Tokio `test-util` is enabled only in the broker's dev dependency. The
transport changes are inside the test module; release transport logic is
unchanged. These controls do not establish a hard real-time deadline.

## Controlled process pause

The qualification-only witness injected a gate into the fake reader's first
body poll, stopped only the newly spawned disposable test process for 200 ms
using `SIGSTOP`, released the gate, then resumed and joined the child. It
compared the old test and the candidate in fresh worktrees. In
[run 42457719844](https://github.com/mingley/partitionline/actions/runs/42457719844),
the old test failed at 203.890713 ms with `deadline true`; the candidate
returned `ReadDeadline` at 204.124489 ms with 1,047,552 bytes unread. In
[run 42457826681](https://github.com/mingley/partitionline/actions/runs/42457826681),
the old test failed at 204.183784 ms with `deadline true`; the candidate
returned `ReadDeadline` at 204.167379 ms with the same bytes unread. Both
witnesses completed and joined their children. The latter installs termination
handlers to resume and join the child on ordinary interruption.

This demonstrates that controlled descheduling can produce the old test's
failure signature while the deadline is reported. The gate is synthetic and
changes the fake poll; it does not establish that descheduling caused the
original 82.68 ms failure, nor does it bound blocking inside a real poll.

## Qualification

The first candidate [7f522f2](https://github.com/mingley/partitionline/commit/7f522f230aa54ccb05586d166384be728d6a4cc3)
was based on main `6298f6b`. In
[42457719844](https://github.com/mingley/partitionline/actions/runs/42457719844),
default and all-feature all-target suites passed 763 and 911 tests respectively,
with three ignored in each. Formatting, strict all-target/all-feature Clippy,
inventory and all 55 inventory negative controls passed. Both focused jobs
failed before running tests because a shallow checkout did not contain the
parent used for source verification. The process-pause witness passed.

The corrected workflow [cc983cc](https://github.com/mingley/partitionline/commit/cc983cc06138ad8e687666a188160d0c2f6d2ac0)
used the same transport and manifest blobs. In
[42457826681](https://github.com/mingley/partitionline/actions/runs/42457826681),
four focused controls passed in each profile. A temporary mutant removing the
body's direct deadline checks failed the first-poll count assertion in each
profile; another removing the 64 KiB yield failed the manually-polled
pending assertion in each profile. Both were verified test assertion failures,
not compilation or setup failures. Formatting, strict Clippy, inventory,
55 inventory controls and the process-pause witness passed. Its all-feature
all-target suite passed 911 tests, with three ignored.

The corrected run's default suite failed in the library after 471 passed,
four failed and three ignored; later targets did not run. Two minority
histories received `NotLeader` after proposals went to nodes that subsequently
reported follower roles. The three-peer observer checkpoint base was 4 versus
expected index 3. The five-peer observer image was not over 64 KiB; its log
did not preserve the actual size, base, term or payload identities. The test
expected two 40 KiB committed payloads and an uncompressed full-prefix image.
The common-prefix check does not independently establish the identity of both
expected proposals. These four Raft outcomes remain open; the logs do not
establish their cause or whether this test change contributed.

The [combined tree 99651ea](https://github.com/mingley/partitionline/commit/99651ea9b0ec4e098e00fd412fa6699b23c8d959)
overlaid the same two source blobs on main
`1036a393eb85f23032b935bf86862b7e98e36e14`, which also contains the
CreateTopics diagnostic. [Run 42458219074](https://github.com/mingley/partitionline/actions/runs/42458219074)
passed 764 default and 912 all-feature all-target tests, with three ignored
in each, plus both-profile focused and mutation controls, formatting, strict
Clippy, inventory and all 55 inventory controls. The ignored descriptor tests
were not run in this gate. This pass does not explain the earlier failures.

The combined `transport.rs` SHA-256 is
`e5b0544fd50c5a4908cfecf13cbab3a06e85de37dfb6c09865118075899ea681`;
the broker `Cargo.toml` SHA-256 is
`17dddc953a84293ddcec5604083b9ee6d34ef65866313d86ac5e638d5586725b`.
The [combined default controls](https://github.com/mingley/partitionline/actions/runs/42458219074/artifacts/13608875488),
[combined all-feature controls](https://github.com/mingley/partitionline/actions/runs/42458219074/artifacts/13608297968)
and [second process-pause logs](https://github.com/mingley/partitionline/actions/runs/42457826681/artifacts/13608074073)
are retained as GitHub Actions artifacts for 30 days. GitHub reported SHA-256
values `1b96c5ce7b773111cdeabc36ab61dc60d357a0737919dcb211b825d1f3931f15`,
`00644763c54c520bde82aa01dcfc7ff23981eefeafbed7905ccda5f0707257e8` and
`f9bc4eefafd3f2e3b8a3a609991ec49d94b1ffe10c7ec667f70faffb3af6b29a`
respectively. The archives were not independently downloaded.
