# Recovery fixture I/O guard

Both recovery targets previously returned success when their temporary path
could not be used. They counted executions without exercising recovery. The
guard now treats fixture creation, writing, cleanup and limit setup failures
as failed attempts.

A real invalid-directory control reproduced exit 0 before the fix. After the
fix, the harness requests exit 86 and libFuzzer reports exit 77 with the
infrastructure error. Its empty failing input is a deliberate control artifact,
not a parser discovery. Nine supervisor checks pass against the real binaries.

The first campaign was deliberately stopped after about 950 seconds and joined
all five workers. The second lost its supervisor during an environment
reconnect and has no final receipt. Both remain unqualified; neither contributes
time to a replacement attempt.

This archive retains builds, controls, the complete stopped first attempt and
a prefix snapshot of the second. The snapshot's old `running` state describes
its capture time. It does not establish current health. The bundled controls
include deliberately corrupted metadata copies. Their indexes were checked
against every archived member when retained.

The detached replacement is recorded in
[the next launch record](../20261027-detached-launch/README.md). Qualification
requires its actual uninterrupted completion and successful audit. The current
broker's broader failures and conformance gaps remain open.
