# Fetch admission fixture

The original integration test expected the first poll of an immediate Fetch to
return pending. The storage actor can complete on another thread before that
poll reaches the reply receiver. The [main CI failure](https://github.com/mingley/partitionline/actions/runs/42439941625)
did not record the returned value, so its exact cause is not known. An
instrumented baseline handoff forced the actor response ready before the
receiver poll; that test failed after returning a successful Fetch response.
It demonstrates a possible schedule, not the original CI failure's exact cause.

The replacement parks the real storage actor before the first poll. A later
metadata reply proves the actor processed the Fetch and drained its queue
without the test polling the Fetch reply again. The test checks that the
unconsumed reply blocks another data request, and that both consuming and
canceling it allow another valid Fetch. A temporary early-permit-release
mutation made the test fail in both feature profiles. The retained-byte
configuration boundary remains covered by the integration test.

The [full x86 qualification](https://github.com/mingley/partitionline/actions/runs/42442254839)
tested commit `fcca3ff78c96e270e57b0d7c7e6ee28adccf4834`, based on
`b631809f0e03ba6f65f808e51d8e47e3fd9380d1`, with stable Rust 1.99.1.
The default and all-feature broker suites, formatting, all-target Clippy,
Rustdoc, doctests, API inventory and all 55 inventory controls passed.
The targeted admission case was repeated ten times per feature profile.
The [logs](https://github.com/mingley/partitionline/actions/runs/42442254839/artifacts/13601934037)
contain the individual results.

The [latest integrated-tree run](https://github.com/mingley/partitionline/actions/runs/42442982874)
tested `7b96c6c1023504bf18897681985a21ce16dd0779`, based on
`264322ff430b0594f4c3864e53dcd832de2a930b`. The inventory, all 55 controls,
the admission witnesses, all-feature suite, Clippy and documentation checks
passed. The default Fetch integration suite reported 25 passes and one
failure: `transport_joined_shutdown_cancels_live_long_poll_without_stopping_store`
returned `QueueFull`. Its cause remains unverified. The
[logs](https://github.com/mingley/partitionline/actions/runs/42442982874/artifacts/13602142836)
retain the failure. Uploaded artifacts were not independently inspected.

The earlier [main run](https://github.com/mingley/partitionline/actions/runs/42441209278)
reported four Raft runtime failures. One later passing run does not establish
that those failures are fixed. A separate [setup attempt](https://github.com/mingley/partitionline/actions/runs/42441580219)
failed to compile the full suite because its sparse checkout omitted an
existing test fixture; that failure and its logs are retained.

This test covers a completed, unconsumed actor reply. When a caller cancels
while the actor is still processing, the dispatch future can release its
admission permit before the actor releases the request and work buffers.
The actor checks for cancellation, but this overlap and the resulting memory
bound remain unqualified. It is not evidence of a process-wide memory bound.
