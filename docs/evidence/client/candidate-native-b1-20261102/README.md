# Client candidate v10 results

The isolated v10 candidate ran on x86_64 at checkout `b1b048809` in
[run 41574318102](https://github.com/mingley/partitionline/actions/runs/41574318102).
Both feature configurations passed 2,280 library tests and strict Clippy.
Those runs include all 153 required library controls and the 26 added producer
ownership controls. Formatting also passed.

The candidate did not pass overall. In each configuration, Mock Produce had
32 passes and 15 failures; retained backing had eight passes, one timeout and
one ignored test. The new fixture and wire-phase controls passed. The original
retained-backing test still timed out after its mock's zero-duration timer was
removed, so that timer alone does not explain the failure. The same 15 Mock
Produce identities failed in v8 and v10.

One legacy coordinator cancellation control failed after passing in v7, so its
predicted baseline pass did not reproduce. The other selected binaries passed.
The candidate changes are not yet adopted on `main`.

Both runs used one test thread and an 8 MiB stack. A separate 2 MiB mock group
probe passed; live Kafka stack safety remains open. These ten selected binaries
are not the full all-target matrix.

The [analysis](analysis.json), [manifest](manifest.json) and object archive
retain all 610 compact-report members. Source inventories stayed stable, with
no reported guard stop or remaining owned child; OOM counters were unavailable.
The separate full executable and source artifact remains on GitHub with finite
retention and was not downloaded for this record.
