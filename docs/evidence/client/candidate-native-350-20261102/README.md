# Client candidate v8 results

The isolated v8 candidate ran on x86_64 at checkout `350b5e534` in
[run 41572635763](https://github.com/mingley/partitionline/actions/runs/41572635763).
In each feature configuration, all 2,280 library tests passed, including the
153 required library controls and 26 producer ownership controls. Formatting
and strict all-target, all-feature Clippy also passed.

The candidate did not pass overall. In each configuration, Mock Produce had
31 passes and 15 failures; retained backing had seven passes, one timeout and
one ignored test. Those are the same 15 Mock Produce failure identities seen
in v7. A legacy coordinator cancellation control failed after passing in v7,
so its expected baseline outcome did not reproduce. The other selected
integration binaries passed.

These runs used one test thread and an 8 MiB stack. A separate 2 MiB mock group
probe passed; live Kafka stack safety remains open. The ten selected binaries
are not the full all-target matrix. The candidate changes are not yet adopted.

The [analysis](analysis.json), [manifest](manifest.json) and object archive
retain all 610 members of the compact report. All 133 source before/after
pairs match; no guard stop or remaining owned child was reported. OOM counters
were unavailable. The separate full executable and source artifact remains on
GitHub with finite retention and was not downloaded for this record.
