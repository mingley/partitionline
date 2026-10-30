# Broker inventory source pins

The inventory at `fe65ac783` has 11 stale source hashes. This refresh changes
only those digest strings. Advertised APIs, version ranges, fixture maps,
implementation gates and `qualification: not_run` are unchanged. The checker
is unchanged. Its fresh static inventory check passes; compiled handler reports
were not attached and no new runtime behavior is qualified by this check.

`historical-source-pins.json` traces each old digest to its matching revision.
The historical review explains the source changes. `review.json` verifies
that the same 11 source files still match that review at `fe65ac783`.
`changed-fields.json` independently checks that no other JSON values changed.
The patch retains its exact bytes in gzip form.

The complete 55 negative controls remain required in CI. Local attempts were
incomplete and receive no passing credit. The existing broker runtime matrix
passes 701 default and 849 all-feature tests at `fe65ac783`, but a digest
refresh does not establish production readiness or broader API support.

The isolated replication candidate changes four of these sources again.
Its source pins must be refreshed separately after those exact bytes qualify.
