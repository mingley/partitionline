# WriteTxnMarkers evidence

The current client qualification is in qualification/. It contains actual
Apache SDK frames, public Admin wire traces, old-source regressions, final
latest-stable checks, source snapshots and checksums.

WriteTxnMarkers v2 adds a signed transaction-version field. Public abort sends
its default0, routes to the partition leader and requires one exact matching
result. Errors8/9 refresh that leader within the original deadline. Null fields
forbidden by the schema are rejected.

The original pins, preparation attempts and handwritten schema controls remain
separate. They are historical inputs, not executed SDK evidence. The current
qualification does not establish broker transaction recovery or mixed-node
rolling upgrades. Share-offset API90 qualification has its own evidence path.
