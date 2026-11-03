# CI checker temporary paths

The earlier attempt-binding checker split live temporary paths on colons. A
`TMPDIR` containing a colon therefore rejected valid fake-service evidence;
the self-test also failed. Both failures were closed rather than reported as
successful CI.

The [fix](candidate.patch.gz) treats live paths literally. Fixture inputs keep
their colon-delimited list format by default; `PL_CI_FIXTURE_SINGLE_PATHS=1`
selects literal paths for all fixture inputs. The self-test uses that mode.
The [validation](validation.json) records 122 passing offline release tests
with sockets denied and real Cargo and registry tools guarded. An
[independent review](independent-review.json) checked the old failures,
literal paths, list compatibility and failed-job rejection. The compressed
logs and scripts are identified in [stored files](stored-files.json).

These are offline checks. Live GitHub pagination, end-to-end current-attempt
validation and the remaining release gates are still open.
