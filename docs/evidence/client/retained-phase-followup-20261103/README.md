# Retained-backing timeout diagnostics

The v16 job reported `Timeout` but did not expose the test's optional numeric
flush diagnostic, so the failing phase is unknown. This isolated
[follow-up](candidate.patch.gz) projects that line only for the exact test and
lanes when its schema and placement match; raw logs and test gates stay intact.

The [validation](validation.json) records 36 guarded offline tests. Two
independent reviews ([first](review.json), [second](second-review.json))
checked ambiguous and malformed output, escaping, and unchanged gate logic.
Libtest output is unframed and the projection is not secret-redacted. This
patch is not integrated: the candidate helper and benchmark pins must be
updated and reviewed before a hosted rerun.
