# Candidate failure diagnostics

The client qualification job now prints bounded failure context, compiler
errors and selection errors while retaining complete logs and the existing
qualification decisions. Expected negative baseline cases remain quiet.
The diagnostic blocks and summary escape GitHub workflow-command delimiters.

The [validation](validation.json) records 30 passing offline tests with
sockets denied and Cargo and registry tools guarded. An [independent review](independent-review.json)
replayed two retained Rust test logs, checked output bounds and spoofed lines,
and verified that raw receipts and gate inputs remain unchanged. A separate
[scope review](scope-review.json) checked the original controls and gate logic.
The [stored files](stored-files.json) identify the patch and logs.

Libtest output is unframed, so attribution is best effort and printed failure
text is not secret-redacted. Output bounds apply to each projection, not to the
runner's in-memory log reads or total workflow output. The change has not yet
run in hosted qualification.
