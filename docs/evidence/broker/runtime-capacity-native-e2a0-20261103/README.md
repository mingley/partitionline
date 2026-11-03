# Broker runtime capacity qualification

The isolated runtime-capacity candidate passed on the GitHub Linux x86_64
runner using Rust 1.99.1 and LLVM 23.1.1. The job checked out commit
`e2a04e229514d3a2662a9f765b57145c467de237`; the candidate broker source was
reconstructed from commit `6fa8cdb3588d884580d01331e44df657c02f7670`.

- [Workflow run](https://github.com/mingley/partitionline/actions/runs/41591455215)
- [Broker qualification job](https://github.com/mingley/partitionline/actions/runs/41591455215/job/125768893688)
- [Compact reports artifact](https://github.com/mingley/partitionline/actions/runs/41591455215/artifacts/13211115523)

On the baseline, the held-permit deadline witness and allocation identity test
passed, and the capacity test failed at its expected assertion. Both capacity
tests passed with the fixed source in default and all-feature profiles. The
full default suite passed 729 tests; the all-feature suite passed 877. Format,
strict all-feature Clippy and Rustdoc, the retained focused tests, and all 55
API inventory negative controls passed. Both doctest commands succeeded but
found zero doctests. All 47 command receipts record successful source checks
and process cleanup. OOM counters were unavailable for every command, so the
run provides no OOM-counter observation.

The tested patch SHA256 is
`7f36df7c94bfd5265c5586c8a2749d3180f5e7da0534e053848154b3cdbdfbab`.
The runtime source changed from
`a0c136fcc728cfc4580ef30b2ca3fa964f32329f65fce301ef70985dfcd91754` to
`38c50bcd93e5dea91d10264c984fef1a5bfe9e4e33686a5ec64c03ae7945b66d`.
`analysis.json` records the source pins, gate results and limits;
`qualification.json` is the original qualification report. Its
`baseline_expectations` sentence describes preparation before execution; the
retained baseline logs and result fields record what actually ran.

`reports.zip` is the downloaded compact artifact. `members.json` records the
hash, length and CRC of each of its 117 members. `job.log` is the decoded job
log returned by the GitHub connector. `SHA256SUMS` covers all files in this
directory except itself. The larger full artifact, including retained binaries,
was not downloaded; its identity and digest are recorded in `analysis.json`.

This result applies to the tested broker candidate and these checks. It does
not qualify the complete main branch, prove a process-wide memory bound, resolve
the separate ControllerHandler accounting issue, establish production
readiness, or support a performance ranking.
