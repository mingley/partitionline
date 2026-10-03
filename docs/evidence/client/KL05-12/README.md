KL05-12 Fetch18 source milestone

The client implements KIP-1166 partition tag1 with explicit replica sidecars and ordinary consumer default omission. Existing public partition fields and codec signatures are preserved. Response layouts are unchanged.

Apache4.1.0 and4.1.2/4.2.1/4.3.1 independently regenerated thirty bodies and parsed four fresh Rust request/response pairs each. Focused stable integration suites passed115cases with two existing live ignores; all56Fetch unit cases and strict focused all-featureClippy passed. Full immutable-source stable/MSRV qualification is pending.

Development logs retain obsolete version18-rejection assertions, a helper signature error, unusedSplice/borrow checks, nullable empty-record representation mismatch, and strict-lint failures before correction. Official4.1.0 archive bytes passed the corrected grouped SHA512 parser and exact SDK SHA256; both Maven429 failures and the original checksum-parser failure remain. Apache EMPTY records and Rust nullable null are separately parsed as equal empty records.

No follower acknowledgment/quorum-commit behavior, deployment, sustained fault campaign or performance gain is claimed by this milestone.
