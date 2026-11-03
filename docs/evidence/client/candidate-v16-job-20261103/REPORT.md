# Client v16 hosted qualification

Run `41587661156`, job `125756817431`, used checkout `6fa8cdb3588d884580d01331e44df657c02f7670` on stable x86_64. Checkout verification, both helper guards, ownership controls, client qualification guards, baseline/default/all-feature builds, formatting and strict Clippy passed. The native qualification failed; both artifact upload steps succeeded.

All 21 required baseline pass/fail expectations matched: five required passes and 16 required failures. Both observation-only cases passed. The fixed default and all-feature profiles each reported 2,432 passed, 16 failed and two ignored across ten targets. The library target passed 2,293 tests per profile, including the revised deadline fixture. Each profile reported 15 Mock Produce failures and one retained-backing failure.

The original 450 ms send-all test reported 756.0635 ms in default and 756.451082 ms with all features. Retained backing printed only `Error: Timeout` in both profiles. The log does not locate that timeout: the driver projects bounded diagnostic text, so other phase details may be absent from this projection. The projected test names, counts and diagnostic bodies are best-effort; see the complete decoded log and exact extracted diagnostic lines.

The compact report artifact is GitHub artifact `13210125242`, 15,523,523 bytes, with GitHub-reported SHA-256 `dc12805972973f5b97f132cf44ce5a706d7ec47389399b1dfa667f4ae0c73a32`. The connector returned a File Service reference but this workspace has no supported reader for it; artifact contents were not independently inspected. No live Kafka, production readiness, publish readiness or fastest-performance ranking follows from this run.
