# Client candidate v16

This candidate updates release-checker source pins and adjusts the deadline
cancellation test fixture to exercise an unordered suffix scan. The existing
23 baseline cases, 183 controls, targets, timing and cancellation assertions
remain required. The original v15 assets and release-only v16 metadata are
retained. The qualification workflow verifies six helper files and emits
bounded failure diagnostics. It does not verify dependencies or redact secrets.

Source patch roundtrips passed. V16 has not run natively. The v14 hosted log
omitted the cancellation panic detail, so the fixture mismatch is a possible
explanation rather than a confirmed cause. Client correctness, live Kafka
compatibility, release readiness and performance ranking remain open.
