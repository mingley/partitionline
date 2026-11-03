# Client candidate v14

This candidate adds a missing test-module import to v13. The change addresses
the E0433 reported by Clippy and the default test build in the v12 run. It
keeps all 23 baseline cases, 183 required controls, test targets and timing
thresholds. V13 assets and the independent import review are retained here.

Source reconstruction and patch roundtrips passed. V14 has not run natively;
the import fix may expose further compiler errors. The v10 mock failures,
retained-backing timeout and intermittent topic-encoding timing failure remain
open. Live Kafka compatibility, release readiness and performance ranking are
not established.
