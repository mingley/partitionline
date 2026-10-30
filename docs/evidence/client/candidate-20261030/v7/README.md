# Client candidate v7

V6 reached native library-test compilation and stopped on one unused decoder
return value. Four new test-only peer methods also had unnecessary public
visibility. No Rust tests ran in that attempt.

V7 binds the decoded topic-page cursor and asserts that it is absent, matching
the fixture. It restricts the peer methods to `pub(crate)`, preserving calls
from sibling test modules. These changes are identical in baseline and fixed
source; the production fix delta is byte-identical to v6.

The shared broker registry has a separate metadata refresh after adoption of
the qualified broker source. The same digest is recorded in all three client
input catalogs. The broker source tree remains outside the client catalog.

All 141 required identities, 22 baseline cases, ten full test binaries and
existing budgets remain required. Source reconstruction and Rustfmt checks
pass. V7 has not been compiled or runtime-qualified. Ordinary full-main CI,
live Kafka stack safety, remaining producer ownership/replay work, production
readiness and performance rank remain open. V6 source and evidence are kept.
