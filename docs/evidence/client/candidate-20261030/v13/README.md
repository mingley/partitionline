# Client candidate v13

This candidate preserves the v12 client Rust patches, 183 required identities,
23 baseline cases, test targets and timing thresholds. It repins the reviewed
release-tooling changes and adds any new release test inputs in all phases.
The base catalog is the recorded parent checkout plus the retained release
patch; the checkout running the driver must contain those exact resulting bytes.

Full source patch reconstruction passed. No Cargo, Clippy or Rust runtime has
run for v13. The retained-backing timeout, failing mock handshake tests,
live Kafka compatibility and release qualification remain open. The original
v12 assets and review evidence are retained here.
