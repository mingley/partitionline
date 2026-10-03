These public deterministic controller fixtures are generated independently by
`docs/evidence/broker/KL11-69/ControllerOracle.java` against three pinned Apache
Kafka releases. `manifest.json` is the descriptive manifest; per-release
`cases.tsv` provides the twelve-column Rust-test input. `observations.json`
preserves actual upstream outcomes and deliberate local-profile distinctions.

Primary `.request.bin` / `.response.bin` files contain header and body without
the four-byte TCP length; `.frame.bin` files add that length. An
`.apache-response.bin` file is an actual Apache handler result, which can differ
from the local policy `.response.bin`. A `close` row has no local response file.

The scope is fixed trusted membership and caller-driven Vote/Begin/End version0
transitions, with ApiVersions0–4. Apache parser/handler probes and byte fixtures
do not establish a complete KRaft controller implementation or network session.
