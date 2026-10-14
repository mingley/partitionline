# Q-CONF-VerifiableProducerOptions (KL01-26)

Independent qualification of the finite verifiable Producer option
profile against pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1
and 4.3.1.

The registered `verifiable-producer-options-3-9-1` case ID keeps its
original `local_consistency` disposition in
`tests/conformance/cases.json` (source pin
`3e1803560e081d44f8e5ae4a3e4b449f69ff0a54`, peer 3.9.1, adapter
`verifiable-scenario`). This directory adds independent Apache-built
Produce v9 option-profile bodies; it does not promote that row or the
local `verifiable_contract` mock.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/verifiable_producer_options/`
- Qualification receipts: `qualification-20261014/`
