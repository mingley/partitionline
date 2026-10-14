# Q-CONF-VerifiableConsumerOffsets (KL01-27)

Independent qualification of the finite verifiable Consumer reset and
autocommit profiles against pinned Apache kafka-clients 3.9.1, 4.1.0,
4.1.2, 4.2.1 and 4.3.1.

The registered `verifiable-consumer-reset-policies-3-9-1` and
`verifiable-consumer-autocommit-3-9-1` case IDs keep their original
`local_consistency` disposition in
`tests/conformance/cases.json` (source pin
`3e1803560e081d44f8e5ae4a3e4b449f69ff0a54`, peer 3.9.1, adapter
`verifiable-scenario`). This directory adds independent Apache-built
Fetch v12, ListOffsets v5, OffsetCommit v8 and OffsetFetch v8 bodies;
it does not promote those rows or the local `verifiable_contract` mock.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/verifiable_consumer_offsets/`
- Qualification receipts: `qualification-20261014/`
