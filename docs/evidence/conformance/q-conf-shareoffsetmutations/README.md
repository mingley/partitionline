# Q-CONF-ShareOffsetMutations (KL01-29)

Independent qualification of public AlterShareGroupOffsets (api 91)
and DeleteShareGroupOffsets (api 92) v0 against pinned Apache
kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1.

The registered `current-public-api-091-v0-4-1-2/4-2-1/4-3-1` and
`current-public-api-092-v0-4-1-2/4-2-1/4-3-1` case IDs keep their
original `not_run` disposition in `tests/conformance/cases.json`
(source pin `f0ca05da9c04942b211e2f56ddc615b9860cdea2`, adapter
`none`). This directory adds independent Apache-built v0 bodies; it
does not promote those rows or the local `tests/common` Mock.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/share_offset_mutations/`
- Qualification receipts: `qualification-20261014/`
