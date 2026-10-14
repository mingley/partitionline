# Q-CONF-FetchDeliveryRegressions (KL01-22)

Independent replay of the five delivered-position / fetch-boundary
regression histories against pinned Apache kafka-clients 3.9.1, 4.1.0,
4.1.2, 4.2.1 and 4.3.1.

The five registered `audit-regression-a0*` case IDs keep their original
`failed` dispositions in `tests/conformance/cases.json` (source pin
`cb7e97d3b92a8555aea34d59266a2990c206395f`, peer 4.1.0, adapter
`audit-probe`). This directory adds independent Apache-built Fetch v12
and OffsetCommit/OffsetFetch v8 evidence; it does not promote those rows.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/fetch_delivery_regressions/`
- Qualification receipts: written after the bounded Rust/Python run
