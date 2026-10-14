# Q-CONF-FetchSessionRuntime (KL01-24)

Independent qualification of incremental Fetch session runtime and
bounded reset histories against pinned Apache kafka-clients 3.9.1,
4.1.0, 4.1.2, 4.2.1 and 4.3.1.

The registered `feature-fetch-incremental-session-runtime` case ID keeps
its original `not_run` disposition in `tests/conformance/cases.json`
(source pin `cb7e97d3b92a8555aea34d59266a2990c206395f`, peer 4.1.0,
adapter `none`). This directory adds independent Apache-built Fetch v12
session bodies; it does not promote that row or the local
`FetchSessionOracle` campaign.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/fetch_session_runtime/`
- Qualification receipts: `qualification-20261014/`
