# Q-CONF-QuotaThrottleRuntime (KL01-25)

Independent qualification of Producer/Consumer quota throttle runtime
histories against pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2,
4.2.1 and 4.3.1.

The registered `feature-produce-fetch-quota-throttle-runtime` case ID
keeps its original `not_run` disposition in
`tests/conformance/cases.json` (source pin
`cb7e97d3b92a8555aea34d59266a2990c206395f`, peer 4.1.0, adapter
`none`). This directory adds independent Apache-built Produce v9 and
Fetch v12 `ThrottleTimeMs` bodies; it does not promote that row or
local `BrokerThrottle` unit tests.

- Applicability and source hashes: `applicability.json`
- Generated bodies: `tests/fixtures/quota_throttle_runtime/`
- Qualification receipts: `qualification-20261014/`
