# Documentation

| Topic | Read |
|---|---|
| Install and send your first records | [README](../README.md) |
| Configuration, recipes, and troubleshooting | [Operator guide](guide.md) |
| Methods and types | [API docs](https://docs.rs/partitionline) |
| Java client names | `java_crosswalk` in the [API docs](https://docs.rs/partitionline) |
| Port a rust-rdkafka application | [Migration guide](migrate-from-rdkafka.md) |
| Delivery, memory, and cancellation | [Resource contract](resource-contract.md) |
| API stability and errors | [API stability](api-stability.md) |
| Tested platforms and Kafka versions | [Support matrix](support.md) |
| Protocol versions and missing features | [Protocol support](gaps.md) |
| TLS, SASL, and credential handling | [Security](security.md) and [token lifecycle](auth-refresh.md) |
| Client internals | [Design](design.md) |
| Broker development | [Broker README](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/partitionline-broker/README.md) |
| Schema Registry and serialization | [Companion README](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/partitionline-schema/README.md) |
| Versions this client speaks | [Wire versions](wire-versions.md) |
| Run and compare benchmarks | [Benchmark instructions](benchmark.md) and [measurement rules](benchmark-contract.md) |
| Release process | [Release guide](RELEASE.md) |
| Plan an application rollout | [Adoption guide](ADOPTION.md) and [exercise template](adopter-exercise.md) |

## Development plans

The [task registry](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/tasks.json) tracks status and dependencies. Use the
[session guide](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/README.md) to select work. The [roadmap](ROADMAP.md),
[client performance plan](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/performance-leadership.md),
[gateway plan](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/gateway-adoption.md), and
[broker plan](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/broker-implementation.md) describe the goals and checks.

[STATUS.md](STATUS.md), [CIVILIZATION.md](CIVILIZATION.md), and dated audits
preserve earlier plans and results. Use the task registry for current status.
