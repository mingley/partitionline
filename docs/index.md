# Documentation

| Topic | Read |
|---|---|
| Install and send your first records | [README](../README.md) |
| Configuration, recipes, and troubleshooting | [Operator guide](guide.md) |
| Published methods and types | [API docs](https://docs.rs/partitionline) |
| Java client names (current source) | [`java_crosswalk`](https://github.com/mingley/partitionline/blob/dfe179d7066e2c1afecb88df5ba76d2920308c46/src/lib.rs#L165) |
| Port a rust-rdkafka application | [Migration guide](migrate-from-rdkafka.md) |
| Delivery, memory, and cancellation | [Resource contract](resource-contract.md) |
| API stability and errors | [API stability](api-stability.md) |
| Tested platforms and Kafka versions | [Support matrix](support.md) |
| Protocol versions and missing features | [Protocol support](gaps.md) |
| TLS, SASL, and credential handling | [Security](security.md) and [token lifecycle](auth-refresh.md) |
| Client internals | [Design](design.md) |
| Broker development | [Broker README](https://github.com/mingley/partitionline/blob/dfe179d7066e2c1afecb88df5ba76d2920308c46/partitionline-broker/README.md) |
| Schema Registry and serialization | [Companion README](https://github.com/mingley/partitionline/blob/main/partitionline-schema/README.md) |
| Versions this client speaks | [Wire versions](wire-versions.md) |
| Run and compare benchmarks | [Benchmark instructions](benchmark.md) and [measurement rules](benchmark-contract.md) |
| Release process | [Release guide](RELEASE.md) |
| Plan an application rollout | [Adoption guide](ADOPTION.md) and [exercise template](adopter-exercise.md) |

## Development plans

The [task registry](https://github.com/mingley/partitionline/blob/main/docs/plan/tasks.json) tracks status and dependencies. Use the
[work plan](https://github.com/mingley/partitionline/blob/main/docs/plan/README.md) to select work. The [roadmap](ROADMAP.md),
[client performance plan](https://github.com/mingley/partitionline/blob/main/docs/plan/performance-leadership.md),
[gateway plan](https://github.com/mingley/partitionline/blob/main/docs/plan/gateway-adoption.md), and
[broker plan](https://github.com/mingley/partitionline/blob/main/docs/plan/broker-implementation.md) describe the goals and checks.

[STATUS.md](STATUS.md) records current qualification results and open work.
[CIVILIZATION.md](CIVILIZATION.md) and dated audits preserve earlier plans and
results. Links to development plans and the companion guide follow `main`;
they may differ from the version in your checkout.
