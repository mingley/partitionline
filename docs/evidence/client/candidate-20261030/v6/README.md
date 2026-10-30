# Client candidate v6

The v5 native CI attempt compiled the baseline library, then stopped while compiling its tests. A worker initializer in `rediscover_stop_tests` used `Preparing` without importing that type into the nested module. No Rust test ran.

V6 changes that single initializer to `super::Preparing::default()` in both baseline and fixed source. Every other source hash is unchanged. The production fix delta, its compressed bytes, all 141 required control identities, the 22 baseline cases and the 22 changed Rust paths are identical to v5.

`preparation.json` records both one-line repairs, source hashes, the native diagnostic hash, stable rustfmt checks and exact forward/reverse source reconstruction. V5 source and proof files remain unchanged. The copied source-review archive describes the earlier v5 changes; this version adds only the test type qualification.

The [v5 qualification scope and limitations](../v5/README.md) still apply. This source repair has not been compiled or runtime-qualified. It does not establish production readiness, release readiness or a performance ranking.
