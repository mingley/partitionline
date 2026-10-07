# Producer startup retries (KL03-24)

Producer startup retries coordinator errors 14, 15 and 16 with configured backoff. Transaction coordinator discovery and producer-ID allocation share one request deadline. Authentication, authorization, transaction-state and fencing errors stop initialization.

`summary.json` records the scope, source, toolchain and outcomes. `source-before/` retains the failing producer and test injection; `source/` pins the candidate. `validation/` retains failures as well as passes. Java verified all 101,000 acknowledged records after restarting the fresh Kafka data directory without new produces. Broker closure receipts show joined processes and reusable listener ports.

This RF1 development run checks startup and acknowledged-record integrity. It does not establish production readiness or a performance ranking. The phase starts after initial bootstrap negotiation/authentication; those existing operations keep their own configured deadlines.

Reproduce the deterministic cases with `cargo test --locked --test producer_initialization`. Run `cargo test --locked --all-targets --all-features` and strict Clippy on latest stable. The retained native drivers/configuration record the Kafka and Java commands; paths under `/workspace/work` are local execution inputs.
