# ListTransactions pattern filter

API66 v2 accepts a bounded nullable transaction-ID pattern alongside state,
producer-ID and duration filters. Public null/empty patterns remain unfiltered.
Nonempty patterns require v2 on every broker. Complete calls fail if any broker
fails; partial calls retain each broker’s result or error. Reconnects negotiate
again under the original deadline. Regex evaluation remains with the broker.

The retained latest-stable default/all suites passed2,011/2,023 tests. Seven
focused tests passed per build. Both external lanes ran: three actual Apache
SDKs each generated144 raw cases (58 supported pairs and86 refusals), then
parsed116 Rust frames per build. The66 public process profiles per build
cover null/empty, matching/nonmatching, invalid regex, combined filters, older
brokers and reconnect downgrade. All final processes were waited; peers joined
workers and verified closed, reusable ports.

The four package consumers compiled21 examples each. Strict formatting, Clippy,
rustdoc and53 coverage-checker tests passed. Source, binary, SDK, class and wire
hashes, exact commands, earlier failures and outcomes are under `validation/`.
The package archive predates the final card count in STATUS.

These are selected component checks against independent scripted broker policy.
They do not qualify live broker regex evaluation, transaction-state recovery,
production use or performance ranking. Source is locally uncommitted.

`summary.json` records the counts and limits. `SHA256SUMS` covers this snapshot.
