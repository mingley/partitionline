# Open-card integration checks, 2026-10-06

The broker passes 484 all-feature and 344 default tests on latest stable, with strict Clippy and formatting. Private peer sockets disable Nagle in both directions. The test cluster now lets each child bind an ephemeral port and publish its actual address, avoiding a reserve/release race. The renewal-admission test opens its second connection after the first has authenticated, so unrelated pre-authentication time does not consume the tested renewal deadline.

`validation/` retains the original election, authentication and bind failures and the successful final suites. `source/` identifies the tested candidate. The source registry was reconciled for four reviewed inputs, then checked against actual compiled Apache response fixtures and 55 negative controls. API ranges, independent fixtures and feature qualification claims were preserved.

These checks support the current implementation. Full causal Raft, metadata-quorum and independent SASL SDK qualification remain open on KL11-76, KL11-14 and KL11-71. No production or performance rank is claimed.
