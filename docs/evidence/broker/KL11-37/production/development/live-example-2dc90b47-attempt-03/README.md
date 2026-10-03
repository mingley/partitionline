Coordinator review rejected the previous1c920fe8 example's live ownership scope:
its compile/lint matrix passed, but fallible operations after Service startup and
question-mark shutdowns could skip cleanup of already-started components. The
source review, exact original bytes and all passing narrow receipts are retained
in attempt02; attempt01 retains the earlier actual Clippy failure. No fabricated
runtime failure or rewritten raw receipt replaces either history.

The revised source92d4f88154fbbfe13e6e806e66000dff22783131a5a8132c20e0a998567a6cc9
loads all configured files, TLS material and static typed budgets before owned
router/manager startup. Router starts first. Service failure attempts router
shutdown; profile/bind failure attempts Service and router shutdown. Normal
teardown attempts listener, Service and router even if an earlier result is an
error. Each attempt emits only redacted stage/owner/outcome flags. A failed
attempt remains failed and returns a static error; only successful attempts and
matching accepted/joined counters with zero worker failures emit joined. Public
constructors retain their private validation/startup contracts; this does not
claim forcible cancellation of underlying operating-system calls or a successful
join when a component returns an error.

Fresh stable/Rust1.85 default/all-features example check and strict Clippy plus
both formatting gates pass all ten commands. Twenty whole70,818-file byte/full
07777-mode checks have identical aggregates. Inputs are the complete actual
2dc90b471b384e88741a3d8db97963530a754d97 tree plus only this new example overlay;
the six pushed socket sources are unchanged. Minimum half-second sampled free
space was1,001,316,352 bytes with no guard hold. All current target ELF bytes/modes
were losslessly retained after every command before later overwrite. Audit JSON
is losslessly compressed with raw SHA/mode/restore metadata; archives stay in
scratch, excluded from publication.

This is compile/lint/format qualification: zero behavior or live SDK cases.
Actual pushed-source full broker51 gates, real HTTPS OAuth acquisition and
provider/connection/ordinary read-write/failed-startup/shutdown histories remain
required. Metadata6 and ordinary read-write9 scope is explicit; groups,
transactions, idempotence, replication, application ACLs and reauthentication are
not established. Session lifetime remains zero.
