# Broker CI at 0708730

[Main broker run 42471273652](https://github.com/mingley/partitionline/actions/runs/42471273652)
tested commit
[`0708730155d869ab4fa52654a2e95d4013c4e594`](https://github.com/mingley/partitionline/commit/0708730155d869ab4fa52654a2e95d4013c4e594).
Its default all-target command stopped at **702 passed, one failed and four
ignored**. The all-feature suite, broker Clippy, Rustdoc and doctests in that
job did not run. The separate pinned inventory and instrumented fuzz
supervision jobs passed.

The failed `actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join`
test attempted `observer-addition` on node 1 and received
`Node(NotLeader)` instead of the expected change response. Later separately
sampled diagnostics showed node 0 as term-5 leader and node 1 as term-5
follower. The samples were not atomic and do not establish when leadership
changed or the cause of the failure. The operation was not retried.

The preceding [main broker run](https://github.com/mingley/partitionline/actions/runs/42470021226)
passed 779 default and 927 all-feature tests with four ignored per profile.
That pass does not explain this failure. The intervening main commits changed
client Admin source and documentation, not broker source or manifests; the
log does not establish whether the failure is related to any earlier broker
change.

The corresponding [client run](https://github.com/mingley/partitionline/actions/runs/42471273752)
and [capability run](https://github.com/mingley/partitionline/actions/runs/42471273683)
were still in progress when this note was prepared. They have no final result
in this note. This partial record does not qualify production readiness.
