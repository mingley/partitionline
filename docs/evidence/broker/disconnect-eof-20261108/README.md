# Controlled broker disconnect observations

The [main 6902 CI failure](../../releases/main-ci-6902-20261108/README.md)
remains unexplained. Its test saw a handler-drop count of zero after dropping
the client socket and yielding 32 times; the log did not say whether EOF was
observed later or the handler remained live.

[42476777782](https://github.com/mingley/partitionline/actions/runs/42476777782)
used isolated test-only commit `43c6e5ee8ec9d792fd2523d147b6527cecfd9df6`,
parent incident commit `6902d3b262e7d25772b66bc68dda7baba882495e`. The
production transport at that parent was blob
`b84ca76cab8d8340147795e9665584326657cc9c`; the witness appended tests to
transport blob `87f88e329ff180d3c673df10988e687aa7332fa5`, without changing
the production code. Stable rustc was 1.99.1.

- A real loopback TCP stream was passed directly to the production `exchange`.
  A test-only reader wrapper observed actual TCP EOF, then withheld it and
  retained the waker. With the handler pending and its release signal unset,
  after 32 yields the handler guard had not dropped and the exchange task was
  still live. Opening the gate caused the guard to drop before release and the
  exchange joined with `PeerClosed`. This deliberate gate establishes a
  possible delayed-observation schedule, not the cause of the main failure.
- A separate unwrapped `Transport` listener accepted a real loopback
  connection. Its handler-drop signal was observed before either handler
  release or `Transport::shutdown`; shutdown then reported the accepted
  connection joined. Both positive controls passed with default and all
  features.
- In the default profile, changing the watcher's EOF arm to await a pending
  future made the gated control fail at “handler remained pending after EOF
  release” and the unwrapped transport control fail at “disconnect did not
  stop handler before release or shutdown”. The gated task was aborted and
  awaited on that failure path; the unwrapped transport was released and
  reported joined before its assertion. The two-second bounds are test
  harness limits, not production latency guarantees.
- The unchanged original disconnect test passed 40/40 isolated default
  invocations, with zero assertion failures, timeouts or other failures in
  this run. Its test binary also included the appended witness tests. This
  sample does not override the earlier failure or establish its cause.

Separately, main `7fb973f531f63aca32da18cccf7f1a852f49ee68`
[broker run 42476521782](https://github.com/mingley/partitionline/actions/runs/42476521782)
failed `actual_five_node_tcp_minority_partition_late_ack_image_restart_and_removal`:
`fault-delayed-old-reply-proposal on node2` returned
`error private peer runtime: Node(NotLeader)`. Its post-failure diagnostic
sample showed node0 leader term 4 and nodes 1–4 followers term 4; those
samples were taken separately, not atomically. The default suite reported
490 passed, 1 failed and 4 ignored; later broker checks were skipped. The
separate inventory and instrumented fuzz-supervision jobs passed. No connection
between this Raft failure and the transport observation is established.
