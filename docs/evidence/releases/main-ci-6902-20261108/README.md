# Broker CI at main 6902d3b

Main commit `6902d3b262e7d25772b66bc68dda7baba882495e`,
[broker run 42476127572](https://github.com/mingley/partitionline/actions/runs/42476127572),
job [128717843431](https://github.com/mingley/partitionline/actions/runs/42476127572/job/128717843431),
failed the locked default-feature broker tests. Stable rustc was 1.99.1
(`58cf8f9c3`); broker transport blob was
`b84ca76cab8d8340147795e9665584326657cc9c`, Cargo.toml
`4e23c42f8d9ac722005098a4b58e1046b711e1a3`, and Cargo.lock
`19652acd96b929402b1e4a9394a33d12a7e69449`.

The exact failure was:

```text
---- transport::disconnect_tests::client_disconnect_stops_the_handler stdout ----
thread 'transport::disconnect_tests::client_disconnect_stops_the_handler' (4324) panicked at src/transport.rs:2022:9:
assertion `left == right` failed: client disconnect left the handler running
  left: 0
 right: 1

test result: FAILED. 490 passed; 1 failed; 4 ignored; 0 measured; 0 filtered out; finished in 3.90s
```

The current-thread test sends an empty frame and waits until its handler has
started. The handler holds a guard that increments `stopped` when dropped and
waits on an unreleased notification. The test drops the client socket, calls
`yield_now` 32 times, and then reads that counter. The observed zero means
the guard had not been dropped by the assertion. The log does not establish
whether the handler remained alive indefinitely, whether the disconnect was
detected later, or why; 32 yields alone do not establish that the server saw
the socket close or that a particular task ran.

Broker formatting and the separate stale-leader fencing test passed before the
failure. All-feature behavioral tests, strict all-target/all-feature Clippy,
strict Rustdoc and both doctest steps were skipped. The separate instrumented
broker fuzz-supervision job and the pinned Apache inventory and negative-control
job passed separately. This result is partial CI evidence, not a broker
readiness or performance qualification.
