# Client candidate v12

V12 adds timeout diagnostics to the isolated v11 retained-backing test. The
mock log length is read with a nonblocking lock attempt only after the existing
flush timeout. The test keeps its prior assertions and 2 s admission, 10 s
flush and 15 s delivery thresholds. Retry and mock values are separate
observations, do not establish acknowledgement counts and may be unavailable.
Formatting output to stderr can itself block.

The prior 183 required identities, 23 baseline cases, runtime targets and
producer changes are unchanged. Original v11 and its older evidence are
retained. Independent source review and full-source patch roundtrips passed.
No v12 Cargo, Clippy, Rust runtime or performance result exists. The prior
retained-backing timeout, other client failures, live Kafka compatibility
and release qualification remain open.
