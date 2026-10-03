The ae313800 CI run exposed a ListTransactions peer bootstrap mismatch,
12 synchronous fixture-I/O lints, and a broker test format-string compile error.
The retained decoded job logs and tool results preserve those failures unchanged.

The peer now exercises the real client's ApiVersions4 to unsupported-v0 to
ApiVersions0 negotiation on the same connection. Three synchronous golden
fixture tests declare their intentional blocking filesystem operations locally.
The broker proxy fixes the JSON closing brace and replaces deprecated
fetch_update with an equivalent checked, capped compare-exchange loop.

The source packets include originals, candidates and checks. Formatting and
source controls passed; no new Rust compilation or behavioral result is claimed
by this checkpoint. The std-only harness remains proposed and unexecuted.
Next GitHub CI runs the corrected tests on stable Rust and Rust1.85.
