# Cross-driver record vectors

The Rust benchmark, C librdkafka peer and Go franz-go peer generated identical
keys and values for 224 combinations of seed, record ID and payload length.
The inputs include zero, values around the 32- and 64-bit boundaries, and
payload lengths on both sides of an eight-byte block boundary. Raw outputs,
compiler and execution receipts, source hashes, extracted functions, harnesses
and gzip-compressed executables are retained here. `results.json` records zero
vector mismatches and pins each original source file, extracted fragment,
executable, input and output. The compressed executables decompress to those
binary hashes.

The Rust harness substitutes a minimal byte-container wrapper while retaining
the generator functions verbatim. The Go harness changes the copied package
declaration to `main`. The zero-length case checks the generator boundary even
though the Rust benchmark's configuration requires at least one byte. These
are offline source-vector checks; no broker, actual benchmark driver, queue,
batching, performance or production-readiness result is implied. Output logs
and commands from the three compilations and executions are included.
