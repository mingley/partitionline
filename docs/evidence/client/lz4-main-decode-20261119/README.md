# LZ4 decode instructions across two main commits

On one hosted x86_64 runner, the unchanged `iai_decompress_lz4/text`
benchmark measured 903,590 instructions at
`ce1de24a9f81db66d35a76194e8c8ad1dbfa1982` and 920,116 at
`51a4d86ec8c0e51e1c6dd231487bfe27e0725a70`. Both locked builds used
stable Rust 1.100.0, LLVM 23.1.5 and Valgrind 3.26.0.
[Trace](https://github.com/mingley/partitionline/actions/runs/44623596661/job/135911426075)

Each build reproduced the benchmark's seeded 500-record LZ4 fixture
in a separate identity check. Both generated and decoded 28,710
bytes with SHA-256
`7c0ec3a611c64af5e61a1b26c51423b469380feeaeb83f15a85c1f9b497abc46`. The decoder sources and dependency locks also matched.

| File in both commits | Git blob |
| --- | --- |
| `src/protocol/records.rs` | `bf77f06a753a9341d5f7528816b9f9891a526c4f` |
| `src/protocol/buf.rs` | `1527858ec3eec4efd4de38fb422b2d6574ca9e32` |
| `Cargo.lock` | `a050a7cceeba712729faa1e33d771ba8f21a94f6` |
| `benchmarks/codec/Cargo.lock` | `008474bfe517f24234c927cef0ee85fbc2e0aff1` |
| `benchmarks/codec/benches/iai.rs` | `cd07a156747aaab37e138655f8f0cbaf86f300f1` |

Callgrind attributed all 16,526 additional self instructions to the
LZ4 frame decoder's `read` function.

| Function | `ce1de24` | `51a4d86` | Change |
| --- | ---: | ---: | ---: |
| `lz4_flex::FrameDecoder<&[u8]>::read` | 441,470 | 457,996 | +16,526 |
| `try_plain_record` | 91,796 | 91,796 | 0 |
| `decode_record_batch_wire_budgeted` | 54,724 | 54,724 | 0 |

The trace locates the additional instructions. The cause inside the
linked decoder and its effect on wall-clock decode latency remain open.
The first job retained both profiles and fixture identities, then
stopped when its symbol lookup could not locate the executable files.
[Symbol comparison follow-up](https://github.com/mingley/partitionline/actions/runs/44625334847)
