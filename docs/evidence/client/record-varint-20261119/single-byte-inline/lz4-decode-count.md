# LZ4 decode instruction count

The inline single-byte varint candidate exceeded the LZ4 decode instruction
budget. The unchanged benchmark reproduced the increase in a separate
paired x86_64 run with rustc 1.100.0, LLVM 23.1.5 and Valgrind 3.26.0.
The builds shared the same codec dependency lockfile.
[Attribution run](https://github.com/mingley/partitionline/actions/runs/44618356197/job/135892998486)

A separate binary in each build generated the benchmark's seeded fixture
of 500 records with 16-byte keys and 100-byte values. Both produced and
decoded the same 28,710-byte Kafka record batch. Its SHA-256 was
`7c0ec3a611c64af5e61a1b26c51423b469380feeaeb83f15a85c1f9b497abc46`.

| Callgrind self instructions | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| Total | 903,590 | 920,084 | +16,494 |
| `lz4_flex::FrameDecoder<&[u8]>::read` | 441,470 | 457,976 | +16,506 |
| libc `__memcpy_avx_unaligned_erms` | 41,354 | 41,342 | −12 |

The frame decoder accounts for the instruction increase, with a small offset
in libc's copy routine. The main Partitionline decode and record parsing
functions showed the same counts. The candidate remains above the 1% limit.
The specific branch responsible inside the frame decoder and its timing
impact remain open. [Candidate measurements](README.md)

The first trace stopped after measuring the baseline when the runner lacked
`rg`. The retry used Python to find the Callgrind captures and completed
both builds. [First trace](https://github.com/mingley/partitionline/actions/runs/44617486464)
