# Linked LZ4 decoder symbols

The second exact-build trace repeated the same 903,590 to 920,116
instruction increase on the identical 28,710-byte fixture. The
`FrameDecoder<&[u8]>::read` self-instruction counts again accounted for
the 16,526 extra instructions. [Input and source identities](README.md)

The job copied each executable from Cargo's reported benchmark output
before building the separate fixture helper. The executable paths
matched the paths in the Callgrind profiles. `nm` identified the
decoder symbol and `objdump` decoded its instructions.
[Trace and assembly artifact](https://github.com/mingley/partitionline/actions/runs/44625334847/job/135917499685)

| Main commit | Symbol bytes | Disassembled mnemonics | Symbol SHA-256 |
| --- | ---: | ---: | --- |
| `ce1de24` | 9,000 | 2,104 | `252de0849e847b30ee871b3debc1de6fbcc74d4e10ee7c3240464dd751236bc1` |
| `51a4d86` | 8,962 | 2,061 | `43bf53683bc2f06868ccdf8f8634e638b2ae32e4e36d62032ab1423407bf0d03` |

The linked symbol bytes and the mnemonic sequences differed. The first
ordinal mismatch was `nop` in the older build and `lea` in the newer one.
The comparison includes padding and link displacements. The specific
executed branch behind the instruction increase and its latency effect
remain open.

Benchmark executable SHA-256 values

- `ce1de24` `9df9e096da0cc20345057e8620c6da1b14eeeb768c6d32b4f8115a56cfeaa595`
- `51a4d86` `a503be4aaaece0ac7c7464abff044372bc6106573e6ee11513fc0495b2fb1703`
