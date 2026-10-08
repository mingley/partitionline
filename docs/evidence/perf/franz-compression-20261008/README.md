# Go peer compression

The Go benchmark peer reported `none` while leaving franz-go's default
`snappy, none` preference in place. It now applies `NoCompression()` explicitly.
The other four supported codec settings keep their requested preferences.

The unchanged driver failed the default and explicit `none` cases against the
actual franz-go 1.22.0 SDK. The corrected driver passes nine tests, including
six codec cases, with Go 1.26.0's race detector. Build, vet, module verification
and formatting checks pass. The retained executable identifies clean source
commit `f4297cb6e73bfe096a996bf1d80c00dc207efb97`.

These are SDK configuration checks with dialing disabled. No broker workload
or performance comparison ran. The actual compiled CLI still lacks ten fields
required by the matrix, and its configuration is refused. Result-adapter and
campaign qualification remain open.

`capture/` retains the original regression failures, corrected checks and
executable. It also retains an initial vet invocation from the wrong directory;
the corrected module invocation passed. `source/` contains the tested peer
and relevant SDK sources. Verify stored and reconstructed bytes with:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/perf/franz-compression-20261008
```
