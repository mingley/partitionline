# Segment recovery cleanup ordering

A recovery can delete files before it has checked all selected segment data or all directory entries. This note records two narrowly scoped witnesses and a candidate repair. It does not claim that process exit alone corrupts a synced journal, that a corrupt selected generation can be automatically recovered, or that cleanup is transactional.

## Selected replacement corruption

The baseline is `1f371635261b8c222f597100c06f83d06c3dab18`. In [run 42462064029](https://github.com/mingley/partitionline/actions/runs/42462064029), a fresh child appended two ordinary records successfully, wrote and synchronized a separate receipt, then exited during a sealed-generation replacement immediately after manifest rename. Before deliberately damaging anything, the parent verified that old generation 0 and selected replacement generation 2 were byte-identical, and read the old generation's first record. It then flipped one byte in the selected replacement payload and synchronized that file.

A single reopen returned `Storage(Corrupt { byte_offset: 24, kind: PayloadChecksum })`, but had already removed the intact old journal and index. Both default and all-feature controls observed this result. The interrupted, deliberately corrupted and post-reopen files are retained in the run artifacts. The test-only witness source is `28679f16e06920bddee55371701cd23e0feec2f8`; its `segments.rs` SHA-256 is `94b0e69ecdb2b44fab9822986e6bb260effb12661916abc7fdb4f18ba226d1c7`.

This is a process-exit **plus deliberately injected corruption** scenario. The mutation was made after verifying the copies were identical; the result does not suggest that the process exit caused it. The old generation was an intact fallback after corruption, not the manifest-selected generation.

## Unknown entry after a staging file

In [run 42462357536](https://github.com/mingley/partitionline/actions/runs/42462357536), another fresh log acknowledged one record. The test added a recognized `manifest.tmp` and an unknown regular file, then used a test-only sort to place the staging name first. Reopen deleted the recognized staging file and only then returned `InvalidLayout` for the unknown file. Both profiles reproduced it; the unknown file was preserved. The test controls enumeration order because filesystem directory order is not guaranteed. Before/after files are retained. Test-only source `213a98ae0d488e05ee4e0bd925247ffdcc5731ac` has `segments.rs` SHA-256 `0aa11005ded294b181de97c733f4e1e8d21c51753eadf0a5e19abcb33c0a1178`.

## Candidate and controls

The isolated candidate `890ccad6a7cf01b46d64d30b1037229fdde8327c` is based on `02c443cacd39a59d3991faeaf577032020ecb071`. It scans and certifies every manifest-selected sealed segment before cleanup. Cleanup first classifies every enumerated name, then removes the recognized set; it partitions the existing names vector in place. Only after cleanup does it validate and rebuild derived indexes, so stale index temp files are removed before `create_new` and before the rebuild reserves disk. The `segments_source_sha256` inventory pin is updated.

The default and all-feature focused controls passed in [run 42462582945](https://github.com/mingley/partitionline/actions/runs/42462582945), on the same production logic before the final formatting and explicit assertion messages. They retained the verified old generation on the injected checksum failure; preserved files when a later selected segment was corrupt or missing; and preserved both staging and unknown files on rejection. The tight-budget index test used 364 bytes of existing files, a 3,731-byte stale temp and a 96-byte index under a 4,096-byte cap. Exits during index write, file sync and rename each reopened all three previously acknowledged inputs. Earlier replacement and rollover controls also passed.

On the first exact candidate `890ccad6`, [run 42462823333](https://github.com/mingley/partitionline/actions/runs/42462823333) passed both full broker profiles and the inventory plus its negative controls, but strict Clippy failed on test-only snapshot types and synchronous convenience methods. The snapshot had also checked file length after allocation. The follow-up `cbef4a5c28da9ab14834ac496cd195b7d89e3202` uses the existing bounded reader before allocation, synchronous file handles for these synchronous fixtures, and a named snapshot type; its [focused and strict-Clippy run](https://github.com/mingley/partitionline/actions/runs/42463075985) is in progress. The earlier quality failure is retained.

[Run 42462908442](https://github.com/mingley/partitionline/actions/runs/42462908442) passed negative controls in both profiles. Restoring early cleanup made both first- and later-selected-generation preservation tests fail at explicit markers. Restoring one-pass deletion made the unknown-entry preservation test fail at its explicit marker. Mutated sources and failing logs are retained. An earlier [negative-control setup](https://github.com/mingley/partitionline/actions/runs/42462886660) failed in the one-pass mutation script because of indentation; it is retained rather than counted as a control result.

A matched x86 reopen benchmark on the final source is pending.

The new ordering prevents cleanup deletion when selected-data scanning or name classification fails. It does not make a sequence of unlinks atomic: a later unlink or directory-sync I/O error can still leave partial cleanup. The active journal's existing recovery can truncate a valid incomplete tail before sealed validation, so a failed open is not promised to be wholly read-only. External processes can race the process-local ownership guards; cross-process locking is not provided. Physical power-loss behavior is not established by these process-exit tests.
