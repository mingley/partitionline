# Interrupted initial segment creation

A fresh segmented log can fail to reopen after its process exits during initial creation. Before the initial manifest is published, the active journal can already exist and be synchronized. Reopening a directory with no manifest and any file returns `InvalidLayout`, even when the only file is that new, empty journal.

This is an availability finding for an unacknowledged initialization. The witness does not show loss of an acknowledged append. An ordinary store opens its discovered segmented partitions during startup, so source inspection indicates such a partition can prevent store startup; this report does not claim an end-to-end broker restart measurement.

## Process-exit witness

The baseline is `1f371635261b8c222f597100c06f83d06c3dab18`. The isolated [test-only source](https://github.com/mingley/partitionline/commit/4d10b67d530f959f924bc2118e040b4798aeeb9a) adds phase hooks and controls without changing production behavior. Its `segments.rs` Git blob is recorded in the commit; its SHA-256 is `bf531856bdb784e71678bd5dfecd9a8b054cec7356b41f158eb81ea5e45d8c19`. The baseline source SHA-256 is `dc437576653833f172aa8363e9ad21adbb3ee39df142e6036c05560b5cd61dc5`.

[Run 42461571064](https://github.com/mingley/partitionline/actions/runs/42461571064) passed both controls in default and all-feature profiles on x86_64 Linux with rustc 1.99.1. Each phase used a fresh temporary directory and a child process that exited with code 73 before `Log::open` returned. It never attempted an append. The parent then inspected the files and attempted one reopen.

| Exit point | Observed files | Reopen |
| --- | --- | --- |
| Active journal opened and synchronized | 24-byte journal | `InvalidLayout` |
| Half of initial manifest written and synchronized by the test hook | 24-byte journal, 24-byte `manifest.tmp` | `InvalidLayout` |
| Full manifest written, before manifest file sync | 24-byte journal, 48-byte `manifest.tmp` | `InvalidLayout` |
| Manifest file synced, before rename | 24-byte journal, 48-byte `manifest.tmp` | `InvalidLayout` |
| Manifest renamed, before directory sync | 24-byte journal, 48-byte `manifest` | Empty log reopened |
| Directory synced after rename | 24-byte journal, 48-byte `manifest` | Empty log reopened |

For the four rejected cases the test compared every remaining filename and byte before and after the failed reopen. For the two successful cases it appended one new record after recovery and verified it after another reopen. The run retains the interrupted files and receipts as artifacts.

A separate fresh control opened a log, successfully appended one record, closed it, removed the manifest and verified that reopen returned `InvalidLayout` without changing the remaining bytes. A manifestless directory can therefore contain acknowledged data. It must not be reset or treated as a fresh log merely because the manifest is absent.

## Publication ordering and limits

For a new directory, the current implementation creates the directory and syncs its parent. `Journal::open` creates or opens the active journal, writes its 24-byte header when empty, synchronizes its contents and then synchronizes the segment directory. The segment owner subsequently creates `manifest.tmp`, writes and syncs the manifest, renames it to `manifest`, and synchronizes the segment directory before returning `Log::open`.

The `manifest-written` and `manifest-renamed` observations are process-exit results on the tested filesystem: page-cache contents and rename visibility survived that exit. They do not establish power-loss ordering, directory-fsync semantics on other filesystems, or a physical durability guarantee. The partial-manifest case is a deliberately written and synced prefix, not a claim that every interrupted write has that shape.

## Prevention protocol to investigate

A future creation protocol could persist a versioned initialization record before creating the active journal, and remove that record only after the journal and initial manifest are synchronized and the directory is synchronized again. `Log::open` must not return an append-capable handle before the record's removal is synchronized. A crash during recovery must leave a state that can itself be recovered or rejected without guessing.

A zero-length marker alone is not adequate provenance. An existing user file with the proposed name and zero length is indistinguishable from such a marker; automatically deleting or accepting it would change today's fail-closed behavior. A candidate needs an explicitly reserved, versioned record with strict magic, length, base and identity fields, and a checksum; it must reject malformed, partial, unexpected or symbolic-link entries without modifying them. A checksum detects accidental corruption, not a deliberately forged record. A byte-identical valid user file cannot be distinguished from the implementation's record without a stronger authority or a trusted reserved namespace, so the trust assumption must be explicit.

The record itself has a creation window. A staged record must be fully written and file-synced, atomically installed and directory-synced before the active journal can be created. Crashes before installation can leave a partial staging file; blindly deleting it would again discard an unknown user file. The design must either preserve and fail closed on that ambiguous state or supply a separately justified exclusive staging namespace and cleanup authority. Merely adding a marker does not by itself make every interrupted creation recoverable.

After an authorized record exists, recovery should validate the complete directory before changing anything: accept only the exact allowed initial manifest state and an absent, empty or verified initial-header journal; reject data-bearing journals, unknown names, invalid records and symlinks. If repairing a partial header or temporary manifest, prove from the protocol that no acknowledged append could exist and make every repair step restartable. Legacy markerless nonempty directories, including the four reproduced layouts, remain ambiguous and must continue to fail closed.

The existing `DirectoryOwnership` guard and journal ownership leases must remain held through validation, publication and cleanup, including failure paths and directory aliases. They provide process-local exclusion, not cross-process locking. Repeated exits before and after every record, file, rename, directory-sync and removal phase, plus marker collisions, hard-link/symlink and acknowledged-data negative controls, are required before a repair is proposed. No recovery implementation is included in this report.
