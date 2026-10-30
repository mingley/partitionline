# Reconnect qualification handoff

The final patch is ready to integrate with explicit qualification limits. It adds fresh-socket API negotiation and typed offset re-encoding across reconnects. Full production readiness and release eligibility remain open.

- Product base: `02ce806d7ea4970897d51604e71e6a1fa069ca4c`.
- Patch: `current-main-to-candidate.patch`, SHA256 `cd906c757b582c394f4a0cbf995d892cc7d8b6a3ab4aac013bf94cafb969e6ea`.
- Exactly 16 paths, listed in `candidate.json`; no README, documentation, manifest or lockfile changes. Apply with `git apply` from the main worktree. The candidate worktree has a deliberately sparse old index; do not use its unbounded Git diff.
- Product modules: `src/consumer.rs`, `src/net.rs`, `src/group.rs`, `src/group/offsets.rs`, `src/producer.rs`, `src/share.rs`. New private unit fixture: `src/group/offsets/reconnect_tests.rs`.
- Every product and unit-fixture byte remains identical to phase07. The current-main records decoder, protocol files, and capacity-to-length reserve fixes are included in the candidate input snapshot.

New sockets use their own ApiVersions ranges and authentication. Typed name-based offset requests re-encode for the new range while preserving membership, instance, epoch and stability fields, or fail unsupported before application bytes are sent. UUID-only v10 requests refuse downgrade. LOAD retries reclassify the next coordinator error and rediscover. Request work shares one deadline; cancellation retires its connection and does not guarantee that the broker received nothing. The actual negotiated, authenticated lookup socket is reused when it is the coordinator.

## Validation

Rust stable 1.99.1 / LLVM23.1.1, x86_64; one Cargo job and sequential test execution. Per-case receipts pin 1809 client files in later phases, with full7731-file snapshots before/after. The scope includes all root source/tests/common/fixtures, Cargo/lock/toolchain/lint/.cargo/README. Selected inputs have no broker-tree include/runtime dependency. External installed dependency/toolchain bytes are not in the source archive.

| Check | Result and exact scope |
| --- | --- |
| Phase07 focused library controls | 14/14 pass, including six authenticated reconnect wire cells; no product/unit fixture changed afterward |
| Phase09 authenticated lookup | 2 pass |
| Phase09 live bootstrap rediscovery | 5 pass |
| Phase09 heartbeat LOAD reuse | 2 pass |
| Phase09 bounded wire trace fixture | 2 pass |
| Phase09 legacy discovery | 17 pass, 2 existing ignored |
| Phase10 bootstrap handshake | 86 pass |
| Phase10 consumer Fetch semantics | 167 pass, **2 fail**, 1 existing ignored |
| Phase11 final role-predicate correction | Fresh executable, exact changed test passes; the broad Fetch suite was not repeated |
| Strict all-feature Clippy | Phase09 lib + all7 targets; phase10 lib + changed2; phase11 lib + changed1, all pass |
| Broader common fixture users | Phase09 all-feature `full_surface` + `mock_produce` compile checks pass |
| Formatting | All16 changed paths pass |

Fresh Cargo artifacts have `fresh:false`. Exact executed ELFs are retained separately as local `.elf.gz` artifacts with compressed/reconstructed hashes and sizes independently verified. Each runtime verifies its executable digest after execution; unit controls also check immediately before execution. Regenerable target executables were removed only after verified retention. No executable is included in the compact source/evidence bundle or promised as a repository download.

The future focused-run disk floor was1.5GiB plus8MiB log reserve, continuously sampled every250ms for builds and runtimes. Receipts contain the actual threshold and sampled minimum. There was no low-disk stop or memory-event increase. Historical root archives retain their own original guards.

## Failures and limits

Phase07 Fetch was165/4/1, phase08 was162/7/1, phase09 was160/9/1, and phase10 was167/2/1 (pass/fail/ignored). All original failures remain in immutable logs. `failure-index.json` lists15 distinct failing tests across those phases and the exact latest broad status; passing a focused case does not replace a failed broad run.

The two failures in phase10 were:

- `fetch_session_recovery_recreated_topic_reassignment_discards_previous_id_buffer`: wrong ownership/recreation count, actual1 versus expected2. The separate assignment repair is excluded.
- `group_join_dials_reset_leaders_while_fetching_offsets`: true join/Fetch/leader-dial overlap remains unqualified,759ms in phase10. The unrelated cancellation/ReadCommitted repair was neither applied nor validated.

Other earlier runs failed coordinator/heartbeat/rejoin/metadata/leave timing bounds and an idle-heartbeat reconnect timeout. Their cause is not established merely by later passing values. Full consumer timing and concurrency behavior remains unqualified.

Four test assumptions were corrected with wire evidence while preserving their active-work bounds: overlap of both actual ApiVersions intervals before discovery; separate periodic heartbeat scheduling from its450ms active reconnect work; ranges/order/one negotiation on each actual idle leader socket with400ms bound; and lookup/OffsetCommit identity on the actual commit socket with450ms bound. The last trace proves a concurrent independent heartbeat lookup, so a process-wide node1 Accept count cannot identify a duplicate commit handshake. Final phase11 selects FindCoordinator by the known OffsetCommit socket and retains the later same-socket commit check.

This is targeted reconnect evidence. It does not qualify a full default/all-feature/release matrix, external Kafka compatibility, sustained operation, release readiness, or a world-fastest claim. Remaining v10 encoding/decode work is not all cooperatively bounded; duplicate offset-response classification decoding remains a performance follow-up.

## Artifacts and reconstruction

`handoff-evidence.json` pins the final patch, before/current product hashes, manifests, phase scopes and final results. `independent-verification.json` records every checked receipt/ELF across phases07–11. `failure-index.json` is the compact per-failure history.

`client-source-snapshot.tar.gz` contains the exact final1809-file client input scope, including common fixtures and generated protocol inputs. Every member was read back and checked against `inputs-before.json`; see `source-archive.json`. Extract into an empty directory to reconstruct final client source without the mutable candidate worktree. Historical client sources can be reconstructed by reversing the final patch there, then applying the corresponding phase patch. Phase07’s original full7731-file guard also covered the excluded broker tree; reproducing that historical guard requires those unchanged source bytes from the product base.

All archives are scratch evidence until the root agent chooses a repository or release artifact destination. Do not add documentation that promises these local files at a repository URL.
