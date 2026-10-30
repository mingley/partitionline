# Replacement sockets and offset retries

New connections negotiate their own API ranges and authenticate before use.
Name-based offset requests are re-encoded for the destination's minimum and
maximum versions. Unsupported identity, membership or stability fields fail
before the application request is sent. UUID requests never downgrade to names.
Coordinator errors after a LOAD retry continue through rediscovery, within the
original deadline. Cancelling a write retires its socket; delivery can still
be ambiguous.

The candidate is based on `02ce806d7`. Its 14 focused unit controls pass. Six
integration targets pass, including 86 bootstrap cases. The latest broad Fetch
run reported **167 passed, two failed and one ignored**. The failures concern
topic-recreation ownership and join/reset-leader overlap. Earlier runs had
additional timing, count and timeout failures; all 15 distinct historical
failures remain in [the index](failure-index.json).

The final change to the role-specific lookup test passed a fresh focused run;
the broad Fetch suite was not repeated afterward. Strict Clippy passed for
the library and seven selected integration targets with all features. These
results do not qualify the full client matrix, all-feature runtime behavior,
external Kafka compatibility, sustained operation or a release.

[HANDOFF.md](HANDOFF.md) explains the phases and their source scopes.
[handoff-evidence.json](handoff-evidence.json) pins results, source hashes and
the final patch. `qualification-evidence.tar.gz` contains exact client sources,
patches, runners, receipts and complete logs for phases 07–11. Its 287 members
were checked before import. Executable bytes remain in local retained gzip
artifacts; they are not included in this repository bundle.

Dependency updates and test-lint cleanup applied during integration have
separate records. They require new runtime validation; the results above used
the candidate's recorded dependency graph.
