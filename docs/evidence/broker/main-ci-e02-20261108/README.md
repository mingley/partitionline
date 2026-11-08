# Broker CI Create error 56

[Broker run 42455943182](https://github.com/mingley/partitionline/actions/runs/42455943182/job/128654864950)
on main `e02c1b84a6e50e01eb12e530ed53d0f1cb7b4c93` passed its
default-feature process test. Its all-feature execution of
`three_process_metadata_quorum_fails_over_and_replays` failed when a
CreateTopics v3 response for `before-failover` returned error 56. This was
before the test intentionally stopped any node. All three processes had
started, their API-versions checks had passed, and the duplicate data-directory
lock check had passed. The differing default and all-feature results do not
identify a feature as the cause.

The test checked correlation 77, one topic result and the matching name; it
did not retain the encoded error message, contacted node, term, role, topic
UUID, internal request token, proposal position or resulting state.
Correlation 77 is reused and is not a request identity. The helper stops when
it receives 56; it can retry earlier 41 responses, whose number is not logged.
The failure log and the run's retained inventory and fuzz artifacts do not
contain the original process journals. The original operation was not replayed
during this investigation.

For this response, the code had parsed the request, completed its outer catalog
refresh and passed topic validation. Error 56 can still originate while
allocating a topic UUID, before a catalog command or token exists, or from
the catalog/quorum path before or after proposal. That path uses a fresh
internal token and can return various runtime, identity, allocation or
application errors; a replaced proposal position is one possible
post-proposal error, not an established cause. A directly reported deadline
maps to 7 and a directly reported `NotLeader` maps to 41; application
poisoning can instead cause the handler to close the connection.

CreateTopics v3 includes an error-message field, but the test parser reads
only the code. The topic-UUID allocation messages differ from the generic
catalog fallback; the unlogged message could have narrowed the stage, but
would not by itself prove a proposal's outcome. The exact cause and mutation
outcome remain unknown. The earlier
[Delete diagnostic](../quorum-delete-diagnostic-20261108/README.md) used a
separate disposable fixture and does not explain this Create failure.
