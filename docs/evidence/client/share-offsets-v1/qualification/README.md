# Share-offset qualification

DescribeShareGroupOffsets v1 exposes signed lag through a typed result. Every
negative value means absent;0 and positive values remain available. Existing
methods retain their result types by projecting through the same routing and
retry path. Null fields forbidden by the schema are rejected, and element
counts are checked against remaining input before allocation.

Actual Apache4.1.2/4.2.1/4.3.1 SDKs generated100 request/response pairs,
recorded20 unsupported cells, rejected45 null controls and parsed200
Rust-emitted frames. Default and all-feature frames agree. Genuine Java public
Admin calls passed72 profiles in each build. Their returned types, offsets,
leader epochs and Optional lag match the official handler. All owned processes,
listener/worker tasks and ports close.

Java omits partitions with errors and returns empty maps for missing groups.
Rust retains partition errors and rejects a missing requested group. Duplicate
request groups retain their order through FIFO matching. These are explicit
projection policies, not interchangeable public outcomes.

The final default suite passed2,004 tests; all features passed2,016. Strict
Clippy/rustdoc and four packaged feature profiles pass, with20 documentation
examples per profile. Original failures, old-source regressions, source hashes
and executable hashes remain available. No live share-state broker, mixed-node
upgrade, production or performance qualification is claimed. Source is locally
uncommitted. summary.json records exact scope and limits.
