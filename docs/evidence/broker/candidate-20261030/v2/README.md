# Broker candidate v2

This candidate derives from the pinned v1 specification and source capsule.
It adds a four-file patch; the original capsule and qualification records stay
unchanged. No production source has adopted it.

Three replication tests now use Tokio file reads and writes. Tokio's file module
is enabled only for tests. Every original assertion remains in place; the
production dependency features and lockfiles are unchanged. Strict Clippy remains
required.

The Fetch cancellation test keeps its 100 ms handler limit, two-second socket
limits, exact record checks and single-deadline assertion. After the first EOF,
an existing metadata barrier ensures the canceled snapshot has left the actor
queue. This separates queue occupancy from a held data permit. The test prepares
its record fixture before opening the second socket, and records its phase and
joined transport counters if an I/O operation fails. These changes do not prove
that durable writes complete within 100 ms or establish the cause of the retained
EOF failure.

The driver still requires all seven baseline failures and twelve fixed controls
in both profiles. It also runs the three converted tests explicitly and launches
the same Fetch test in three fresh processes per profile. Full default and
all-feature tests, strict Clippy, formatting, documentation, doctests and all
55 inventory controls remain mandatory. One source digest is updated to bind
the edited Fetch test; API ranges and qualification flags stay unchanged.

Preparation checks cover patch reconstruction, unchanged source assets and
lockfiles, preserved assertions and deadlines, Rustfmt, fifteen driver controls
and six small Python process-tree controls. They do not compile or run the
broker. Native v2 qualification remains pending.
