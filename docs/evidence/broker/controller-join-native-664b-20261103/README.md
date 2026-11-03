# Broker controller-join hosted job evidence

GitHub Actions job [125788111325](https://github.com/mingley/partitionline/actions/runs/41594361732/job/125788111325), in run 41594361732, used checkout `664ba30c5991416e026e82add3e2445dab935b1a`. It evaluated the controller-join candidate against the pinned 6fa8 source archive, plus the reviewed capacity and join patches. This is not a qualification of all current-main source.

The decoded job console directly reports that 11 static controls passed. Its final line reports `passed: false`, `error: null`, and exactly two gate errors: `broker-inventory` and `broker-inventory-negative-controls`. Each error says only `command failed`. Their displayed `test_failures` and `test_result_counts` arrays use the legacy Rust-test log patterns, so empty arrays do not establish that the Python inventory controls had no failures. The console does not expose the commands' exact exit statuses, stderr, individual test counts or cause of either inventory failure.

The following is a control-flow inference from the console's final JSON and the exact pinned wrapper and legacy runner in `source/`, not individual results printed by the console. The wrapper stops on a baseline failure, records fixed command failures as gate errors, and reaches the final two inventory gates only after the earlier checks. The `error: null` summary with no earlier gate errors therefore implies:

- The capacity baseline's two positive witnesses matched, and its negative witness failed at the required assertion site.
- In each of the default and all-feature join baselines, the successful-cancellation control passed and all four negative cases failed at their required panic sites.
- The two fixed capacity cases and all five fixed join cases passed in each profile. The earlier focused and async-filesystem controls, both all-target runs with the runner's minimum test-count guard, Fetch repeats, stale-leader check, formatting, strict Clippy, Rustdoc and both doctest gates had no recorded errors.

These inferences depend on the captured runner's behavior; they do not supply actual counts or replace the unavailable per-command reports. The join tests use synthetic actors, and these gates do not establish real controller WAL or transport correctness, general production readiness, or performance leadership.

The GitHub connector listed compact report artifact `13213351269`, size 442,148 bytes, with GitHub-reported SHA-256 `01e8ac6eb2c3b1f60c1536aabb825ba4bc1e1a3fcde9ba58515be9e2ff3e3879`. It returned a File Service reference without a local path. The current enforced network allowlist excludes that host, and the available workspace download tool is documented for Sediment-backed IDs rather than this File Service reference. The report bytes were not retrieved or independently hashed; no blocked-host bypass or unsupported download was attempted. `analysis.json` also records the separate full artifact's metadata.

`connector-job.log` is the connector's decoded text re-encoded as UTF-8, not the original compressed GitHub log. `source/` contains the exact checkout blobs whose hashes match the hosted loader and the pinned specification. `build-evidence.py` records how they were obtained using read-only Git object access. `SHA256SUMS` binds the package. No local Cargo build or native test was run.
