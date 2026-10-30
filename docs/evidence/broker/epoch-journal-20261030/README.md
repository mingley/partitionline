# Broker epoch and journal changes

The adopted source matches the broker v2 candidate at `209d0b068`.
Its x86 CI run passed 720 default and 868 all-feature tests, strict Clippy,
formatting, docs, both doctest profiles and all 55 inventory controls.
All seven expected old-code failures were observed. Twelve fixed controls
and three asynchronous filesystem controls passed in each profile, followed
by three fresh-process Fetch deadline checks per profile.

The local replica model now persists accepted leader epochs in a protected
journal header and rejects unsupported or corrupted headers. Epoch publication
validates the existing image, writes a replacement, syncs the file and parent,
and handles uncertain publication explicitly. This rewrites the file and needs
another complete image on disk. It is not a replicated partition data path.

After adoption, `broker.yml` checks the merged source. The archived candidate
specification remains bound to its pre-adoption checkout and is not rerun
against a different base tree.

Journal and epoch replacements preserve private permission bits before writing
data and restore set-ID bits after writes. UID/GID and ACL preservation are
outside the tested contract. Five response-buffer paths now reserve relative
to their current length. Test synchronization observes canceled queued work;
the original deadlines and assertions remain required.

`adoption-receipt.json` records the exact eleven changed paths. Preparation
verified all 5,982 source capsule members, then reconstructed and checked both
patches against the qualified hashes. The integrated static inventory also
passes, with its implementation claim false and qualification still `not_run`.

The native receipt has 39 completed commands with stable source and runner
checks and joined child processes. OOM counters were unavailable. The full
executable artifact is GitHub artifact 13017576458, SHA-256
`4633b41fee5077591b377dc9bc990b904805d6a2fc630e988c4d4bc4daaf61f1`.
`native-reports.zip` retains the smaller command reports and logs.

Earlier Fetch EOF, joined-shutdown `QueueFull` and Raft observer parse failures
remain in the [failed-round record](../../releases/candidate-native-20261030/README.md).
These passes do not establish their cause, a 100 ms storage SLO, production
readiness or a performance rank. Replicated data, authenticated network
leadership, committed-watermark protection and transaction propagation remain
open. Broker publication remains disabled.

Source: [candidate CI](https://github.com/mingley/partitionline/actions/runs/41137428983).
