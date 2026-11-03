# Local broker inventory validation

The pinned r1 fixed source reproduces the controller-source checksum mismatch in `before-inventory.log`; the one-field registry repair produces the passing `after-inventory.log` and `inventory-report.json`. The pre-repair control run was interrupted at 240 seconds and is incomplete. The unchanged 55 Python controls passed after repair; their full output and the 900-second monitored result are retained. Source hashes, sizes and modes were rechecked after the run.

The candidate source/workflow is bound by `candidate-manifest.json`. The 12 static controls and verified-byte workflow-loader replay also passed; these stop before qualification. This package is local source-only evidence. The hosted report artifact was unavailable, and no r2 Cargo/native test or hosted qualification ran. The scripts record the paths and commands used in this workspace.

The local Python-only monitor used a 320 MiB disk floor and observed at least 410,361,856 free bytes during the repaired controls. The hosted qualification retains its separate 1,536 MiB disk floor; no local Rust qualification was attempted below that floor.
