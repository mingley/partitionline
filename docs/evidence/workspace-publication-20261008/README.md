# Workspace evidence

This archive preserves 139,045 evidence files from the workspace when the
remaining changes were published to `main` on October 8, 2026. It retains
original failures, source snapshots, SDK inputs, executables and process logs.
It adds no qualification or performance claim.

The original files total 5.34 GB. Identical contents share one archive object;
the compressed stream is split into files of at most 32 MiB. The manifest
records each original path, SHA-256, size and permissions. `SHA256SUMS` covers
the manifest and chunks.

Verify every chunk and reconstructed file from the repository root:

```sh
python3 scripts/restore-workspace-evidence.py docs/evidence/workspace-publication-20261008
```

Restore a selected directory under a separate destination:

```sh
python3 scripts/restore-workspace-evidence.py docs/evidence/workspace-publication-20261008 \
  --destination work/restored-evidence \
  --prefix docs/evidence/client/list-transactions-routing/qualification
```

Omit `--prefix` to restore all original paths. Restoration refuses existing
files and paths that escape the destination. Check `summary.json` for counts
and `validation/` for the publication checks. Earlier qualifications retain
their original source and tool versions.
