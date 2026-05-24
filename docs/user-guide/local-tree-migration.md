# Local Tree Migration

Use `transfer reflow` with a `file://` source when a local directory should be
published through the same audit, rewrite, collision, metadata, and checkpoint
machinery used for object-store sources.

```bash
gonimbus transfer reflow 'file:///absolute/source-root/' \
  --dest 's3://bucket/landing/' \
  --rewrite-from '{path}/{file}' \
  --rewrite-to '{path}/{file}' \
  --dry-run
```

Review the dry-run JSONL first, then rerun without `--dry-run`.

## Hidden-File Inventory

Local-tree reflow includes hidden files and dot-directories by default. This is
intentional for byte-faithful migration, but it can publish files that operators
did not mean to send. Review dry-run output and add excludes before the first
cloud write.

Common excludes:

```bash
--exclude '.git/*' \
--exclude '.env' \
--exclude '.env.*' \
--exclude '.DS_Store' \
--exclude '__pycache__/*' \
--exclude '*.pyc' \
--exclude '.idea/*' \
--exclude '.vscode/*' \
--exclude '*.swp'
```

Checkpoint files and run metadata for file-source runs may contain the absolute
source root. Treat those artifacts as sensitive local operational metadata.
Default per-object JSONL uses `file://local/<relative-path>` and does not expose
the absolute source root.

## Validation Pattern

For a migration replacing an existing `aws s3 sync` habit, keep the known-good
command as a fallback during rollout:

1. Run `aws s3 sync` to a temporary validation prefix.
2. Run `gonimbus transfer reflow file:///...` to a second validation prefix.
3. Compare object counts and total bytes.
4. Spot-check representative object bytes.
5. For Parquet, verify embedded key-value metadata with a reader such as
   DuckDB against both prefixes.

When the outputs match, demote `aws s3 sync` to a documented emergency fallback
rather than deleting the institutional knowledge.
