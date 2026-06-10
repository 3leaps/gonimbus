# Provider Dispatch Architecture

Gonimbus builds transfer providers from parsed URI schemes, then runs copy and
metadata behavior through the shared `provider.Provider` interface and optional
capability interfaces.

This document is transitional for GON-042 PR-1: the shared construction seam is
now `internal/providerdispatch`, and `transfer reflow` is the first migrated
caller. Full CLI-wide command adoption, provider registration checklist updates,
and broader doctor/hub/read-command migration remain tracked by the GON-042
follow-up slices.

## Reflow Dispatch

`transfer reflow` parses the source URI with `pkg/uri.ParseURI` and the
destination URI with the reflow destination parser, then delegates provider
construction to `internal/providerdispatch`. Source construction is scheme-based:

- `s3://bucket/key` constructs `pkg/provider/s3` with the source bucket and
  source AWS flags.
- `file:///absolute/root/` constructs `pkg/provider/file` rooted at the local
  source directory.

Destination construction follows the same provider split: S3 destinations use
the S3 provider, and file destinations use the file provider rooted at the
destination directory. Once providers are constructed, copy behavior is routed
through `provider.Provider`, `ObjectGetter`, `ObjectPutter`,
`ConditionalPutter`, and `MetadataAwarePutter`.

## Source Shape

S3 sources use bucket plus key. File sources use an absolute source root plus a
source-relative key. The public URI model represents file URIs as:

- `Provider="file"`
- `Bucket="local"`
- `Key="/absolute/path"`

Per-object file-source audit output intentionally emits
`file://local/<relative-path>` as an output-only identifier. Internal provider
dispatch and checkpoint identity keep the absolute source root separate from
that redacted field.

## Adding Another Provider

For a future S3-shaped provider such as `gcs://bucket/key`, expected touch
points are:

1. Register the accepted scheme and URI semantics in `pkg/uri`.
2. Add `pkg/provider/gcs` implementing `provider.Provider` and needed optional
   capability interfaces.
3. Register the scheme in `internal/providerdispatch` source/destination
   construction.
4. Add provider-specific auth flags only if the default provider config cannot
   cover the use case.

The transfer engine should not need copy-loop changes for providers that map
cleanly to bucket/key objects plus the existing capability interfaces.

## Known Limits

File-source traversal has filesystem-specific policy that object stores do not:
symlink handling, default hidden-path skipping, and local path disclosure
controls.
`--symlinks=preserve`, hardlink preservation, xattrs, and TOCTOU-resistant
opens are intentionally outside the current provider interface and need their
own capability design if they become required.

File-backed crawl selection defaults to a fail-closed filesystem posture:
selected keys are always relative to the configured local root, symlinks are not
listed by default, and non-regular files are skipped. Key relativity prevents
absolute-path leakage in ordinary object records, but it is not the same as
content-origin confinement; traversal policy must also ensure selected bytes
come from inside the configured root.

The crawl `--emit reflow-input` mode is a pipe-internal adapter for
`transfer reflow --stdin`. Its `source_uri` field carries the exact local
`file:///...` path needed for the transfer process to open the selected file.
Destination-written artifacts such as provenance sidecars and normal reflow
records use the redacted `file://local/<relative-path>` form instead.

File-backed selection is faithful to the manifest allow-list. It does not treat
ignored files, dotfiles, or credential-like filenames as special security
controls. If those files match the manifest selection, their contents are in
scope for transfer. Operators must express exclusions explicitly; gitignore
support is a separate future policy layer, not a confidentiality boundary.
