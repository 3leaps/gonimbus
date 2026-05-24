# Provider Dispatch Architecture

Gonimbus builds transfer providers from parsed URI schemes, then runs copy and
metadata behavior through the shared `provider.Provider` interface and optional
capability interfaces.

## Reflow Dispatch

`transfer reflow` parses the source URI with `pkg/uri.ParseURI` and the
destination URI with the reflow destination parser. Source construction is
scheme-based:

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
3. Register the scheme in the reflow source/destination provider factory.
4. Add provider-specific auth flags only if the default provider config cannot
   cover the use case.

The transfer engine should not need copy-loop changes for providers that map
cleanly to bucket/key objects plus the existing capability interfaces.

## Known Limits

File-source traversal has filesystem-specific policy that object stores do not:
symlink handling, hidden-file warnings, and local path disclosure controls.
`--symlinks=preserve`, hardlink preservation, xattrs, and TOCTOU-resistant
opens are intentionally outside the current provider interface and need their
own capability design if they become required.
