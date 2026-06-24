# Provider Dispatch Architecture

Gonimbus builds storage providers from parsed URI schemes in one internal seam:
`internal/providerdispatch`. Command code parses URI or manifest input, maps
command flags into source/destination option structs, asks the seam for a
`provider.Provider`, and then asserts any optional capability it needs.

`pkg/uri` remains parse-only and Stable. Provider construction stays internal
because it depends on command policy, auth/profile/endpoint flags, destination
directory creation, and provider package imports.

## Shared Dispatch

The shared dispatch package owns the scheme-to-provider switch for CLI
construction:

- `s3://bucket/key` constructs `pkg/provider/s3` with the command-supplied
  region, endpoint, profile, and path-style policy.
- `file:///absolute/path` constructs `pkg/provider/file` with an absolute local
  root and file-provider policy such as metadata sidecar suffix and source
  symlink behavior.

Commands no longer construct `pkg/provider/s3` directly in production code.
Adopted callers include transfer/reflow, crawl, inspect, tree, stream get/head,
content head/probe, atlas build, inspect-pair, preflight, index build/enrich,
index hub hydrate/export output paths, doctor probes, and exact output
destinations. Commands that need more than the base `provider.Provider`
interface use `providerdispatch.RequireCapability` and fail closed with a
provider-named error rather than silently skipping work.

## Source Shape

Object-store sources use bucket plus key. File sources use an absolute provider
root plus a source-relative key. The public URI model represents file URIs as:

- `Provider="file"`
- `Bucket="local"`
- `Key="/absolute/path"`

Exact file read commands normalize `file:///root/object.txt` into provider root
`/root` plus key `object.txt`. Prefix commands normalize `file:///root/` into
provider root `/root` plus an empty listing prefix. This keeps the file provider
key contract relative even when the user enters an absolute `file://` URI.

Per-object file-source audit output intentionally emits
`file://local/<relative-path>` as an output-only identifier. Internal provider
dispatch and checkpoint identity keep the absolute source root separate from
that redacted field.

## File Read Confinement

The file provider enforces read-side filesystem policy in its shared read path,
not only in individual commands. `Head`, `GetObject`, `GetObjectVersioned`, and
`GetRange` all resolve reads through the configured provider root, reject
non-regular files, and reject symlink traversal by default. When a command
explicitly opts into follow behavior, resolved targets must remain under the
provider root and the final open uses the resolved path.

The file provider resolves its real base directory during construction when the
base exists. This lets callers use a symlinked root intentionally while keeping
per-object reads confined to the canonical tree beneath that root.

## Adding Another Provider

For a future provider such as `gs://bucket/key`, expected touch points are:

1. Register the accepted scheme and URI semantics in `pkg/uri`.
2. Add `pkg/provider/gcs` implementing `provider.Provider` and any needed
   optional capability interfaces.
3. Register the scheme once in `internal/providerdispatch` source/destination
   construction.
4. Map provider-specific auth/options in command option structs only where the
   default provider config cannot cover the use case.
5. Add per-command capability smoke tests for commands expected to work with the
   new provider.
6. Document any command-specific known limits here.

The transfer and read engines should not need copy-loop changes for providers
that map cleanly to bucket/key objects plus the existing capability interfaces.

## Known Limits

Some command contracts remain S3-shaped even though construction now routes
through the shared seam. Index build scope planning still validates an
`s3://` `base_uri`; widening index-set identity and scope semantics for another
provider is follow-up work. `transfer reflow` currently accepts the registered
S3 and file schemes explicitly; adding a new provider still requires a URI
registration and dispatch entry before that command can ingest it.

File-source traversal has filesystem-specific policy that object stores do not:
symlink handling, default hidden-path skipping, and local path disclosure
controls. `--symlinks=preserve`, hardlink preservation, xattrs, and fully
TOCTOU-resistant directory traversal are intentionally outside the current
provider interface and need their own capability design if they become required.

File-backed crawl selection defaults to a fail-closed filesystem posture:
selected keys are always relative to the configured local root, symlinks are not
listed by default, and non-regular files are skipped. Key relativity prevents
absolute-path leakage in ordinary object records, but it is not the same as
content-origin confinement; provider-level read checks ensure selected bytes
come from inside the configured root.

The crawl `--emit reflow-input` mode is a pipe-internal adapter for
`transfer reflow --stdin`. Its `source_uri` field carries the exact local
`file:///...` path needed for the transfer process to open the selected file.
For file manifests, that URI is emitted under the resolved source root so a
symlinked root such as `/tmp/...` does not make crawl accept a file that reflow
then refuses.
Destination-written artifacts such as provenance sidecars and normal reflow
records use the redacted `file://local/<relative-path>` form instead.

File-backed selection is faithful to the manifest allow-list. It does not treat
ignored files, dotfiles, or credential-like filenames as special security
controls. If those files match the manifest selection, their contents are in
scope for transfer. Operators must express exclusions explicitly; gitignore
support is a separate future policy layer, not a confidentiality boundary.
