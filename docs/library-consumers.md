# Library Consumers

Gonimbus exposes a small Go library surface for applications that want the
same object-storage parsing, matching, and provider behavior used by the CLI.
This page is the supported embedded-use contract for that surface.
See [`docs/api-stability.md`](api-stability.md) for stability tiers and the
breakage-notification protocol for Stable packages.

## Supported Import Surface

Recommended packages:

- `github.com/3leaps/gonimbus/pkg/uri` for cloud object URI parsing
- `github.com/3leaps/gonimbus/pkg/match` for object-key pattern matching
- `github.com/3leaps/gonimbus/pkg/provider` for provider interfaces and capability types
- `github.com/3leaps/gonimbus/pkg/provider/s3` for AWS S3 and S3-compatible storage
- `github.com/3leaps/gonimbus/pkg/provider/file` for local filesystem-backed tests and workflows
- `github.com/3leaps/gonimbus/pkg/content` for provider-backed object head-byte reads

Discouraged surfaces:

- `internal/...` is private to gonimbus and cannot be imported by Go consumers
- CLI command packages and server packages are not part of the library contract
- Index-store packages are internal product substrate, not a stable embedding API

Experimental workflow surface:

- `github.com/3leaps/gonimbus/pkg/reflow` exposes shared reflow workflow
  substrate and the migrated stdin reflow execution subset. As of v0.3.5 it
  includes metadata planning, dry-run planning, record-stream copy execution,
  collision decisions, adaptive concurrency, typed run/summary records, and the
  provider-error redaction helpers used by CLI `transfer reflow`.
- `github.com/3leaps/gonimbus/pkg/indexbuild` is the durable index build
  workflow engine (v0.4.0 default artifact path). See
  [Durable index builds (pkg/indexbuild)](#durable-index-builds-pkgindexbuild) below.
- `github.com/3leaps/gonimbus/pkg/indexreader` is the format-aware local index
  read seam: `ResolveIndexReader` dispatches on `sqlite-v1` / `durable-v2`
  markers and exposes streaming query over durable segments. **Experimental**;
  durable-v2 remains internal-render-only.

Gonimbus is pre-v1.0. Stable packages are supported for embedded use under the
notification protocol documented in [`docs/api-stability.md`](api-stability.md);
Experimental packages may change with only an in-release note. Pin consumers to
a specific gonimbus release.

## URI Contract

`pkg/uri.ParseURI` accepts `s3://` object URIs and local `file://` URIs.
S3 URIs keep the historical shape:

- `Provider`: `s3`
- `Bucket`: bucket name
- `Key`: object key or listing prefix

File URIs are additive in v0.2.2 and use a sentinel bucket:

- `Provider`: `file`
- `Bucket`: `local`
- `Key`: absolute local filesystem path

Only local absolute file paths are accepted. `file:///absolute/path` is the
canonical form. Relative forms and remote-host forms such as
`file://host/path` are rejected. `file://local/<relative-path>` is reserved for
redacted audit output from `transfer reflow`; it is not an accepted input URI.

## Credential Injection

Use each provider package's explicit config type. For S3, construct
`s3.Config` and pass it to `s3.New(ctx, cfg)`.

S3 credential resolution is:

1. `Anonymous: true` for unsigned read requests.
2. `CredentialsProvider` for caller-managed AWS credential handles.
3. `AccessKeyID` plus `SecretAccessKey`.
4. `Profile`.
5. The AWS SDK default credential chain.

`Anonymous` is mutually exclusive with every credential source. It does not
fall back to environment variables, profiles, or instance credentials, even
when those are present in the embedding process.

```go
package main

import (
	"context"

	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

func main() {
	ctx := context.Background()

	cfg := s3.Config{
		Bucket:          "your-bucket-here",
		Region:          "us-east-1",
		AccessKeyID:     "access-key-managed-by-your-app",
		SecretAccessKey: "secret-key-managed-by-your-app",
	}

	provider, err := s3.New(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer provider.Close()
}
```

To use the AWS SDK default chain intentionally, pass a profile or leave
credentials empty and let the SDK resolve them:

```go
cfg := s3.Config{
	Bucket:  "your-bucket-here",
	Region:  "us-east-1",
	Profile: "my-aws-profile",
}
provider, err := s3.New(ctx, cfg)
```

For public buckets that allow unauthenticated reads, use `Anonymous`. This
sends unsigned `List`, `Head`, `GetObject`, `GetObjectVersioned`, and
`GetRange` requests with no `Authorization` header:

```go
cfg := s3.Config{
	Bucket:    "public-bucket",
	Region:    "us-east-1",
	Anonymous: true,
}
provider, err := s3.New(ctx, cfg)
```

Anonymous does not enable anonymous writes. S3 write methods such as
`PutObject`, `DeleteObject`, conditional PUT, and multipart upload fail closed
with `provider.ErrAnonymousReadOnly` and classify as
`provider.ErrAccessDenied`. Treat anonymous as "unsigned public read", not
"public write".

For applications that already manage AWS credential handles, inject an AWS SDK
credentials provider:

```go
cfg := s3.Config{
	Bucket:              "your-bucket-here",
	Region:              "us-east-1",
	CredentialsProvider: yourProvider,
}
provider, err := s3.New(ctx, cfg)
```

`CredentialsProvider` overrides credential resolution and skips lower-priority
credential sources. If `Profile` is also set, gonimbus does not load that
profile for credentials or profile-derived non-credential config such as
region. Pass `Region`, `Endpoint`, and `ForcePathStyle` directly when those
values are needed alongside an injected provider. Use injected credentials when
the embedding application owns credential sources beyond simple static keys or
shared AWS profiles.

Multiple providers can coexist in one process. Each provider instance carries
its own config and SDK client stack:

```go
sourceProvider, err := s3.New(ctx, s3.Config{
	Bucket:  "source-bucket",
	Profile: "source-readonly",
	Region:  "us-east-1",
})
if err != nil {
	panic(err)
}
defer sourceProvider.Close()

destProvider, err := s3.New(ctx, s3.Config{
	Bucket:          "dest-bucket",
	AccessKeyID:     "dest-access-key",
	SecretAccessKey: "dest-secret-key",
	Region:          "us-east-2",
	Endpoint:        "https://s3.us-east-2.example.com",
	ForcePathStyle:  true,
})
if err != nil {
	panic(err)
}
defer destProvider.Close()
```

## Environment Reads

Importing the supported library packages does not read `AWS_*`, `GONIMBUS_*`,
or other environment variables as a package side effect. A struct literal such
as `s3.Config{}` is also passive and reads nothing.

S3 provider construction is different. `s3.New(ctx, cfg)` calls the AWS SDK v2
default config loader. During that call, the SDK may read environment variables
and shared AWS config files to resolve credentials, region, profile, and
configured endpoints.

Explicit `AccessKeyID` and `SecretAccessKey` values suppress credential-chain
lookup for credentials only. They do not suppress ambient region, profile, or
endpoint settings. In particular, when `cfg.Endpoint` is empty, the AWS SDK can
use `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, or shared-config `endpoint_url`
to redirect S3 traffic.

Consumers that need hermetic S3 construction should pass explicit credentials
and region, then choose one endpoint posture:

- Pass a non-empty `cfg.Endpoint`, which gonimbus forwards as the S3 client
  `BaseEndpoint`.
- Or set `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` in the embedding process.
  If shared AWS config must also be suppressed, set `AWS_SDK_LOAD_CONFIG=0`
  and point `AWS_CONFIG_FILE` at an empty file or `/dev/null`.

Leaving `cfg.Endpoint` empty without those mitigations is intentionally
non-hermetic. In that posture, ambient AWS endpoint configuration is part of
the embedding application's threat model.

The supported library packages do not read `GONIMBUS_*` variables. Those belong
to the gonimbus CLI configuration layer.

Provider construction convenience APIs must not accept credential file paths
from parsed object URIs or untrusted option maps. Credential material must come
from typed injected handles, explicit operator-owned config, or documented SDK
default credential chains. Some credential-file formats can execute commands
while loading credentials, so treating URI-sourced or caller-option-sourced
credential paths as config is a code-execution risk, not just a logging risk.

## Secret Handling

Preferred: pass handles managed by the embedding application, such as AWS
profile names or credentials supplied by the embedding process.

Acceptable: pass explicit credential values in `s3.Config`. The embedding
application remains responsible for not logging or persisting those values.
Gonimbus does not automatically zero the caller's config value after provider
construction.

Not supported: storing secrets in recipes, examples, or other persisted
gonimbus artifacts.

## Reflow Redaction Boundary

`pkg/reflow` redaction helpers define the sanitized provider-error surface used
by CLI `transfer reflow` and future reflow engine callbacks. Records,
warnings, summaries, and checkpoint/error payloads should cross public callback
or storage boundaries only after provider errors have been sanitized with this
surface. Raw `error` values remain useful for control flow, but callers should
treat them as unredacted and not safe for logs unless they have been passed
through the reflow redaction helpers.

## Experimental Reflow Engine

`pkg/reflow` is a workflow-oriented library surface in Gonimbus. It is useful
when an embedding application wants the same reflow decision engine that the
CLI uses for stdin record-stream copy runs, while still owning provider
construction, orchestration, logging, and release pinning.

The package remains **Experimental**. Embedders should pin an exact Gonimbus
release and expect the API to evolve until at least one downstream embedder has
exercised the engine in production-shaped workflows and the package is promoted
through the stability process. Experimental changes are called out in release
notes; they do not carry the Stable advance-notice guarantee.

Use the CLI when you want a supported operator interface. Use `pkg/reflow` when
you are building a Go application that needs to compose the reflow data and
decision plane directly. Do not import `internal/cmd` to reuse CLI behavior;
that path is intentionally private and outside the library contract.

## Durable index builds (pkg/indexbuild)

As of v0.4.0, durable-v2 is the default CLI index artifact. `pkg/indexbuild` is
the **Experimental** embeddable engine behind that path: crawl with an injected
provider, seal journals, compact, publish immutable segments and an internal
manifest, write the complete marker, and advance the local latest pointer.

Embedding contract highlights:

- **Providers are injected.** The package does not import concrete provider
  packages, command packages, cobra/viper, or SQLite-backed `pkg/indexstore`.
  Callers construct a `pkg/provider` handle and pass it as `Config.Source`.
- **Plan input is explicit.** Optional `Config.CrawlPrefixes` is the exact
  provider-prefix observation plan (the library form of a compiled
  `build.scope`). Faithful-coverage publication expects coverage attestations
  to match that plan.
- **Paths are caller-owned; continuity is canonical.** `PathConfig` points at
  journal/segment/manifest locations resolved by the adapter. The engine
  rejects journal/segment paths under a supplied `IndexDBDir` so v2 working
  state is not silently nested under a legacy SQLite directory. Multi-run
  continuity additionally requires the canonical latest-owned layout
  (`<dir(LatestPath)>/runs/<run_id>/complete.json`, manifest/segments contained
  in the run directory): a build that records a state parent refuses — before
  any crawl or sink — a parent or target outside that layout, because
  continuity edges are pathless and the production ancestry lookup rediscovers
  parents only under the latest-owned `runs/` root. A standalone first
  publication may use any layout but cannot be extended until canonical.
  Same-run recovery must target the run's exact recorded locus.
- **Whole-set mutation authority is shared.** `pkg/indexbuild` and
  `pkg/indexenrich` acquire an OS-backed `pkg/indexcoord` lease before opening
  or creating set state. Its lock lives in the stable sibling
  `.gonimbus-set-authority`, outside the renameable segment-set root, so a GC
  quarantine cannot let a new library writer re-create the canonical root.
  The inner durable publish lock remains a second, narrower commit guard.
- **Caller-owned SQLite sinks join the same authority.** An embedder that opens
  or migrates the canonical `index.db`, including an observation sink used
  beside a durable build, must first call `indexcoord.Acquire` and pass that
  same lease in `indexbuild.Config.Authority` or
  `indexenrich.Config.Authority`. The engine validates its set/root binding and
  leaves caller-supplied leases open. Direct canonical mutations through
  `pkg/indexstore` carry the same requirement; `indexstore.Open` alone is not a
  whole-set concurrency guard.
- **Observation fanout is library-owned.** `ObservationSinks` receive the same
  crawl stream as the durable journal materializer — this is how dual-format
  CLI builds share one crawl without forking providers.
- **Progress is observational.** Optional `OnSegmentProgress` emits count-only
  segment progress; it must never be a publish failure vector.
- **Segment packing default** is 500,000 rows per segment when
  `TargetRowsPerSegment` is unset. Treat that as an engine default, not a
  Stable configuration surface.

Hub and parity consumers outside the build engine should:

1. Branch on hub markers `sqlite-v1` and `durable-v2`; **reject unknown formats**.
2. For durable artifacts, verify manifest and per-segment digests before trust
   (the CLI hydrate path does this).
3. Read `gonimbus.index.compare_result.v1` `projection_semantics` when consuming
   dual-format parity — green means LIST-projection fidelity, not reflow
   readiness or HEAD-enrichment parity.
4. Stop assuming every successful build produces `index.db`. Open SQLite only
   when the embedding path requested sqlite or dual-format materialization.
5. Use `indexreader.OpenSQLiteSnapshot` for canonical SQLite reads, supplying
   the full index-set ID and its segment-set root. The snapshot holds stable
   whole-set authority for its lifetime, rejects WAL/SHM, rollback, master, and
   statement journals before and after inspection, binds the base file without
   following links, and requires the current schema without migration. For an
   authority-held read, the database must contain exactly one index set and its
   ID must equal the authority ID. Canonical CLI resolution also requires a
   valid `identity.json`; a missing or corrupt marker fails closed rather than
   probing SQLite without authority. `ListIndexReaders` preserves such a
   canonical `index.db` as a report-only `ListedIndex` with typed
   `IdentityStatusMissing`, `IdentityStatusInvalid`, or
   `IdentityStatusMismatch`; consumers must not treat those entries as live
   readers or infer zero counts from their absent metadata. The CLI marks these
   rows with `metadata_trusted: false` and omits object/byte/run counts. Always
   check the snapshot `Close` error before beginning a later mutation. Raw
   `indexstore.OpenLocalReadOnly` is an
   immutable base-file primitive, not a concurrency contract; use it directly
   only when the caller has independently excluded writers/GC and performs the
   documented pre/post sidecar checks. Older schemas require a guarded writer
   migration.
6. Use `indexreader.OpenSQLiteWriteTarget` for canonical SQLite mutation under
   a caller-held `indexcoord.Lease`. The operation validates a valid matching
   marker and the single database index-set ID before reusing an existing
   target, atomically reserves an absent target, and retains a no-follow file
   binding while SQLite opens the canonical `index.db` name. A dedicated
   per-connection VFS attests the exact driver `sqlite3_file` OS handle against
   that retained file; it does not infer the connection from process-wide
   descriptor or handle deltas. The ordinary writer passes an explicit
   sidecar-absence expectation from validation into VFS registration; a WAL,
   SHM, rollback, master, or statement journal that appears in that handoff
   interval is refused rather than adopted. For every absent-to-present
   transition, the VFS retains a no-follow canonical-directory handle and
   exclusively creates the sidecar relative to that handle from the retained
   main path plus a known suffix: WAL and main rollback journals are selected
   from SQLite open-type flags (not opportunistic zName string matching alone),
   and SHM is reserved before delegated shared-memory mapping. The VFS requires
   the eventual SQLite fd/handle to match the reserved object and keeps the
   reservation handle through SQLite close (or SHM unmap). Classified sidecar
   cleanup captures the live name into a transaction-owned quarantine entry,
   opens and attests that capture, and truncates reserved content only through
   that open descriptor. Ordinary mutation and close do not pathname-unlink
   quarantine names after attestation. Transaction inventory is read-only and
   reports every quarantine-prefix name as blocking residue without deleting
   unproven entries. Whole-set authority alone does not authorize prefix-wide or
   emptiness-based quarantine deletion; no production library API deletes by
   prefix possession. Directory-entry removal requires an explicit receipt- or
   exact-binding-backed recovery transaction. Ordinary open of later canonical
   readers and writers refuses while unreclaimed residue exists, including empty
   fd-truncated captures. Restore of a mismatched capture uses an atomic
   no-replace rename and never overwrites a newly live canonical name.
   The VFS then reasserts authority, the main-file binding, and each opened
   sidecar binding at SQLite access, open, read, write, lock, sync, and
   shared-memory boundaries through connection close. The concrete canonical name
   is important: guarded and uncoordinated SQLite writers share one lock/WAL
   namespace instead of creating alias-named transaction state. Use the returned
   `DB`; do not reopen `Path`,
   publish `identity.json` and `manifest.json` with the target's
   `PublishIdentity` and `PublishManifest` methods, and check `Close`. Those
   publishers bind the canonical directory, reject link/non-regular
   destinations, and fsync an atomic same-directory replacement. Close never
   deletes transaction sidecars after a close/checkpoint error. A durable-only
   workflow that publishes canonical metadata but intentionally does not create
   SQLite must retain validation through both metadata boundaries with
   `indexreader.OpenSQLiteIdentityPublicationGuard`, then call
   `PublishIdentity`, `PublishManifest`, and `Close`.
   `ValidateSQLiteWriteTarget` remains the check-only compatibility helper; it
   refuses an untrusted existing database while leaving an absent target
   absent, but does not retain proof for a later write. The CLI delegates these
   boundaries to the same library operations.

An explicit SQLite path outside the configured canonical indexes root remains
caller-owned and externally quiesced. It does not receive a canonical identity
marker or whole-set authority automatically. An explicit path lexically inside
that root, or resolving into it through links, cannot downgrade to this weaker
contract: it must be the requested set's canonical target and uses the guarded
workflow above.

`pkg/indexbuild` and `pkg/indexstore` remain **Experimental**. Pin an exact
release. `pkg/reflow` is unchanged in stability tier in v0.4.0 (still
Experimental; no Stable library API break). See
[`docs/api-stability.md`](api-stability.md) and the
[v0.4.0 release notes](releases/v0.4.0.md).

## Dependency Boundary

The library-consumer dependency boundary is enforced by
`testdata/library-consumers/dep-boundary.json` and the
`internal/embeddingtest` package. Consumers importing the recommended embed
packages should not pull in CLI-only dependencies such as gofulmen, viper, chi,
libsql, or sqlite.

The current recommended surface may include the AWS SDK v2 for S3, doublestar
for matching, and standard Go packages. It must not include the CLI config,
server routing, or index-store substrate dependencies listed in the boundary
artifact.

When a dependency boundary changes intentionally, update the machine-readable
artifact and review that change alongside the code or documentation that
requires it.

### Storage-Free and Storageful Packages

The supported storage-free public package set is:

- `pkg/uri`
- `pkg/match`
- `pkg/provider`
- `pkg/provider/s3`
- `pkg/provider/file`
- `pkg/content`

These packages are expected to stay outside the index-store compile graph.
CI runs `go list -deps ./internal/embeddingtest` and fails if this surface pulls
in `pkg/indexstore`, `pkg/reflowstate`, `github.com/tursodatabase/go-libsql`,
or `modernc.org/sqlite`.

Storageful public packages are:

- `pkg/indexcoord`
- `pkg/indexstore`
- `pkg/opcheckpoint`
- `pkg/reflowstate`

Those packages intentionally use local persistence substrates and are
Experimental-tier. Import them only when your application deliberately wants
gonimbus index or operation-checkpoint persistence.

`pkg/reflow` is Experimental workflow substrate. It is not part of the
recommended lightweight storage-free surface, and it has its own dependency
boundary tests so it can remain free of CLI, provider-dispatch, and concrete
provider SDK dependencies while the embeddable engine matures.

### Libsql Build Tag

The default build uses the pure-Go `modernc.org/sqlite` driver for local index
databases, regardless of `CGO_ENABLED`. Remote libsql/Turso URLs are opt-in:
build gonimbus with `-tags gonimbus_libsql` when you need that driver.

Default builds fail remote libsql URLs with a clear rebuild diagnostic instead
of silently falling back to a local database. The libsql flavor keeps the
current remote-DB behavior, but it requires a CGO-capable build environment.
