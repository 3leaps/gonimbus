# Library Consumers

Gonimbus exposes a small Go library surface for applications that want the
same object-storage parsing, matching, and provider behavior used by the CLI.
This page is the supported embedded-use contract for that surface.

## Supported Import Surface

Recommended packages:

- `github.com/3leaps/gonimbus/pkg/uri` for cloud object URI parsing
- `github.com/3leaps/gonimbus/pkg/match` for object-key pattern matching
- `github.com/3leaps/gonimbus/pkg/provider` for provider interfaces and capability types
- `github.com/3leaps/gonimbus/pkg/provider/s3` for AWS S3 and S3-compatible storage
- `github.com/3leaps/gonimbus/pkg/provider/file` for local filesystem-backed tests and workflows

Discouraged surfaces:

- `internal/...` is private to gonimbus and cannot be imported by Go consumers
- CLI command packages and server packages are not part of the library contract
- Index-store packages are internal product substrate, not a stable embedding API

Gonimbus is pre-v1.0. The packages above are intended for library use, but
breaking changes can still happen across minor versions. Pin consumers to a
specific gonimbus release.

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

## Secret Handling

Preferred: pass handles managed by the embedding application, such as AWS
profile names or credentials supplied by the embedding process.

Acceptable: pass explicit credential values in `s3.Config`. The embedding
application remains responsible for not logging or persisting those values.
Gonimbus does not automatically zero the caller's config value after provider
construction.

Not supported: storing secrets in recipes, examples, or other persisted
gonimbus artifacts.

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
