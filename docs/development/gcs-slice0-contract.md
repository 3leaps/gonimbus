# GCS Slice 0 Contract Notes

GON-004 Slice 0 pins the provider contract before the implementation slices.
This note is intentionally generic and contains no bucket names, account ids, or
credential paths.

## Dependency Intake

Initial SDK pins:

| Module                              | Version    | Proxy publish time     |
| ----------------------------------- | ---------- | ---------------------- |
| `cloud.google.com/go/storage`       | `v1.62.3`  | `2026-06-04T04:52:32Z` |
| `google.golang.org/api`             | `v0.285.0` | `2026-06-16T17:20:33Z` |
| `golang.org/x/oauth2`               | `v0.36.0`  | `2026-02-11T19:14:10Z` |
| `github.com/fsouza/fake-gcs-server` | `v1.54.0`  | `2026-02-14T05:17:49Z` |

The intake also moves `go` to `1.25.8` because `google.golang.org/api v0.285.0`
declares that minimum. `google.golang.org/api v0.286.0` was intentionally not
selected because its proxy publish time (`2026-06-22T16:57:59Z`) was still
inside the seven-day cooling window at review time, and Slice 0 has no critical
fix requiring a dated override. Changed transitive `x/*` modules observed
during the pin:

| Module                          | Version   | Proxy publish time     |
| ------------------------------- | --------- | ---------------------- |
| `golang.org/x/net`              | `v0.56.0` | `2026-06-09T16:58:42Z` |
| `golang.org/x/crypto`           | `v0.53.0` | `2026-06-08T15:52:49Z` |
| `golang.org/x/text`             | `v0.38.0` | `2026-06-08T15:10:20Z` |
| `cloud.google.com/go/pubsub/v2` | `v2.4.0`  | `2026-02-04T19:21:45Z` |
| `github.com/klauspost/cpuid/v2` | `v2.2.11` | `2025-06-25T09:40:59Z` |

Secrev should run the fresh-cache SBOM/vulnerability lane before merge. The
cooling check source of truth is the Go module proxy `Time` field per changed
module, not a local cache scan.

## URI Identity

The public canonical GCS object URI scheme is `gs://`. The internal provider id
is `gcs`. `pkg/uri.ObjectURI.String` must render `Provider: "gcs"` as `gs://`.
The `gcs://` spelling is rejected with a hint instead of accepted as an alias,
so identity-emitting surfaces do not need to carry dual spelling.

## Credentials

`pkg/provider/gcs.Config` intentionally accepts only:

- `Anonymous` unauthenticated public reads
- a typed `oauth2.TokenSource`
- ambient Google Application Default Credentials from the operator environment

There is no config field for credentials JSON, credentials filepath,
`GOOGLE_APPLICATION_CREDENTIALS`, endpoint, emulator host, or TLS verification.
This avoids turning manifest or CLI config into a GCP external-account
`credential_source.executable` control point.

## Conditional Writes

GCS can atomically create-if-absent with `DoesNotExist`, but it cannot honor
S3-style `IfMatchETag` atomically. Providers must return
`provider.ErrUnsupportedPrecondition` for a validated predicate they cannot
honor. This is distinct from `provider.ErrPreconditionFailed`, which means a
supported predicate evaluated false.

## Writer Memory And Retry Contract

`cloud.google.com/go/storage v1.62.3` uses `storage.Writer` for JSON/HTTP
uploads. The SDK sets `Writer.ChunkSize` to `googleapi.DefaultUploadChunkSize`
and documents the default as 16 MiB. Each active writer allocates a buffer of
size `ChunkSize`; the buffer enables retry/resume for failed upload requests.
Setting `ChunkSize` to zero disables chunking and reduces memory, but also
disables retry/resume for that writer.

GCS write slices must account for this in the GON-048 resource model:

```text
active_copy_memory >= effective_ceiling * writer_chunk_size
```

`gcs.Config.WriterChunkSizeBytes` is the contract hook for transfer/reflow to
tune this from `effective_ceiling` before enabling high-concurrency GCS
destinations.

## fake-gcs-server Fidelity

Slice 0 pins `github.com/fsouza/fake-gcs-server v1.54.0` and exercises it
through the official storage client in
`pkg/provider/gcs/fake_server_contract_test.go`. The confirmed behaviors are:

- `DoesNotExist` accepts the first write and returns `googleapi.Error{Code:412}`
  on the second create for the same object
- object metadata, content type, storage class, and generation round-trip through
  the Go SDK
- range reads return the expected byte subset
- pagination drives a continuation token through the SDK pager

The fake server also has upstream coverage for `GenerationMatch`, but GON-004's
first release deliberately does not expose `IfMatchGeneration`. Real GCS Slice 4
must still pin server behavior for the final provider error mapping and any
emulator gaps found during implementation.
