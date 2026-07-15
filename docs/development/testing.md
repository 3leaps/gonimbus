# Testing Strategy

This document describes the testing approach for Gonimbus, including unit tests, cloud integration tests, and coverage philosophy.

## Test Categories

### Unit Tests

Unit tests run without external dependencies and form the primary quality gate.

```bash
make test          # Run unit tests
make test-cov      # Run with coverage report
```

Unit tests cover:

- Pattern matching (`pkg/match/`) - 98%+ coverage
- Output formatting (`pkg/output/`) - 92%+ coverage
- Manifest validation (`pkg/manifest/`) - 81%+ coverage
- Crawler pipeline logic (`pkg/crawler/`) - 86%+ coverage
- Server middleware and handlers (`internal/server/`) - 72-94% coverage
- Configuration loading (`internal/config/`) - 86%+ coverage

### Cloud Integration Tests

Cloud integration tests validate AWS SDK interactions against a local S3-compatible endpoint (moto). These tests are gated by the `cloudintegration` build tag.

```bash
make moto-start    # Start moto server (Docker)
make test-cloud    # Run cloud integration tests
make moto-stop     # Stop moto server
```

Cloud integration tests cover:

- S3 provider initialization and configuration
- List operations (pagination, prefix filtering, MaxKeys)
- Head operations (metadata retrieval, error handling)
- CLI inspect command (end-to-end validation)

**Why moto?** We use [moto](https://docs.getmoto.org/) for S3 integration testing:

- Lightweight Docker image (~200MB)
- Fully open source
- Built-in reset API for test isolation
- Sufficient for S3 operations

**Known Moto Limitations:**

Moto is a best-effort AWS emulator and does not fully implement all S3 behaviors:

| Feature                                         | Behavior                          | Mitigation                                                       |
| ----------------------------------------------- | --------------------------------- | ---------------------------------------------------------------- |
| `s3:CreateMultipartUpload` bucket policy denial | Ignored (succeeds despite policy) | Unit test with mock provider (`pkg/preflight/preflight_test.go`) |
| `s3:PutObject` bucket policy denial             | Enforced correctly                | Cloud integration test validates                                 |

See [CI Configuration](ci.md#cloud-integration-testing) for detailed test strategy and moto limitations.

### Real-cloud integration (bring-your-own bucket)

The cloud lanes above run entirely against **local fakes** — moto for S3, an
in-process `fake-gcs-server` for GCS — so they need no credentials and run for
everyone, including fork PRs. Those fakes are the only cloud lanes CI runs.

For higher-fidelity validation you can point the suite at a **real** bucket.
This is **bring-your-own**: the project does not host a shared bucket, and none
is exposed publicly. Supply your own bucket and credentials. (Developers
collaborating with 3 Leaps may be granted access to internal QA buckets
out-of-band; that access is never published here.) The real-cloud tests skip
automatically when their environment is absent — they never fail for lack of a
bucket.

> **Credential discipline.** Credentials are read **only** from your ambient
> environment — an AWS profile or the standard AWS credential chain for S3, or
> Application Default Credentials / a key-file path you control for GCS.
> gonimbus never accepts a credential path from a manifest, CLI flag, or config
> file. See the provider security notes.

#### S3-compatible (AWS, Wasabi, R2, MinIO, …)

```bash
export GONIMBUS_S3_TEST_BUCKET=<your-bucket>
export GONIMBUS_S3_TEST_ENDPOINT=<your-endpoint>   # omit for AWS
export GONIMBUS_S3_TEST_REGION=<your-region>
export GONIMBUS_S3_TEST_PROFILE=<your-profile>      # optional; or use AWS_PROFILE/default chain
export GONIMBUS_S3_TEST_PREFIX=<scratch-prefix>     # optional; default is generated

make test-cloud-real         # real-bucket lane; skips when the vars are unset
```

Real S3-compatible tests create objects only under a generated test prefix
inside your bucket and clean up that prefix at test end. They do not create or
delete the bucket.

#### S3-compatible release stress validation

The release stress lane is separate from `make test-cloud-real` because it
uploads and reflows a sparse local `index.db` larger than 5 GiB. It is intended
for pre-tag validation against a bucket you control.

```bash
export GONIMBUS_S3_TEST_BUCKET=<your-bucket>
export GONIMBUS_S3_TEST_ENDPOINT=<your-endpoint>   # omit for AWS
export GONIMBUS_S3_TEST_REGION=<your-region>
export GONIMBUS_S3_TEST_PROFILE=<your-profile>     # optional
export GONIMBUS_S3_TEST_PREFIX=<scratch-prefix>    # optional

# Optional tuning:
export GONIMBUS_S3_RELEASE_STRESS_SIZE_BYTES=5435817984
export GONIMBUS_S3_RELEASE_STRESS_PARALLEL=128
export GONIMBUS_S3_RELEASE_STRESS_MIN_BACKOFFS=0
export GONIMBUS_S3_RELEASE_STRESS_TIMEOUT=4h

make test-cloud-real-s3-release-stress
```

The stress lane builds a small valid local index database outside the repository
working tree, sparsely extends it past the S3 single-PUT wall, exports it to an
S3-compatible hub through `index export`, then reflows the exported run artifacts
with high `--parallel`. It asserts the exported and reflowed `index.db` object
sizes and removes all objects under the generated scratch prefix on exit.

#### GCS

```bash
export GONIMBUS_GCS_TEST_BUCKET=<your-bucket>
export GONIMBUS_GCS_TEST_PROJECT=<your-project>     # optional project hint
export GOOGLE_APPLICATION_CREDENTIALS=<path-to-your-sa-key.json>
#   — or — use Application Default Credentials:
#   gcloud auth application-default login

make test-cloud-real         # skips when the vars are unset
```

The real GCS lane creates objects only under a generated test prefix inside
your bucket and cleans up that prefix at test end. It does not create or delete
the bucket.

### CLI Integration Tests

CLI tests in `internal/cmd/inspect_cloudintegration_test.go` run the built binary via `exec.Command`. This approach:

- Tests actual CLI behavior (argument parsing, exit codes, output format)
- Validates the binary works end-to-end
- Catches integration issues between packages

**Note on coverage**: These tests do not contribute to `go test` coverage metrics because they execute an external binary. This is intentional - we prioritize testing real CLI behavior over inflating coverage numbers. The trade-off is acceptable because:

1. Core logic is covered by unit tests in the respective packages
2. CLI tests catch integration issues that unit tests miss
3. Direct cobra command testing would duplicate unit test coverage

## Coverage Philosophy

We track coverage as a health indicator, not a target to game. Current baseline:

| Package           | Unit Tests | With Cloud Tests |
| ----------------- | ---------- | ---------------- |
| `pkg/provider/s3` | 49%        | **97%**          |
| `pkg/match`       | 98%        | 98%              |
| `pkg/output`      | 92%        | 92%              |
| `pkg/crawler`     | 86%        | 86%              |
| `internal/cmd`    | 46%        | 46%\*            |

\*CLI coverage doesn't increase with cloud tests because `exec.Command` tests run externally.

### Coverage Gaps We Accept

1. **CLI commands via exec.Command** - Functional testing is more valuable than coverage metrics
2. **Observability setup** (`internal/observability/`) - Logger/metrics initialization is low-risk
3. **Error paths in SDK integration** - Some error conditions require real AWS to trigger

### Coverage Gaps to Address

1. **Provider error wrapping** - Could add more unit tests for `wrapError` edge cases
2. **Crawl command** - Add cloud integration tests similar to inspect (future)
3. **Doctor command SSO paths** - Validated manually; complex to automate

## CI Integration

### Unit Tests (Always Run)

```yaml
- name: Test
  run: make test
```

Unit tests run on every push and PR. No external dependencies required.

### Cloud Integration Tests (CI Service Container)

```yaml
cloud-integration:
  services:
    moto:
      image: motoserver/moto:latest
      ports:
        - 5555:5000
  steps:
    - run: go test ./... -v -tags=cloudintegration
```

Cloud tests run in CI with moto as a service container. The container is automatically pulled and started by GitHub Actions.

**Failure behavior**: If moto is unavailable, tests skip via `cloudtest.SkipIfUnavailable(t)`. This prevents CI failures from infrastructure issues but means cloud tests silently skip if the service container fails. Monitor CI logs if cloud coverage regresses.

## Local Development

### Quick Workflow

```bash
# Unit tests (no Docker needed)
make test

# Full validation including cloud tests
make moto-start
make test-cloud
make moto-stop
```

### Note: `GONIMBUS_READONLY` and tests

If you export `GONIMBUS_READONLY=1` while dogfooding, some tests that execute the CLI may fail because readonly mode intentionally refuses provider-side mutations (e.g. `write-probe` preflight and transfer execution).

Run tests with the safety latch unset:

```bash
env -u GONIMBUS_READONLY make test
# or:
env -u GONIMBUS_READONLY go test ./...
```

### Port Configuration

Moto runs on port 5555 locally (not 5000) to avoid conflicts with macOS AirTunes. The `MOTO_ENDPOINT` environment variable controls the endpoint:

- Local: `http://localhost:5555`
- CI: `http://moto:5000` (service container DNS)

## Adding New Tests

When a deterministic test vector is bound to a credential-like name and trips
gosec G101, triage and suppress finding-by-finding — see
[gosec G101 suppressions](ci.md#gosec-g101-hardcoded-credentials).
Do not pre-annotate schema fields or fixture constants that the scanner has not
flagged.

### Unit Tests

Add to `*_test.go` files alongside the code. No special tags needed.

### Cloud Integration Tests

1. Add `//go:build cloudintegration` at the top of the file
2. Use `cloudtest.SkipIfUnavailable(t)` at the start of each test
3. Use `cloudtest.CreateBucket(t, ctx)` to create isolated test buckets
4. Buckets are automatically cleaned up via `t.Cleanup()`

Example:

```go
//go:build cloudintegration

package mypackage_test

import (
    "context"
    "testing"
    "github.com/3leaps/gonimbus/test/cloudtest"
)

func TestMyFeature_CloudIntegration(t *testing.T) {
    cloudtest.SkipIfUnavailable(t)
    ctx := context.Background()

    bucket := cloudtest.CreateBucket(t, ctx)
    cloudtest.PutObjects(t, ctx, bucket, []string{"key1", "key2"})

    // Test your feature against the bucket
}
```

## References

- [CI Configuration](ci.md) - Container-based CI setup
- [test/cloudtest/](../../test/cloudtest/) - Cloud test helper package
- [moto documentation](https://docs.getmoto.org/) - AWS mock server
