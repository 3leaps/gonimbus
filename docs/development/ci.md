# CI/CD Configuration

This document explains the CI/CD setup for this repository.

## Container-Based CI Pattern

This repository uses the **goneat-tools-runner container** (`ghcr.io/fulmenhq/goneat-tools-runner:v0.2.1`) for CI jobs. This is the recommended "low friction" approach from goneat v0.3.14+.

### Why Containers?

The container provides all foundation tools pre-installed:

- `prettier` - Markdown/JSON formatting
- `yamlfmt` - YAML formatting
- `jq` / `yq` - JSON/YAML processing
- `rg` (ripgrep) - Fast search
- `curl` / `wget` - HTTP tools

This eliminates tool installation friction in CI - no package manager setup, no version conflicts, no install failures.

### Container Permissions (`--user 1001`)

This template uses `options: --user 1001` for `goneat-tools-runner` container jobs.

```yaml
container:
  image: ghcr.io/fulmenhq/goneat-tools-runner:v0.2.1
  options: --user 1001
```

#### Why 1001?

GitHub Actions mounts the workspace and temp directories into the container under `/__w`. Using UID 1001 aligns with GitHub-hosted runner workspace ownership and avoids `EACCES` errors when actions write state files (e.g. checkout).

If your org uses self-hosted runners with different ownership, adjust the UID accordingly.

### Additional Hardening Patterns

#### Minimize `GITHUB_TOKEN` capabilities

Explicitly declare the workflow `permissions` block (for this template: `contents: read`) so the implicit token cannot mutate repository state even if a step is compromised. This keeps the example aligned with GitHub's least-privilege guidance.

#### Avoid persisting checkout credentials

Pass `persist-credentials: false` to `actions/checkout@v4`. CI jobs in this template never push, so there's no reason to store the short-lived token inside `.git/config`. Downstream users can override when they need to push tags or release artifacts.

#### Enforce strict shell options in scripts

Add `set -euo pipefail` at the top of every multi-line `run` script. This catches unset variables, stops on the first failing command, and prevents silent formatting or build failures inside the container.

### CI Jobs

1. **format-check**: Validates formatting using container tools (yamlfmt, prettier)
2. **build-test**: Builds and tests the application using container tools + goneat
3. **cloud-integration**: Runs cloud integration tests against moto (S3 emulation)

Note: `actions/setup-go` installs Go inside the container job, and `golangci-lint-action` installs `golangci-lint` (not currently included in the runner image).

### CGO Parity Tests

CI runs with `CGO_ENABLED=0`, which exercises the pure-Go SQLite driver. That driver returns timestamps as strings, so we run an additional CGO-disabled test pass locally to mirror CI behavior.

`make check-all` now runs:

```bash
make test       # CGO-enabled (default)
make test-nocgo # CGO-disabled, matches CI
```

This catches driver-specific scan and timestamp parsing differences before pushing.

### Cloud Integration Testing

#### Infrastructure

Cloud integration tests run against **moto** (`motoserver/moto:latest`), an AWS service emulator:

```yaml
services:
  moto:
    image: motoserver/moto:latest
    ports:
      - 5555:5000
```

#### Test Execution

CI runs cloud integration tests directly (no make target):

```bash
go test ./... -v -tags=cloudintegration
```

Local equivalent:

```bash
make moto-start  # Start moto on localhost:5555
make test-cloud  # Run tests with -tags=cloudintegration
make moto-stop   # Clean up
```

#### Known Moto Limitations

Moto is a best-effort AWS emulator. Some S3 behaviors are not fully implemented:

| Feature                                         | Moto Behavior                                                    | Mitigation                                                                                                 |
| ----------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `s3:CreateMultipartUpload` bucket policy denial | **Ignored** - CreateMultipartUpload succeeds despite deny policy | Unit test with stub provider (`pkg/preflight/preflight_test.go:TestWriteProbe_MultipartAbort_Denied_Unit`) |
| `s3:PutObject` bucket policy denial             | Enforced correctly                                               | Cloud integration test validates denied path                                                               |

**Impact on Test Strategy:**

- **Allowed scenarios**: Validated via cloud integration tests against moto (real SDK flow)
- **Denied scenarios**: For actions moto doesn't enforce, we use **unit tests with mock providers** to verify error handling behavior
- **Real AWS testing**: When adding real AWS integration environments, denied-path tests can be run directly without mocks

This approach ensures:

- CI runs fast and deterministically (no external dependencies)
- Critical error handling logic is tested (unit tests)
- SDK integration is validated (cloud integration tests)

#### Test File Tag Convention

Files with cloud integration tests use a build constraint:

```go
//go:build cloudintegration
```

This excludes these tests from `make test` (unit tests only). They only run when `go test -tags=cloudintegration` is explicitly invoked.

### Local Development

For local development, you have two options:

1. **Use the container** (recommended for consistency):

   ```bash
   docker run --rm -v "$(pwd)":/work -w /work --entrypoint "" \
     ghcr.io/fulmenhq/goneat-tools-runner:v0.2.1 yamlfmt -lint .
   ```

2. **Install tools locally via sfetch + goneat**:

   ```bash
   # Install the trust anchor (sfetch)
   curl -sSfL https://github.com/3leaps/sfetch/releases/latest/download/install-sfetch.sh | bash -s -- --yes --dir "$HOME/.local/bin"
   export PATH="$HOME/.local/bin:$PATH"

   # Verify sfetch install (trust anchor)
   sfetch --self-verify

   # Install goneat via sfetch
   sfetch --repo fulmenhq/goneat --tag v0.3.16 --dest-dir "$HOME/.local/bin"

   # Install foundation tools via goneat
   goneat doctor tools --scope foundation --install --install-package-managers --yes
   ```

## References

- [fulmen-toolbox (goneat-tools-runner image source)](https://github.com/fulmenhq/fulmen-toolbox)
- [goneat documentation](https://github.com/fulmenhq/goneat)
- [GitHub Actions container jobs](https://docs.github.com/en/actions/using-jobs/running-jobs-in-a-container)
