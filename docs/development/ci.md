# CI/CD Configuration

This document explains the CI/CD setup for this repository.

## Container-Based CI Pattern

This repository uses the **goneat-tools-runner container** for CI jobs. Specifically the **glibc variant** at the current fulmen-toolbox release:

```
ghcr.io/fulmenhq/goneat-tools-runner-glibc:v0.4.2
```

See [Image variant](#image-variant-musl-default-vs--glibc) below for why gonimbus uses `-glibc` rather than the default musl image.

### Why Containers?

The container provides all foundation tools pre-installed:

- `prettier` - Markdown/JSON formatting
- `yamlfmt` - YAML formatting
- `jq` / `yq` - JSON/YAML processing
- `rg` (ripgrep) - Fast search
- `curl` / `wget` - HTTP tools

This eliminates tool installation friction in CI - no package manager setup, no version conflicts, no install failures.

### Image variant: musl default vs `-glibc`

`fulmenhq/fulmen-toolbox` publishes two flavors of the runner image:

- `ghcr.io/fulmenhq/goneat-tools-runner` — Alpine/musl base. Smaller image, the default choice for pure-Go projects with no cgo or only musl-compatible cgo.
- `ghcr.io/fulmenhq/goneat-tools-runner-glibc` — Debian/glibc base. Required when cgo links against glibc-compiled static libraries that cannot resolve under musl.

**gonimbus uses `-glibc`** because the cgo path pulls in `github.com/tursodatabase/go-libsql/lib/linux_amd64/libsql_experimental.a`, which is a glibc-compiled `.a`. Under musl it fails to link with "undefined reference to `lseek64` / `open64` / `pread64` / `mmap64` / `sendfile64` / `gnu_get_libc_version` / `__res_init`" and similar glibc-specific symbols (musl is natively 64-bit-clean and does not emit the `64`-suffixed LFS symbols).

Once the pure-Go `modernc.org/sqlite` default lands and libsql becomes opt-in
via `-tags gonimbus_libsql`, the default CI build will be CGO-free and the
standard musl image will work for the default lane. The `-glibc` variant remains
the right choice for any release-lane or CI run that explicitly enables the
libsql build tag.

### Container Permissions (`--user 1001`)

This template uses `options: --user 1001` for `goneat-tools-runner` container jobs.

```yaml
container:
  image: ghcr.io/fulmenhq/goneat-tools-runner-glibc:v0.4.2
  options: --user 1001
```

#### Why 1001?

GitHub Actions mounts the workspace and temp directories into the container under `/__w`. Using UID 1001 aligns with GitHub-hosted runner workspace ownership and avoids `EACCES` errors when actions write state files (e.g. checkout).

If your org uses self-hosted runners with different ownership, adjust the UID accordingly.

### Workspace-relative `GOPATH` for non-root runner

Runner images from `v0.3+` no longer have `/opt/gopath/bin` writable by UID 1001. `actions/setup-go@v5` calls `mkdir` on `$GOPATH/bin` during cache setup and fails with `EACCES: permission denied` unless `GOPATH` points to a writable location.

The workaround is small but mandatory for every job that runs Go:

```yaml
jobs:
  build-test:
    container:
      image: ghcr.io/fulmenhq/goneat-tools-runner-glibc:v0.4.2
      options: --user 1001
    env:
      GOPATH: ${{ github.workspace }}/../_go
    steps:
      - uses: actions/checkout@v4
      - name: Prepare Go directories
        run: mkdir -p "$GOPATH/bin" "$GOPATH/pkg"
      - uses: actions/setup-go@v5
        with:
          go-version: "1.26.3"
```

The same workspace-relative-`GOPATH` + `Prepare Go directories` pattern is used
across sibling projects that run as non-root users in containerized CI jobs.

Jobs that do not invoke `actions/setup-go` (e.g. our `format-check` job, which only uses foundation tools) do not need this workaround.

### Pin Go to an exact patch release

CI and release workflows pin `actions/setup-go` to `1.26.3` rather than the
floating `1.26.x` selector. Vulnerability scans report standard-library CVEs
against the Go toolchain used to build or scan the repo, so release builds and
SBOM/vulnerability reports need a fixed patched toolchain for attributable
results. Local scans should use Go `1.26.3` or newer on the 1.26 lane.

### Bash shell required for `run:` steps

The `-glibc` image does not expose `bash` as GitHub Actions' default container shell. GHA falls back to `sh`, which does not support `set -o pipefail`. The first `run:` step that uses bash-only syntax (we use `set -euo pipefail` widely) fails with:

```
/__w/_temp/<...>.sh: 1: set: Illegal option -o pipefail
```

Two equivalent fixes:

1. **Job-level default** (concise, what `ci.yml` uses):

   ```yaml
   jobs:
     format-check:
       container: { image: ..., options: --user 1001 }
       defaults:
         run:
           shell: bash
   ```

2. **Per-step `shell: bash`** (what `release.yml` uses):

   ```yaml
   - name: Lint
     shell: bash
     run: |
       set -euo pipefail
       make lint
   ```

Pick one per workflow file; both are correct. The job-level default is preferred for new jobs.

### `CGO_ENABLED=0` for release cross-compiles

The `-glibc` image sets `CGO_ENABLED=1` image-wide. That image-wide pin cascades into the cross-compile targets in `make release-build` — a step like `GOOS=darwin GOARCH=amd64 go build` inherits `CGO_ENABLED=1` and asks for `clang` (cgo's default C compiler for the darwin target). The image does not ship clang, and the non-root runner user cannot `apt-get install` it. The release workflow fails with:

```
cgo: C compiler "clang" not found: exec: "clang": executable file not found in $PATH
make: *** [Makefile:199: release-build] Error 1
```

Pin `CGO_ENABLED=0` at the release job's `env:` block so all release cross-compiles default to the pure-Go path:

```yaml
jobs:
  release:
    container:
      image: ghcr.io/fulmenhq/goneat-tools-runner-glibc:v0.4.2
      options: --user 1001
    env:
      GOPATH: ${{ github.workspace }}/../_go
      CGO_ENABLED: "0"
```

Released binaries use `modernc.org/sqlite` for the index store, which is the
same shape v0.1.x released as. Consumers who need libsql should build from
source with `CGO_ENABLED=1`; after libsql becomes opt-in, also use
`-tags gonimbus_libsql`.

The `build-test` job is the opposite case — it intentionally runs with cgo enabled to exercise libsql against the test suite, which is why the `-glibc` image is required there. The `cloud-integration` job inherits the same image and the same image-wide `CGO_ENABLED=1` default, and that is correct for its scope.

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

`make prepush` runs `check-all`, so local pushes catch CI-sensitive test failures.

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
     ghcr.io/fulmenhq/goneat-tools-runner-glibc:v0.4.2 yamlfmt -lint .
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
