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

Note: `actions/setup-go` installs Go inside the container job, and `golangci-lint-action` installs `golangci-lint` (not currently included in the runner image).

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
