# ADR-0001: Embedded Assets Over Directory Walking

**Status:** Accepted
**Date:** 2024-12-26
**Decision Makers:** @3leapsdave

## Context

Gonimbus produces standalone binaries (CLI) and can be consumed as a library by other Go projects. Both use cases require access to configuration assets such as:

- Application identity (`app.yaml`)
- JSON schemas for manifest validation (`job-manifest.schema.json`)

The naive approach of walking up the directory tree to find project root markers (`go.mod`, `.git`) and then locating assets relative to that root **fails in production scenarios**:

1. **Installed CLI binaries** - Users run `gonimbus` from arbitrary directories (e.g., `/home/user/data/`). There is no `go.mod` or `.git` to find.

2. **Library consumers** - When another project imports `github.com/3leaps/gonimbus/pkg/manifest`, the working directory is the consumer's project, not the gonimbus repository.

3. **CI/CD environments** - While workarounds exist (workspace boundary hints), they add complexity and fragility.

This issue was discovered during Phase 5 (job manifest schema) implementation where schema validation depended on `pathfinder.FindRepositoryRoot()` to locate schema files.

## Decision

**Use Go's `//go:embed` directive to compile assets into the binary.**

Assets that must be available at runtime are embedded at compile time, ensuring they are always accessible regardless of working directory or installation location.

### Canonical Implementation Pattern

**1. Create an assets package under `internal/assets/<category>/`:**

```go
// internal/assets/schemas/embedded.go
package schemasassets

import _ "embed"

//go:embed job-manifest.schema.json
var JobManifestSchema []byte
```

**2. Place the asset file alongside the embed directive:**

```
internal/assets/schemas/
  embedded.go              # Contains //go:embed directive
  job-manifest.schema.json # The actual asset file
```

**3. Import and use the embedded asset:**

```go
// pkg/manifest/validate.go
import schemasassets "github.com/3leaps/gonimbus/internal/assets/schemas"

func getValidator() (*schema.Validator, error) {
    return schema.NewValidator(schemasassets.JobManifestSchema)
}
```

### Reference Implementations

| Asset                | Location                       | Consumer                   |
| -------------------- | ------------------------------ | -------------------------- |
| Application identity | `internal/assets/appidentity/` | `internal/appid/appid.go`  |
| Job manifest schema  | `internal/assets/schemas/`     | `pkg/manifest/validate.go` |

## Consequences

### Positive

- **Works everywhere** - Binaries are self-contained; no runtime file discovery needed
- **No I/O at runtime** - Assets are in memory, eliminating file system errors
- **Simpler code** - No complex directory walking, boundary detection, or fallback logic
- **Testable** - Tests work from any directory without special setup
- **Reproducible builds** - Assets are fixed at compile time

### Negative

- **Binary size** - Embedded assets increase binary size (typically negligible for schemas/configs)
- **Sync discipline** - When source assets change (e.g., `schemas/gonimbus/v1.0.0/job-manifest.schema.json`), the embedded copy must be updated
- **Duplication** - Assets exist in two locations (source and `internal/assets/`)

### Mitigations for Negative Consequences

1. **Sync scripts** - Use `make sync-embedded-*` targets to copy source assets to embed locations
2. **CI verification** - Add CI checks that embedded assets match source assets
3. **Clear documentation** - Document the sync requirement in `MAINTAINERS.md`

## Alternatives Considered

### 1. Directory Walking with Fallbacks

Walk up from CWD to find project root, with environment variable overrides for CI.

**Rejected because:**

- Fails for installed binaries (no project markers to find)
- Fails for library consumers (wrong project root)
- Complex fallback logic is error-prone

### 2. Environment Variable for Asset Path

Require users to set `GONIMBUS_SCHEMA_DIR` or similar.

**Rejected because:**

- Poor user experience (manual configuration required)
- Easy to misconfigure
- Doesn't solve the library consumer case

### 3. Embed at Package Level

Embed assets directly in the consuming package (e.g., `pkg/manifest/schema.json`).

**Rejected because:**

- Mixes concerns (package code + assets)
- Harder to maintain sync with canonical schema location
- Inconsistent with existing `internal/assets/` pattern

## Related

- `internal/assets/appidentity/` - Existing pattern for app identity embedding
- `internal/appid/appid.go` - Uses `appidentity.RegisterEmbeddedIdentityYAML()`
- gofulmen's `schema.NewValidator([]byte)` - Designed for embedded schema bytes
