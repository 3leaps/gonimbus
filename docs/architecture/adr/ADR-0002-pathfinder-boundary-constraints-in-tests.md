# ADR-0002: Pathfinder Boundary Constraints in Tests

**Status:** Accepted
**Date:** 2025-01-04
**Decision Makers:** @3leapsdave

## Context

Gonimbus uses gofulmen's `pathfinder.FindRepositoryRoot()` for safe upward directory traversal when discovering project roots. This function includes security boundaries to prevent runaway traversal:

1. **Default boundary ceiling** - `$HOME` (user's home directory)
2. **Max depth limit** - Configurable, defaults to 10 directories
3. **Filesystem root guard** - Never traverses past `/` or `C:\`

During CI testing, we discovered a subtle failure mode:

- Tests using `t.TempDir()` create directories in system temp locations (`/var/folders/...` on macOS, `/tmp` on Linux)
- These locations are **outside** `$HOME`
- `pathfinder.FindRepositoryRoot()` rejects searches starting outside its boundary ceiling
- Result: "no repository markers found within search boundaries" even when markers exist

This manifested in `TestFindProjectRootCIBoundaryEdgeCases` failing in GitHub Actions CI with the error:

```
project root not found: [REPOSITORY_NOT_FOUND] medium: no repository markers found within search boundaries
```

## Decision

**Create test temp directories inside `$HOME` when testing pathfinder-dependent code.**

For tests that verify repository root discovery behavior:

```go
// Create temp dir inside HOME to satisfy pathfinder's default boundary
homeDir, err := os.UserHomeDir()
require.NoError(t, err)

tempDir, err := os.MkdirTemp(homeDir, "gonimbus-test-*")
require.NoError(t, err)
t.Cleanup(func() { _ = os.RemoveAll(tempDir) })
```

This ensures:

- The temp directory is within pathfinder's default boundary ceiling
- Marker discovery works as expected
- Tests pass in both local and CI environments

### Why Not Use `t.TempDir()`?

Go's `t.TempDir()` uses `os.TempDir()` which returns:

- macOS: `/var/folders/...` (outside `$HOME`)
- Linux: `/tmp` (outside `$HOME`)
- Windows: `%TEMP%` (may or may not be under `%USERPROFILE%`)

None of these are guaranteed to be within the pathfinder boundary.

## Consequences

### Positive

- **Tests work in CI** - GitHub Actions runners have writable `$HOME`
- **Consistent behavior** - Same boundary semantics in tests and production
- **Explicit about constraints** - Test code documents the boundary requirement

### Negative

- **Pollutes $HOME** - Test dirs are created in user's home directory (mitigated by cleanup)
- **Non-standard pattern** - Differs from typical Go test patterns using `t.TempDir()`
- **Platform considerations** - Must verify `$HOME` is writable in all CI environments

### Mitigations

1. **Cleanup handlers** - Always use `t.Cleanup()` to remove temp directories
2. **Unique naming** - Use `gonimbus-test-*` prefix for easy identification
3. **Documentation** - This ADR explains the rationale

## Alternatives Considered

### 1. Pass Explicit Boundary to pathfinder

Override the default boundary with the temp directory itself:

```go
pathfinder.FindRepositoryRoot(cwd, markers, pathfinder.WithBoundary(tempDir))
```

**Rejected because:**

- Tests should exercise the same code paths as production
- Explicit boundaries test different behavior than default boundaries
- The bug we're testing for is specifically about fallback behavior

### 2. Skip Tests in CI When Boundaries Don't Work

```go
if os.Getenv("CI") == "true" {
    t.Skip("skipping: pathfinder boundaries incompatible with CI temp dirs")
}
```

**Rejected because:**

- Defeats the purpose of CI testing
- Masks real issues that could affect production

### 3. Copy gofulmen's pathfinder Into This Repo

Vendor the pathfinder code and modify boundary defaults.

**Rejected because:**

- Adds maintenance burden
- Diverges from upstream improvements
- The boundary default is a security feature, not a bug

## Related

- gofulmen pathfinder library: `github.com/fulmenhq/gofulmen/pathfinder`
- `pathfinder.FindRepositoryRoot()` - Core function with boundary constraints
- `pathfinder.DetectCIBoundaryHint()` - CI-aware boundary detection
- ADR-0001 - Documents why we prefer embedded assets over directory walking
