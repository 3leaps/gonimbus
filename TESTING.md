# Testing Guide

## Overview

This document describes the test suites for gonimbus, with special focus on verifying gofulmen integration and embedded crucible dependency.

## Running Tests

### All Tests

```bash
go test -v ./...
```

### Specific Package Tests

```bash
# Gofulmen integration tests
go test -v ./internal/observability/...

# Future: Server tests
go test -v ./internal/server/...

# Future: Command tests
go test -v ./internal/cmd/...
```

### With Coverage

```bash
make test-cov
```

## Test Suites

### 1. Gofulmen Integration Tests

**Location:** `internal/observability/gofulmen_test.go`

**Purpose:** Verify that gofulmen is properly integrated with gonimbus.

**Test Cases:**

#### `TestGofulmenIntegration`

- ✅ CLI logger creation (SIMPLE profile)
- ✅ Structured logger creation (STRUCTURED profile)
- ✅ Logger with verbose/DEBUG mode
- ✅ Structured profile with correlation middleware

#### `TestEmbeddedCrucible`

- ✅ Crucible version access
- ✅ Crucible version string formatting
- ✅ Schema registry access
- ✅ Standards registry access
- ✅ Config registry access

#### `TestGofulmenCrucibleIntegration`

- ✅ Logger uses crucible schemas for validation
- ✅ Logger with crucible version in logs

**Expected Output:**

```
PASS: TestGofulmenIntegration
PASS: TestEmbeddedCrucible
PASS: TestGofulmenCrucibleIntegration
```

### 2. Future Test Suites

As implementation progresses, we'll add:

- **Server Tests** (`internal/server/server_test.go`)
  - HTTP server lifecycle
  - Route registration
  - Middleware chain

- **Handler Tests** (`internal/server/handlers/*_test.go`)
  - Health endpoint
  - Version endpoint
  - Metrics endpoint

- **Command Tests** (`internal/cmd/*_test.go`)
  - CLI command execution
  - Flag parsing
  - Configuration loading

## Test Conventions

### 1. Naming

- Test files: `*_test.go`
- Test functions: `TestFunctionName`
- Subtests: Use `t.Run("description", func(t *testing.T) { ... })`

### 2. Package Naming

- Use `package_test` for external/black-box tests
- Use `package` for internal/white-box tests
- Example: `observability_test` vs `observability`

### 3. Test Structure

```go
func TestFeature(t *testing.T) {
    t.Run("specific case", func(t *testing.T) {
        // Setup

        // Execute

        // Verify
        if got != want {
            t.Errorf("got %v, want %v", got, want)
        }
    })
}
```

### 4. Logging in Tests

- Use `t.Log()` or `t.Logf()` for informational output
- Use `t.Error()` or `t.Errorf()` for non-fatal failures
- Use `t.Fatal()` or `t.Fatalf()` for fatal failures

## Critical Tests

These tests verify core functionality that must never break:

### ✅ Gofulmen Embedded Crucible

**File:** `internal/observability/gofulmen_test.go`
**Why Critical:** Ensures the foundational dependency embedding works correctly.
**Must Pass:** Always, on every commit.

**What it verifies:**

- Crucible schemas accessible via gofulmen
- Version information correct
- Registry access working
- Logger validation against schemas

### Future Critical Tests

1. **Health Endpoint**
   - Must return 200 OK
   - Must return valid JSON
   - Must include version info

2. **Graceful Shutdown**
   - Must handle SIGTERM
   - Must finish in-flight requests
   - Must respect shutdown timeout

3. **Configuration Loading**
   - Must load from all three layers
   - Must respect precedence
   - Must handle missing config gracefully

## Test Data

### Fixtures

Location: `testdata/fixtures/`

Use for:

- Sample configuration files
- Mock request/response data
- Schema examples

### Golden Files

Location: `testdata/golden/`

Use for:

- Expected output snapshots
- Regression testing
- Output format validation

## Continuous Integration

When CI/CD is added, all tests will run on:

- Pull requests
- Main branch commits
- Release tags

**Required:** All tests must pass before merge.

## Troubleshooting

### Tests Fail After Dependency Update

1. Check `go.mod` for correct versions
2. Run `go mod tidy`
3. Clear module cache: `go clean -modcache`
4. Rebuild: `make clean && make build`

### Logger Tests Produce Unexpected Output

- Logger output goes to `stderr` by design
- Use `2>&1` to capture in shell: `go test ... 2>&1`
- Tests capture logger behavior, not output

### Import Cycle Errors

- Use `package_test` naming to break cycles
- Import only public API in tests
- Consider test-specific interfaces

## Documentation

For detailed test results and verification, see:

- `.plans/gofulmen-integration-test-results.md` - Gofulmen v0.1.7 verification
- Individual test files for inline documentation
