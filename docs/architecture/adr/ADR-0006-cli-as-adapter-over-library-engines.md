# ADR-0006: CLI as Adapter Over Library Engines

**Status:** Accepted
**Date:** 2026-06-18
**Decision Makers:** @3leapsdave

## Context

Gonimbus is both a command-line tool and a Go library. The current supported
library surface is useful for object URI parsing, matching, provider
construction, and provider-backed content helpers, but some higher-level
workflows are still implemented primarily inside CLI command code.

That shape creates a recurring product and architecture risk:

- embedding applications must shell out to the CLI for workflows that should be
  available in-process;
- CLI code can accumulate provider, transfer, redaction, checkpoint, and
  concurrency policy that is not importable by library consumers;
- a later extraction can silently produce weaker library behavior than the CLI,
  especially around credential handling, provider-error redaction, resume
  classification, conditional-write fallback, and adaptive concurrency; and
- new providers can accidentally become command-only integrations instead of
  implementations of the shared provider contract.

The immediate forcing case is `transfer reflow`, whose data movement and
decision-plane semantics are useful outside terminal workflows. The broader
concern applies to every provider-general operation that can reasonably be
embedded.

## Decision

Gonimbus will use a library-first architecture for provider-general workflows:
the CLI is an adapter over embeddable engines, not the canonical implementation
of workflow behavior.

Provider-general operations that are useful outside a terminal MUST have an
embeddable engine before, or at the same time as, meaningful CLI expansion.
Command packages may bind flags, config files, terminal I/O, progress display,
and exit codes, but they must delegate the data plane and decision plane to
library-owned code.

### Behavioral Parity Rule

CLI behavior and library behavior MUST share the same engine for:

- data movement;
- provider capability checks;
- collision and conditional-write decisions;
- metadata and provenance semantics;
- resume, checkpoint, and terminal-state classification;
- error classification;
- provider-error redaction and no-leak handling;
- adaptive concurrency and resource-cap decisions; and
- typed records that back published `gonimbus.*.v1` output.

Parity is behavioral, not presentation-level. The following remain valid
CLI-only surfaces:

- cobra and viper command/config binding;
- stdout, stderr, and terminal wording;
- process exit-code mapping;
- shell stdin/stdout ergonomics;
- progress rendering; and
- default config-file discovery.

Any intentional library limitation versus the CLI MUST be documented in the
library-consumer and API-stability documentation for the affected release. A
weaker library path must be explicit; it must not be an incidental consequence
of splitting code out of `internal/cmd`.

### Reflow Engine Anchor

`transfer reflow` is the first workflow governed by this ADR. Gonimbus will
introduce an Experimental `pkg/reflow` workflow-engine surface and migrate the
CLI onto it incrementally.

The engine should expose typed configuration, typed sources and destinations,
context cancellation, injected providers, event sinks, summaries, and terminal
errors. JSONL output should be one adapter over typed events, not the only
library integration mode.

The `pkg/reflow` surface starts Experimental. Promotion to Stable requires
successful downstream embedding, parity coverage, and a deliberate API-stability
review.

### Package and Dependency Boundaries

Gonimbus keeps two public-library tiers:

- storage-free object access packages, such as URI parsing, matching, provider
  interfaces and constructors, and provider-backed content helpers; and
- workflow or storageful packages, such as reflow engines, operation
  checkpoints, and index persistence.

The storage-free surface must not inherit CLI, server, index-store, or workflow
dependencies just because a workflow engine is importable. Dependency-boundary
tests must guard this split.

`pkg/reflow` depends on `pkg/provider` contracts and capability interfaces. It
MUST NOT import a concrete provider package or provider SDK solely to support a
provider matrix entry. Provider-specific coverage should use injected
providers, fakes, or opt-in concrete constructors outside the engine. This keeps
provider work parallel-safe and prevents a workflow package from pulling a new
provider SDK into embedders that did not request it.

### Credential and Redaction Boundary

Library engines widen the surface where credentials and provider errors can be
observed. The library path must preserve the CLI no-leak posture.

Engine-owned records, warnings, summaries, checkpoint payloads, and default
event-sink payloads MUST be redacted before crossing the public callback or
storage boundary. If an API intentionally exposes a raw `error` for embedder
control flow or diagnostics, that value must be documented as unredacted and
not safe for logs.

Embedder-facing config types that can hold credential handles or credential
values must implement redacted `String()` and `GoString()` behavior before they
are treated as a supported public surface.

Provider construction convenience APIs must not accept credential file paths
from parsed object URIs or untrusted option maps. Credential material must come
from typed injected handles, explicit operator-owned config, or documented SDK
default credential chains. Resolver APIs that violate that rule are out of
scope for this architecture. This is an RCE-grade rule, not merely disclosure
hygiene: some credential-file formats can execute commands while loading
credentials, so a credential file path sourced from a URI or untrusted option
map is a remote-code-execution vector.

### Conditional-Write Probe and Fallback Parity

Conditional-write behavior is part of the workflow contract, not merely a CLI
detail. The CLI and library engine must emit equivalent typed events and summary
fields for conditional-write capability detection and fallback paths, including
honored, not-honored, and inconclusive probe results.

Fallback state such as destination IfAbsent support, probe status, fallback
activation, fallback object counts, warning records, and dry-run or read-only
non-mutation behavior must stay shared between the CLI and the engine.

### Adaptive Concurrency as a Library Contract

Adaptive concurrency and resource caps belong to the shared engine path.
Embedders should receive the same safe defaults and throttling behavior as CLI
users without manually recreating goroutine limits.

The library contract should expose requested and effective ceilings, cap
reasons, adaptive-mode settings, observed final/max active workers, throttle
backoffs, additive increases, and provider-unavailable freezes as typed config
and stats. Providers participate by mapping provider throttling and outage
conditions into shared provider error classes.

## Consequences

### Positive

- Embedding applications can call workflow engines in-process instead of
  shelling out to the CLI.
- CLI and library consumers get the same operational semantics for data
  movement, safety checks, redaction, checkpointing, and concurrency.
- Provider implementations remain aligned to `pkg/provider` capabilities rather
  than command-specific code paths.
- JSONL output remains available while typed events become the canonical engine
  surface.
- Dependency-boundary tests can keep lightweight embedders from inheriting CLI,
  server, provider-SDK, or index-store dependencies by accident.

### Negative

- Extracting workflows from CLI command code requires a staged migration and
  broad parity tests.
- Public workflow packages increase API surface area before v1.0.
- Some implementation details currently convenient in command code must become
  explicit contracts, including event-sink redaction and checkpoint-store
  serialization.
- The engine/adapter split can initially duplicate small amounts of glue while
  command behavior is preserved.

### Mitigations

1. Introduce new workflow packages as Experimental until at least one real
   embedder validates the contract and the API-stability review promotes them.
2. Build parity tests before migrating the CLI adapter fully onto the engine.
   Parity coverage must include collision behavior, metadata, provenance,
   dry-run behavior, resume/checkpoint classification, error classification,
   error redaction, conditional-write probe/fallback behavior, adaptive
   concurrency, and typed-record marshal compatibility.
3. Keep CLI-specific code in command packages and keep workflow packages free of
   cobra, viper, gofulmen command binding, terminal progress rendering, and
   process exit-code concerns.
4. Enforce storage-free and workflow/storageful dependency boundaries with the
   library-consumer dependency-boundary artifact and CI checks.
5. Document every intentional library limitation in the library-consumer,
   API-stability, and operation-specific docs before release.

## Delivery Sequence

The implementation is intentionally incremental.

1. Add this ADR, update package/dependency-boundary documentation, and define
   the initial library contract.
2. Move adaptive concurrency, typed reflow records, and provider-error redaction
   helpers into library-owned code while preserving CLI behavior.
3. Introduce an Experimental `pkg/reflow` skeleton behind parity tests.
4. Migrate the `transfer reflow` CLI data plane and decision plane onto
   `pkg/reflow`; keep command code as flag/config/output adaptation.
5. Expand provider-matrix hooks through injected providers and shared
   capabilities without importing concrete provider SDKs into `pkg/reflow`.

The foundation work may ship before the full CLI migration. A foundation
release can include the ADR, boundary docs, shared concurrency/record/redaction
extraction, and a tested `pkg/reflow` skeleton while leaving CLI behavior
unchanged. A later refactor can complete the data-plane migration, checkpoint
interface coverage, and provider-matrix expansion.

## Alternatives Considered

### 1. Keep Workflow Behavior in CLI Commands

Rejected because it forces embedders to shell out for provider-general
workflows and lets CLI-only policy drift away from importable library behavior.

### 2. Expose Command Packages as the Library API

Rejected because command packages carry terminal, flag, config, and process
concerns that are inappropriate for embedders. They also risk pulling CLI-only
dependencies into lightweight consumers.

### 3. Treat JSONL Output as the Only Library Contract

Rejected because JSONL is a good wire and CLI output format, but embedders need
typed configuration, callbacks, summaries, context cancellation, and direct
provider injection.

### 4. Promote Reflow Directly to Stable

Rejected because the workflow API has not yet been validated by enough
downstream embedding. Experimental status preserves room to adjust the surface
while retaining explicit release-note obligations for meaningful changes.

## Related

- `docs/library-consumers.md` - Supported embedded-use contract and dependency
  boundary
- `docs/api-stability.md` - Stability tiers and promotion requirements
- `docs/architecture/adr/ADR-0003-index-build-provider-capabilities.md` -
  Provider capability contracts
- `docs/architecture/adr/ADR-0004-language-neutral-content-stream-contract.md` -
  JSONL and typed stream contract direction
- `docs/architecture/adr/ADR-0005-sensitive-local-data-policy-conformance.md` -
  Public repository and sensitive local data posture
