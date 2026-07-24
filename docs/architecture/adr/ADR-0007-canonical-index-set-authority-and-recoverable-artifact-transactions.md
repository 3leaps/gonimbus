# ADR-0007: Canonical Index-Set Authority and Recoverable Artifact Transactions

**Status:** Proposed
**Date:** 2026-07-13
**Decision Makers:** @3leapsdave

## Context

Gonimbus supports SQLite and durable index artifacts through both CLI adapters
and public Go packages. A canonical index set can span an identity directory,
an SQLite database and transaction sidecars, a durable segment-set root,
journals, lifecycle/checkpoint records, and maintenance intent/receipts.

Those artifacts have different path and transaction mechanics but represent one
logical set. A writer, reader, recovery operation, or garbage collector that
coordinates only one path can race another operation acting through a sibling
root. A lock stored inside a target being quarantined or deleted also stops
protecting the canonical name as soon as the target moves.

Path validation alone is insufficient. Between validation and use, another
process can replace a pathname or change canonical state. SQLite adds a related
constraint: a database opened through a different pathname can acquire a
different WAL/SHM namespace even when the pathname initially names the same
file. Crash residue under an alternate name can then be invisible to later
canonical readers, writers, and maintenance.

ADR-0006 requires CLI and library workflows to share their behavior. This ADR
defines the canonical-state authority and artifact-transaction behavior that
the shared engine must enforce.

## Decision

Gonimbus will coordinate every canonical index-set operation through one
stable, library-owned whole-set authority. Authority is a live capability, not
a completed preflight check. It remains held from discovery and trust
validation through the final side effect, artifact/database close,
publication, and terminal receipt or recoverable failure record.

### Canonical Set Scope

A canonical authority scope binds:

- the full canonical index-set ID, never an abbreviation;
- the normalized canonical segment-set root;
- the canonical identity and journal roots derived for that set; and
- the operation holder and live OS lock binding.

The authority namespace MUST be outside every set-specific target that a
maintenance transaction can rename, quarantine, or delete. Moving a target
must not make its original canonical name available to another writer while
the first operation still holds authority.

Acquiring an authority lock does not establish artifact identity and does not
adopt unknown state. The lock authorizes a scope only after the operation also
verifies the artifact identity required below.

### Library Ownership and CLI Parity

The whole-set authority primitive and canonical-state engines belong to public
or internal library packages with typed APIs. CLI commands may resolve flags,
render results, and map exit codes, but MUST NOT implement a stronger or
different coordination path.

Every public library entry point that can read state to authorize a mutation,
create or mutate canonical state, publish canonical identity, resume work, or
delete canonical artifacts MUST either:

1. acquire and retain the canonical authority for its complete operation; or
2. accept a typed authority capability and verify that it authorizes the exact
   set and root before each trust-to-side-effect boundary.

A boolean such as `validated`, a path-only token, or a helper that releases its
lease before the caller performs the side effect is not an authority
capability.

### Canonical Path Classification

Canonicality is determined from the normalized resolved target and configured
canonical roots, not from how a caller spelled an argument or whether a path
was explicit or defaulted.

An explicit path that resolves to canonical state MUST receive canonical
authority, identity, and recovery behavior. A caller-owned external path may
have a deliberately weaker contract only when the engine proves that it is
outside every canonical root and documents the limitation. Symlink aliases,
root aliases, unsafe components, and ambiguous containment fail closed.

### Exact Artifact Identity Binding

Artifacts that authorize canonical reads, mutation, publication, resume, or
deletion MUST bind the exact authority scope and their authoritative contents:

- SQLite requires authority index-set ID = authoritative marker index-set ID =
  the sole `index_sets.index_set_id` stored in the database.
- Durable publication requires the authority set/root to match the verified
  latest → complete → manifest chain, including run and digest bindings.
- Lifecycle, checkpoint, maintenance intent, pin, and receipt records bind the
  exact set/root, operation identity, schema kind/version, and relevant
  artifact digests.

Missing, corrupt, ambiguous, or mismatched proof fails closed. Gonimbus MUST
NOT reconstruct a marker from an untrusted database or manifest, write a new
marker beside unknown state to bless it, infer identity from a directory name,
or serialize unavailable values as authoritative zero values.

### Lifetime-Complete Validation

Authority and identity proof remain live across the side effect they
authorize. Engines MUST close or durably checkpoint the affected artifact
before releasing authority and before reporting terminal success.

Operations reassert authority and any mutable parent/latest/marker binding at
the last responsible boundary before publication or destructive mutation. If
a pathname or parent can change after validation, the operation must retain an
exact file/object binding or revalidate under the same authority; a prior
successful check is not sufficient.

### Discoverable Transaction and Recovery State

Canonical mutation uses one discoverable transaction namespace per artifact.
For SQLite, writers use the canonical database transaction namespace. A
hard-link, alternate basename, or temporary database MUST NOT create an
independent WAL/SHM namespace for canonical mutation.

If an implementation necessarily uses temporary, journal, quarantine, or
alternate names, every possible crash residue MUST be durably inventoried and
recoverable before any later canonical reader, writer, resume, or maintenance
operation proceeds. Recovery ownership and ordering are part of the artifact
contract.

A failed database close, checkpoint, fsync, rename, or cleanup preserves the
state needed to recover or diagnose the operation. Cleanup MUST NOT remove the
only WAL, journal, intent, temporary artifact, or receipt evidence after a
failure whose commit state is uncertain.

The canonical SQLite implementation uses a connection-specific VFS boundary,
not a process descriptor/handle census. That boundary directly binds the
driver's main-file handle to the retained canonical object. Ordinary mutation
hands its validated sidecar-absence requirement into VFS registration; an
intervening WAL, SHM, rollback, master, or statement journal is refused rather
than adopted. The VFS retains a no-follow binding to the canonical directory and
exclusively reserves every absent sidecar relative to that binding from the
retained main path plus known suffixes. WAL and main rollback-journal creates
are driven by SQLite open-type flags (the same connection-owned strategy as
SHM before delegated shared-memory mapping), not solely by opportunistic
classification of the zName string SQLite passes to xOpen. The VFS accepts
SQLite's eventual fd/handle only when it identifies that exact reserved object
and retains the reservation through SQLite close or shared-memory unmap.
Classified sidecar cleanup MUST NOT rely on check-then-unlink pathname removal
on the ordinary mutation path: it captures the live name into a
transaction-owned quarantine entry, opens and attests that capture, and destroys
reserved content only by truncating the open descriptor. Ordinary mutation and
ordinary close never pathname-unlink a replaceable quarantine name after
attestation. Mismatch restore MUST use a platform atomic no-replace rename so a
newly live epoch at the canonical name is never overwritten; when restore cannot
proceed (or the platform lacks atomic no-replace), both objects remain and the
capture stays discoverable under its quarantine name. Transaction inventory is
read-only and MUST report every quarantine-prefix name as blocking residue
without deleting or reclaiming unproven entries. Ordinary open of any later
canonical reader or writer refuses while unreclaimed residue exists — including
empty fd-truncated captures — until recovery completes. Acquiring whole-set
authority does not establish artifact identity and MUST NOT authorize
prefix-wide or emptiness-based deletion of quarantine names. No production
library API may delete quarantine-prefix names by prefix possession alone.
Directory-entry removal of a retained capture requires an explicit recovery
transaction that validates a durable receipt or exact binding for that specific
object and uses an exact-object removal primitive; where the platform lacks one,
residue remains discoverable and blocking. The VFS reasserts authority plus
exact main/sidecar identity at SQLite namespace and I/O boundaries for the
driver connection lifetime.

### Readers and Maintenance

Canonical readers whose result can race whole-set quarantine or replacement
hold authority for the lifetime of the returned reader/snapshot, including
iteration and close. Snapshot implementations bind the bytes they validated
to the bytes they read and reject transaction state that could contain newer
authoritative data until recovery completes.

Destructive maintenance acquires the same authority as writers and canonical
readers. It revalidates its immutable plan under that authority, persists
intent outside all targets, moves/deletes only the exact bound artifacts, and
recovers or finishes an interrupted transaction before allowing later access.

### Whole-Set Authority Lease Observation and Recovery

The whole-set authority lock file is itself a durable artifact. Its stale
residue MUST be observable and recoverable without ever endangering a live
holder. The lifecycle of lock-state observation and reclaim is governed by the
following pins.

- **Typed lock-state.** Observation classifies each authority lock into one of
  four distinct, explicitly reported states — held, unheld, missing, or invalid
  (indeterminate). "Unheld" is never conflated with "missing" or "invalid."
  An artifact judged against the name, artifact-type, or schema/scope gates MUST
  report the invalid state. For read-only observation that is a **successful
  classification** — invalid with no error — because saying what an artifact is
  IS the probe's job, not a failure of it. Where such a judgement does surface as
  an error — a malformed target name, or any refusal on the mutating path — the
  typed state MUST accompany that error rather than being replaced by an untyped
  result. A failure of the surrounding infrastructure — an unusable authority
  root, or an unexpected lock or unlink failure — MUST return an error WITHOUT
  claiming any artifact state, because no verdict was reached and a manufactured
  one would be indistinguishable from an observed one. The public wrapper MUST
  NOT downgrade a typed state it received from the library.
- **Read-only, byte-preserving probe.** Lock-state observation opens the existing
  lock file without creating, truncating, or rewriting it, and leaves every byte
  of its holder document identical before and after. Observation MUST NOT acquire
  authority in the mutating sense (it MUST NOT rewrite the holder document), so it
  never destroys the provenance it reports.
- **The OS lock is the sole live-holder verdict; identity is separate proof.** A
  non-blocking advisory-lock attempt is the only authority for held versus
  unheld. A holder document, job record, or process id is attribution only and
  MUST NOT manufacture an "unheld" verdict or authorize a removal. The lock
  proves that no process holds the file; it does not prove the artifact carries
  the expected schema or set identity. Attribution is additionally best-effort
  and platform-dependent: where file locks are mandatory rather than advisory the
  holder document is unreadable while the lease is held, so attribution is absent
  until the holder exits. Reporting MUST degrade to "unattributed" there and MUST
  NOT let the difference reach the verdict. Removal additionally requires exact-identity
  proof — correct document type and an exact index-set id — validated under the
  acquired lock; a corrupt, wrong-type, or scope-mismatched document is invalid
  residue that fails closed and is retained for recovery, never reaped on lock
  alone. The public library and the CLI adapter MUST share this decision.
- **Rooted same-file revalidation.** Exact-file probe and every mutation open the
  authority lock through a no-follow rooted handle bound to the named inode, and
  mutation revalidates that path-to-inode binding under the lock immediately
  before any removal. Enumeration classifies directory entries by their listed
  type and reports symlinked or non-regular lock artifacts as invalid without
  following them.
- **Unlink under the held lock.** Reclaim of a provably-unheld, exactly-identified
  lock unlinks the file while the acquiring descriptor still holds the lock, then
  releases — never unlink-after-release. Holding the lock across the unlink makes
  a successor's acquire-then-lose-its-pathname race impossible, so authority is
  never split across inodes.
- **Reap is not process-stop.** Removing an unheld lock artifact and stopping a
  live holder are separate authorities. No observation or reclaim operation
  removes, overrides, or stops a held lease as a side effect; a force flag may
  substitute only for the operator's explicit mutation opt-in, never for the lock
  or identity gate. Stopping a live holder is an explicit, separately-authorized
  operation.

### Legacy and Migration Posture

Legacy artifacts satisfying a verifiable historical identity contract remain
readable. Artifacts that cannot prove the authority required for a new
mutation remain visible as report-only/unknown and are retained by garbage
collection. They require a documented guarded migration or source rebuild.

Authority-directory creation alone is not migration evidence. Migration MUST
NOT invent lineage, metrics, lifecycle, or identity that the legacy artifact
cannot prove.

## Required Evidence

Every canonical state engine must cover, as applicable:

- direct public-library and CLI-adapter behavioral conformance;
- default and explicit paths resolving to the same canonical target;
- authority survival across target quarantine and canonical-name recreation;
- authority/marker/database/manifest identity mismatch and ambiguity;
- pathname replacement plus symlink, hard-link, and root-alias attacks between
  validation, open, mutation, close, and publication;
- process termination after authority acquisition, after validation, during
  mutation, before/after close or checkpoint, and before terminal receipt;
- discovery and recovery of every WAL, journal, temporary, alias, quarantine,
  intent, and receipt residue;
- two-process reader/writer/resume/maintenance contention and stale authority;
- a real-process authority-lease fixture that genuinely holds the lock and is
  terminated to leave a real held-to-unheld residue transition;
- a single shared invalid-artifact matrix — malformed document, wrong document
  type, exact filename/document index-set mismatch including whitespace-padded
  identity, a valid document prefix padded past the size bound, a non-canonical
  or uppercase lease name, and a directory or symlink lock artifact — driven from
  one fixture through the read-only probe, the direct library reclaim, the
  coordination wrapper, and the CLI list/reap adapters. Every row reports the
  typed invalid state, is never reclaimed, and survives unchanged — content and
  metadata for a regular file, the entry itself and its link destination for a
  non-regular one, plus any external symlink target, which proves no layer
  follows the link. Each row's artifact MUST carry exactly one defect, so that no
  row is kept green by an unrelated defect — a name-gate row, for example, must
  not also be malformed, or weakening the name gate would leave the row passing on
  the parse failure. Where several layers independently reject the same defect —
  a non-regular lock artifact is refused by the explicit artifact-type check, by
  rooted path resolution, and again by the under-lock binding check — that
  redundancy is deliberate defense in depth; such a row asserts the outcome and
  is not evidence about any single gate. The matrix runs against a canonical
  valid-unheld positive control, and a mutation that reclassifies or reaps any
  row at any layer must fail;
- a control proving reclaim re-validates identity under the acquired lock and not
  only at enumeration, mutation-verifiable in that moving the decisive read ahead
  of lock acquisition fails it;
- destructive-command-path flag hygiene proven through the real command, not a
  helper: incompatible target/health flags, mode conflicts, and a mutation opt-in
  supplied without its mutating mode are all rejected before any listing or
  reclaim; a two-root control proves an explicitly named target never mutates the
  default store; and removing the guard from the command path must fail on that
  mutation. Clean modes — list, dry run, and confirmed reclaim — still behave as
  specified;
- an anti-split proof and an adversarial reclaim-versus-acquire race showing a
  held successor's artifact is never removed; and
- Unix and native Windows locking, path, transaction, and recovery behavior,
  plus supported release cross-compilation.

## Consequences

### Positive

- CLI users and embedders receive the same canonical-state safety posture.
- Whole-set quarantine cannot accidentally reopen the canonical name to a
  concurrent writer.
- Identity is fail-closed across SQLite, durable, lifecycle, and maintenance
  artifacts.
- Crash recovery observes all transaction residue before later operations.
- Later lineage, resume, retention, and streaming writers inherit one
  coordination contract instead of adding command-specific locks.

### Negative

- Canonical reads may contend with writers and maintenance for longer than a
  path-only preflight would.
- Library APIs must make authority ownership and close lifetimes explicit.
- Legacy artifacts without sufficient identity evidence cannot be silently
  adopted and may require source rebuild.
- Native cross-platform recovery tests require more CI infrastructure than
  cross-compilation alone.

### Mitigations

- Keep authority APIs Experimental until direct embedding and native platform
  evidence validate their lifecycle.
- Return typed contention, lost-authority, scope, identity, and recovery errors
  so callers can distinguish retryable conditions from migration boundaries.
- Keep read/report-only legacy discovery available without opening untrusted
  databases or inventing metadata.
- Document authority ownership and `Close` obligations on every returned
  reader, writer, or transaction handle.

## Alternatives Considered

### 1. Coordinate Only in CLI Commands

Rejected because public library callers could mutate canonical state without
the safety policy, contrary to ADR-0006.

### 2. Store the Authority Lock Inside Each Set Target

Rejected because quarantine or rename detaches the lock from the original
canonical name while the operation is still active.

### 3. Validate Under a Lease, Release, Then Open or Mutate

Rejected because it leaves a validation-to-use race and turns authority into a
historical assertion instead of a live capability.

### 4. Mutate SQLite Through a Private Hard-Link Alias

Rejected because an alternate basename can create a separate WAL/SHM namespace
whose crash residue is invisible to canonical recovery.

### 5. Reconstruct Missing Identity Markers From Artifact Contents

Rejected because it silently promotes untrusted legacy state into canonical
authority and cannot prove the external set/root binding that was absent.

## Related

- `docs/architecture/adr/ADR-0006-cli-as-adapter-over-library-engines.md` -
  shared library-engine and CLI-adapter behavior
- `docs/architecture/adr/ADR-0003-index-build-provider-capabilities.md` -
  provider listing and error-classification contract, orthogonal to local
  canonical-state authority
- `docs/architecture/indexing.md` - indexing architecture and artifact model
- `docs/library-consumers.md` - public package boundaries and stability posture
