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
  non-blocking OS file-lock attempt is the only authority for held versus unheld.
  Whether that lock is advisory or mandatory is a platform property that MUST NOT
  change the verdict. A holder document, job record, or process id is attribution only and
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
- **The holder cleans up after itself, through the descriptor it holds.** A
  holder that reaches any trappable outcome — success, failure, or cancellation —
  removes its own lock artifact as part of releasing it, so a completed run leaves
  no residue for recovery to collect. That removal goes through the same
  post-acquisition boundary as reclaim: it unlinks while the acquiring descriptor
  still holds the lock. The holder MUST NOT reopen or re-lock its own artifact to
  clean it up — an OS file lock belongs to an open-file description, not to a
  process, so a second open contends with the holder itself — and MUST NOT unlink
  after releasing. If the pathname no longer names the held inode, cleanup MUST
  refuse to remove and report the refusal rather than deleting whatever occupies
  the name; the lock MUST still be released on that path and on every other, so a
  refused cleanup never strands authority. The reported refusal MUST reach the
  operation's own result: a path that owns the authority it releases MUST join a
  cleanup failure with its primary outcome — never replacing it, since a failed
  operation must still say why it failed. A borrowed authority stays the lender's
  to release. Removal on this path is authorized by possession of the held
  descriptor plus the pathname-to-inode identity check, NOT by re-proving the
  holder document's schema: the holder is removing its own artifact, not
  reclaiming foreign residue. What a refused cleanup leaves behind is
  operator-visible, but its typed verdict follows the artifact rather than the
  failure: an intact original reports unheld and is reclaimable, while a
  replacement swapped in over the pathname reports invalid and is never reaped
  automatically.
- **Operation outcome and artifact provenance are separate planes.** Cleanup
  failure is an operational outcome: the operation returns an error, and job
  lifecycle state follows the operation. It MUST NOT be back-propagated into
  artifact provenance. A commit receipt attests that a snapshot was published, and
  a later cleanup failure does not unmake that commit, so the receipt stands. The
  two planes may therefore disagree — failed operation, committed artifact — and
  each MUST report its own truth.
- **Lifecycle correction is attempted, and its failure is reported.** Where a
  terminal lifecycle state was already persisted, correcting it after a cleanup
  failure is best-effort: correction MUST be attempted, and a correction that
  cannot be persisted MUST be joined into the operation's result. This contract
  does not guarantee that a persisted record never reports success after a failed
  cleanup; it guarantees the operation says so.
- **Owner cleanup is best-effort, and is never described as more.** Untrappable
  termination runs no cleanup and leaves a real artifact behind. Trappable
  interrupts MUST be translated into cancellation on operation paths that hold
  authority so the release runs, and a repeated interrupt MUST NOT be absorbed by
  that translation. Detection and reclaim remain the contract for residue cleanup
  cannot reach, and documentation MUST NOT present owner cleanup as closing it.
- **A departing owner's removal is not contention.** An acquirer may find the
  pathname binding changed under its lock because a completing owner removed it.
  Acquisition MUST re-attempt a bounded number of times, so a concurrent
  same-scope operation resolves to either acquisition or the held-authority error.
  Exhausting the bound MUST fail closed — no authority returned — and MUST report
  the held-authority outcome, retaining the binding-change cause inside that error
  rather than introducing a second taxonomy for callers to handle.

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
- a completion-path residue guard shipping with the completion-path behavior it
  asserts, covering success, failure, and trappable cancellation, and proven
  non-vacuous: the run is observed holding its authority artifact while it runs.
  Skipping the holder's own removal MUST fail it;
- an owner-cleanup negative control in which the lease pathname is rebound under
  the held descriptor: cleanup refuses, the artifact occupying the name survives,
  and the lock is still released so a successor can acquire. Reopening or
  re-locking the artifact instead of using the held descriptor, and unlinking
  after release instead of under the lock, MUST each fail it;
- cleanup-refusal reporting proven at each boundary that owns a release, not only
  where the refusal originates: the library entry point and the command both
  return an error naming the failed release, and the job record and its commit
  receipt are asserted together — record not reporting success, receipt still
  reporting the artifact it committed. A discarded release error at any of those
  boundaries MUST fail it, and a failed operation whose cleanup also failed MUST
  carry its original cause asserted independently of the cleanup cause;
- the compound case, driven through the real command: cleanup refused and the
  lifecycle correction unpersistable. The result MUST carry both causes, and the
  resulting state MUST be asserted as it stands — record reporting success, valid
  commit receipt, failed command naming it. Discarding the caller-side correction
  error MUST fail it;
- interrupt-to-cancellation coverage on the command path: an interrupted run
  verified to be holding its authority leaves no residue, and a repeated interrupt
  terminates the process rather than being absorbed, including where the bridge
  finishes on context cancellation rather than on a signal. Restricting the
  translation to one execution mode, absorbing the second signal, and leaving the
  handler installed on the cancellation branch MUST each fail it. Signalling a
  running process is not portable to Windows, so this evidence does not execute
  there and MUST NOT be presented as cross-platform;
- a bounded-retry control for the completion race: an acquisition whose pathname
  is removed between locking and binding revalidation still acquires, while a
  pathname invalidated on every attempt fails closed within the bound and reports
  the held-authority outcome with its binding-change cause still reachable;
- for test harnesses that terminate spawned children on teardown, two rules that
  differ by what the harness owns.

  Descendants it does not directly own — a detached child of a child — MUST be
  terminated through a process group the harness creates and keeps in existence
  with a member of its own, live when the group is signalled and still unreaped
  through confirmation. Such teardown MUST NOT derive a destructive target from a
  job record, a process listing, or any other reconstructed identifier: neither a
  process id nor a group id is identity once the thing it named is gone. Any
  record is consulted only to decide whether to reap, and command-line identity is
  matched as an exact flag-and-value pair.

  A child the harness started directly MAY be signalled through its retained
  process object, which is safe precisely while that process is unreaped. That
  requires exactly one owner of its wait, established at start: teardown either
  observes that the owner has completed and does nothing, or signals the retained
  process and joins the owner. Teardown MUST NOT wait on the process itself, and
  completion MUST be published before any result derived from it is observable,
  so a caller that has seen an exit cannot leave teardown believing the child is
  still running.

  Evidence MUST cover: a record naming a live unrelated process (left running);
  the target disappearing after verification, driven by releasing it to exit on
  its own and confirmed absent BEFORE any signal, with the harness's member still
  holding the group and an unrelated process untouched; a genuine leaked child
  terminated and observed dead within a bound; group-owner cleanup terminating
  every member of its group on a path that never reaches an explicit reap; and,
  for directly owned children, both the already-completed case (teardown signals
  nothing) and the teardown-before-completion case, each asserting exactly one
  wait for the child's lifetime. The single-wait rule MUST additionally be
  enforced against the implementation's shape rather than only its behavior: a
  second waiter corrupts nothing until an id is reused, so no run is guaranteed to
  observe one. An unanchored target, a target released only after the absence
  check, anchor-only cleanup, a wait of any form in teardown, exposing a
  wait-capable handle to callers, and publishing a result ahead of completion MUST
  each fail that evidence;

- destructive-command-path flag hygiene proven through the real command, not a
  helper: incompatible target/health flags, mode conflicts, and a mutation opt-in
  supplied without its mutating mode are all rejected before any listing or
  reclaim; a two-root control proves an explicitly named target never mutates the
  default store; and removing the guard from the command path must fail on that
  mutation. Clean modes — list, dry run, and confirmed reclaim — still behave as
  specified;
- an anti-split proof and an adversarial reclaim-versus-acquire race showing a
  held successor's artifact is never removed. The anti-split guard is
  platform-split and MUST be recorded as such rather than claimed uniformly:
  where a pathname whose file is open and locked can be rebound, the swap is
  staged and the application's under-lock revalidation is what refuses the
  removal; where the platform refuses the rebind outright, that code path does
  not execute and the OS is what prevents the split. The delete-while-open
  unlink and the adversarial race MUST run natively on both;
- Unix and native Windows locking, path, transaction, and recovery behavior,
  plus supported release cross-compilation. Evidence MUST record what a platform
  could not execute rather than presenting partial coverage as uniform — a
  byte-identity assertion that a mandatory lock prevents, for instance, is
  reported as the code path plus unchanged metadata, not as byte proof.

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
