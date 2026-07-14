# Durable spill/merge row source (dark)

**Status**: internal library primitive only

**Does not**: activate continuous-state publication, rewrite `PublishSnapshot`,
stream segment writers, load prior-run state for ordinary builds, or raise
enrich scale ceilings

## Purpose

Provide a **prepare-then-drain** current-state row source that reproduces the
materialized `Compact` projection under explicit resource budgets:

```text
parent rows (sorted, unique) + sealed journals
  → protected spill workspace
  → sorted CurrentObjectRow iterator
```

Production publication still uses `Compact` → `WriteSegmentSet`. This primitive
exists so a later activation path can stream without holding a full in-memory
state map, while remaining differential-tested against `Compact` today.

## API (`internal/indexsubstrate`)

| Symbol                                         | Role                                                                                                      |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `SpillMergeConfig`                             | Identity, parent source, journal paths, coverage, mode, spill root, budget                                |
| `SpillMergeBudget` / `DefaultSpillMergeBudget` | Prospective memory/disk/fan-in/pass bounds                                                                |
| `ParentRowSource` / `NewSliceParentRows`       | Already-authorized parent stream (no latest lookup)                                                       |
| `PrepareCurrentStateSource`                    | Validate → stage → READY (no rows until ready)                                                            |
| `CurrentStateSource.Next`                      | Owned rows in strict bytewise `RelKey` order                                                              |
| `CurrentStateSource.Close`                     | Idempotent; cleans this attempt under the SpillRoot trust model (below)                                   |
| `SpillMergeStats`                              | High-water counters + complete flag (true only after full EOF + successful Close)                         |
| `SpillMergeError`                              | Typed categories: invalid_config, parent_order, journal, budget, workspace, spill_integrity, io, canceled |

### Lifecycle

```text
config validate
  → protected workspace (unique attempt under SpillRoot/spillmerge/<id>)
  → stage/validate parent (strict increasing unique RelKey)
  → same-open stage/validate each journal (bounded line/record; footer required)
  → multi-pass event merge if needed
  → READY
  → Next* → EOF
  → final integrity / Close (success = full EOF + close)
```

Callers that stop early or observe a mid-drain error **do not** have a complete
snapshot. `Stats.Complete` becomes true only after full EOF **and** a successful
`Close` (parent + spill readers + owned cleanup). Cleanup failure is sticky and
retryable via a subsequent `Close`.

### Sort algebra

Logical events: `(rel_key raw bytes, phase, journal_id raw bytes, sequence)`
with `phase: observe < enrich`.

Per key:

1. Start from zero or one parent row (parent is initial state, not an event)
2. Apply all observes in `(journal_id, sequence)` order
3. Apply all enriches in that order (missing-key enrich ignored, not counted)
4. Coverage tombstone decision (default mode only; enrich-only never invents deletes)

Output topology (in-memory buffer limits, fan-in, multi-pass) must not change
normalized rows or counters.

### Budgets (defaults)

| Knob              | Default |
| ----------------- | ------- |
| MaxBufferedRows   | 64_000  |
| MaxBufferedBytes  | 64 MiB  |
| MaxRecordBytes    | 1 MiB   |
| MaxJournalSources | 256     |
| MaxWorkspaceBytes | 512 MiB |
| MaxSpillRuns      | 4096    |
| MaxFanIn          | 16 (min **3**; concurrent **spill-run** FDs only — roots/lock/journal sources excluded) |
| MaxMergePasses    | 64      |

Invalid budgets fail before workspace creation. Exact limit succeeds; the next
encoded byte/row/run/pass refuses with category `budget`. Workspace accounting
includes headers, footers, and payload; charges are prospective before writes.

Field RSS evidence remains a later activation gate; hermetic tests use the
deterministic high-water counters.

`MaxBufferedBytes` hard-limits estimated encoded residency for parent rows and
journal event buffers (exact limit succeeds; over-limit refuses). Scanner lines
are capped by `MaxRecordBytes`. Merge inputs require validated footer **and**
physical EOF — trailing spill data is `spill_integrity`. Rendered errors expose
category/phase/message/attempt/`cause_class` only; `Unwrap` keeps classification
without path material in `Error()`.

### Spill trust

#### Operator trust model

`SpillRoot` is an **exclusive, operator-controlled** workspace root for gonimbus
spill state. Callers must not share it with concurrent writers that rename,
replace, or plant dentries under `spillmerge/` while a prepare/drain/close is
in flight. The attempt lock is **cooperative/advisory** among gonimbus
operations — not a security boundary against a same-privilege peer that can
mutate the parent directory namespace.

Portable Go/POSIX can bind and wipe a directory through an open FD, but the
final empty-directory unlink is still name-based. A concurrent actor with write
authority on the parent can still swap an **empty** dentry in that last window.
**Hostile concurrent namespace mutation is out of scope for this dark primitive.** If a
future product requirement needs adversarial dentry-swap immunity, that is a
separate OS-specific handle-disposition / authority-boundary slice — not more
SameFile checks on pathname delete.

#### Hardened under this model (in scope)

- SpillRoot must be an **absolute**, non-symlink directory
- Attempt dirs/files: `0700` / `0600` where the platform allows
- Filenames are sequence/attempt ids only (never object keys)
- Errors expose phase + counts + sanitized attempt id — not `RelKey`, URIs, or endpoints
- One attempt workspace; never resume partial spill after process failure
- No-follow opens for journals (and directory open for cleanup where available)
- Content wipe of the owned attempt through a **bound directory handle** opened
  at create time (`wsRoot`), before any quarantine by name
- Quarantine/rename of the live attempt name only when a pre-check still
  `SameFile`-matches the create-time `attemptBound`; a **live-name** substitute
  that fails SameFile is left untouched
- Bound-FD open of trash for child cleanup on Unix (`unlinkat`); non-empty
  post-bind substitutes fail the empty-only final remove (fail closed)
- Partial finalization retains `ownedTrash` for retry; state is not cleared
  until that path reports success
- Workspace byte charges for spill runs are released only after successful
  `os.Remove` of the run file
- MaxFanIn bounds concurrent **spill-run** FDs only (not roots/lock/journal)

#### Explicit non-claims (out of scope)

- Protection against a same-privilege peer that renames/swaps dentries under
  `SpillRoot` after FD bind (including **empty** post-bind replacements)
- That the attempt lock remains held through final rmdir, or alone guarantees
  exclusive ownership of the parent namespace
- That Windows child cleanup is fully handle-relative disposition: after
  no-follow open and identity check, Windows removes children via paths derived
  from the opened directory name (not `FILE_DISPOSITION` / exact-handle delete
  APIs). Runtime ownership under hostile Windows namespace mutation is not
  asserted in this slice
- Whole-tree orphan reclaim of abandoned `spillmerge/*` attempts (deferred)

Crash-abandoned attempt directories may remain under `spillmerge/` until a later
reclaim helper; this slice does not scan or delete unowned attempt trees.

### RunStartedAt

Caller-supplied `RunStartedAt` is validated **before** any `.UTC()` laundering
or workspace creation. Non-zero zone offsets refuse as invalid config (same
discipline as lineage write seams).

## Darkness

| Path                        | Behavior                                 |
| --------------------------- | ---------------------------------------- |
| `PublishSnapshot`           | Still uses `Compact` / materialize path  |
| CLI / durable build adapter | No PriorRows load for ordinary builds    |
| Lineage emission            | Unchanged (schema-only from prior slice) |
| Streaming segment writer    | Separate later slice                     |
| Enrich 2M ceiling           | Unchanged                                |

## Related

- Lineage schema (dark): `docs/architecture/durable-lineage.md`
- Materialized oracle: `Compact` in `internal/indexsubstrate`
- Canonical authority: ADR-0007 (not reopened here)
