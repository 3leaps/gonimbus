# Durable streaming segment writer (dark)

**Status**: internal library primitive only

**Does not**: activate continuous-state publication, rewrite `PublishSnapshot`,
advance complete/latest, load prior-run state for ordinary builds, raise enrich
scale ceilings, or claim exclusive adversarial isolation over the segment
directory

## Purpose

Provide a **pull-to-seal** segment-set writer that consumes an already-sorted
unique current-state row stream and produces the same in-memory
`InternalManifest` / parquet segment contract as batch `WriteSegmentSet`,
without materializing a full-set `[]CurrentObjectRow`.

```text
OrderedRowSource (strict increasing RelKey)
  → convert each row to segmentParquetRow at ingress
  → seal parquet segments at TargetRowsPerSegment
  → InternalManifest (counts + descriptors)
```

Production publication still uses `Compact` → `WriteSegmentSet`. This primitive
exists so a later activation path can stream from the spill/merge row source
into segments without holding the full state map.

## API (`internal/indexsubstrate`)

| Symbol                     | Role                                                      |
| -------------------------- | --------------------------------------------------------- |
| `OrderedRowSource`         | `Next` + `Close` pull stream (already sorted unique keys) |
| `WriteStreamingSegmentSet` | Drain source → segments + in-memory manifest              |
| `NewSliceOrderedRows`      | Test/fixture adapter over a pre-sorted slice              |
| `StreamSegmentError`       | Typed failure surface (category/phase/message)            |

### Lifecycle

```text
validate config + lineage (raw RunStartedAt before UTC clone)
  → own source Close (exactly one terminal call)
  → MkdirAll(Dir)
  → Next* until EOF
       normalize/validate row; refuse duplicate/out-of-order RelKey
       convert to segmentParquetRow; append open segment buffer
       when full → seal (temp → digest → linkImmutable)
  → seal trailing partial segment
  → source Close must succeed
  → return InternalManifest
```

**Success** requires full source EOF **and** successful `Close`. A close failure
after EOF is a writer failure: only segments this call newly linked are removed,
and the returned manifest is zero-value.

**Caller ownership:** the writer takes exclusive ownership of one terminal
`Close`. Do not `Close` the same source after handing it to the writer.
Independent adapters are required for differential drains (a pull source is
consumed after one pass).

### Input contract vs batch

| Behavior                 | `WriteSegmentSet`           | `WriteStreamingSegmentSet` |
| ------------------------ | --------------------------- | -------------------------- |
| Sort                     | Sorts input                 | Does **not** re-sort       |
| Duplicate keys           | Keeps last after sort order | **Refuse** (fail closed)   |
| Out-of-order keys        | Sorted into place           | **Refuse** (fail closed)   |
| Full-set materialization | Yes (`[]CurrentObjectRow`)  | No (open segment only)     |

Differential fixtures must use **pre-sorted unique** keys so both paths compare
byte-for-byte on segment digests and manifest fields.

### Memory envelope (honest peak)

Per seal attempt the writer may hold concurrently:

1. Open-segment `[]segmentParquetRow` (≤ `TargetRowsPerSegment`)
2. Parquet encoder buffers during seal
3. Whole-run distinct-ETag set for `ManifestCounts.DistinctETags`

It does **not** hold a full-set `[]CurrentObjectRow` or a second full-set Parquet
row slice. After each seal, the open buffer is cleared so prior segment pointer
fields are not retained.

Field RSS / multi-million scale evidence remains an activation gate, not this
dark slice.

### Progress

`OnSegmentProgress` is observational only (never a write failure vector). On
the streaming path every callback reports **`Total=0`**. Final segment count is
`len(returnedManifest.Segments)`. Callbacks are attempt-local: a later failure
may roll back a segment already reported.

### Failure cleanup

- Track only finals this call **created** via successful `link` (`created=true`).
- `AllowExistingIdentical` reuse of a pre-existing same-digest final is
  **not owned** and is never deleted on abort.
- Never glob `Dir`; never delete unrelated names (e.g. foreign files).
- Cleanup failure is joined into the returned error; no success manifest on
  the error path.
- Leftover `.segment-*.parquet.tmp` files are removed by the seal helper.

### Directory authority

This call assumes **single-writer mutation authority** over `config.Dir` for the
duration of the call. Concurrent writers into the same directory are unsupported
and can create create/reuse/cleanup races. Activation under a write lease is
expected to satisfy exclusivity; this dark primitive does not invent locking.

### Error disclosure

`StreamSegmentError.Error()` exposes category, phase, stable message, optional
1-based row index, and a short `cause_class`. It never renders RelKey values,
configured directory paths, or provider URIs. `Unwrap` preserves classification
(`errors.Is` for cancel, etc.).

Source `Close` failures are always wrapped in a `StreamSegmentError` (category
`source`, phase `close`) before any `errors.Join`, so joined multi-error
surfaces never render raw Close text (paths/secrets). Primary failure category
remains discoverable on the join tree.

### Temp retirement and post-link ownership

Every staged `.segment-*.parquet.tmp` is retired through a **checked** remove
(joined into the returned error). There is no silent deferred `_ = os.Remove`.

| Path                                                      | Ownership                          | Temp cleanup                                                                   |
| --------------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------------ |
| Pre-link failure (write/close/link, non-identical EEXIST) | Owns **only** the temp             | Checked retire; never removes a conflicting final                              |
| Identical reuse (`AllowExistingIdentical`)                | Owns **nothing** (`created=false`) | Checked temp unlink before success                                             |
| New final linked                                          | Owns the final (`created=true`)    | Checked temp unlink; post-link/stat failures keep ownership for outer rollback |

Once an immutable final is successfully linked, that path is **owned** by the
attempt even if subsequent temp-unlink or final-stat steps fail. Ownership is
recorded before the seal error is returned so outer rollback can remove the
final.

### Darkness

| Surface            | Status                                    |
| ------------------ | ----------------------------------------- |
| `PublishSnapshot`  | Unchanged (`Compact` → `WriteSegmentSet`) |
| complete / latest  | Not written by this primitive             |
| CLI / flags / env  | No activation                             |
| Production callers | None (library + tests only)               |

## Compatibility

Hermetic tests compare streaming output to batch `WriteSegmentSet` on identical
sorted unique fixtures (manifest fields + per-segment digests) and reconstruct
rows via `WalkManifestRows` / verified segment readers.

## Related

- [Durable spill/merge row source](durable-spill-merge.md) — sorted current-state
  iterator that can feed this writer
- [Durable lineage schema](durable-lineage.md) — optional dark lineage fields
  accepted on the segment writer config
