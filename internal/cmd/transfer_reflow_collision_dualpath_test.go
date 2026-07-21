package cmd

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

// Engine<->CLI-pool behavioral parity for the migrated overwrite (#2) and
// overwrite-if-source-newer (#4) collision cells. Each scenario runs the SAME
// live stdin input through the real command on both execution paths — the engine
// (default routing) and the genuinely-routed CLI pool (poolRoute) — against
// identically-seeded providers, then asserts the terminal record, collision
// metadata, and destination state are identical. This is the ADR-0006 dual-path
// gate for the collision-cell migration.

type collisionDualPathArm struct {
	stdout string
	err    error
	dst    *reflowMemoryProvider
}

// runReflowCollisionDualPath executes the standard one-object stdin baseline on
// both paths against freshly, identically-seeded destinations. The engine arm is
// default routing; the pool arm appends the non-migrated source-failure policy
// (a no-op on this success fixture) to force genuine pool dispatch.
func runReflowCollisionDualPath(t *testing.T, seedDst func(dst *reflowMemoryProvider), extra ...string) (engine, pool collisionDualPathArm) {
	t.Helper()

	envEngine := newFlagProbeEnv(t)
	if seedDst != nil {
		seedDst(envEngine.dst)
	}
	engineOut, engineErr := envEngine.run(t, extra...)

	envPool := newFlagProbeEnv(t)
	if seedDst != nil {
		seedDst(envPool.dst)
	}
	poolOut, poolErr := envPool.runPool(t, extra...)

	return collisionDualPathArm{stdout: engineOut, err: engineErr, dst: envEngine.dst},
		collisionDualPathArm{stdout: poolOut, err: poolErr, dst: envPool.dst}
}

// soleTerminalReflowRecord returns the single terminal object record (the one
// record whose status is not in_progress), asserting exactly one exists.
func soleTerminalReflowRecord(t *testing.T, stdout string) testReflowData {
	t.Helper()
	var terminal []testReflowData
	for _, rec := range requireReflowRecords(t, stdout) {
		if rec.Status == "in_progress" {
			continue
		}
		terminal = append(terminal, rec)
	}
	require.Len(t, terminal, 1, "exactly one terminal object record")
	return terminal[0]
}

// requireReflowSummaryParity asserts the terminal summary's outcome tallies —
// per-status counts, per-collision-kind counts, invalid inputs, and errors —
// are identical across the two execution paths.
func requireReflowSummaryParity(t *testing.T, engineStdout, poolStdout string) {
	t.Helper()
	type summaryTally struct {
		Statuses      map[string]int64 `json:"statuses"`
		Collisions    map[string]int64 `json:"collisions"`
		InvalidInputs int64            `json:"invalid_inputs"`
		Errors        int64            `json:"errors"`
	}
	parse := func(stdout string) summaryTally {
		rec := requireRecord(t, stdout, reflowpkg.SummaryRecordType, "")
		var s summaryTally
		require.NoError(t, json.Unmarshal(rec.Data, &s))
		return s
	}
	engine, pool := parse(engineStdout), parse(poolStdout)
	require.Equal(t, pool.Statuses, engine.Statuses, "summary status tallies must match across paths")
	require.Equal(t, pool.Collisions, engine.Collisions, "summary collision tallies must match across paths")
	require.Equal(t, pool.InvalidInputs, engine.InvalidInputs)
	require.Equal(t, pool.Errors, engine.Errors)
}

// requireReflowTerminalEqual asserts two terminal records carry identical
// semantic fields — status, reason, byte count, keys, and the full collision
// decision (kind, path, reason, observed dest etag/size, source/dest
// timestamps). Presentation-only differences (URIs are already sanitized
// identically) are not in scope.
func requireReflowTerminalEqual(t *testing.T, engine, pool testReflowData) {
	t.Helper()
	require.Equal(t, pool.Status, engine.Status, "status must match across paths")
	require.Equal(t, pool.Reason, engine.Reason, "reason must match across paths")
	require.Equal(t, pool.Bytes, engine.Bytes, "byte count must match across paths")
	require.Equal(t, pool.SourceKey, engine.SourceKey)
	require.Equal(t, pool.DestKey, engine.DestKey)

	if pool.Collision == nil {
		require.Nil(t, engine.Collision, "engine must also emit no collision")
		return
	}
	require.NotNil(t, engine.Collision, "engine must emit a collision to match the pool")
	require.Equal(t, pool.Collision.Kind, engine.Collision.Kind)
	require.Equal(t, pool.Collision.DecisionPath, engine.Collision.DecisionPath)
	require.Equal(t, pool.Collision.DecisionReason, engine.Collision.DecisionReason)
	require.Equal(t, pool.Collision.DestETagObserved, engine.Collision.DestETagObserved)
	require.Equal(t, pool.Collision.DestSizeObserved, engine.Collision.DestSizeObserved)
	require.Equal(t, pool.Collision.SrcLastModified, engine.Collision.SrcLastModified)
	require.Equal(t, pool.Collision.DestLastModifiedObserved, engine.Collision.DestLastModifiedObserved)
}

func TestTransferReflowDualPath_OverwriteReplacesUnconditionally(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		// A stale, conflicting object at the destination key.
		dst.putFixture("data/source/file.xml", "stale-content", "other-etag", time.Time{})
	}, "--on-collision", "overwrite", "--overwrite")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engine.stdout))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, pool.stdout))

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "complete", engineRec.Status)
	// The pre-copy head reports the pre-existing (differing) object as a conflict
	// on the unconditional-overwrite decision path, then overwrites it.
	requireCollisionEqual(t, engineRec, collisionConflict, decisionOverwrite, "other-etag", int64(len("stale-content")))
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)

	// Both paths landed the real body over the stale destination.
	require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
	require.Equal(t, "payload", string(pool.dst.mustObject("data/source/file.xml")))
}

func TestTransferReflowDualPath_SourceNewerReplacesWithIfMatch(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		// Destination older than the source (input LastModified is 2026-01-15).
		dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
	}, "--on-collision", "overwrite-if-source-newer")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engine.stdout))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, pool.stdout))

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "complete", engineRec.Status)
	requireCollisionEqual(t, engineRec, collisionOverwritten, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, engineRec, reasonSrcNewer, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)

	require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
	require.Equal(t, "payload", string(pool.dst.mustObject("data/source/file.xml")))
}

func TestTransferReflowDualPath_SourceOlderSkips(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		// Destination newer than the source (input LastModified is 2026-01-15).
		dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC))
	}, "--on-collision", "overwrite-if-source-newer")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engine.stdout))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, pool.stdout))

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "skipped", engineRec.Status)
	require.Equal(t, "collision.skipped_src_older", engineRec.Reason)
	requireCollisionEqual(t, engineRec, collisionSrcOlder, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, engineRec, reasonSrcOlder, "2026-01-15T20:53:44Z", "2026-01-16T20:53:44Z")
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)

	// Neither path replaced the newer destination.
	require.Equal(t, "old payload", string(engine.dst.mustObject("data/source/file.xml")))
	require.Equal(t, "old payload", string(pool.dst.mustObject("data/source/file.xml")))
}

func TestTransferReflowDualPath_SourceNewerConcurrentMutationSkips(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
		// The dest mutates out from under the conditional If-Match overwrite.
		dst.mutateBeforeIfMatch = true
	}, "--on-collision", "overwrite-if-source-newer")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engine.stdout))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, pool.stdout))

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "skipped", engineRec.Status)
	require.Equal(t, "collision.skipped_concurrent_mutation", engineRec.Reason)
	requireCollisionEqual(t, engineRec, collisionConcurrentMut, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, engineRec, reasonConcurrentMut, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)

	// The concurrent write is preserved on both paths (never clobbered).
	require.Equal(t, "concurrent mutation", string(engine.dst.mustObject("data/source/file.xml")))
	require.Equal(t, "concurrent mutation", string(pool.dst.mustObject("data/source/file.xml")))
}

// TestTransferReflowDualPath_SourceNewerResumeSkipsNoDoubleLand pins the
// strict-authority checkpoint contract for the migrated source-newer overwrite:
// the completing overwrite writes a resume-authoritative item, and a --resume
// rerun skips it (resume.complete) rather than landing the object a second time.
func TestTransferReflowDualPath_SourceNewerResumeSkipsNoDoubleLand(t *testing.T) {
	env := newFlagProbeEnv(t)
	env.dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))

	// Conditional PUTs targeting the actual destination key (the IfAbsent write
	// probe on the run's dedicated preflight prefix is excluded — it fires once
	// per run regardless of resume).
	destKeyPuts := func() int {
		n := 0
		for _, k := range env.dst.conditionalPutCallsSnapshot() {
			if k == "data/source/file.xml" {
				n++
			}
		}
		return n
	}

	// First run: the newer source overwrites the destination and completes.
	stdout1, err := env.run(t, "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout1))
	require.Equal(t, "complete", soleTerminalReflowRecord(t, stdout1).Status)
	require.Equal(t, "payload", string(env.dst.mustObject("data/source/file.xml")))
	destPutsAfterFirst := destKeyPuts()
	require.Positive(t, destPutsAfterFirst, "the first run must have written the destination key")

	// Resume: the item is already durable, so it skips without a second land.
	stdout2, err := env.run(t, "--on-collision", "overwrite-if-source-newer", "--resume")
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout2))
	resumed := soleTerminalReflowRecord(t, stdout2)
	require.Equal(t, "skipped", resumed.Status)
	require.Equal(t, "resume.complete", resumed.Reason)
	require.Equal(t, "payload", string(env.dst.mustObject("data/source/file.xml")))
	require.Equal(t, destPutsAfterFirst, destKeyPuts(),
		"resume must not issue any additional conditional PUT for the settled destination key")
}
