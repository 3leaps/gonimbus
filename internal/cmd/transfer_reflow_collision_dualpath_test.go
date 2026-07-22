package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
)

// runTransferReflowRawStdin drives the real transfer-reflow command with the
// given stdin and args, returning stdout and the execution error.
func runTransferReflowRawStdin(t *testing.T, input string, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), err
}

// Engine<->CLI-pool behavioral parity for the migrated overwrite (#2) and
// overwrite-if-source-newer (#4) collision cells. Each parity scenario runs the
// SAME live stdin input through the real command on both execution paths — the
// engine (default routing) and the genuinely-routed CLI pool (poolRoute) —
// against identically-seeded providers, then asserts the terminal record,
// collision metadata, summary tallies, and destination state are identical.
// This is the ADR-0006 dual-path gate for the collision-cell migration.
//
// Alongside the parity scenarios, this file pins two properties that are engine
// contracts rather than pool comparisons: NoteCollision is auxiliary (a store
// failure never changes a terminal), and the mandatory-vs-optional source-HEAD
// distinction.

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

type reflowSummaryTally struct {
	Statuses      map[string]int64 `json:"statuses"`
	Collisions    map[string]int64 `json:"collisions"`
	InvalidInputs int64            `json:"invalid_inputs"`
	Errors        int64            `json:"errors"`
}

func reflowSummaryTallyOf(t *testing.T, stdout string) reflowSummaryTally {
	t.Helper()
	rec := requireRecord(t, stdout, reflowpkg.SummaryRecordType, "")
	var s reflowSummaryTally
	require.NoError(t, json.Unmarshal(rec.Data, &s))
	return s
}

// requireReflowSummaryParity asserts the terminal summary's outcome tallies —
// per-status counts, per-collision-kind counts, invalid inputs, and errors —
// are identical across the two execution paths.
func requireReflowSummaryParity(t *testing.T, engineStdout, poolStdout string) {
	t.Helper()
	engine, pool := reflowSummaryTallyOf(t, engineStdout), reflowSummaryTallyOf(t, poolStdout)
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

func TestTransferReflowDualPath_OverwriteReplacesConflict(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		// A stale, differing object at the destination key.
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

	require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
	require.Equal(t, "payload", string(pool.dst.mustObject("data/source/file.xml")))
}

func TestTransferReflowDualPath_OverwriteReplacesDuplicate(t *testing.T) {
	engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
		// An identical object (same etag + size as the source) already present.
		dst.putFixture("data/source/file.xml", "payload", "src-etag", time.Time{})
	}, "--on-collision", "overwrite", "--overwrite")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "complete", engineRec.Status)
	// Identical dest -> classified duplicate (not conflict), still unconditional.
	requireCollisionEqual(t, engineRec, collisionDuplicate, decisionOverwrite, "src-etag", int64(len("payload")))
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)
	require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
}

func TestTransferReflowDualPath_OverwriteAbsentDestNoCollision(t *testing.T) {
	// No destination seed: overwrite against an absent key lands with no collision.
	engine, pool := runReflowCollisionDualPath(t, nil, "--on-collision", "overwrite", "--overwrite")

	require.NoError(t, engine.err)
	require.NoError(t, pool.err)

	engineRec := soleTerminalReflowRecord(t, engine.stdout)
	poolRec := soleTerminalReflowRecord(t, pool.stdout)
	require.Equal(t, "complete", engineRec.Status)
	require.Nil(t, engineRec.Collision, "an absent destination carries no collision")
	requireReflowTerminalEqual(t, engineRec, poolRec)
	requireReflowSummaryParity(t, engine.stdout, pool.stdout)
	require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
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
// strict-authority checkpoint contract for the migrated source-newer overwrite
// on BOTH execution paths: the completing overwrite writes a resume-authoritative
// item, and a --resume rerun skips it (resume.complete) rather than landing the
// object a second time.
func TestTransferReflowDualPath_SourceNewerResumeSkipsNoDoubleLand(t *testing.T) {
	arms := []struct {
		name string
		path string
		pool bool
	}{
		{name: "engine", path: reflowpkg.ExecutionPathEngine, pool: false},
		{name: "cli-pool", path: reflowpkg.ExecutionPathCLIPool, pool: true},
	}
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))

			runArm := func(extra ...string) (string, error) {
				if arm.pool {
					return env.runPool(t, extra...)
				}
				return env.run(t, extra...)
			}
			// Conditional PUTs targeting the actual destination key (the IfAbsent
			// write probe on the run's dedicated preflight prefix is excluded — it
			// fires once per run regardless of resume).
			destKeyPuts := func() int {
				n := 0
				for _, k := range env.dst.conditionalPutCallsSnapshot() {
					if k == "data/source/file.xml" {
						n++
					}
				}
				return n
			}

			stdout1, err := runArm("--on-collision", "overwrite-if-source-newer")
			require.NoError(t, err)
			require.Equal(t, arm.path, executionPathOf(t, stdout1))
			require.Equal(t, "complete", soleTerminalReflowRecord(t, stdout1).Status)
			require.Equal(t, "payload", string(env.dst.mustObject("data/source/file.xml")))
			destPutsAfterFirst := destKeyPuts()
			require.Positive(t, destPutsAfterFirst, "the first run must have written the destination key")

			stdout2, err := runArm("--on-collision", "overwrite-if-source-newer", "--resume")
			require.NoError(t, err)
			require.Equal(t, arm.path, executionPathOf(t, stdout2))
			resumed := soleTerminalReflowRecord(t, stdout2)
			require.Equal(t, "skipped", resumed.Status)
			require.Equal(t, "resume.complete", resumed.Reason)
			require.Equal(t, "payload", string(env.dst.mustObject("data/source/file.xml")))
			require.Equal(t, destPutsAfterFirst, destKeyPuts(),
				"resume must not issue any additional conditional PUT for the settled destination key")
		})
	}
}

// noteCollisionFailingReflowState fails every NoteCollision audit write while
// passing all other store operations through, for the auxiliary-classification
// disposition (E-CVG-S2-F1): an audit-row failure must never change a terminal.
type noteCollisionFailingReflowState struct {
	reflowStateStore
	err error
}

func (s noteCollisionFailingReflowState) NoteCollision(context.Context, string, reflowstate.CollisionKind, string, string, int64, string, int64) error {
	return s.err
}

// TestTransferReflowNoteCollisionFailureIsAuxiliary proves NoteCollision is
// auxiliary on the migrated collision branches: when the audit write fails, the
// engine emits the sanitized checkpoint-state warning and preserves the exact
// terminal (overwrite lands, source-older/concurrent-mutation skip), with zero
// run errors. Matches the CLI pool's checkpointWriteFailed contract.
func TestTransferReflowNoteCollisionFailureIsAuxiliary(t *testing.T) {
	// A credential-bearing cause proves the warning message is sanitized.
	const noteFailureCause = "note write failed for https://user:s3cr3t-token@collision.example/audit?sig=DEADBEEF"
	cases := []struct {
		name         string
		seedDst      func(dst *reflowMemoryProvider)
		args         []string
		wantStatus   string
		wantReason   string
		wantDest     string
		wantKind     string
		wantDecision string
	}{
		{
			name: "overwrite_conflict",
			seedDst: func(dst *reflowMemoryProvider) {
				dst.putFixture("data/source/file.xml", "stale", "other-etag", time.Time{})
			},
			args:         []string{"--on-collision", "overwrite", "--overwrite"},
			wantStatus:   "complete",
			wantReason:   "",
			wantDest:     "payload",
			wantKind:     collisionConflict,
			wantDecision: decisionOverwrite,
		},
		{
			name: "source_older_skip",
			seedDst: func(dst *reflowMemoryProvider) {
				dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC))
			},
			args:         []string{"--on-collision", "overwrite-if-source-newer"},
			wantStatus:   "skipped",
			wantReason:   "collision.skipped_src_older",
			wantDest:     "old payload",
			wantKind:     collisionSrcOlder,
			wantDecision: decisionHeadCompare,
		},
		{
			name: "concurrent_mutation_skip",
			seedDst: func(dst *reflowMemoryProvider) {
				dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
				dst.mutateBeforeIfMatch = true
			},
			args:         []string{"--on-collision", "overwrite-if-source-newer"},
			wantStatus:   "skipped",
			wantReason:   "collision.skipped_concurrent_mutation",
			wantDest:     "concurrent mutation",
			wantKind:     collisionConcurrentMut,
			wantDecision: decisionHeadCompare,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			oldStateStore := newReflowStateStore
			newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
				store, err := oldStateStore(ctx, cfg)
				if err != nil {
					return nil, err
				}
				return noteCollisionFailingReflowState{reflowStateStore: store, err: fmt.Errorf("%s", noteFailureCause)}, nil
			}
			t.Cleanup(func() { newReflowStateStore = oldStateStore })

			tc.seedDst(env.dst)
			stdout, err := env.run(t, tc.args...)
			require.NoError(t, err, "an auxiliary NoteCollision failure must not fail the run")
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))

			rec := soleTerminalReflowRecord(t, stdout)
			require.Equal(t, tc.wantStatus, rec.Status, "terminal status must survive the audit-write failure")
			require.Equal(t, tc.wantReason, rec.Reason)
			require.Equal(t, tc.wantDest, string(env.dst.mustObject("data/source/file.xml")))

			// The collision evidence must survive the audit-write failure — an audit
			// failure cannot erase the terminal's collision object.
			require.NotNil(t, rec.Collision, "the terminal must retain its collision object")
			require.Equal(t, tc.wantKind, rec.Collision.Kind)
			require.Equal(t, tc.wantDecision, rec.Collision.DecisionPath)

			require.Zero(t, reflowSummaryTallyOf(t, stdout).Errors, "an auxiliary failure must not raise the run error count")
			warn := requireRecord(t, stdout, reflowpkg.WarningRecordType, "")
			require.Contains(t, string(warn.Data), "REFLOW_ARBITRATION_STATE_WRITE_FAILED",
				"the sanitized checkpoint-state warning must be emitted")
			// The credential-bearing cause must be redacted out of the warning message.
			require.NotContains(t, string(warn.Data), "s3cr3t-token", "credential userinfo must be redacted")
			require.NotContains(t, string(warn.Data), "DEADBEEF", "credential query value must be redacted")
		})
	}
}

// TestTransferReflowSourceHeadMandatoryVsOptional pins the source-HEAD
// distinction the overwrite migration restored across all migrated modes: a HEAD
// probed only to recover an absent size is optional (its failure is tolerated,
// unknown size reserving the conservative cap and the copy still landing), while
// a HEAD required to recover source-newer's LastModified is mandatory (its
// failure fails the object with no destination mutation). It also pins
// adjudication #3: a successful HEAD that yields a zero LastModified fails with
// the source-LastModified terminal, no mutation, one run error.
func TestTransferReflowSourceHeadMandatoryVsOptional(t *testing.T) {
	newSourceHeadEnv := func(t *testing.T, headFails bool, srcLastMod time.Time) (*reflowMemoryProvider, *reflowMemoryProvider) {
		t.Helper()
		withTransferReflowTestState(t)
		srcBase := newReflowMemoryProvider()
		srcBase.putFixture("source/file.xml", "payload", "src-etag", srcLastMod)
		dst := newReflowMemoryProvider()
		reflowResourceProbeForRun = reflowpkg.ResourceProbe{
			MemoryLimitBytes: func() (int64, string, error) { return int64(8) << 20, "cgroup_v2", nil },
			FDSoftLimit:      func() (int64, error) { return 100000, nil },
		}
		var src provider.Provider = srcBase
		if headFails {
			src = headFailingSourceProvider{srcBase}
		}
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				switch cfg.Bucket {
				case "source-bucket":
					return src, nil
				case "dest-bucket":
					return dst, nil
				default:
					return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
				}
			},
			File: func(providerfile.Config) (provider.Provider, error) { return dst, nil },
		})
		return srcBase, dst
	}
	runSourceHead := func(t *testing.T, input string, extra ...string) (string, error) {
		t.Helper()
		return runTransferReflowRawStdin(t, input, append([]string{
			"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2",
		}, extra...)...)
	}

	// Optional HEAD (absent size only) tolerates failure and still lands, for
	// every migrated mode.
	for _, mode := range [][]string{
		{"--on-collision", "skip-if-duplicate"},
		{"--on-collision", "fail"},
		{"--on-collision", "overwrite", "--overwrite"},
	} {
		t.Run("optional_head_failure_lands"+mode[1], func(t *testing.T) {
			_, dst := newSourceHeadEnv(t, true, time.Now().UTC())
			// Size 0 in the record triggers the optional HEAD; the input carries a
			// LastModified so source-newer does not make the HEAD mandatory.
			stdout, err := runSourceHead(t, reflowInputLine("source/file.xml", "src-etag", 0, "", "")+"\n", mode...)
			require.NoError(t, err, stdout)
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
			require.Equal(t, "complete", soleTerminalReflowRecord(t, stdout).Status)
			require.Equal(t, "payload", string(dst.mustObject("data/source/file.xml")))
			require.Contains(t, string(requireRecord(t, stdout, reflowpkg.SummaryRecordType, "").Data), `"memory_reserved_peak_bytes":2097152`)
		})
	}

	t.Run("source_newer_mandatory_head_failure_fails_no_mutation", func(t *testing.T) {
		_, dst := newSourceHeadEnv(t, true, time.Now().UTC())
		// No source_last_modified in the record makes the source HEAD mandatory
		// for overwrite-if-source-newer; the failing HEAD fails the object.
		stdout, err := runSourceHead(t, reflowInputLineWithoutLastModified("source/file.xml", "src-etag", int64(len("payload")))+"\n", "--on-collision", "overwrite-if-source-newer")
		require.Error(t, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		require.Equal(t, "failed", soleTerminalReflowRecord(t, stdout).Status)
		require.False(t, dst.hasObject("data/source/file.xml"), "a mandatory-HEAD failure must not mutate the destination")
	})

	t.Run("source_newer_zero_lastmodified_head_fails_terminal", func(t *testing.T) {
		// The HEAD succeeds but yields a zero LastModified; the source-LastModified
		// guard fails the object with its typed terminal and no mutation.
		_, dst := newSourceHeadEnv(t, false, time.Time{})
		dst.putFixture("data/source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
		stdout, err := runSourceHead(t, reflowInputLineWithoutLastModified("source/file.xml", "src-etag", int64(len("payload")))+"\n", "--on-collision", "overwrite-if-source-newer")
		require.Error(t, err)
		rec := soleTerminalReflowRecord(t, stdout)
		require.Equal(t, "failed", rec.Status)
		require.Equal(t, "collision.missing_source_last_modified", rec.Reason)
		require.Equal(t, int64(1), reflowSummaryTallyOf(t, stdout).Errors)
		require.Equal(t, "old payload", string(dst.mustObject("data/source/file.xml")), "the guard must not mutate the destination")
	})
}
