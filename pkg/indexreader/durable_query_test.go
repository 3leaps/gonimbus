package indexreader

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestDurableQuery_WithoutIndexDB(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a/one.json", 100, "etag-a", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		durableRow("a/two.xml", 200, "etag-b", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
		durableRow("b/three.json", 300, "etag-c", time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)),
	})

	// No index.db anywhere — default-build gap must be closed.
	require.NoFileExists(t, filepath.Join(env.identityDir, "index.db"))

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	require.Equal(t, FormatDurableV2, reader.Meta().Format)
	require.Equal(t, env.indexSetID, reader.Meta().IndexSetID)
	require.Equal(t, env.baseURI, reader.Meta().BaseURI)

	results, stats, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "**/*.json",
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "a/one.json", results[0].RelKey)
	require.Equal(t, "b/three.json", results[1].RelKey)
	require.EqualValues(t, 3, stats.Examined)
	require.EqualValues(t, 2, stats.Matched)
	require.Equal(t, 1, stats.SegmentsWalked)
	require.Equal(t, 1, stats.SegmentsVerified)
	require.Zero(t, stats.SegmentsManifestPruned)

	count, err := reader.QueryObjectCount(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "a/**",
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestDurableQuery_BaseURIResolve(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatDurableV2, reader.Meta().Format)

	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestDurableQuery_RejectLayoutGuessing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	indexes := filepath.Join(root, "indexes")
	segments := filepath.Join(root, "segments")
	// Segment-shaped directory without latest/complete markers.
	require.NoError(t, os.MkdirAll(filepath.Join(segments, "idx_abcdef0123456789", "runs", "run_1"), 0o755))

	_, err := ResolveIndexReader(ctx, ResolveOptions{
		IndexesRoot:      indexes,
		SegmentCacheRoot: segments,
	}, ResolveTarget{IndexSetID: "idx_abcdef0123456789"})
	require.Error(t, err)
}

func TestDurableQuery_SinceRunUnsupportedFailClosed(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Sibling complete markers must not enable approximate --since-run.
	writeRunComplete(t, env, "run_unlinked_sibling", time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC))

	_, err = reader.ResolveSinceRunFilter(ctx, "run_unlinked_sibling")
	require.ErrorIs(t, err, ErrDurableSinceRunUnsupported)

	_, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		SinceRun:   &indexstore.SinceRunFilter{RunID: "run_unlinked_sibling", StartedAt: time.Now().UTC()},
	})
	require.ErrorIs(t, err, ErrDurableSinceRunUnsupported)
}

func TestDurableQuery_PinnedAncestorDelta(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, nil)
	baselineStarted := time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC)
	middleStarted := baselineStarted.Add(24 * time.Hour)
	currentStarted := middleStarted.Add(24 * time.Hour)
	baselineRun := "run_baseline_0001"
	middleRun := "run_middle_0002"
	currentRun := "run_current_0003"

	baselineSHA := publishDurableLineageRun(t, env, baselineRun, baselineStarted,
		&indexsubstrate.LineageRecord{Version: 1, Generation: 1, Baseline: true},
		nil, nil,
	)
	middleSHA := publishDurableLineageRun(t, env, middleRun, middleStarted,
		&indexsubstrate.LineageRecord{Version: 1, Generation: 2},
		&indexsubstrate.StateParent{
			IndexSetID: env.indexSetID, RunID: baselineRun, ManifestSHA256: baselineSHA,
		},
		nil,
	)
	rows := []indexsubstrate.CurrentObjectRow{
		{
			RelKey: "delta/added.json", SizeBytes: 10, ETag: "added",
			FirstSeenRunID: middleRun, FirstSeenAt: middleStarted,
			LastChangedRunID: middleRun, LastChangedAt: middleStarted,
			LastSeenRunID: currentRun, LastSeenAt: currentStarted,
		},
		{
			RelKey: "delta/changed.json", SizeBytes: 20, ETag: "changed",
			FirstSeenRunID: baselineRun, FirstSeenAt: baselineStarted.Add(10 * time.Minute),
			LastChangedRunID: currentRun, LastChangedAt: currentStarted,
			LastSeenRunID: currentRun, LastSeenAt: currentStarted,
		},
		{
			RelKey: "delta/reseen.json", SizeBytes: 30, ETag: "reseen",
			FirstSeenRunID: baselineRun, FirstSeenAt: baselineStarted.Add(10 * time.Minute),
			LastChangedRunID: baselineRun, LastChangedAt: baselineStarted.Add(20 * time.Minute),
			LastSeenRunID: currentRun, LastSeenAt: currentStarted,
		},
	}
	publishDurableLineageRun(t, env, currentRun, currentStarted,
		&indexsubstrate.LineageRecord{Version: 1, Generation: 3},
		&indexsubstrate.StateParent{
			IndexSetID: env.indexSetID, RunID: middleRun, ManifestSHA256: middleSHA,
		},
		rows,
	)

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: currentRun})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	filter, err := reader.ResolveSinceRunFilter(ctx, baselineRun)
	require.NoError(t, err)
	require.Equal(t, baselineRun, filter.RunID)
	require.Equal(t, baselineStarted, filter.StartedAt)
	require.Equal(t, []string{currentRun, middleRun}, filter.VerifiedDescendantRunIDs)
	require.False(t, filter.SameRun)

	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID, SinceRun: filter})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "delta/added.json", results[0].RelKey)
	require.Equal(t, indexstore.QueryChangeKindAdded, results[0].ChangeKind)
	require.Equal(t, "delta/changed.json", results[1].RelKey)
	require.Equal(t, indexstore.QueryChangeKindChanged, results[1].ChangeKind)

	count, err := reader.QueryObjectCount(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID, SinceRun: filter})
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	sameRun, err := reader.ResolveSinceRunFilter(ctx, currentRun)
	require.NoError(t, err)
	require.True(t, sameRun.SameRun)
	results, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID, SinceRun: sameRun})
	require.NoError(t, err)
	require.Empty(t, results)

	_, err = reader.ResolveSinceRunFilter(ctx, "run_unlinked_9999")
	require.True(t, indexsubstrate.IsLineageCode(err, indexsubstrate.LineageCodeBaselineNotAncestor), "got %v", err)
}

func TestDurableQuery_PinnedDeltaRejectsUnverifiedRowProvenance(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, nil)
	baselineStarted := time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC)
	currentStarted := baselineStarted.Add(24 * time.Hour)
	baselineRun := "run_baseline_0001"
	currentRun := "run_current_0002"
	baselineSHA := publishDurableLineageRun(t, env, baselineRun, baselineStarted,
		&indexsubstrate.LineageRecord{Version: 1, Generation: 1, Baseline: true},
		nil, nil,
	)
	publishDurableLineageRun(t, env, currentRun, currentStarted,
		&indexsubstrate.LineageRecord{Version: 1, Generation: 2},
		&indexsubstrate.StateParent{
			IndexSetID: env.indexSetID, RunID: baselineRun, ManifestSHA256: baselineSHA,
		},
		[]indexsubstrate.CurrentObjectRow{{
			RelKey: "delta/unbound.json", SizeBytes: 1,
			FirstSeenRunID: baselineRun, FirstSeenAt: baselineStarted,
			LastChangedRunID: "run_unverified_9999", LastChangedAt: currentStarted,
			LastSeenRunID: currentRun, LastSeenAt: currentStarted,
		}},
	)
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: currentRun})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	filter, err := reader.ResolveSinceRunFilter(ctx, baselineRun)
	require.NoError(t, err)
	_, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID, SinceRun: filter})
	require.ErrorContains(t, err, "last_changed_run_id")
	require.ErrorContains(t, err, "verified descendant")
}

func TestDurableQuery_PinnedDeltaRefusals(t *testing.T) {
	t.Run("legacy lineage", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		reader, err := ResolveIndexReader(context.Background(), env.opts, ResolveTarget{
			IndexSetID: env.indexSetID,
			RunID:      env.runID,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close() })
		_, err = reader.ResolveSinceRunFilter(context.Background(), env.runID)
		require.True(t, indexsubstrate.IsLineageCode(err, indexsubstrate.LineageCodeRequireContinuous), "got %v", err)
	})

	t.Run("corrupt parent digest binding", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		baselineStarted := time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC)
		currentStarted := baselineStarted.Add(time.Hour)
		baselineRun := "run_baseline_0001"
		currentRun := "run_current_0002"
		publishDurableLineageRun(t, env, baselineRun, baselineStarted,
			&indexsubstrate.LineageRecord{Version: 1, Generation: 1, Baseline: true},
			nil, nil,
		)
		publishDurableLineageRun(t, env, currentRun, currentStarted,
			&indexsubstrate.LineageRecord{Version: 1, Generation: 2},
			&indexsubstrate.StateParent{
				IndexSetID: env.indexSetID, RunID: baselineRun, ManifestSHA256: strings.Repeat("a", 64),
			},
			nil,
		)
		reader, err := ResolveIndexReader(context.Background(), env.opts, ResolveTarget{
			IndexSetID: env.indexSetID,
			RunID:      currentRun,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close() })
		_, err = reader.ResolveSinceRunFilter(context.Background(), baselineRun)
		require.True(t, indexsubstrate.IsLineageCode(err, indexsubstrate.LineageCodeDigestMismatch), "got %v", err)
	})

	t.Run("nonmonotonic run starts", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		baselineStarted := time.Date(2025, 2, 2, 12, 0, 0, 0, time.UTC)
		currentStarted := baselineStarted.Add(-time.Hour)
		baselineRun := "run_baseline_0001"
		currentRun := "run_current_0002"
		baselineSHA := publishDurableLineageRun(t, env, baselineRun, baselineStarted,
			&indexsubstrate.LineageRecord{Version: 1, Generation: 1, Baseline: true},
			nil, nil,
		)
		publishDurableLineageRun(t, env, currentRun, currentStarted,
			&indexsubstrate.LineageRecord{Version: 1, Generation: 2},
			&indexsubstrate.StateParent{
				IndexSetID: env.indexSetID, RunID: baselineRun, ManifestSHA256: baselineSHA,
			},
			nil,
		)
		reader, err := ResolveIndexReader(context.Background(), env.opts, ResolveTarget{
			IndexSetID: env.indexSetID,
			RunID:      currentRun,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close() })
		_, err = reader.ResolveSinceRunFilter(context.Background(), baselineRun)
		require.True(t, indexsubstrate.IsLineageCode(err, indexsubstrate.LineageCodeInvalidTime), "got %v", err)
	})

	t.Run("canceled", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		reader, err := ResolveIndexReader(context.Background(), env.opts, ResolveTarget{
			IndexSetID: env.indexSetID,
			RunID:      env.runID,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close() })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = reader.ResolveSinceRunFilter(ctx, env.runID)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestDurableQuery_CanonicalByETag(t *testing.T) {
	ctx := context.Background()
	mod := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "same", mod),
		durableRow("b.txt", 2, "same", mod),
		durableRow("c.txt", 3, "", mod),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	out, stats, err := reader.QueryCanonicalObjects(ctx, indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMinKey,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.CanonicalGroups)
	require.Equal(t, 1, stats.PassthroughRows)
	require.Equal(t, 2, stats.TotalRecords)
	require.Len(t, out, 2)

	out, stats, err = reader.QueryCanonicalObjects(ctx, indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMinKey,
		Limit:             1,
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, 1, stats.TotalRecords)
	require.Equal(t, 2, stats.AvailableRecords)
	require.Equal(t, 1, stats.AvailablePassthroughRows)
	require.True(t, stats.Truncated)
}

func TestDurablePreferredWhenBothPresent(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	// Also write a sqlite index.db with same identity.
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// A verified durable latest outranks the set-root index.db, including for
	// base-URI targets that discover the set through the identity directory.
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	require.Equal(t, FormatDurableV2, reader.Meta().Format)
	require.NoError(t, reader.Close())

	// Without a durable latest the set keeps its existing SQLite selection.
	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	require.NoError(t, os.Rename(latestPath, latestPath+".bak"))
	reader, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatSQLiteV1, reader.Meta().Format)
}

// TestValidDurableWinsOverResidueBlockedSQLite pins the precedence order: a
// verified durable latest is the current authority for the set, so it must be
// selected without opening the lower-priority set-root SQLite at all — a
// historical canonical index.db carrying transaction-quarantine residue (which
// correctly fail-closes the ordinary SQLite open) must not veto durable
// resolution. Absent durable, the same residue keeps refusing targeted SQLite
// reads.
func TestValidDurableWinsOverResidueBlockedSQLite(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	// Historical quarantine residue beside the legacy canonical SQLite.
	residue := filepath.Join(env.identityDir, ".gonimbus-sqlite-quarantine-historical")
	require.NoError(t, os.WriteFile(residue, []byte("captured"), 0o600))

	for _, target := range []ResolveTarget{
		{BaseURI: env.baseURI},
		{IndexSetID: env.indexSetID},
	} {
		reader, err := ResolveIndexReader(ctx, env.opts, target)
		require.NoError(t, err, "valid durable must not be vetoed by residue-blocked SQLite (%+v)", target)
		require.Equal(t, FormatDurableV2, reader.Meta().Format)
		require.NoError(t, reader.Close())
	}

	// Absent durable: the ordinary SQLite residue refusal is unchanged.
	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	require.NoError(t, os.Remove(latestPath))
	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sidecars")
}

// TestCorruptDurableBesideSQLiteFailsClosedNotDowngrade pins the tri-state
// sibling contract: a present-but-invalid durable latest beside a canonical
// SQLite makes targeted reads refuse (no silent downgrade to possibly-stale
// SQLite), while an absent latest keeps the SQLite-only fallback and list mode
// stops advertising the SQLite as current for the durable-authoritative set.
func TestCorruptDurableBesideSQLiteFailsClosedNotDowngrade(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	require.NoError(t, os.WriteFile(latestPath, []byte(`{"type":"","index_set_id":"x"`), 0o600))

	// Targeted reads refuse with the durable diagnostic — never sqlite-v1.
	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable")
	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)

	// List mode advertises neither entry as current for the set; doctor owns
	// the diagnosis surface.
	listed, err := ListIndexReaders(ctx, env.opts)
	require.NoError(t, err)
	for _, item := range listed {
		require.NotEqual(t, env.indexSetID, item.Meta.IndexSetID,
			"durable-authoritative invalid set must not list %s as current", item.Meta.Format)
	}

	// Absent latest restores the SQLite-only fallback.
	require.NoError(t, os.Remove(latestPath))
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatSQLiteV1, reader.Meta().Format)
}

// corruptLegacyIdentity variants make the legacy identity marker untrusted in
// each of the three ways the resolver distinguishes: unreadable JSON, missing
// file, and a validly parsed identity that does not match the canonical
// directory.
func corruptLegacyIdentityVariants(t *testing.T) map[string]func(t *testing.T, env durableTestEnv) {
	t.Helper()
	return map[string]func(t *testing.T, env durableTestEnv){
		"malformed": func(t *testing.T, env durableTestEnv) {
			require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte("{invalid\n"), 0o600))
		},
		"missing": func(t *testing.T, env durableTestEnv) {
			require.NoError(t, os.Remove(filepath.Join(env.identityDir, "identity.json")))
		},
		"mismatch": func(t *testing.T, env durableTestEnv) {
			otherParams := env.params
			otherParams.BaseURI = "s3://test-bucket/other/"
			otherIdentity, err := indexstore.ComputeIndexSetID(otherParams)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte(otherIdentity.CanonicalJSON+"\n"), 0o600))
		},
	}
}

func seedLegacySQLiteBesideDurable(t *testing.T, env durableTestEnv) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// TestValidDurableWinsForFullSetIDWhenLegacyIdentityMalformed pins that a
// corrupt lower-priority legacy identity marker cannot veto a verified durable
// authority identified by the caller's explicit full set ID: the durable
// snapshot is selected without parsing or opening the legacy SQLite. Covers
// malformed, missing, and directory-mismatched identity.json.
func TestValidDurableWinsForFullSetIDWhenLegacyIdentityMalformed(t *testing.T) {
	ctx := context.Background()
	for name, corrupt := range corruptLegacyIdentityVariants(t) {
		t.Run(name, func(t *testing.T) {
			env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
				durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
			})
			seedLegacySQLiteBesideDurable(t, env)
			corrupt(t, env)

			reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
			require.NoError(t, err, "valid durable must win for the full set ID despite untrusted legacy identity")
			require.Equal(t, FormatDurableV2, reader.Meta().Format)
			require.Equal(t, env.indexSetID, reader.Meta().IndexSetID)
			require.NoError(t, reader.Close())
		})
	}
}

// TestInvalidDurableWinsDiagnosticOverMalformedLegacyIdentity pins the
// present-but-invalid arm of the tri-state: the durable diagnostic surfaces —
// never the lower-priority legacy identity error and never a SQLite downgrade.
func TestInvalidDurableWinsDiagnosticOverMalformedLegacyIdentity(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	seedLegacySQLiteBesideDurable(t, env)
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte("{invalid\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(env.segmentRoot, "latest.json"), []byte(`{"type":"","index_set_id":"x"`), 0o600))

	_, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable")
	require.NotContains(t, err.Error(), "requires a valid full index_set_id",
		"the durable diagnostic must win over the legacy identity refusal")
}

// TestAbsentDurableFallsBackToLegacyIdentityRefusal pins the absent arm: with
// no durable latest, the ordinary legacy identity refusal is unchanged.
func TestAbsentDurableFallsBackToLegacyIdentityRefusal(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	seedLegacySQLiteBesideDurable(t, env)
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte("{invalid\n"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(env.segmentRoot, "latest.json")))

	_, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a valid full index_set_id")
}

// TestBaseURIStillRefusesWhenIdentityCannotBindSet pins that a base-URI target
// never rides the durable shortcut: a corrupt identity marker cannot bind a
// base URI, so the targeted read refuses even though a verified durable latest
// exists for the set.
func TestBaseURIStillRefusesWhenIdentityCannotBindSet(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	seedLegacySQLiteBesideDurable(t, env)
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte("{invalid\n"), 0o600))

	_, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.Error(t, err, "a corrupt marker must not bind a base URI to the durable authority")
}

// TestListShowsDurableCurrentWhenLegacyIdentityMalformed pins the list-mode
// mirror: the verified durable lists as current for the set and the corrupt
// legacy SQLite projection is not advertised as a current peer (doctor remains
// the anomaly inventory surface).
func TestListShowsDurableCurrentWhenLegacyIdentityMalformed(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	seedLegacySQLiteBesideDurable(t, env)
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "identity.json"), []byte("{invalid\n"), 0o600))

	listed, err := ListIndexReaders(ctx, env.opts)
	require.NoError(t, err)
	var durableSeen bool
	for _, item := range listed {
		if item.Meta.SourcePath == filepath.Join(env.identityDir, "index.db") {
			t.Fatalf("corrupt legacy SQLite projection must not list as a current peer")
		}
		if item.Meta.IndexSetID == env.indexSetID {
			require.Equal(t, FormatDurableV2, item.Meta.Format)
			durableSeen = true
		}
	}
	require.True(t, durableSeen, "verified durable must list as current for the set")
}

type durableTestEnv struct {
	opts        ResolveOptions
	baseURI     string
	indexSetID  string
	identityDir string
	segmentRoot string
	runID       string
	params      indexstore.IndexSetParams
}

func setupDurableTestEnv(t *testing.T, rows []indexsubstrate.CurrentObjectRow) durableTestEnv {
	t.Helper()
	root := t.TempDir()
	indexesRoot := filepath.Join(root, "indexes")
	segmentCacheRoot := filepath.Join(root, "segments")
	baseURI := "s3://test-bucket/data/"
	params := indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"**"},
		},
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	identityDir := filepath.Join(indexesRoot, identity.DirName)
	require.NoError(t, os.MkdirAll(identityDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(identityDir, "identity.json"), []byte(identity.CanonicalJSON+"\n"), 0o600))

	runID := "run_test_1"
	segmentRoot := filepath.Join(segmentCacheRoot, identity.IndexSetID)
	runDir := filepath.Join(segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))

	for i := range rows {
		rows[i].IndexSetID = identity.IndexSetID
		if rows[i].FirstSeenRunID == "" {
			rows[i].FirstSeenRunID = runID
		}
		if rows[i].LastChangedRunID == "" {
			rows[i].LastChangedRunID = runID
		}
		if rows[i].LastSeenRunID == "" {
			rows[i].LastSeenRunID = runID
		}
	}

	createdAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           identity.IndexSetID,
		RunID:                runID,
		CreatedAt:            createdAt,
		RunStartedAt:         &createdAt,
		TargetRowsPerSegment: 100,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)

	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)

	complete := map[string]any{
		"type":                          indexsubstrate.CompleteMarkerTypeV2,
		"index_set_id":                  identity.IndexSetID,
		"run_id":                        runID,
		"snapshot_completed_at":         createdAt.Add(time.Minute).Format(time.RFC3339Nano),
		"snapshot_completion_semantics": indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
		"manifest_path":                 manifestPath,
		"manifest_sha256":               manifestSHA,
		"segment_dir":                   runDir,
		"segments":                      len(manifest.Segments),
	}
	completePath := filepath.Join(runDir, "complete.json")
	writeJSON(t, completePath, complete)

	latest := map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  identity.IndexSetID,
		"run_id":        runID,
		"updated_at":    createdAt.Format(time.RFC3339Nano),
		"complete_path": completePath,
	}
	writeJSON(t, filepath.Join(segmentRoot, "latest.json"), latest)

	return durableTestEnv{
		opts: ResolveOptions{
			IndexesRoot:      indexesRoot,
			SegmentCacheRoot: segmentCacheRoot,
		},
		baseURI:     baseURI,
		indexSetID:  identity.IndexSetID,
		identityDir: identityDir,
		segmentRoot: segmentRoot,
		runID:       runID,
		params:      params,
	}
}

func writeRunComplete(t *testing.T, env durableTestEnv, runID string, createdAt time.Time) {
	t.Helper()
	runDir := filepath.Join(env.segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           env.indexSetID,
		RunID:                runID,
		CreatedAt:            createdAt,
		RunStartedAt:         &createdAt,
		TargetRowsPerSegment: 100,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, nil)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)
	writeJSON(t, filepath.Join(runDir, "complete.json"), map[string]any{
		"type":                          indexsubstrate.CompleteMarkerTypeV2,
		"index_set_id":                  env.indexSetID,
		"run_id":                        runID,
		"snapshot_completed_at":         createdAt.Add(time.Minute).Format(time.RFC3339Nano),
		"snapshot_completion_semantics": indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
		"manifest_path":                 manifestPath,
		"manifest_sha256":               manifestSHA,
		"segment_dir":                   runDir,
		"segments":                      len(manifest.Segments),
	})
}

func publishDurableLineageRun(
	t *testing.T,
	env durableTestEnv,
	runID string,
	startedAt time.Time,
	lineage *indexsubstrate.LineageRecord,
	parent *indexsubstrate.StateParent,
	rows []indexsubstrate.CurrentObjectRow,
) string {
	t.Helper()
	runDir := filepath.Join(env.segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	for i := range rows {
		rows[i].IndexSetID = env.indexSetID
	}
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           env.indexSetID,
		RunID:                runID,
		CreatedAt:            startedAt,
		RunStartedAt:         &startedAt,
		StateParent:          parent,
		Lineage:              lineage,
		TargetRowsPerSegment: 100,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)
	writeJSON(t, filepath.Join(runDir, "complete.json"), map[string]any{
		"type":                          indexsubstrate.CompleteMarkerTypeV2,
		"index_set_id":                  env.indexSetID,
		"run_id":                        runID,
		"snapshot_completed_at":         startedAt.Add(time.Minute).Format(time.RFC3339Nano),
		"snapshot_completion_semantics": indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
		"manifest_path":                 manifestPath,
		"manifest_sha256":               manifestSHA,
		"segment_dir":                   runDir,
		"segments":                      len(manifest.Segments),
	})
	return manifestSHA
}

func durableRow(relKey string, size int64, etag string, mod time.Time) indexsubstrate.CurrentObjectRow {
	return indexsubstrate.CurrentObjectRow{
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &mod,
		ETag:             etag,
		FirstSeenRunID:   "run_test_1",
		FirstSeenAt:      mod,
		LastChangedRunID: "run_test_1",
		LastChangedAt:    mod,
		LastSeenRunID:    "run_test_1",
		LastSeenAt:       mod,
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}

func hashFileSHA256(path string) (string, error) {
	// Use OpenLatest trust path hashing via reading publish helpers: recompute with os.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256Sum(data)
	return sum, nil
}

func TestSegmentMayMatch(t *testing.T) {
	tests := []struct {
		name      string
		minRelKey string
		maxRelKey string
		filter    *rowFilter
		want      bool
	}{
		{
			name:      "nil filter never skips",
			minRelKey: "c/01",
			maxRelKey: "c/99",
			filter:    nil,
			want:      true,
		},
		{
			name:      "empty prefix never skips",
			minRelKey: "c/01",
			maxRelKey: "c/99",
			filter:    &rowFilter{prefix: ""},
			want:      true,
		},
		{
			name:      "segment wholly before prefix",
			minRelKey: "a/01",
			maxRelKey: "a/99",
			filter:    &rowFilter{prefix: "b/"},
			want:      false,
		},
		{
			name:      "segment wholly after prefix",
			minRelKey: "c/01",
			maxRelKey: "c/99",
			filter:    &rowFilter{prefix: "b/"},
			want:      false,
		},
		{
			name:      "delimiter-adjacent lower bound after prefix ('0' > '/')",
			minRelKey: "site/dev/2026-01-010/a.xml",
			maxRelKey: "site/dev/2026-01-010/z.xml",
			filter:    &rowFilter{prefix: "site/dev/2026-01-01/"},
			want:      false,
		},
		{
			name:      "delimiter-adjacent upper bound before prefix ('.' < '/')",
			minRelKey: "site/dev/2026-01-01.a",
			maxRelKey: "site/dev/2026-01-01.z",
			filter:    &rowFilter{prefix: "site/dev/2026-01-01/"},
			want:      false,
		},
		{
			name:      "minRelKey starts with prefix",
			minRelKey: "b/01",
			maxRelKey: "c/99",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "maxRelKey starts with prefix",
			minRelKey: "a/01",
			maxRelKey: "b/01",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "segment straddles prefix without endpoints starting with prefix",
			minRelKey: "a/z",
			maxRelKey: "c/a",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "prefix equals minRelKey",
			minRelKey: "b/",
			maxRelKey: "c/a",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "prefix equals maxRelKey",
			minRelKey: "a/a",
			maxRelKey: "b/",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "minRelKey is strict prefix of filter prefix",
			minRelKey: "b/",
			maxRelKey: "b/z",
			filter:    &rowFilter{prefix: "b/sub/"},
			want:      true,
		},
		{
			name:      "high-byte prefix matching segment",
			minRelKey: "b/\xff/a",
			maxRelKey: "b/\xff/z",
			filter:    &rowFilter{prefix: "b/\xff/"},
			want:      true,
		},
		{
			name:      "high-byte prefix lower-bound skip",
			minRelKey: "b/\xff0/a",
			maxRelKey: "b/\xff0/z",
			filter:    &rowFilter{prefix: "b/\xff/"},
			want:      false,
		},
		{
			name:      "empty minRelKey and maxRelKey never skip",
			minRelKey: "",
			maxRelKey: "",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "empty minRelKey with maxRelKey after prefix never skips via lower bound",
			minRelKey: "",
			maxRelKey: "c/99",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "empty maxRelKey with minRelKey before prefix never skips via upper bound",
			minRelKey: "a/01",
			maxRelKey: "",
			filter:    &rowFilter{prefix: "b/"},
			want:      true,
		},
		{
			name:      "inverted bounds fail-safe never skips",
			minRelKey: "z/01",
			maxRelKey: "a/01",
			filter:    &rowFilter{prefix: "m/"},
			want:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			desc := indexsubstrate.SegmentDescriptor{
				MinRelKey: tc.minRelKey,
				MaxRelKey: tc.maxRelKey,
			}
			require.Equal(t, tc.want, segmentMayMatch(desc, tc.filter))
		})
	}
}

func TestSegmentMayMatch_RandomSortedDisjointParity(t *testing.T) {
	alphabet := []byte{'a', 'b', 'c', '.', '/', '0', '\xff'}
	var state uint64 = 0x9e3779b97f4a7c15
	nextRand := func() uint64 {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	randString := func(minLen, maxLen int) string {
		n := minLen + int(nextRand()%uint64(maxLen-minLen+1))
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = alphabet[nextRand()%uint64(len(alphabet))]
		}
		return string(buf)
	}

	mod := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	deletedAt := mod.Add(time.Hour)

	for trial := 0; trial < 60; trial++ {
		keySet := make(map[string]struct{}, 80)
		for len(keySet) < 64 {
			keySet[randString(1, 8)] = struct{}{}
		}
		keys := make([]string, 0, len(keySet))
		for k := range keySet {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		rows := make([]indexsubstrate.CurrentObjectRow, len(keys))
		for i, k := range keys {
			row := durableRow(k, int64(i+1), "etag", mod)
			if nextRand()%4 == 0 {
				row.DeletedAt = &deletedAt
			}
			rows[i] = row
		}

		// Partition sorted rows into disjoint segments of varying sizes (1..7 rows).
		type segData struct {
			desc indexsubstrate.SegmentDescriptor
			rows []indexsubstrate.CurrentObjectRow
		}
		var segments []segData
		for idx := 0; idx < len(rows); {
			step := 1 + int(nextRand()%7)
			if idx+step > len(rows) {
				step = len(rows) - idx
			}
			chunk := rows[idx : idx+step]
			segments = append(segments, segData{
				desc: indexsubstrate.SegmentDescriptor{
					MinRelKey: chunk[0].RelKey,
					MaxRelKey: chunk[len(chunk)-1].RelKey,
				},
				rows: chunk,
			})
			idx += step
		}

		for q := 0; q < 40; q++ {
			var prefix string
			switch q {
			case 0:
				prefix = ""
			case 1:
				prefix = keys[int(nextRand()%uint64(len(keys)))]
			default:
				prefix = randString(1, 5)
			}
			pattern := prefix + "**"
			if prefix == "" {
				pattern = "**"
			}

			for _, includeDeleted := range []bool{false, true} {
				params := indexstore.QueryParams{
					Pattern:        pattern,
					IncludeDeleted: includeDeleted,
				}
				filter, err := compileRowFilter(params)
				require.NoError(t, err)

				var unpruned, pruned []string
				for _, seg := range segments {
					for _, row := range seg.rows {
						res, ok, err := filterCurrentRow(row, filter, params)
						require.NoError(t, err)
						if ok {
							unpruned = append(unpruned, res.RelKey)
						}
					}
					if !segmentMayMatch(seg.desc, filter) {
						continue
					}
					for _, row := range seg.rows {
						res, ok, err := filterCurrentRow(row, filter, params)
						require.NoError(t, err)
						if ok {
							pruned = append(pruned, res.RelKey)
						}
					}
				}
				require.Equal(t, unpruned, pruned, "trial=%d q=%d prefix=%q includeDeleted=%v", trial, q, prefix, includeDeleted)
			}
		}
	}
}

func TestDurableQuery_PrefixPrunesBeforeAndAfterSegments(t *testing.T) {
	ctx := context.Background()
	mod := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	deletedAt := mod.Add(time.Hour)

	env := setupDurableTestEnv(t, nil)
	runID := "run_multiseg_0001"
	runDir := filepath.Join(env.segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))

	tombstoneBoundary := durableRow("bravo/2026-01-01/00-deleted.json", 15, "etag-del", mod)
	tombstoneBoundary.DeletedAt = &deletedAt

	rows := []indexsubstrate.CurrentObjectRow{
		// Segment 0: wholly before bravo/2026-01-01/
		durableRow("alpha/01.json", 10, "e1", mod),
		durableRow("alpha/02.json", 20, "e2", mod),
		// Segment 1: target prefix bravo/2026-01-01/ (MinRelKey is a tombstone)
		tombstoneBoundary,
		durableRow("bravo/2026-01-01/active.json", 30, "e3", mod),
		// Segment 2: delimiter-adjacent lower bound ('0' > '/') after bravo/2026-01-01/
		durableRow("bravo/2026-01-010/a.json", 40, "e4", mod),
		durableRow("cappa/01.json", 50, "e5", mod),
		// Segment 3: wholly after
		durableRow("delta/01.json", 60, "e6", mod),
		durableRow("delta/02.json", 70, "e7", mod),
	}
	for i := range rows {
		rows[i].IndexSetID = env.indexSetID
		rows[i].FirstSeenRunID = runID
		rows[i].LastChangedRunID = runID
		rows[i].LastSeenRunID = runID
	}

	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           env.indexSetID,
		RunID:                runID,
		CreatedAt:            mod,
		RunStartedAt:         &mod,
		TargetRowsPerSegment: 2,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 4)

	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)

	completePath := filepath.Join(runDir, "complete.json")
	writeJSON(t, completePath, map[string]any{
		"type":                          indexsubstrate.CompleteMarkerTypeV2,
		"index_set_id":                  env.indexSetID,
		"run_id":                        runID,
		"snapshot_completed_at":         mod.Add(time.Minute).Format(time.RFC3339Nano),
		"snapshot_completion_semantics": indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
		"manifest_path":                 manifestPath,
		"manifest_sha256":               manifestSHA,
		"segment_dir":                   runDir,
		"segments":                      len(manifest.Segments),
	})
	writeJSON(t, filepath.Join(env.segmentRoot, "latest.json"), map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  env.indexSetID,
		"run_id":        runID,
		"updated_at":    mod.Format(time.RFC3339Nano),
		"complete_path": completePath,
	})

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: runID})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	// 1. Middle segment query: prunes 1 segment before (upper-bound) and 2 segments after (lower-bound).
	results, stats, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "bravo/2026-01-01/**",
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "bravo/2026-01-01/active.json", results[0].RelKey)
	require.EqualValues(t, 2, stats.Examined)
	require.EqualValues(t, 1, stats.Matched)
	require.Equal(t, 1, stats.SegmentsWalked)
	require.Equal(t, 1, stats.SegmentsVerified)
	require.Equal(t, 3, stats.SegmentsManifestPruned)

	// IncludeDeleted on the same prefix still walks segment 1 and returns both the tombstone and active row.
	resultsWithDeleted, statsWithDeleted, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		Pattern:        "bravo/2026-01-01/**",
		IncludeDeleted: true,
	})
	require.NoError(t, err)
	require.Len(t, resultsWithDeleted, 2)
	require.Equal(t, 1, statsWithDeleted.SegmentsWalked)
	require.Equal(t, 1, statsWithDeleted.SegmentsVerified)
	require.Equal(t, 3, statsWithDeleted.SegmentsManifestPruned)

	// 2. First segment query: prunes all 3 trailing segments via lower-bound skip.
	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "alpha/**",
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.EqualValues(t, 2, stats.Examined)
	require.Equal(t, 1, stats.SegmentsWalked)
	require.Equal(t, 1, stats.SegmentsVerified)
	require.Equal(t, 3, stats.SegmentsManifestPruned)

	// 3. Last segment query: prunes all 3 leading segments via upper-bound skip.
	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "delta/**",
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.EqualValues(t, 2, stats.Examined)
	require.Equal(t, 1, stats.SegmentsWalked)
	require.Equal(t, 1, stats.SegmentsVerified)
	require.Equal(t, 3, stats.SegmentsManifestPruned)

	// 4. Prefix falling strictly between segments 1 and 2: prunes all 4 segments without opening any file.
	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "bravo/2026-01-01/z/**",
	})
	require.NoError(t, err)
	require.Empty(t, results)
	require.Zero(t, stats.Examined)
	require.Zero(t, stats.SegmentsWalked)
	require.Zero(t, stats.SegmentsVerified)
	require.Equal(t, 4, stats.SegmentsManifestPruned)

	// 5. Unfiltered query and leading-glob query (empty derived prefix): walks and verifies all 4 segments.
	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
	})
	require.NoError(t, err)
	require.Len(t, results, 7)
	require.EqualValues(t, 8, stats.Examined)
	require.Equal(t, 4, stats.SegmentsWalked)
	require.Equal(t, 4, stats.SegmentsVerified)
	require.Zero(t, stats.SegmentsManifestPruned)

	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "**/*.json",
	})
	require.NoError(t, err)
	require.Len(t, results, 7)
	require.EqualValues(t, 8, stats.Examined)
	require.Equal(t, 4, stats.SegmentsWalked)
	require.Equal(t, 4, stats.SegmentsVerified)
	require.Zero(t, stats.SegmentsManifestPruned)

	// 6. Corrupt pruned segments (0, 2, 3) on disk: narrow prefix query still succeeds without reading them,
	// while an unfiltered query fails digest verification.
	for _, idx := range []int{0, 2, 3} {
		segPath := filepath.Join(runDir, filepath.FromSlash(manifest.Segments[idx].Path))
		require.NoError(t, os.WriteFile(segPath, []byte("corrupted-segment-bytes"), 0o600))
	}
	results, stats, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "bravo/2026-01-01/**",
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 1, stats.SegmentsWalked)
	require.Equal(t, 1, stats.SegmentsVerified)
	require.Equal(t, 3, stats.SegmentsManifestPruned)

	_, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
	})
	require.Error(t, err)
}

func TestOpenDurableReaderRejectsSetIDMismatchBetweenDiscoveryAndOpen(t *testing.T) {
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a/one.json", 100, "etag-a", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	opts := normalizeResolveOptions(env.opts)

	_, err := openDurableReader(opts, candidate{
		meta: Meta{
			Format:     FormatDurableV2,
			IndexSetID: "idx_" + strings.Repeat("f", 64),
			SourcePath: latestPath,
		},
		latest: latestPath,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "discovered index_set_id")
}
