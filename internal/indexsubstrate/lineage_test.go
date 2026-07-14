package indexsubstrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateManifestLineageStructure_LegacyAbsent(t *testing.T) {
	t.Parallel()
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID: "idx_test",
		RunID:      "run_a",
	})
	require.NoError(t, err)
	require.False(t, HasContinuousLineage(InternalManifest{IndexSetID: "idx_test", RunID: "run_a"}))
}

func TestValidateManifestLineageStructure_StateParentWithoutLineageRefused(t *testing.T) {
	t.Parallel()
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID: "idx_test",
		RunID:      "run_a",
		StateParent: &StateParent{
			IndexSetID:     "idx_test",
			RunID:          "run_b",
			ManifestSHA256: strings.Repeat("a", 64),
		},
	})
	require.True(t, IsLineageCode(err, LineageCodePartial), "got %v", err)
}

func TestValidateManifestLineageStructure_BaselineRequiresRunStart(t *testing.T) {
	t.Parallel()
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID: "idx_test",
		RunID:      "run_a",
		Lineage:    &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.True(t, IsLineageCode(err, LineageCodeInvalidTime), "got %v", err)
}

func TestValidateManifestLineageStructure_ZeroRunStartRefused(t *testing.T) {
	t.Parallel()
	zero := time.Time{}
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &zero,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.True(t, IsLineageCode(err, LineageCodeInvalidTime), "got %v", err)
}

func TestValidateManifestLineageStructure_NonUTCRunStartRefused(t *testing.T) {
	t.Parallel()
	// Fixed zone +01:00 must fail closed (wire rule is UTC only).
	nonUTC := time.Date(2026, 7, 14, 12, 0, 0, 0, time.FixedZone("CET", 3600))
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &nonUTC,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.True(t, IsLineageCode(err, LineageCodeInvalidTime), "got %v", err)
}

func TestValidateManifestLineageStructure_DeterministicFarFutureUTCAccepted(t *testing.T) {
	t.Parallel()
	// No wall-clock-relative refuse: far-future UTC is structurally valid.
	far := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &far,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.NoError(t, err)
}

func TestValidateManifestLineageStructure_BaselineWithStateParentAllowed(t *testing.T) {
	t.Parallel()
	// Generation-1 baseline may bind one exact pre-continuity state source.
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID:     "idx_test",
			RunID:          "run_legacy",
			ManifestSHA256: strings.Repeat("a", 64),
		},
	})
	require.NoError(t, err)
}

func TestValidateManifestLineageStructure_UnsafeOwnIdentityRefused(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "../run_base",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.True(t, IsLineageCode(err, LineageCodeMalformed), "got %v", err)
}

func TestValidateManifestLineageStructure_ValidBaseline(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.NoError(t, err)
}

func TestValidateManifestLineageStructure_UnknownVersion(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_a",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: 99, Generation: 1, Baseline: true},
	})
	require.True(t, IsLineageCode(err, LineageCodeUnknownVersion), "got %v", err)
}

func TestValidateManifestLineageStructure_CrossSetParent(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_child",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID:     "idx_other",
			RunID:          "run_parent",
			ManifestSHA256: strings.Repeat("b", 64),
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeCrossSet), "got %v", err)
}

func TestValidateManifestLineageStructure_UppercaseDigestRefused(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_child",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID:     "idx_test",
			RunID:          "run_parent",
			ManifestSHA256: strings.Repeat("A", 64),
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeInvalidDigest), "got %v", err)
}

func TestValidateManifestLineageStructure_SelfCycle(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	err := ValidateManifestLineageStructure(InternalManifest{
		IndexSetID:   "idx_test",
		RunID:        "run_self",
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID:     "idx_test",
			RunID:          "run_self",
			ManifestSHA256: strings.Repeat("1", 64),
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeCycle), "got %v", err)
}

func TestManifestLineageSerializeReparse_Additive(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()

	legacy, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_legacy", CreatedAt: base, TargetRowsPerSegment: 10,
	}, []CurrentObjectRow{testRow("idx_test", "run_legacy", "a", base)})
	require.NoError(t, err)
	require.Nil(t, legacy.RunStartedAt)
	require.Nil(t, legacy.StateParent)
	require.Nil(t, legacy.Lineage)

	legacyPath := filepath.Join(dir, "legacy-manifest.json")
	require.NoError(t, WriteInternalManifestFile(legacyPath, legacy))
	raw, err := os.ReadFile(legacyPath)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "run_started_at")
	require.NotContains(t, string(raw), "state_parent")
	require.NotContains(t, string(raw), `"lineage"`)

	parentDigest := strings.Repeat("c", 64)
	with := legacy
	with.RunID = "run_child"
	started := base.Add(-time.Minute)
	with.RunStartedAt = &started
	with.Lineage = &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false}
	with.StateParent = &StateParent{IndexSetID: "idx_test", RunID: "run_parent", ManifestSHA256: parentDigest}
	require.NoError(t, ValidateManifestLineageStructure(with))

	childPath := filepath.Join(dir, "child-manifest.json")
	require.NoError(t, WriteInternalManifestFile(childPath, with))
	loaded, err := ReadInternalManifestFile(childPath)
	require.NoError(t, err)
	require.NotNil(t, loaded.RunStartedAt)
	require.Equal(t, started.UTC(), loaded.RunStartedAt.UTC())
	require.Equal(t, with.Lineage, loaded.Lineage)
	require.Equal(t, with.StateParent, loaded.StateParent)

	type legacyMirror struct {
		Type               string    `json:"type"`
		IndexSetID         string    `json:"index_set_id"`
		RunID              string    `json:"run_id"`
		IndexSchemaVersion int       `json:"index_schema_version"`
		CreatedAt          time.Time `json:"created_at"`
	}
	childRaw, err := os.ReadFile(childPath)
	require.NoError(t, err)
	var mirror legacyMirror
	require.NoError(t, json.Unmarshal(childRaw, &mirror))
	require.Equal(t, "run_child", mirror.RunID)
	require.Equal(t, IndexSchemaVersion, mirror.IndexSchemaVersion)
}

func TestResolveAncestry_Legacy(t *testing.T) {
	t.Parallel()
	fx := publishLineageFixtureAt(t, t.TempDir(), lineageFixtureSpec{RunID: "run_legacy"})
	got, err := ResolveAncestry(fx.snap, AncestryResolveConfig{})
	require.NoError(t, err)
	require.Equal(t, AncestryModeLegacy, got.Mode)
	require.Empty(t, got.Chain)

	_, err = ResolveAncestry(fx.snap, AncestryResolveConfig{RequireContinuous: true})
	require.True(t, IsLineageCode(err, LineageCodeRequireContinuous), "got %v", err)
}

func TestResolveAncestry_BaselineOnly(t *testing.T) {
	t.Parallel()
	fx := publishLineageFixtureAt(t, t.TempDir(), lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	got, err := ResolveAncestry(fx.snap, AncestryResolveConfig{})
	require.NoError(t, err)
	require.Equal(t, AncestryModeContinuous, got.Mode)
	require.Len(t, got.Chain, 1)
	require.True(t, got.Chain[0].Baseline)
	require.True(t, got.Chain[0].DeltaBoundary)
	require.False(t, got.Chain[0].RunStartedAt.IsZero())
	require.NotNil(t, got.DeltaBoundary)
	require.Equal(t, "run_base", got.DeltaBoundary.RunID)
	require.Equal(t, fx.snap.AccountedBytes(), got.AccountedBytes)
}

func TestResolveAncestry_BudgetBytesRootOnly(t *testing.T) {
	t.Parallel()
	fx := publishLineageFixtureAt(t, t.TempDir(), lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	require.Greater(t, fx.snap.AccountedBytes(), int64(1))

	_, err := ResolveAncestry(fx.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxAggregateBytes: 1, MaxDepth: 64, MaxNodes: 64},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetBytes), "got %v", err)

	// Exact limit succeeds.
	got, err := ResolveAncestry(fx.snap, AncestryResolveConfig{
		Budget: AncestryBudget{
			MaxAggregateBytes: fx.snap.AccountedBytes(),
			MaxDepth:          64,
			MaxNodes:          64,
		},
	})
	require.NoError(t, err)
	require.Equal(t, fx.snap.AccountedBytes(), got.AccountedBytes)
}

func TestResolveAncestry_MultiHop(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	lookup := map[string]string{}

	base := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_0",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	lookup[manifestKey("idx_test", "run_0")] = base.completePath

	mid := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_1",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_0", ManifestSHA256: base.manifestSHA,
		},
	})
	lookup[manifestKey("idx_test", "run_1")] = mid.completePath

	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_2",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 3, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_1", ManifestSHA256: mid.manifestSHA,
		},
	})

	got, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) {
			path, ok := lookup[manifestKey(indexSetID, runID)]
			if !ok {
				return "", os.ErrNotExist
			}
			return path, nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, AncestryModeContinuous, got.Mode)
	require.Len(t, got.Chain, 3)
	require.Equal(t, []string{"run_2", "run_1", "run_0"}, []string{got.Chain[0].RunID, got.Chain[1].RunID, got.Chain[2].RunID})
	require.True(t, got.DeltaBoundary.Baseline)
	require.Equal(t, "run_0", got.DeltaBoundary.RunID)
	require.Equal(t, child.snap.AccountedBytes()+mid.snap.AccountedBytes()+base.snap.AccountedBytes(), got.AccountedBytes)
}

func TestResolveAncestry_BudgetBytesMultiHopPlusOne(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	lookup := map[string]string{}
	base := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_0",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	lookup[manifestKey("idx_test", "run_0")] = base.completePath
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_1",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_0", ManifestSHA256: base.manifestSHA,
		},
	})
	total := child.snap.AccountedBytes() + base.snap.AccountedBytes()
	require.Greater(t, total, int64(0))

	// Exact total succeeds.
	got, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxAggregateBytes: total, MaxDepth: 64, MaxNodes: 64},
		Lookup: func(indexSetID, runID string) (string, error) {
			return lookup[manifestKey(indexSetID, runID)], nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, total, got.AccountedBytes)

	// total-1 refuses.
	_, err = ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxAggregateBytes: total - 1, MaxDepth: 64, MaxNodes: 64},
		Lookup: func(indexSetID, runID string) (string, error) {
			return lookup[manifestKey(indexSetID, runID)], nil
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetBytes), "got %v", err)
}

func TestResolveAncestry_DigestMismatch(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	parent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_p",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_p", ManifestSHA256: strings.Repeat("d", 64),
		},
	})
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return parent.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeDigestMismatch), "got %v", err)
}

func TestResolveAncestry_MissingParent(t *testing.T) {
	t.Parallel()
	child := publishLineageFixtureAt(t, t.TempDir(), lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_missing", ManifestSHA256: strings.Repeat("e", 64),
		},
	})
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return "", os.ErrNotExist },
	})
	require.True(t, IsLineageCode(err, LineageCodeMissingParent), "got %v", err)
}

func TestResolveAncestry_TwoNodeCycle(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	// A gen=3 → B gen=2 → A. First hop generation-valid; second hop cycle before IO.
	// Build B first as temporary baseline, then rewrite both edges after digests exist.
	b := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_b",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		// Temporary parent; rewritten after A exists.
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_a", ManifestSHA256: strings.Repeat("f", 64),
		},
	})
	a := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_a",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 3, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_b", ManifestSHA256: b.manifestSHA,
		},
	})
	// Rewrite B's published artifacts so state_parent points at A with correct digest.
	rewriteStateParent(t, b, StateParent{
		IndexSetID: "idx_test", RunID: "run_a", ManifestSHA256: a.manifestSHA,
	}, &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false})
	bReopen, err := OpenPublishedRunSnapshot(b.completePath, "idx_test", "run_b")
	require.NoError(t, err)
	// B's digest changed after rewrite; point A at the new B digest.
	rewriteStateParent(t, a, StateParent{
		IndexSetID: "idx_test", RunID: "run_b", ManifestSHA256: bReopen.Complete.ManifestSHA256,
	}, &LineageRecord{Version: LineageVersionV1, Generation: 3, Baseline: false})
	aReopen, err := OpenPublishedRunSnapshot(a.completePath, "idx_test", "run_a")
	require.NoError(t, err)

	_, err = ResolveAncestry(aReopen, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) {
			switch runID {
			case "run_a":
				return a.completePath, nil
			case "run_b":
				return b.completePath, nil
			default:
				return "", os.ErrNotExist
			}
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeCycle), "got %v code=%s", err, LineageCodeOf(err))
}

func TestResolveAncestry_ParentUnknownVersionPreserved(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	// Parent baseline first, then corrupt lineage.version to 99 in-place and
	// refresh complete digest so open reaches structural lineage validation.
	parent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_p",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	corruptLineageVersion(t, parent, 99)
	parentReopen, err := OpenPublishedRunSnapshot(parent.completePath, "idx_test", "run_p")
	// Open itself should refuse unknown version.
	require.True(t, IsLineageCode(err, LineageCodeUnknownVersion), "open got %v", err)
	_ = parentReopen

	// Child still points at pre-corrupt digest would fail digest; point at new digest.
	// Re-read complete after corrupt for new digest.
	completeRaw, err := os.ReadFile(parent.completePath)
	require.NoError(t, err)
	var complete publishedCompleteDoc
	require.NoError(t, json.Unmarshal(completeRaw, &complete))

	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_p", ManifestSHA256: complete.ManifestSHA256,
		},
	})
	_, err = ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return parent.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeUnknownVersion), "got %v code=%s", err, LineageCodeOf(err))
}

func TestResolveAncestry_GenerationSkip(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	parent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_p",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 5, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_p", ManifestSHA256: parent.manifestSHA,
		},
	})
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return parent.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeGeneration), "got %v", err)
}

func TestResolveAncestry_BudgetDepth(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	lookup := map[string]string{}
	base := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_0",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	lookup[manifestKey("idx_test", "run_0")] = base.completePath
	r1 := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:       "run_1",
		Lineage:     &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{IndexSetID: "idx_test", RunID: "run_0", ManifestSHA256: base.manifestSHA},
	})
	lookup[manifestKey("idx_test", "run_1")] = r1.completePath
	r2 := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:       "run_2",
		Lineage:     &LineageRecord{Version: LineageVersionV1, Generation: 3, Baseline: false},
		StateParent: &StateParent{IndexSetID: "idx_test", RunID: "run_1", ManifestSHA256: r1.manifestSHA},
	})

	_, err := ResolveAncestry(r2.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 1, MaxNodes: 64, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			path, ok := lookup[manifestKey(indexSetID, runID)]
			if !ok {
				return "", os.ErrNotExist
			}
			return path, nil
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetDepth), "got %v", err)

	got, err := ResolveAncestry(r2.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 2, MaxNodes: 64, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			return lookup[manifestKey(indexSetID, runID)], nil
		},
	})
	require.NoError(t, err)
	require.Len(t, got.Chain, 3)
}

func TestResolveAncestry_BudgetNodesZeroLookup(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	base := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_0",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_1",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_0", ManifestSHA256: base.manifestSHA,
		},
	})
	lookups := 0
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 64, MaxNodes: 1, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			lookups++
			return base.completePath, nil
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetNodes), "got %v", err)
	require.Equal(t, 0, lookups, "MaxNodes=1 must not invoke parent lookup")
}

func TestResolveAncestry_SyntheticRootRefused(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	synthetic := PublishedSnapshot{
		Manifest: InternalManifest{
			IndexSetID:   "idx_test",
			RunID:        "run_synth",
			RunStartedAt: &started,
			Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		},
		AccountedMarkerBytes:   1,
		AccountedManifestBytes: 1,
	}
	_, err := ResolveAncestry(synthetic, AncestryResolveConfig{})
	require.True(t, IsLineageCode(err, LineageCodeMalformed), "got %v", err)
}

func TestResolveAncestry_BaselineWithLegacyParent(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	legacy := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: legacy.manifestSHA,
		},
	})
	// Exact allowance: baseline + legacy parent = 2 graph nodes, depth edge 1.
	got, err := ResolveAncestry(baseline.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 1, MaxNodes: 2, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			if runID == "run_pre" {
				return legacy.completePath, nil
			}
			return "", os.ErrNotExist
		},
	})
	require.NoError(t, err)
	require.Equal(t, AncestryModeContinuous, got.Mode)
	require.Len(t, got.Chain, 1)
	require.True(t, got.DeltaBoundary.Baseline)
	require.Equal(t, "run_base", got.DeltaBoundary.RunID)
	require.Equal(t, baseline.snap.AccountedBytes()+legacy.snap.AccountedBytes(), got.AccountedBytes)
}

func TestResolveAncestry_BaselineWithLegacyParentMaxNodesOneZeroLookup(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	legacy := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: legacy.manifestSHA,
		},
	})
	lookups := 0
	_, err := ResolveAncestry(baseline.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 64, MaxNodes: 1, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			lookups++
			return legacy.completePath, nil
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetNodes), "got %v", err)
	require.Equal(t, 0, lookups)
}

func TestResolveAncestry_ChildBaselineLegacyMaxDepthOneZeroLegacyLookup(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	legacy := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: legacy.manifestSHA,
		},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_child",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_base", ManifestSHA256: baseline.manifestSHA,
		},
	})
	legacyLookups := 0
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 1, MaxNodes: 64, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			switch runID {
			case "run_base":
				return baseline.completePath, nil
			case "run_pre":
				legacyLookups++
				return legacy.completePath, nil
			default:
				return "", os.ErrNotExist
			}
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetDepth), "got %v", err)
	require.Equal(t, 0, legacyLookups, "legacy parent must not be looked up when depth budget is exhausted")
}

func TestResolveAncestry_BaselineRejectsContinuousParent(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	contParent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_other_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_other_base", ManifestSHA256: contParent.manifestSHA,
		},
	})
	_, err := ResolveAncestry(baseline.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 2, MaxNodes: 4, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			return contParent.completePath, nil
		},
	})
	require.True(t, IsLineageCode(err, LineageCodeBaselineConflict), "got %v", err)
}

func TestResolveAncestry_BaselineWithLegacyParentDigestMismatch(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	legacy := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: strings.Repeat("d", 64),
		},
	})
	_, err := ResolveAncestry(baseline.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return legacy.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeDigestMismatch), "got %v", err)
}

func TestResolveAncestry_ChildToBaselineWithLegacyParent(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	legacy := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	baseline := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_base",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: legacy.manifestSHA,
		},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_child",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_base", ManifestSHA256: baseline.manifestSHA,
		},
	})
	lookup := map[string]string{
		manifestKey("idx_test", "run_pre"):  legacy.completePath,
		manifestKey("idx_test", "run_base"): baseline.completePath,
	}
	// Exact allowance: child→baseline continuous edge (depth 1) + baseline→legacy edge (depth 2).
	got, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 2, MaxNodes: 3, MaxAggregateBytes: DefaultAncestryMaxAggregateBytes},
		Lookup: func(indexSetID, runID string) (string, error) {
			path, ok := lookup[manifestKey(indexSetID, runID)]
			if !ok {
				return "", os.ErrNotExist
			}
			return path, nil
		},
	})
	require.NoError(t, err)
	require.Len(t, got.Chain, 2)
	require.Equal(t, "run_base", got.DeltaBoundary.RunID)
	// Child + baseline continuous hops; pre-continuity legacy open also charged at baseline.
	require.Equal(t, child.snap.AccountedBytes()+baseline.snap.AccountedBytes()+legacy.snap.AccountedBytes(), got.AccountedBytes)
}

func TestResolveAncestry_AggregateBudgetNoOverread(t *testing.T) {
	// Serial: package-level read hook must not race with parallel tests.
	rootDir := t.TempDir()
	base := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_0",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_1",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_0", ManifestSHA256: base.manifestSHA,
		},
	})
	require.Greater(t, child.snap.AccountedBytes(), int64(0))
	require.Greater(t, base.snap.AccountedBytes(), int64(10))

	// Exact root bytes + 1: parent open has only 1 byte remaining.
	budget := child.snap.AccountedBytes() + 1
	var mu sync.Mutex
	var parentBytes int
	afterFileReadForTestMu.Lock()
	prev := afterFileReadForTest
	afterFileReadForTest = func(path string, n int) {
		if path == base.completePath || path == base.snap.Complete.ManifestPath {
			mu.Lock()
			parentBytes += n
			mu.Unlock()
		}
	}
	afterFileReadForTestMu.Unlock()
	t.Cleanup(func() {
		afterFileReadForTestMu.Lock()
		afterFileReadForTest = prev
		afterFileReadForTestMu.Unlock()
	})

	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Budget: AncestryBudget{MaxDepth: 64, MaxNodes: 64, MaxAggregateBytes: budget},
		Lookup: func(indexSetID, runID string) (string, error) { return base.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeBudgetBytes), "got %v", err)
	mu.Lock()
	gotParent := parentBytes
	mu.Unlock()
	// Full parent open would charge base.AccountedBytes(). Cap leaves 1 remaining.
	require.Less(t, gotParent, int(base.snap.AccountedBytes()),
		"parent same-bytes open must not complete under remaining=1 (parentBytes=%d full=%d)",
		gotParent, base.snap.AccountedBytes())
	require.LessOrEqual(t, gotParent, 2, "parent over-read under aggregate remaining cap: %d", gotParent)
}

func TestResolveAncestry_TOCTOUSameBytes(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	parent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_p",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_p", ManifestSHA256: parent.manifestSHA,
		},
	})
	// Replace parent manifest bytes while complete digest still names the original.
	require.NoError(t, os.WriteFile(parent.snap.Complete.ManifestPath, []byte(`{"type":"tampered"}`), 0o600))

	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return parent.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeDigestMismatch), "got %v code=%s", err, LineageCodeOf(err))
}

func TestResolveAncestry_ParentWithoutLineageRefused(t *testing.T) {
	t.Parallel()
	rootDir := t.TempDir()
	parent := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{RunID: "run_pre"})
	child := publishLineageFixtureAt(t, rootDir, lineageFixtureSpec{
		RunID:   "run_c",
		Lineage: &LineageRecord{Version: LineageVersionV1, Generation: 2, Baseline: false},
		StateParent: &StateParent{
			IndexSetID: "idx_test", RunID: "run_pre", ManifestSHA256: parent.manifestSHA,
		},
	})
	_, err := ResolveAncestry(child.snap, AncestryResolveConfig{
		Lookup: func(indexSetID, runID string) (string, error) { return parent.completePath, nil },
	})
	require.True(t, IsLineageCode(err, LineageCodeGeneration), "got %v", err)
}

func TestOpenPublishedSnapshot_LegacyStillOpens(t *testing.T) {
	t.Parallel()
	fx := publishLineageFixtureAt(t, t.TempDir(), lineageFixtureSpec{RunID: "run_legacy"})
	reopened, err := OpenPublishedRunSnapshot(fx.completePath, "idx_test", "run_legacy")
	require.NoError(t, err)
	require.Equal(t, "run_legacy", reopened.Manifest.RunID)
	require.Nil(t, reopened.Manifest.Lineage)
	require.Greater(t, reopened.AccountedBytes(), int64(0))
	require.Equal(t, fx.completePath, reopened.CompletePath)
}

func TestOpenPublishedSnapshot_MalformedLineageRefused(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	segDir := filepath.Join(root, "segments")
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: segDir, IndexSetID: "idx_test", RunID: "run_bad", CreatedAt: base, TargetRowsPerSegment: 10,
	}, []CurrentObjectRow{testRow("idx_test", "run_bad", "k", base)})
	require.NoError(t, err)
	started := base
	manifest.RunStartedAt = &started
	manifest.Lineage = &LineageRecord{Version: 0, Generation: 1, Baseline: true}
	manifestPath := filepath.Join(root, "manifest.json")
	require.NoError(t, WriteInternalManifestFile(manifestPath, manifest))
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	digest := ManifestSHA256Of(data)
	completePath := filepath.Join(root, "complete.json")
	require.NoError(t, writeJSONImmutableOrEqual(completePath, publishedCompleteDoc{
		Type: "gonimbus.index.complete.v1", IndexSetID: "idx_test", RunID: "run_bad",
		CompletedAt: base.Format(time.RFC3339Nano), ManifestPath: manifestPath,
		ManifestSHA256: digest, SegmentDir: segDir, Segments: len(manifest.Segments),
	}))
	_, err = OpenPublishedRunSnapshot(completePath, "idx_test", "run_bad")
	require.True(t, IsLineageCode(err, LineageCodeUnknownVersion), "got %v", err)
}

func TestPublishSnapshot_DoesNotEmitLineage(t *testing.T) {
	t.Parallel()
	config, _ := publishTestConfig(t)
	result, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.Nil(t, result.Manifest.RunStartedAt)
	require.Nil(t, result.Manifest.StateParent)
	require.Nil(t, result.Manifest.Lineage)
	require.Empty(t, result.Manifest.ParentManifests)
}

func TestWriteSegmentSet_RejectsInvalidLineageBeforeDir(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	started := base
	dir := filepath.Join(t.TempDir(), "segments-should-not-exist")
	_, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_x", CreatedAt: base, TargetRowsPerSegment: 10,
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: 7, Generation: 1, Baseline: true},
	}, []CurrentObjectRow{testRow("idx_test", "run_x", "k", base)})
	require.True(t, IsLineageCode(err, LineageCodeUnknownVersion), "got %v", err)
	_, statErr := os.Stat(dir)
	require.True(t, os.IsNotExist(statErr), "segment dir must not be created on lineage refuse")
}

func TestWriteSegmentSet_NonUTCRunStartedAtWithLineageRefusedBeforeDir(t *testing.T) {
	t.Parallel()
	// Writer seam must not UTC-normalize before validation (would accept +01:00).
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	nonUTC := time.Date(2026, 7, 14, 12, 0, 0, 0, time.FixedZone("CET", 3600))
	dir := filepath.Join(t.TempDir(), "segments-nonutc-must-not-exist")
	_, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_nonutc", CreatedAt: base, TargetRowsPerSegment: 10,
		RunStartedAt: &nonUTC,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	}, []CurrentObjectRow{testRow("idx_test", "run_nonutc", "k", base)})
	require.True(t, IsLineageCode(err, LineageCodeInvalidTime), "got %v", err)
	_, statErr := os.Stat(dir)
	require.True(t, os.IsNotExist(statErr), "segment dir must not be created when non-UTC run_started_at is refused")
}

func TestWriteSegmentSet_NonUTCRunStartedAtAloneRefused(t *testing.T) {
	t.Parallel()
	// Optional run_started_at without lineage still enforces UTC when present.
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	nonUTC := time.Date(2026, 7, 14, 12, 0, 0, 0, time.FixedZone("CET", 3600))
	dir := filepath.Join(t.TempDir(), "segments-nonutc-alone-must-not-exist")
	_, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_nonutc_alone", CreatedAt: base, TargetRowsPerSegment: 10,
		RunStartedAt: &nonUTC,
	}, []CurrentObjectRow{testRow("idx_test", "run_nonutc_alone", "k", base)})
	require.True(t, IsLineageCode(err, LineageCodeInvalidTime), "got %v", err)
	_, statErr := os.Stat(dir)
	require.True(t, os.IsNotExist(statErr))
}

func TestWriteSegmentSet_UTCRunStartedAtWithLineageAccepted(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	started := time.Date(2026, 7, 14, 11, 59, 0, 0, time.UTC)
	dir := t.TempDir()
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_utc", CreatedAt: base, TargetRowsPerSegment: 10,
		RunStartedAt: &started,
		Lineage:      &LineageRecord{Version: LineageVersionV1, Generation: 1, Baseline: true},
	}, []CurrentObjectRow{testRow("idx_test", "run_utc", "k", base)})
	require.NoError(t, err)
	require.NotNil(t, manifest.RunStartedAt)
	require.Equal(t, started.UTC(), manifest.RunStartedAt.UTC())
	_, offset := manifest.RunStartedAt.Zone()
	require.Equal(t, 0, offset)
}

// --- fixtures ---

type lineageFixtureSpec struct {
	RunID       string
	Lineage     *LineageRecord
	StateParent *StateParent
	RunStarted  *time.Time
}

type lineagePublished struct {
	snap         PublishedSnapshot
	completePath string
	manifestSHA  string
	manifestPath string
}

func publishLineageFixtureAt(t *testing.T, root string, spec lineageFixtureSpec) lineagePublished {
	t.Helper()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	if spec.RunStarted == nil && spec.Lineage != nil {
		started := base.Add(-time.Minute)
		spec.RunStarted = &started
	}
	runDir := filepath.Join(root, "runs", spec.RunID)
	segDir := filepath.Join(runDir, "segments")
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  segDir,
		IndexSetID:           "idx_test",
		RunID:                spec.RunID,
		CreatedAt:            base,
		TargetRowsPerSegment: 10,
		RunStartedAt:         spec.RunStarted,
		StateParent:          spec.StateParent,
		Lineage:              spec.Lineage,
	}, []CurrentObjectRow{testRow("idx_test", spec.RunID, "obj/"+spec.RunID, base)})
	require.NoError(t, err)

	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, WriteInternalManifestFile(manifestPath, manifest))
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	digest := ManifestSHA256Of(data)

	completePath := filepath.Join(runDir, "complete.json")
	require.NoError(t, writeJSONImmutableOrEqual(completePath, publishedCompleteDoc{
		Type:           "gonimbus.index.complete.v1",
		IndexSetID:     "idx_test",
		RunID:          spec.RunID,
		CompletedAt:    base.Format(time.RFC3339Nano),
		ManifestPath:   manifestPath,
		ManifestSHA256: digest,
		SegmentDir:     segDir,
		Segments:       len(manifest.Segments),
	}))

	snap, err := OpenPublishedRunSnapshot(completePath, "idx_test", spec.RunID)
	require.NoError(t, err)
	require.Greater(t, snap.AccountedBytes(), int64(0))
	return lineagePublished{
		snap: snap, completePath: completePath, manifestSHA: digest, manifestPath: manifestPath,
	}
}

func rewriteStateParent(t *testing.T, pub lineagePublished, parent StateParent, lin *LineageRecord) {
	t.Helper()
	manifest, err := ReadInternalManifestFile(pub.manifestPath)
	require.NoError(t, err)
	manifest.StateParent = &parent
	manifest.Lineage = lin
	require.NoError(t, ValidateManifestLineageStructure(manifest))
	// Overwrite immutable-style file for test rewrite (direct write).
	data, err := marshalIndentedJSON(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pub.manifestPath, data, 0o600))
	digest := ManifestSHA256Of(data)
	completeRaw, err := os.ReadFile(pub.completePath)
	require.NoError(t, err)
	var complete publishedCompleteDoc
	require.NoError(t, json.Unmarshal(completeRaw, &complete))
	complete.ManifestSHA256 = digest
	cdata, err := marshalIndentedJSON(complete)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pub.completePath, cdata, 0o600))
}

func corruptLineageVersion(t *testing.T, pub lineagePublished, version int) {
	t.Helper()
	manifest, err := ReadInternalManifestFile(pub.manifestPath)
	require.NoError(t, err)
	require.NotNil(t, manifest.Lineage)
	manifest.Lineage.Version = version
	data, err := marshalIndentedJSON(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pub.manifestPath, data, 0o600))
	digest := ManifestSHA256Of(data)
	completeRaw, err := os.ReadFile(pub.completePath)
	require.NoError(t, err)
	var complete publishedCompleteDoc
	require.NoError(t, json.Unmarshal(completeRaw, &complete))
	complete.ManifestSHA256 = digest
	cdata, err := marshalIndentedJSON(complete)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pub.completePath, cdata, 0o600))
}

func testRow(indexSetID, runID, relKey string, at time.Time) CurrentObjectRow {
	return CurrentObjectRow{
		IndexSetID:       indexSetID,
		RelKey:           relKey,
		ETag:             "etag-" + relKey,
		SizeBytes:        1,
		FirstSeenRunID:   runID,
		FirstSeenAt:      at,
		LastChangedRunID: runID,
		LastChangedAt:    at,
		LastSeenRunID:    runID,
		LastSeenAt:       at,
	}
}
