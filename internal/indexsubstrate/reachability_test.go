package indexsubstrate

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeriveReachabilityPlanWalksLatestParentChain(t *testing.T) {
	parent := reachabilityTestManifest("idx_test", "run_parent", []SegmentDescriptor{
		reachabilityTestSegment("seg_shared", "shared.parquet", "a"),
	})
	child := reachabilityTestManifest("idx_test", "run_child", []SegmentDescriptor{
		reachabilityTestSegment("seg_shared", "shared.parquet", "a"),
		reachabilityTestSegment("seg_child", "child.parquet", "b"),
	})
	child.ParentManifests = []ManifestReference{{
		IndexSetID:     "idx_test",
		RunID:          "run_parent",
		ManifestSHA256: strings.Repeat("c", 64),
	}}

	plan, err := DeriveReachabilityPlan(ReachabilityInput{
		Manifests: []InternalManifest{parent, child},
		LatestPointers: []LatestPointerReference{{
			IndexSetID: "idx_test",
			RunID:      "run_child",
		}},
	})
	require.NoError(t, err)
	require.Len(t, plan.ReachableSegments, 2)
	require.Equal(t, "sha256", plan.ReachableSegments[0].Digest.Algorithm)
	require.Equal(t, strings.Repeat("a", 64), plan.ReachableSegments[0].Digest.Hex)
	require.Equal(t, 2, plan.ReachableSegments[0].ReferenceCount)
	require.Equal(t, []ManifestReference{
		{IndexSetID: "idx_test", RunID: "run_child"},
		{IndexSetID: "idx_test", RunID: "run_parent"},
	}, plan.ReachableSegments[0].ReferencedBy)
	require.Equal(t, strings.Repeat("b", 64), plan.ReachableSegments[1].Digest.Hex)
	require.Equal(t, 1, plan.ReachableSegments[1].ReferenceCount)
	require.Equal(t, []DerivedRefcount{
		{SegmentID: "seg_shared", Path: "shared.parquet", Digest: SegmentDigest{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}, Count: 2},
		{SegmentID: "seg_child", Path: "child.parquet", Digest: SegmentDigest{Algorithm: "sha256", Hex: strings.Repeat("b", 64)}, Count: 1},
	}, plan.DerivedRefcounts)
}

func TestDeriveReachabilityPlanDistinguishesSameDigestDifferentPaths(t *testing.T) {
	manifest := reachabilityTestManifest("idx_test", "run_latest", []SegmentDescriptor{
		reachabilityTestSegment("seg_left", "left.parquet", "a"),
		reachabilityTestSegment("seg_right", "right.parquet", "a"),
	})

	plan, err := DeriveReachabilityPlan(ReachabilityInput{
		Manifests:      []InternalManifest{manifest},
		LatestPointers: []LatestPointerReference{{IndexSetID: "idx_test", RunID: "run_latest"}},
	})
	require.NoError(t, err)
	require.Len(t, plan.ReachableSegments, 2)
	require.Equal(t, "left.parquet", plan.ReachableSegments[0].Path)
	require.Equal(t, "right.parquet", plan.ReachableSegments[1].Path)
	require.Equal(t, []DerivedRefcount{
		{SegmentID: "seg_left", Path: "left.parquet", Digest: SegmentDigest{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}, Count: 1},
		{SegmentID: "seg_right", Path: "right.parquet", Digest: SegmentDigest{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}, Count: 1},
	}, plan.DerivedRefcounts)
}

func TestDeriveReachabilityPlanRequiresRetainedParentManifests(t *testing.T) {
	child := reachabilityTestManifest("idx_test", "run_child", []SegmentDescriptor{
		reachabilityTestSegment("seg_child", "child.parquet", "b"),
	})
	child.ParentManifests = []ManifestReference{{IndexSetID: "idx_test", RunID: "run_missing"}}

	_, err := DeriveReachabilityPlan(ReachabilityInput{
		Manifests:      []InternalManifest{child},
		LatestPointers: []LatestPointerReference{{IndexSetID: "idx_test", RunID: "run_child"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced but not retained")
}

func TestDeriveReachabilityPlanUsesRetainedManifestRoots(t *testing.T) {
	retained := reachabilityTestManifest("idx_test", "run_retained", []SegmentDescriptor{
		reachabilityTestSegment("seg_retained", "retained.parquet", "d"),
	})

	plan, err := DeriveReachabilityPlan(ReachabilityInput{
		Manifests: []InternalManifest{retained},
		RetainedManifests: []ManifestReference{{
			IndexSetID: "idx_test",
			RunID:      "run_retained",
		}},
	})
	require.NoError(t, err)
	require.Len(t, plan.ReachableSegments, 1)
	require.Equal(t, strings.Repeat("d", 64), plan.ReachableSegments[0].Digest.Hex)
	require.Equal(t, 1, plan.ReachableSegments[0].ReferenceCount)
}

func reachabilityTestManifest(indexSetID, runID string, segments []SegmentDescriptor) InternalManifest {
	return InternalManifest{
		Type:               ManifestType,
		Render:             ManifestRenderType,
		IndexSetID:         indexSetID,
		RunID:              runID,
		IndexSchemaVersion: IndexSchemaVersion,
		CreatedAt:          time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
		ParentManifests:    []ManifestReference{},
		Reachability:       DefaultManifestReachability(),
		Segments:           segments,
	}
}

func reachabilityTestSegment(segmentID, path, digestChar string) SegmentDescriptor {
	return SegmentDescriptor{
		SegmentID: segmentID,
		Path:      path,
		Format:    SegmentFormatParquet,
		Digest: SegmentDigest{
			Algorithm: "sha256",
			Hex:       strings.Repeat(digestChar, 64),
		},
	}
}
