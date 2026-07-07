package indexsubstrate

import (
	"fmt"
	"sort"
	"strings"
)

type LatestPointerReference struct {
	IndexSetID string `json:"index_set_id"`
	RunID      string `json:"run_id"`
}

type ReachabilityInput struct {
	Manifests         []InternalManifest
	RetainedManifests []ManifestReference
	LatestPointers    []LatestPointerReference
}

type ReachabilityPlan struct {
	ReachableSegments []ReachableSegment `json:"reachable_segments"`
	DerivedRefcounts  []DerivedRefcount  `json:"derived_refcounts"`
}

type ReachableSegment struct {
	SegmentID      string              `json:"segment_id"`
	Path           string              `json:"path"`
	Digest         SegmentDigest       `json:"digest"`
	ReferencedBy   []ManifestReference `json:"referenced_by"`
	ReferenceCount int                 `json:"reference_count"`
}

type DerivedRefcount struct {
	SegmentID string        `json:"segment_id"`
	Path      string        `json:"path"`
	Digest    SegmentDigest `json:"digest"`
	Count     int           `json:"count"`
}

func DefaultManifestReachability() ManifestReachability {
	return ManifestReachability{
		Model:            ManifestReachabilityModel,
		SegmentNamespace: SegmentNamespaceShared,
		RefcountMode:     RefcountModeDerivedAudit,
		CompactOwner:     CompactOwnerIndexCompact,
	}
}

func DeriveReachabilityPlan(input ReachabilityInput) (ReachabilityPlan, error) {
	known := make(map[string]InternalManifest, len(input.Manifests))
	for _, manifest := range input.Manifests {
		manifest = normalizeInternalManifestIdentity(manifest)
		if manifest.IndexSetID == "" || manifest.RunID == "" {
			return ReachabilityPlan{}, fmt.Errorf("manifest index_set_id and run_id are required")
		}
		key := manifestKey(manifest.IndexSetID, manifest.RunID)
		if _, ok := known[key]; ok {
			return ReachabilityPlan{}, fmt.Errorf("duplicate manifest reference %s/%s", manifest.IndexSetID, manifest.RunID)
		}
		known[key] = manifest
	}

	roots := make([]ManifestReference, 0, len(input.RetainedManifests)+len(input.LatestPointers))
	roots = append(roots, normalizeManifestReferences(input.RetainedManifests)...)
	for _, latest := range input.LatestPointers {
		latest.IndexSetID = strings.TrimSpace(latest.IndexSetID)
		latest.RunID = strings.TrimSpace(latest.RunID)
		if latest.IndexSetID == "" || latest.RunID == "" {
			return ReachabilityPlan{}, fmt.Errorf("latest pointer index_set_id and run_id are required")
		}
		roots = append(roots, ManifestReference{IndexSetID: latest.IndexSetID, RunID: latest.RunID})
	}

	visited := map[string]ManifestReference{}
	stack := append([]ManifestReference(nil), roots...)
	for len(stack) > 0 {
		ref := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := manifestKey(ref.IndexSetID, ref.RunID)
		if _, ok := visited[key]; ok {
			continue
		}
		manifest, ok := known[key]
		if !ok {
			return ReachabilityPlan{}, fmt.Errorf("manifest %s/%s is referenced but not retained", ref.IndexSetID, ref.RunID)
		}
		visited[key] = ManifestReference{
			IndexSetID:     manifest.IndexSetID,
			RunID:          manifest.RunID,
			ManifestSHA256: strings.TrimSpace(ref.ManifestSHA256),
		}
		stack = append(stack, normalizeManifestReferences(manifest.ParentManifests)...)
	}

	visitedRefs := make([]ManifestReference, 0, len(visited))
	for _, ref := range visited {
		visitedRefs = append(visitedRefs, ref)
	}
	sortManifestReferences(visitedRefs)

	segments := map[string]ReachableSegment{}
	for _, visitedRef := range visitedRefs {
		manifest := known[manifestKey(visitedRef.IndexSetID, visitedRef.RunID)]
		manifestRef := ManifestReference{IndexSetID: manifest.IndexSetID, RunID: manifest.RunID}
		for _, segment := range manifest.Segments {
			if segment.Digest.Algorithm == "" || segment.Digest.Hex == "" {
				return ReachabilityPlan{}, fmt.Errorf("segment %s in %s/%s has no digest", segment.SegmentID, manifest.IndexSetID, manifest.RunID)
			}
			segmentKey := segmentReachabilityKey(segment)
			reachable := segments[segmentKey]
			if reachable.Digest.Hex == "" {
				reachable = ReachableSegment{
					SegmentID: segment.SegmentID,
					Path:      segment.Path,
					Digest:    segment.Digest,
				}
			}
			reachable.ReferencedBy = append(reachable.ReferencedBy, manifestRef)
			segments[segmentKey] = reachable
		}
	}

	out := ReachabilityPlan{
		ReachableSegments: make([]ReachableSegment, 0, len(segments)),
		DerivedRefcounts:  make([]DerivedRefcount, 0, len(segments)),
	}
	for _, segment := range segments {
		sortManifestReferences(segment.ReferencedBy)
		segment.ReferenceCount = len(segment.ReferencedBy)
		out.ReachableSegments = append(out.ReachableSegments, segment)
		out.DerivedRefcounts = append(out.DerivedRefcounts, DerivedRefcount{
			SegmentID: segment.SegmentID,
			Path:      segment.Path,
			Digest:    segment.Digest,
			Count:     segment.ReferenceCount,
		})
	}
	sort.Slice(out.ReachableSegments, func(i, j int) bool {
		return compareReachableSegments(out.ReachableSegments[i], out.ReachableSegments[j]) < 0
	})
	sort.Slice(out.DerivedRefcounts, func(i, j int) bool {
		if out.DerivedRefcounts[i].Digest.Hex != out.DerivedRefcounts[j].Digest.Hex {
			return out.DerivedRefcounts[i].Digest.Hex < out.DerivedRefcounts[j].Digest.Hex
		}
		return out.DerivedRefcounts[i].Path < out.DerivedRefcounts[j].Path
	})
	return out, nil
}

func normalizeInternalManifestIdentity(manifest InternalManifest) InternalManifest {
	manifest.IndexSetID = strings.TrimSpace(manifest.IndexSetID)
	manifest.RunID = strings.TrimSpace(manifest.RunID)
	manifest.ParentManifests = normalizeManifestReferences(manifest.ParentManifests)
	return manifest
}

func normalizeManifestReferences(in []ManifestReference) []ManifestReference {
	out := make([]ManifestReference, 0, len(in))
	for _, ref := range in {
		ref.IndexSetID = strings.TrimSpace(ref.IndexSetID)
		ref.RunID = strings.TrimSpace(ref.RunID)
		ref.ManifestSHA256 = strings.TrimSpace(ref.ManifestSHA256)
		if ref.IndexSetID == "" && ref.RunID == "" && ref.ManifestSHA256 == "" {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func sortManifestReferences(refs []ManifestReference) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].IndexSetID != refs[j].IndexSetID {
			return refs[i].IndexSetID < refs[j].IndexSetID
		}
		return refs[i].RunID < refs[j].RunID
	})
}

func manifestKey(indexSetID, runID string) string {
	return strings.TrimSpace(indexSetID) + "\x00" + strings.TrimSpace(runID)
}

func segmentReachabilityKey(segment SegmentDescriptor) string {
	return strings.TrimSpace(segment.Path) + "\x00" + strings.TrimSpace(segment.Digest.Algorithm) + "\x00" + strings.TrimSpace(segment.Digest.Hex)
}

func compareReachableSegments(left, right ReachableSegment) int {
	if left.Digest.Hex < right.Digest.Hex {
		return -1
	}
	if left.Digest.Hex > right.Digest.Hex {
		return 1
	}
	if left.Path < right.Path {
		return -1
	}
	if left.Path > right.Path {
		return 1
	}
	return 0
}
