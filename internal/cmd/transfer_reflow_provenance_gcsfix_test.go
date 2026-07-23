package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// TestBuildProvenanceSidecarKeyGCSMirrorHonorsPrefix pins the intentional fix: a
// mirrored-root provenance sidecar on GCS honors the sidecar-root prefix, exactly
// like S3. Before the fix, GCS fell through to the
// default branch and returned destKey+suffix, silently ignoring the mirrored
// root prefix (an audit-placement defect). This is a labeled behavior change, not
// a byte-identical relocation.
func TestBuildProvenanceSidecarKeyGCSMirrorHonorsPrefix(t *testing.T) {
	cfg := provenanceConfig{
		Mode:          provenanceModeSidecar,
		Suffix:        provenanceSuffix,
		PlacementMode: provenancePlaceMirror,
		SidecarRoot: &reflowDestSpec{
			Provider: string(provider.ProviderGCS),
			Bucket:   "dest-bucket",
			Prefix:   "runs/run-001/sidecars/",
		},
	}
	const destRel = "tenant/a/file.xml"
	const destKey = "data/tenant/a/file.xml"

	got := buildProvenanceSidecarKey(cfg, &reflowDestSpec{Provider: string(provider.ProviderGCS), Bucket: "dest-bucket"}, destRel, destKey)

	// Fixed: prefix-relative, matching the S3 object-store layout.
	require.Equal(t, "runs/run-001/sidecars/tenant/a/file.xml"+provenanceSuffix, got)
	// Regression guard: must NOT be the pre-fix destKey+suffix fall-through.
	require.NotEqual(t, destKey+provenanceSuffix, got)
}

// TestBuildProvenanceSidecarKeyS3AndGCSMirrorMatch pins that S3 and GCS mirrored
// object-store roots produce the same prefix-relative layout for the same input.
func TestBuildProvenanceSidecarKeyS3AndGCSMirrorMatch(t *testing.T) {
	const destRel = "x/y/z.xml"
	const destKey = "data/x/y/z.xml"
	mk := func(prov provider.ProviderType) string {
		cfg := provenanceConfig{
			Mode:          provenanceModeSidecar,
			Suffix:        provenanceSuffix,
			PlacementMode: provenancePlaceMirror,
			SidecarRoot:   &reflowDestSpec{Provider: string(prov), Bucket: "b", Prefix: "side/"},
		}
		return buildProvenanceSidecarKey(cfg, &reflowDestSpec{Provider: string(prov), Bucket: "b"}, destRel, destKey)
	}
	require.Equal(t, mk(provider.ProviderS3), mk(provider.ProviderGCS))
	require.Equal(t, "side/x/y/z.xml"+provenanceSuffix, mk(provider.ProviderGCS))
}
