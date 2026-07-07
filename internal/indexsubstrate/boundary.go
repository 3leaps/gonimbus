package indexsubstrate

import (
	"errors"
)

const (
	// BoundaryManifestType identifies the future boundary-safe manifest schema.
	BoundaryManifestType       = "gonimbus.index.boundary_manifest.v1"
	ManifestRenderTypeBoundary = "boundary"

	// BoundaryTrustModel states the security boundary the future render does
	// and does not provide.
	BoundaryTrustModel = "boundary render does not de-identify row-level keys; key-unauthorized recipients require a different representation"
)

// ErrBoundaryRenderNotImplemented is returned for all boundary render attempts
// until boundary segment variants are implemented.
var ErrBoundaryRenderNotImplemented = errors.New("boundary render not implemented")

// BoundaryRenderConfig configures a future boundary manifest render.
type BoundaryRenderConfig struct {
	OutputPath        string
	Source            InternalManifest
	TokenNamespace    string
	RestrictedColumns []string
}

// BoundaryManifest is the schema shape reserved for boundary publication.
type BoundaryManifest struct {
	Type               string                      `json:"type"`
	Render             string                      `json:"render"`
	IndexSetID         string                      `json:"index_set_id"`
	RunID              string                      `json:"run_id"`
	IndexSchemaVersion int                         `json:"index_schema_version"`
	TokenNamespace     string                      `json:"token_namespace"`
	Policies           BoundaryPolicies            `json:"policies"`
	Segments           []BoundarySegmentDescriptor `json:"segments"`
}

// BoundaryPolicies describes hardening rules carried by a boundary manifest.
type BoundaryPolicies struct {
	TrustModel               string `json:"trust_model"`
	IdentifierPolicy         string `json:"identifier_policy"`
	SegmentShapeMetadata     string `json:"segment_shape_metadata"`
	RestrictedColumnMetadata string `json:"restricted_column_metadata"`
}

// BoundarySegmentDescriptor omits internal segment identifiers, paths, key
// ranges, tombstone counts, and restricted-axis distribution fields.
type BoundarySegmentDescriptor struct {
	Token       string        `json:"token"`
	Format      string        `json:"format"`
	Compression string        `json:"compression"`
	Digest      SegmentDigest `json:"digest"`
}

// DefaultBoundaryPolicies returns the Phase 1 boundary render policy text.
func DefaultBoundaryPolicies() BoundaryPolicies {
	return BoundaryPolicies{
		TrustModel:               BoundaryTrustModel,
		IdentifierPolicy:         "opaque identifiers are minted in a separate boundary namespace and are not derived from segment_id or restricted values",
		SegmentShapeMetadata:     "restricted-axis shape metadata is coarsened or omitted",
		RestrictedColumnMetadata: "restricted columns suppress min/max statistics, bloom filters, and dictionary surfaces",
	}
}

// RenderBoundaryManifestFile fails closed until boundary rendering is
// implemented. It creates no files or directories.
func RenderBoundaryManifestFile(config BoundaryRenderConfig) error {
	return ErrBoundaryRenderNotImplemented
}
