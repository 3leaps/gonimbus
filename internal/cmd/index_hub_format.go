package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

const (
	indexHubMarkerSchemaV1  = "gonimbus.index.hub_marker.v1"
	indexHubFormatSQLiteV1  = "sqlite-v1"
	indexHubFormatDurableV2 = "durable-v2"

	maxHubMarkerBytes             = 1 << 20
	maxDurableManifestBytes       = 64 << 20
	maxHubCompleteMarkerBytes     = maxDurableManifestBytes
	maxDurableHubSegments         = 200_000
	maxDurableDeclaredArtifactSum = 100 << 40
)

type durableExportSnapshot struct {
	Manifest     indexsubstrate.InternalManifest
	ManifestPath string
	ManifestSHA  string
	ManifestSize int64
	SegmentDir   string
}

type durableLocalCompleteDoc struct {
	Type           string `json:"type"`
	IndexSetID     string `json:"index_set_id"`
	RunID          string `json:"run_id"`
	CompletedAt    string `json:"completed_at"`
	ManifestPath   string `json:"manifest_path"`
	ManifestSHA256 string `json:"manifest_sha256"`
	SegmentDir     string `json:"segment_dir"`
	Segments       int    `json:"segments"`
}

func normalizeIndexHubFormat(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "sqlite", indexHubFormatSQLiteV1:
		return indexHubFormatSQLiteV1, nil
	case "durable", indexHubFormatDurableV2:
		return indexHubFormatDurableV2, nil
	default:
		return "", fmt.Errorf("--format must be one of: sqlite, sqlite-v1, durable, durable-v2")
	}
}

func completeMarkerFormat(complete completeMarker) string {
	format := strings.TrimSpace(strings.ToLower(complete.Format))
	if format == "" && complete.Artifacts.IndexDB != nil {
		return indexHubFormatSQLiteV1
	}
	return format
}

func validateDurableCompleteMarker(indexSetID, runID string, complete completeMarker) error {
	switch {
	case complete.MarkerSchemaVersion != indexHubMarkerSchemaV1:
		return fmt.Errorf("durable complete marker schema version must be %s", indexHubMarkerSchemaV1)
	case complete.Format != indexHubFormatDurableV2:
		return fmt.Errorf("durable complete marker format must be %s", indexHubFormatDurableV2)
	case complete.FormatVersion != "2":
		return fmt.Errorf("durable complete marker format_version must be 2")
	case strings.TrimSpace(complete.ExportedBy) == "":
		return fmt.Errorf("durable complete marker exported_by is required")
	case complete.IndexSetID != indexSetID:
		return fmt.Errorf("durable complete marker index_set_id mismatch")
	case complete.RunID != runID:
		return fmt.Errorf("durable complete marker run_id mismatch")
	case complete.Artifacts.Manifest == nil:
		return fmt.Errorf("durable hub run is missing manifest artifact")
	default:
		return nil
	}
}

func loadLocalDurableSnapshotForExport(indexSetID, runID string) (durableExportSnapshot, error) {
	segmentRoot, err := indexSubstrateSegmentCacheDir(indexSetID)
	if err != nil {
		return durableExportSnapshot{}, err
	}
	runDir := filepath.Join(segmentRoot, "runs", runID)
	completePath := filepath.Join(runDir, "complete.json")
	completeData, err := os.ReadFile(completePath)
	if err != nil {
		return durableExportSnapshot{}, fmt.Errorf("read local durable complete marker: %w", err)
	}
	var complete durableLocalCompleteDoc
	if err := json.Unmarshal(completeData, &complete); err != nil {
		return durableExportSnapshot{}, fmt.Errorf("parse local durable complete marker: %w", err)
	}
	if complete.IndexSetID != indexSetID || complete.RunID != runID {
		return durableExportSnapshot{}, fmt.Errorf("local durable complete marker identity mismatch")
	}
	if strings.TrimSpace(complete.ManifestPath) == "" || strings.TrimSpace(complete.SegmentDir) == "" {
		return durableExportSnapshot{}, fmt.Errorf("local durable complete marker is missing manifest or segment path")
	}
	manifestSHA, manifestSize, err := hashFile(complete.ManifestPath)
	if err != nil {
		return durableExportSnapshot{}, fmt.Errorf("hash local durable manifest: %w", err)
	}
	if manifestSHA != complete.ManifestSHA256 {
		return durableExportSnapshot{}, fmt.Errorf("local durable manifest digest mismatch")
	}
	manifest, err := indexsubstrate.ReadInternalManifestFile(complete.ManifestPath)
	if err != nil {
		return durableExportSnapshot{}, fmt.Errorf("read local durable manifest: %w", err)
	}
	if err := validateDurableHubManifest(indexSetID, runID, manifest); err != nil {
		return durableExportSnapshot{}, err
	}
	if len(manifest.Segments) != complete.Segments {
		return durableExportSnapshot{}, fmt.Errorf("local durable manifest segment count mismatch")
	}
	if err := verifyLocalDurableSegments(complete.SegmentDir, manifest); err != nil {
		return durableExportSnapshot{}, err
	}
	return durableExportSnapshot{
		Manifest:     manifest,
		ManifestPath: complete.ManifestPath,
		ManifestSHA:  manifestSHA,
		ManifestSize: manifestSize,
		SegmentDir:   complete.SegmentDir,
	}, nil
}

func validateDurableHubManifest(indexSetID, runID string, manifest indexsubstrate.InternalManifest) error {
	switch {
	case manifest.Type != indexsubstrate.ManifestType:
		return fmt.Errorf("durable manifest type mismatch")
	case manifest.Render != indexsubstrate.ManifestRenderType:
		return fmt.Errorf("durable manifest render mismatch")
	case manifest.IndexSetID != indexSetID:
		return fmt.Errorf("durable manifest index_set_id mismatch")
	case manifest.RunID != runID:
		return fmt.Errorf("durable manifest run_id mismatch")
	case manifest.IndexSchemaVersion != indexsubstrate.IndexSchemaVersion:
		return fmt.Errorf("durable manifest schema version mismatch")
	default:
		return nil
	}
}

func validateDurableHubManifestBounds(manifest indexsubstrate.InternalManifest) error {
	switch {
	case len(manifest.Segments) > maxDurableHubSegments:
		return fmt.Errorf("durable manifest segment count %d exceeds limit %d", len(manifest.Segments), maxDurableHubSegments)
	case manifest.Counts.Rows < 0 || manifest.Counts.ActiveRows < 0 || manifest.Counts.Tombstones < 0:
		return fmt.Errorf("durable manifest counts must be non-negative")
	default:
		return nil
	}
}

func verifyLocalDurableSegments(segmentDir string, manifest indexsubstrate.InternalManifest) error {
	for _, segment := range manifest.Segments {
		path, err := safeLocalArtifactPath(segmentDir, segment.Path)
		if err != nil {
			return fmt.Errorf("segment %s: %w", segment.SegmentID, err)
		}
		gotSHA, gotSize, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("hash segment %s: %w", segment.Path, err)
		}
		if segment.Digest.Algorithm != "sha256" || segment.Digest.Hex == "" {
			return fmt.Errorf("segment %s has unsupported digest", segment.Path)
		}
		if gotSHA != segment.Digest.Hex {
			return fmt.Errorf("segment %s digest mismatch", segment.Path)
		}
		if gotSize != segment.SizeBytes {
			return fmt.Errorf("segment %s size mismatch", segment.Path)
		}
	}
	return nil
}

func buildDurableCompleteJSON(indexSet *indexstore.IndexSet, run *indexstore.IndexRun, snapshot durableExportSnapshot) ([]byte, error) {
	type durableInfo struct {
		ManifestType       string `json:"manifest_type"`
		ManifestRender     string `json:"manifest_render"`
		IndexSchemaVersion int    `json:"index_schema_version"`
		SegmentNamespace   string `json:"segment_namespace"`
		Segments           int    `json:"segments"`
		Rows               int    `json:"rows"`
	}
	type artifacts struct {
		Manifest artifactRef   `json:"manifest"`
		Segments []artifactRef `json:"segments"`
	}
	type completeDoc struct {
		Version             string      `json:"version"`
		MarkerSchemaVersion string      `json:"marker_schema_version"`
		Format              string      `json:"format"`
		FormatVersion       string      `json:"format_version"`
		IndexSetID          string      `json:"index_set_id"`
		RunID               string      `json:"run_id"`
		CompletedAt         string      `json:"completed_at"`
		ExportedBy          string      `json:"exported_by"`
		Artifacts           artifacts   `json:"artifacts"`
		Durable             durableInfo `json:"durable"`
	}
	segmentRefs := make([]artifactRef, 0, len(snapshot.Manifest.Segments))
	for _, segment := range snapshot.Manifest.Segments {
		segmentRefs = append(segmentRefs, artifactRef{
			Path:      "segments/" + segment.Path,
			Role:      "segment",
			Required:  true,
			SizeBytes: segment.SizeBytes,
			SHA256:    segment.Digest.Hex,
		})
	}
	doc := completeDoc{
		Version:             "1.0",
		MarkerSchemaVersion: indexHubMarkerSchemaV1,
		Format:              indexHubFormatDurableV2,
		FormatVersion:       "2",
		IndexSetID:          indexSet.IndexSetID,
		RunID:               run.RunID,
		CompletedAt:         time.Now().UTC().Format(time.RFC3339),
		ExportedBy:          exportedByString(),
		Artifacts: artifacts{
			Manifest: artifactRef{
				Path:      "manifest.json",
				Role:      "manifest",
				Required:  true,
				SizeBytes: snapshot.ManifestSize,
				SHA256:    snapshot.ManifestSHA,
			},
			Segments: segmentRefs,
		},
		Durable: durableInfo{
			ManifestType:       snapshot.Manifest.Type,
			ManifestRender:     snapshot.Manifest.Render,
			IndexSchemaVersion: snapshot.Manifest.IndexSchemaVersion,
			SegmentNamespace:   snapshot.Manifest.Reachability.SegmentNamespace,
			Segments:           len(snapshot.Manifest.Segments),
			Rows:               snapshot.Manifest.Counts.Rows,
		},
	}
	return json.MarshalIndent(doc, "", "  ")
}

func safeLocalArtifactPath(root, rel string) (string, error) {
	root = strings.TrimSpace(root)
	rel = strings.TrimSpace(rel)
	if root == "" {
		return "", fmt.Errorf("artifact root is required")
	}
	cleanRel := filepath.Clean(rel)
	if cleanRel == "." || cleanRel == "" || filepath.IsAbs(cleanRel) || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid artifact path")
	}
	return filepath.Join(root, cleanRel), nil
}

func sha256HexBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
