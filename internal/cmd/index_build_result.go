package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// Machine-stable build receipt type emitted by `index build --json` after a
// committed run. Progress and human status remain on stderr.
//
// Consumers that require an authoritative just-built artifact must accept only
// type=gonimbus.index.build_result.v1 with status=success. Partial receipts are
// explicit non-authoritative terminal records (exit 0 may still accompany them
// for a committed SQLite partial run).
const (
	indexBuildResultType    = "gonimbus.index.build_result.v1"
	indexBuildResultVersion = "1.0.0"
)

// indexBuildResultRecord is the post-commit build receipt.
// Metadata-only: no base URI, prefixes/keys, endpoint/profile, or provider detail.
//
// Count fields that apply to a format are always present (including zero).
// omitempty is reserved for format-specific optional identity fields only.
type indexBuildResultRecord struct {
	Type             string                            `json:"type"`
	SchemaVersion    string                            `json:"schema_version"`
	Status           string                            `json:"status"`
	RequestedFormat  string                            `json:"requested_format"`
	FormatsCommitted []string                          `json:"formats_committed"`
	IndexSetID       string                            `json:"index_set_id"`
	RunID            string                            `json:"run_id"`
	ScopeHash        string                            `json:"scope_hash,omitempty"`
	Rows             *int                              `json:"rows,omitempty"`
	ActiveRows       *int                              `json:"active_rows,omitempty"`
	Tombstones       *int                              `json:"tombstones,omitempty"`
	Segments         *int                              `json:"segments,omitempty"`
	ObjectsObserved  *int64                            `json:"objects_observed,omitempty"`
	ObjectsIngested  *int64                            `json:"objects_ingested,omitempty"`
	ManifestSHA256   string                            `json:"manifest_sha256,omitempty"`
	Verification     *indexBuildBothVerificationRecord `json:"verification,omitempty"`
}

// indexBuildBothVerificationRecord reports the run-scoped SQLite
// parity-verification projection of a --format both build. It is evidence
// that verification ran and how it concluded — never a claim that a consumer
// SQLite artifact was committed (formats_committed carries commitment claims).
type indexBuildBothVerificationRecord struct {
	ProjectionMaterialized bool   `json:"projection_materialized"`
	ProjectionClosed       bool   `json:"projection_closed"`
	ObservationRunID       string `json:"observation_run_id"`
	ParityPassed           bool   `json:"parity_passed"`
	ProjectionRows         int64  `json:"projection_rows"`
	ProjectionSHA256       string `json:"projection_sha256,omitempty"`
	// ArtifactID and ArtifactLocator bind the receipt to the exact SQLite
	// database the parity report compared: the opaque per-run attempt ID and a
	// set-relative locator for canonical builds (never a host-absolute path),
	// or the caller-provided path for an explicit external --db.
	ArtifactID      string `json:"artifact_id"`
	ArtifactLocator string `json:"artifact_locator"`
}

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func newDurableBuildResultRecord(summary indexbuild.Summary, scopeHash, requestedFormat string, formats []string) indexBuildResultRecord {
	return indexBuildResultRecord{
		Type:             indexBuildResultType,
		SchemaVersion:    indexBuildResultVersion,
		Status:           "success",
		RequestedFormat:  requestedFormat,
		FormatsCommitted: append([]string(nil), formats...),
		IndexSetID:       summary.IndexSetID,
		RunID:            summary.RunID,
		ScopeHash:        strings.TrimSpace(scopeHash),
		Rows:             intPtr(summary.Manifest.Rows),
		ActiveRows:       intPtr(summary.Manifest.ActiveRows),
		Tombstones:       intPtr(summary.Manifest.Tombstones),
		Segments:         intPtr(len(summary.Manifest.Segments)),
		ObjectsObserved:  int64Ptr(summary.ObjectsObserved),
		ManifestSHA256:   summary.ManifestSHA256,
	}
}

func newSQLiteBuildResultRecord(indexSetID, runID, scopeHash, status string, objectsIngested int64) indexBuildResultRecord {
	return indexBuildResultRecord{
		Type:             indexBuildResultType,
		SchemaVersion:    indexBuildResultVersion,
		Status:           status,
		RequestedFormat:  "sqlite",
		FormatsCommitted: []string{string(indexreader.FormatSQLiteV1)},
		IndexSetID:       indexSetID,
		RunID:            runID,
		ScopeHash:        strings.TrimSpace(scopeHash),
		ObjectsIngested:  int64Ptr(objectsIngested),
	}
}

// newBothBuildResultRecord commits durable only: the SQLite side of a `both`
// build is a run-scoped parity-verification projection, reported in the
// verification block, never in formats_committed.
func newBothBuildResultRecord(summary indexbuild.Summary, scopeHash string, objectsIngested int64, verification *indexBuildBothVerificationRecord) indexBuildResultRecord {
	rec := newDurableBuildResultRecord(summary, scopeHash, "both", []string{
		string(indexreader.FormatDurableV2),
	})
	rec.ObjectsIngested = int64Ptr(objectsIngested)
	rec.Verification = verification
	return rec
}

// validateIndexBuildResultRecord fail-closes before encoding an authoritative
// success receipt. Partial/failure records still require identity + formats so
// consumers can bind the non-authoritative terminal record.
func validateIndexBuildResultRecord(rec indexBuildResultRecord) error {
	if strings.TrimSpace(rec.Type) != indexBuildResultType {
		return fmt.Errorf("build result type must be %s", indexBuildResultType)
	}
	if strings.TrimSpace(rec.SchemaVersion) == "" {
		return fmt.Errorf("build result schema_version is required")
	}
	status := strings.TrimSpace(rec.Status)
	if status == "" {
		return fmt.Errorf("build result status is required")
	}
	if strings.TrimSpace(rec.RequestedFormat) == "" {
		return fmt.Errorf("build result requested_format is required")
	}
	if len(rec.FormatsCommitted) == 0 {
		return fmt.Errorf("build result formats_committed is required")
	}
	for _, f := range rec.FormatsCommitted {
		if strings.TrimSpace(f) == "" {
			return fmt.Errorf("build result formats_committed entries must be non-empty")
		}
		if f == "both" {
			return fmt.Errorf("build result formats_committed must use substrate names, not %q", f)
		}
	}
	if strings.TrimSpace(rec.IndexSetID) == "" {
		return fmt.Errorf("build result index_set_id is required")
	}
	if strings.TrimSpace(rec.RunID) == "" {
		return fmt.Errorf("build result run_id is required")
	}

	hasDurable := false
	hasSQLite := false
	for _, f := range rec.FormatsCommitted {
		switch f {
		case string(indexreader.FormatDurableV2):
			hasDurable = true
		case string(indexreader.FormatSQLiteV1):
			hasSQLite = true
		}
	}

	if hasDurable {
		if rec.Rows == nil || rec.ActiveRows == nil || rec.Tombstones == nil || rec.Segments == nil || rec.ObjectsObserved == nil {
			return fmt.Errorf("build result durable counts are required")
		}
		if status == "success" && strings.TrimSpace(rec.ManifestSHA256) == "" {
			return fmt.Errorf("build result manifest_sha256 is required for successful durable commit")
		}
	}
	if hasSQLite {
		if rec.ObjectsIngested == nil {
			return fmt.Errorf("build result objects_ingested is required for sqlite commit")
		}
	}
	if status == "success" && !hasDurable && !hasSQLite {
		return fmt.Errorf("build result formats_committed must include a known substrate")
	}
	if strings.TrimSpace(rec.RequestedFormat) == "both" {
		// `both` commits durable only; its SQLite side is a run-scoped
		// verification projection and must never appear as a committed format.
		if hasSQLite {
			return fmt.Errorf("build result for requested_format both must not claim a committed sqlite substrate")
		}
		if rec.Verification == nil {
			return fmt.Errorf("build result for requested_format both requires the verification block")
		}
		if rec.Verification.ObservationRunID != rec.RunID {
			return fmt.Errorf("build result verification observation_run_id must bind the receipt run")
		}
		if strings.TrimSpace(rec.Verification.ArtifactID) == "" || strings.TrimSpace(rec.Verification.ArtifactLocator) == "" {
			return fmt.Errorf("build result verification must bind the compared artifact identity")
		}
		if status == "success" {
			if !rec.Verification.ProjectionMaterialized || !rec.Verification.ProjectionClosed {
				return fmt.Errorf("build result for successful both requires a materialized and closed verification projection")
			}
			if !rec.Verification.ParityPassed {
				return fmt.Errorf("build result for successful both requires passed parity")
			}
		}
	} else if rec.Verification != nil {
		return fmt.Errorf("build result verification block is only valid for requested_format both")
	}
	return nil
}

func emitIndexBuildResultJSON(w io.Writer, rec indexBuildResultRecord) error {
	if w == nil {
		return fmt.Errorf("build result writer is required")
	}
	if strings.TrimSpace(rec.Type) == "" {
		rec.Type = indexBuildResultType
	}
	if strings.TrimSpace(rec.SchemaVersion) == "" {
		rec.SchemaVersion = indexBuildResultVersion
	}
	if err := validateIndexBuildResultRecord(rec); err != nil {
		return fmt.Errorf("build result invalid: %w", err)
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(rec); err != nil {
		return fmt.Errorf("emit build result: %w", err)
	}
	return nil
}

func jobBuildReceiptIdentity(rec indexBuildResultRecord) *jobregistry.BuildReceiptIdentity {
	return &jobregistry.BuildReceiptIdentity{
		Type:             rec.Type,
		SchemaVersion:    rec.SchemaVersion,
		Status:           rec.Status,
		RequestedFormat:  rec.RequestedFormat,
		FormatsCommitted: append([]string(nil), rec.FormatsCommitted...),
		IndexSetID:       rec.IndexSetID,
		RunID:            rec.RunID,
		ScopeHash:        rec.ScopeHash,
		ManifestSHA256:   rec.ManifestSHA256,
	}
}
