package indexcompare

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

const (
	// CompareResultType is the JSONL record type for index comparison reports.
	CompareResultType    = "gonimbus.index.compare_result.v1"
	ProjectionVersion    = "gonimbus.index.compare_projection.v1"
	ComparatorVersion    = "gonimbus.index.comparator.v1"
	DefaultMaxMismatches = 20
)

type Artifact struct {
	ID   string `json:"id,omitempty"`
	Path string `json:"path,omitempty"`
}

type Options struct {
	ProjectionVersion string
	MaxMismatches     int
}

type Input struct {
	SQLiteDB             *sql.DB
	SQLiteIndexSetID     string
	SQLiteArtifact       Artifact
	DurableManifest      indexsubstrate.InternalManifest
	DurableSegmentDir    string
	DurableArtifact      Artifact
	ObservationRunID     string
	ObservationStartedAt time.Time
	Options              Options
}

// Report describes a compare_result.v1 parity check. ParityPassed certifies
// only the ProjectionSemantics contract carried on the same result.
type Report struct {
	Type                 string              `json:"type"`
	ProjectionVersion    string              `json:"projection_version"`
	ProjectionSemantics  ProjectionSemantics `json:"projection_semantics"`
	ComparatorVersion    string              `json:"comparator_version"`
	ObservationRunID     string              `json:"observation_run_id,omitempty"`
	ObservationStartedAt string              `json:"observation_started_at,omitempty"`
	SQLiteMaterialized   bool                `json:"sqlite_materialized"`
	DurablePublished     bool                `json:"durable_published"`
	ComparisonRan        bool                `json:"comparison_ran"`
	// ParityPassed means LIST-projection fidelity for one crawl; it does not
	// certify reflow-input readiness or HEAD-enrichment parity.
	ParityPassed            bool                 `json:"parity_passed"`
	SQLiteArtifact          Artifact             `json:"sqlite_artifact"`
	DurableArtifact         Artifact             `json:"durable_artifact"`
	SQLiteRows              int64                `json:"sqlite_rows"`
	DurableRows             int64                `json:"durable_rows"`
	SQLiteProjectionSHA256  string               `json:"sqlite_projection_sha256,omitempty"`
	DurableProjectionSHA256 string               `json:"durable_projection_sha256,omitempty"`
	ProjectionMismatches    int64                `json:"projection_mismatches"`
	ContentIdentityCheck    ContentIdentityCheck `json:"content_identity_check"`
	Mismatches              []Mismatch           `json:"mismatches,omitempty"`
}

// ProjectionSemantics is the machine-carried contract for ProjectionVersion.
// It keeps compare_result.v1 consumers from treating LIST-projection parity as
// a broader reflow-readiness or enrichment-parity claim.
type ProjectionSemantics struct {
	Certifies       string                   `json:"certifies"`
	DoesNotCertify  string                   `json:"does_not_certify"`
	IncludedFields  []string                 `json:"included_fields"`
	ContentIdentity string                   `json:"content_identity"`
	ExcludedFields  []ExcludedFieldSemantics `json:"excluded_fields"`
}

// ExcludedFieldSemantics describes one field class intentionally outside the
// projection and names the gate that owns it.
type ExcludedFieldSemantics struct {
	FieldClass string `json:"field_class"`
	Reason     string `json:"reason"`
	OwningGate string `json:"owning_gate"`
}

type ContentIdentityCheck struct {
	Semantics  string `json:"semantics"`
	Checked    bool   `json:"checked"`
	Mismatches int64  `json:"mismatches"`
}

type Mismatch struct {
	Kind    string `json:"kind"`
	RelKey  string `json:"rel_key,omitempty"`
	Side    string `json:"side,omitempty"`
	Field   string `json:"field,omitempty"`
	SQLite  string `json:"sqlite,omitempty"`
	Durable string `json:"durable,omitempty"`
}

type projectionRow struct {
	RelKey       string `json:"rel_key"`
	SizeBytes    int64  `json:"size_bytes"`
	LastModified string `json:"last_modified,omitempty"`
	StorageClass string `json:"storage_class,omitempty"`
	ETag         string `json:"-"`
}

// DefaultProjectionSemantics returns the static contract for projection v1.
func DefaultProjectionSemantics() ProjectionSemantics {
	return ProjectionSemantics{
		Certifies:       "LIST-projection fidelity (sqlite vs durable row projection over one crawl)",
		DoesNotCertify:  "reflow-input readiness (HEAD-enrichment parity)",
		IncludedFields:  []string{"rel_key", "size_bytes", "last_modified", "storage_class"},
		ContentIdentity: "provider_etag_equivalence: same-provider ETag-to-ETag check; not a portable content hash (multipart/composite ETag can differ from content hash). Checked separately, not in the row digest.",
		ExcludedFields: []ExcludedFieldSemantics{
			{
				FieldClass: "HEAD-derived enrichment metadata",
				Reason:     "not present in LIST; needs a separate enrich-with-HEAD pass",
				OwningGate: "projection v2 / enrichment-parity (over enriched-index runs; future)",
			},
			{
				FieldClass: "run-scoped temporal fields (first_seen, last_seen, last_changed)",
				Reason:     "tracks durable temporal state outside one LIST projection",
				OwningGate: "temporal-delta comparator (durable_delta.v1)",
			},
			{
				FieldClass: "coverage attestation",
				Reason:     "durable-only; sqlite has no equivalent coverage contract",
				OwningGate: "temporal-delta comparator",
			},
			{
				FieldClass: "physical/format-internal metadata",
				Reason:     "segment digests, journal/shard ids, sqlite rowids, and internal manifest metadata are format-specific",
				OwningGate: "excluded by design (format-specific)",
			},
		},
	}
}

func Compare(ctx context.Context, input Input) (Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts := normalizeOptions(input.Options)
	if opts.ProjectionVersion != ProjectionVersion {
		return Report{}, fmt.Errorf("unsupported projection version %q", opts.ProjectionVersion)
	}
	if input.SQLiteDB == nil {
		return Report{}, fmt.Errorf("sqlite db is required")
	}
	if strings.TrimSpace(input.SQLiteIndexSetID) == "" {
		return Report{}, fmt.Errorf("sqlite index_set_id is required")
	}
	report := Report{
		Type:                 CompareResultType,
		ProjectionVersion:    ProjectionVersion,
		ProjectionSemantics:  DefaultProjectionSemantics(),
		ComparatorVersion:    ComparatorVersion,
		ObservationRunID:     strings.TrimSpace(input.ObservationRunID),
		SQLiteMaterialized:   true,
		DurablePublished:     len(input.DurableManifest.Segments) > 0 || input.DurableManifest.Counts.Rows == 0,
		ComparisonRan:        true,
		SQLiteArtifact:       input.SQLiteArtifact,
		DurableArtifact:      input.DurableArtifact,
		ContentIdentityCheck: ContentIdentityCheck{Semantics: "provider_etag_equivalence", Checked: true},
	}
	if !input.ObservationStartedAt.IsZero() {
		report.ObservationStartedAt = input.ObservationStartedAt.UTC().Format(time.RFC3339Nano)
	}

	sqlRows, err := newSQLiteIterator(ctx, input.SQLiteDB, input.SQLiteIndexSetID, report.ObservationRunID)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = sqlRows.Close() }()
	durableRows := newDurableIterator(ctx, input.DurableSegmentDir, input.DurableManifest, report.ObservationRunID)

	sqlHash := sha256.New()
	durableHash := sha256.New()
	left, leftOK, err := sqlRows.Next()
	if err != nil {
		return Report{}, err
	}
	right, rightOK, err := durableRows.Next()
	if err != nil {
		return Report{}, err
	}
	for leftOK || rightOK {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		switch {
		case leftOK && (!rightOK || left.RelKey < right.RelKey):
			report.SQLiteRows++
			hashProjection(sqlHash, left)
			addMismatch(&report, opts.MaxMismatches, Mismatch{Kind: "missing_row", RelKey: left.RelKey, Side: "durable"})
			report.ProjectionMismatches++
			left, leftOK, err = sqlRows.Next()
		case rightOK && (!leftOK || right.RelKey < left.RelKey):
			report.DurableRows++
			hashProjection(durableHash, right)
			addMismatch(&report, opts.MaxMismatches, Mismatch{Kind: "missing_row", RelKey: right.RelKey, Side: "sqlite"})
			report.ProjectionMismatches++
			right, rightOK, err = durableRows.Next()
		default:
			report.SQLiteRows++
			report.DurableRows++
			hashProjection(sqlHash, left)
			hashProjection(durableHash, right)
			compareProjectionRows(&report, opts.MaxMismatches, left, right)
			if left.ETag != right.ETag {
				report.ContentIdentityCheck.Mismatches++
				addMismatch(&report, opts.MaxMismatches, Mismatch{Kind: "content_identity_mismatch", RelKey: left.RelKey, Field: "etag", SQLite: left.ETag, Durable: right.ETag})
			}
			left, leftOK, err = sqlRows.Next()
			if err != nil {
				return Report{}, err
			}
			right, rightOK, err = durableRows.Next()
		}
		if err != nil {
			return Report{}, err
		}
	}
	report.SQLiteProjectionSHA256 = hex.EncodeToString(sqlHash.Sum(nil))
	report.DurableProjectionSHA256 = hex.EncodeToString(durableHash.Sum(nil))
	report.ParityPassed = report.ProjectionMismatches == 0 && report.ContentIdentityCheck.Mismatches == 0 && report.SQLiteRows == report.DurableRows
	return report, nil
}

func normalizeOptions(opts Options) Options {
	if strings.TrimSpace(opts.ProjectionVersion) == "" {
		opts.ProjectionVersion = ProjectionVersion
	}
	if opts.MaxMismatches <= 0 {
		opts.MaxMismatches = DefaultMaxMismatches
	}
	return opts
}

func compareProjectionRows(report *Report, max int, left, right projectionRow) {
	if left.SizeBytes != right.SizeBytes {
		report.ProjectionMismatches++
		addMismatch(report, max, Mismatch{Kind: "projection_mismatch", RelKey: left.RelKey, Field: "size_bytes", SQLite: fmt.Sprintf("%d", left.SizeBytes), Durable: fmt.Sprintf("%d", right.SizeBytes)})
	}
	if left.LastModified != right.LastModified {
		report.ProjectionMismatches++
		addMismatch(report, max, Mismatch{Kind: "projection_mismatch", RelKey: left.RelKey, Field: "last_modified", SQLite: left.LastModified, Durable: right.LastModified})
	}
	if left.StorageClass != right.StorageClass {
		report.ProjectionMismatches++
		addMismatch(report, max, Mismatch{Kind: "projection_mismatch", RelKey: left.RelKey, Field: "storage_class", SQLite: left.StorageClass, Durable: right.StorageClass})
	}
}

func hashProjection(h interface{ Write([]byte) (int, error) }, row projectionRow) {
	data, _ := json.Marshal(struct {
		RelKey       string `json:"rel_key"`
		SizeBytes    int64  `json:"size_bytes"`
		LastModified string `json:"last_modified,omitempty"`
		StorageClass string `json:"storage_class,omitempty"`
	}{
		RelKey: row.RelKey, SizeBytes: row.SizeBytes, LastModified: row.LastModified, StorageClass: row.StorageClass,
	})
	_, _ = h.Write(data)
	_, _ = h.Write([]byte{'\n'})
}

func addMismatch(report *Report, max int, mismatch Mismatch) {
	if int64(len(report.Mismatches)) < int64(max) {
		report.Mismatches = append(report.Mismatches, mismatch)
	}
}
