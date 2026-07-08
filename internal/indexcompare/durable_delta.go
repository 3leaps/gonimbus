package indexcompare

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

const DurableDeltaComparatorVersion = "gonimbus.index.durable_delta.v1"

type DurableSnapshotInput struct {
	Manifest   indexsubstrate.InternalManifest
	SegmentDir string
	Artifact   Artifact
}

type DurableDeltaInput struct {
	Before        DurableSnapshotInput
	After         DurableSnapshotInput
	MaxChanges    int
	RequireParent bool
}

type DurableDeltaReport struct {
	Type              string               `json:"type"`
	ComparatorVersion string               `json:"comparator_version"`
	IndexSetID        string               `json:"index_set_id"`
	BeforeRunID       string               `json:"before_run_id"`
	AfterRunID        string               `json:"after_run_id"`
	BeforeArtifact    Artifact             `json:"before_artifact"`
	AfterArtifact     Artifact             `json:"after_artifact"`
	BeforeRows        int64                `json:"before_rows"`
	AfterRows         int64                `json:"after_rows"`
	Added             int64                `json:"added"`
	Changed           int64                `json:"changed"`
	Tombstoned        int64                `json:"tombstoned"`
	Unchanged         int64                `json:"unchanged"`
	BeforeSHA256      string               `json:"before_projection_sha256,omitempty"`
	AfterSHA256       string               `json:"after_projection_sha256,omitempty"`
	Coverage          DeltaCoverageSummary `json:"coverage"`
	Changes           []DeltaChange        `json:"changes,omitempty"`
}

type DeltaCoverageSummary struct {
	Semantics        string `json:"semantics"`
	BeforeScopes     int    `json:"before_scopes"`
	AfterScopes      int    `json:"after_scopes"`
	AttributedRows   int64  `json:"attributed_rows"`
	UnattributedRows int64  `json:"unattributed_rows"`
}

type DeltaChange struct {
	Kind       string              `json:"kind"`
	RelKey     string              `json:"rel_key"`
	Fields     []FieldDelta        `json:"fields,omitempty"`
	Coverage   CoverageAttribution `json:"coverage"`
	BeforeETag string              `json:"before_etag,omitempty"`
	AfterETag  string              `json:"after_etag,omitempty"`
}

type FieldDelta struct {
	Field  string `json:"field"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

type CoverageAttribution struct {
	Attributed bool   `json:"attributed"`
	Basis      string `json:"basis,omitempty"`
	Scope      string `json:"scope,omitempty"`
}

type durableDeltaRow struct {
	projectionRow
	DeletedAt *time.Time
}

func CompareDurableDelta(ctx context.Context, input DurableDeltaInput) (DurableDeltaReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	maxChanges := input.MaxChanges
	if maxChanges <= 0 {
		maxChanges = DefaultMaxMismatches
	}
	if err := validateDurableSnapshot("before", input.Before.Manifest); err != nil {
		return DurableDeltaReport{}, err
	}
	if err := validateDurableSnapshot("after", input.After.Manifest); err != nil {
		return DurableDeltaReport{}, err
	}
	if input.Before.Manifest.IndexSetID != input.After.Manifest.IndexSetID {
		return DurableDeltaReport{}, fmt.Errorf("durable delta requires matching index_set_id")
	}
	if input.Before.Manifest.RunID == input.After.Manifest.RunID {
		return DurableDeltaReport{}, fmt.Errorf("durable delta requires distinct run_id values")
	}
	if !input.Before.Manifest.CreatedAt.IsZero() && !input.After.Manifest.CreatedAt.IsZero() && input.After.Manifest.CreatedAt.Before(input.Before.Manifest.CreatedAt) {
		return DurableDeltaReport{}, fmt.Errorf("after manifest predates before manifest")
	}
	if input.RequireParent && !manifestReferencesRun(input.After.Manifest, input.Before.Manifest) {
		return DurableDeltaReport{}, fmt.Errorf("after manifest does not reference before manifest as prior row source")
	}
	if err := validateDeltaCoverage("before", input.Before.Manifest.Coverage); err != nil {
		return DurableDeltaReport{}, err
	}
	if err := validateDeltaCoverage("after", input.After.Manifest.Coverage); err != nil {
		return DurableDeltaReport{}, err
	}

	report := DurableDeltaReport{
		Type:              "gonimbus.index.durable_delta_result.v1",
		ComparatorVersion: DurableDeltaComparatorVersion,
		IndexSetID:        input.After.Manifest.IndexSetID,
		BeforeRunID:       input.Before.Manifest.RunID,
		AfterRunID:        input.After.Manifest.RunID,
		BeforeArtifact:    input.Before.Artifact,
		AfterArtifact:     input.After.Artifact,
		Coverage: DeltaCoverageSummary{
			Semantics:    "confirmed_complete_ungapped_before_and_after_scope",
			BeforeScopes: len(input.Before.Manifest.Coverage),
			AfterScopes:  len(input.After.Manifest.Coverage),
		},
	}

	beforeRows := newDurableDeltaIterator(ctx, input.Before.SegmentDir, input.Before.Manifest)
	afterRows := newDurableDeltaIterator(ctx, input.After.SegmentDir, input.After.Manifest)
	beforeHash := sha256.New()
	afterHash := sha256.New()

	before, beforeOK, err := beforeRows.Next()
	if err != nil {
		return DurableDeltaReport{}, err
	}
	after, afterOK, err := afterRows.Next()
	if err != nil {
		return DurableDeltaReport{}, err
	}
	for beforeOK || afterOK {
		if err := ctx.Err(); err != nil {
			return DurableDeltaReport{}, err
		}
		switch {
		case beforeOK && (!afterOK || before.RelKey < after.RelKey):
			report.BeforeRows++
			hashDeltaRow(beforeHash, before)
			if before.DeletedAt != nil {
				before, beforeOK, err = beforeRows.Next()
				break
			}
			if _, ok := coverageAttribution(input.Before.Manifest.Coverage, before.RelKey); !ok {
				return DurableDeltaReport{}, fmt.Errorf("before coverage does not cover tombstone candidate %q", before.RelKey)
			}
			attribution, ok := coverageAttribution(input.After.Manifest.Coverage, before.RelKey)
			if !ok {
				return DurableDeltaReport{}, fmt.Errorf("after coverage does not cover tombstone candidate %q", before.RelKey)
			}
			report.Tombstoned++
			report.Coverage.AttributedRows++
			addDeltaChange(&report, maxChanges, DeltaChange{Kind: "tombstoned", RelKey: before.RelKey, Coverage: attribution, BeforeETag: before.ETag})
			before, beforeOK, err = beforeRows.Next()
		case afterOK && (!beforeOK || after.RelKey < before.RelKey):
			report.AfterRows++
			hashDeltaRow(afterHash, after)
			if after.DeletedAt != nil {
				return DurableDeltaReport{}, fmt.Errorf("after tombstone row %q has no prior row in before snapshot", after.RelKey)
			} else {
				if _, ok := coverageAttribution(input.Before.Manifest.Coverage, after.RelKey); !ok {
					return DurableDeltaReport{}, fmt.Errorf("before coverage does not cover added row %q", after.RelKey)
				}
				attribution, ok := coverageAttribution(input.After.Manifest.Coverage, after.RelKey)
				if !ok {
					return DurableDeltaReport{}, fmt.Errorf("after coverage does not cover added row %q", after.RelKey)
				}
				report.Added++
				report.Coverage.AttributedRows++
				addDeltaChange(&report, maxChanges, DeltaChange{Kind: "added", RelKey: after.RelKey, Coverage: attribution, AfterETag: after.ETag})
			}
			after, afterOK, err = afterRows.Next()
		default:
			report.BeforeRows++
			report.AfterRows++
			hashDeltaRow(beforeHash, before)
			hashDeltaRow(afterHash, after)
			if _, ok := coverageAttribution(input.Before.Manifest.Coverage, after.RelKey); !ok {
				return DurableDeltaReport{}, fmt.Errorf("before coverage does not cover row %q", after.RelKey)
			}
			attribution, ok := coverageAttribution(input.After.Manifest.Coverage, after.RelKey)
			if !ok {
				return DurableDeltaReport{}, fmt.Errorf("after coverage does not cover row %q", after.RelKey)
			}
			changed := deltaFields(before.projectionRow, after.projectionRow)
			if before.ETag != after.ETag {
				changed = append(changed, FieldDelta{Field: "etag", Before: before.ETag, After: after.ETag})
			}
			if before.DeletedAt != nil && after.DeletedAt == nil {
				report.Added++
				report.Coverage.AttributedRows++
				addDeltaChange(&report, maxChanges, DeltaChange{Kind: "added", RelKey: after.RelKey, Coverage: attribution, Fields: changed, BeforeETag: before.ETag, AfterETag: after.ETag})
			} else if before.DeletedAt == nil && after.DeletedAt != nil {
				report.Tombstoned++
				report.Coverage.AttributedRows++
				addDeltaChange(&report, maxChanges, DeltaChange{Kind: "tombstoned", RelKey: after.RelKey, Coverage: attribution, Fields: changed, BeforeETag: before.ETag, AfterETag: after.ETag})
			} else if len(changed) > 0 {
				report.Changed++
				report.Coverage.AttributedRows++
				addDeltaChange(&report, maxChanges, DeltaChange{Kind: "changed", RelKey: after.RelKey, Coverage: attribution, Fields: changed, BeforeETag: before.ETag, AfterETag: after.ETag})
			} else {
				report.Unchanged++
			}
			before, beforeOK, err = beforeRows.Next()
			if err != nil {
				return DurableDeltaReport{}, err
			}
			after, afterOK, err = afterRows.Next()
		}
		if err != nil {
			return DurableDeltaReport{}, err
		}
	}
	report.BeforeSHA256 = hex.EncodeToString(beforeHash.Sum(nil))
	report.AfterSHA256 = hex.EncodeToString(afterHash.Sum(nil))
	return report, nil
}

func validateDurableSnapshot(label string, manifest indexsubstrate.InternalManifest) error {
	switch {
	case manifest.Type != indexsubstrate.ManifestType:
		return fmt.Errorf("%s manifest type %q is unsupported", label, manifest.Type)
	case manifest.Render != indexsubstrate.ManifestRenderType:
		return fmt.Errorf("%s manifest render %q is unsupported", label, manifest.Render)
	case manifest.IndexSchemaVersion != indexsubstrate.IndexSchemaVersion:
		return fmt.Errorf("%s manifest schema version %d is unsupported", label, manifest.IndexSchemaVersion)
	case strings.TrimSpace(manifest.IndexSetID) == "":
		return fmt.Errorf("%s manifest index_set_id is required", label)
	case strings.TrimSpace(manifest.RunID) == "":
		return fmt.Errorf("%s manifest run_id is required", label)
	default:
		return nil
	}
}

func validateDeltaCoverage(label string, coverage []indexsubstrate.CoverageAttestation) error {
	if len(coverage) == 0 {
		return fmt.Errorf("%s manifest coverage is required for durable delta", label)
	}
	for _, entry := range coverage {
		if entry.Basis != indexsubstrate.CoverageBasisConfirmed || !entry.Complete {
			return fmt.Errorf("%s manifest coverage must be confirmed and complete", label)
		}
		if len(entry.Gaps) != 0 {
			return fmt.Errorf("%s manifest coverage must be ungapped", label)
		}
		if entry.Scope == nil || entry.Scope.Window != nil || strings.TrimSpace(entry.Scope.Prefix) == "" {
			return fmt.Errorf("%s manifest coverage scope must be explicit non-windowed prefix coverage", label)
		}
	}
	return nil
}

func manifestReferencesRun(after, before indexsubstrate.InternalManifest) bool {
	for _, ref := range after.ParentManifests {
		if ref.IndexSetID == before.IndexSetID && ref.RunID == before.RunID {
			return true
		}
	}
	return false
}

func coverageAttribution(coverage []indexsubstrate.CoverageAttestation, relKey string) (CoverageAttribution, bool) {
	if coverageHasGapForRelKeyLocal(coverage, relKey) {
		return CoverageAttribution{}, false
	}
	for _, entry := range coverage {
		if entry.Basis != indexsubstrate.CoverageBasisConfirmed || !entry.Complete || len(entry.Gaps) != 0 {
			continue
		}
		if coverageScopeContainsRelKey(entry.Scope, relKey) {
			return CoverageAttribution{
				Attributed: true,
				Basis:      string(entry.Basis),
				Scope:      cleanCoveragePrefixLocal(entry.Scope.Prefix),
			}, true
		}
	}
	return CoverageAttribution{}, false
}

func coverageHasGapForRelKeyLocal(coverage []indexsubstrate.CoverageAttestation, relKey string) bool {
	for _, entry := range coverage {
		for i := range entry.Gaps {
			if gapScopeContainsRelKeyLocal(&entry.Gaps[i], relKey) {
				return true
			}
		}
	}
	return false
}

func coverageScopeContainsRelKey(scope *indexsubstrate.Scope, relKey string) bool {
	if scope == nil || scope.Window != nil {
		return false
	}
	prefix := cleanCoveragePrefixLocal(scope.Prefix)
	if prefix == "" {
		return false
	}
	if prefix == indexsubstrate.RelativeRootScopePrefix {
		return true
	}
	return prefixContainsRelKeyLocal(prefix, relKey)
}

func gapScopeContainsRelKeyLocal(scope *indexsubstrate.Scope, relKey string) bool {
	if scope == nil {
		return true
	}
	prefix := cleanCoveragePrefixLocal(scope.Prefix)
	if prefix == "" || prefix == indexsubstrate.RelativeRootScopePrefix {
		return true
	}
	return prefixContainsRelKeyLocal(prefix, relKey)
}

func cleanCoveragePrefixLocal(prefix string) string {
	return strings.TrimPrefix(strings.TrimSpace(prefix), "/")
}

func prefixContainsRelKeyLocal(prefix string, relKey string) bool {
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(relKey, prefix)
	}
	return relKey == prefix || strings.HasPrefix(relKey, prefix+"/")
}

func deltaFields(before, after projectionRow) []FieldDelta {
	var out []FieldDelta
	if before.SizeBytes != after.SizeBytes {
		out = append(out, FieldDelta{Field: "size_bytes", Before: fmt.Sprintf("%d", before.SizeBytes), After: fmt.Sprintf("%d", after.SizeBytes)})
	}
	if before.LastModified != after.LastModified {
		out = append(out, FieldDelta{Field: "last_modified", Before: before.LastModified, After: after.LastModified})
	}
	if before.StorageClass != after.StorageClass {
		out = append(out, FieldDelta{Field: "storage_class", Before: before.StorageClass, After: after.StorageClass})
	}
	return out
}

func addDeltaChange(report *DurableDeltaReport, max int, change DeltaChange) {
	if int64(len(report.Changes)) < int64(max) {
		report.Changes = append(report.Changes, change)
	}
}

func hashDeltaRow(h interface{ Write([]byte) (int, error) }, row durableDeltaRow) {
	data, _ := json.Marshal(struct {
		Projection projectionRow `json:"projection"`
		Deleted    bool          `json:"deleted"`
	}{
		Projection: row.projectionRow,
		Deleted:    row.DeletedAt != nil,
	})
	_, _ = h.Write(data)
	_, _ = h.Write([]byte{'\n'})
}
