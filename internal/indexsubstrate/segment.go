package indexsubstrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	ManifestType         = "gonimbus.index.manifest.v1"
	SegmentFormatParquet = "parquet"
	ManifestRenderType   = "internal"

	DefaultTargetRowsPerSegment = 500_000
	SegmentSizingRationale      = "real-corpus rebaseline for hub export and hydrate artifact counts"

	ManifestReachabilityModel = "retained_manifests_parent_chain_latest_pointers"
	SegmentNamespaceShared    = "shared_immutable_segments"
	RefcountModeDerivedAudit  = "derived_audit_plan_only"
	CompactOwnerIndexCompact  = "index compact"
)

type SegmentWriterConfig struct {
	Dir                    string
	IndexSetID             string
	RunID                  string
	CreatedAt              time.Time
	TargetRowsPerSegment   int
	AllowExistingIdentical bool
	ParentManifests        []ManifestReference
	Coverage               []CoverageAttestation
}

type InternalManifest struct {
	Type               string                `json:"type"`
	Render             string                `json:"render"`
	IndexSetID         string                `json:"index_set_id"`
	RunID              string                `json:"run_id"`
	IndexSchemaVersion int                   `json:"index_schema_version"`
	CreatedAt          time.Time             `json:"created_at"`
	ParentManifests    []ManifestReference   `json:"parent_manifests"`
	Reachability       ManifestReachability  `json:"reachability"`
	SegmentSizing      SegmentSizing         `json:"segment_sizing"`
	Coverage           []CoverageAttestation `json:"coverage"`
	Counts             ManifestCounts        `json:"counts"`
	Segments           []SegmentDescriptor   `json:"segments"`
}

type ManifestReference struct {
	IndexSetID     string `json:"index_set_id"`
	RunID          string `json:"run_id"`
	ManifestSHA256 string `json:"manifest_sha256,omitempty"`
}

type ManifestReachability struct {
	Model            string `json:"model"`
	SegmentNamespace string `json:"segment_namespace"`
	RefcountMode     string `json:"refcount_mode"`
	CompactOwner     string `json:"compact_owner"`
}

type SegmentSizing struct {
	TargetRowsPerSegment int    `json:"target_rows_per_segment"`
	Rationale            string `json:"rationale"`
}

type ManifestCounts struct {
	Rows          int `json:"rows"`
	ActiveRows    int `json:"active_rows"`
	Tombstones    int `json:"tombstones"`
	DistinctETags int `json:"distinct_etags"`
}

type SegmentDescriptor struct {
	SegmentID     string        `json:"segment_id"`
	Path          string        `json:"path"`
	Format        string        `json:"format"`
	Compression   string        `json:"compression"`
	Rows          int           `json:"rows"`
	Tombstones    int           `json:"tombstones"`
	SizeBytes     int64         `json:"size_bytes"`
	MinRelKey     string        `json:"min_rel_key,omitempty"`
	MaxRelKey     string        `json:"max_rel_key,omitempty"`
	DistinctETags int           `json:"distinct_etags"`
	Digest        SegmentDigest `json:"digest"`
}

type SegmentDigest struct {
	Algorithm string `json:"algorithm"`
	Hex       string `json:"hex"`
}

type segmentParquetRow struct {
	IndexSetID       string  `parquet:"index_set_id"`
	RelKey           string  `parquet:"rel_key"`
	SizeBytes        int64   `parquet:"size_bytes"`
	LastModified     *string `parquet:"last_modified,optional"`
	ETag             string  `parquet:"etag"`
	StorageClass     *string `parquet:"storage_class,optional"`
	ArchiveStatus    *string `parquet:"archive_status,optional"`
	RestoreState     *string `parquet:"restore_state,optional"`
	RestoreExpiry    *string `parquet:"restore_expiry,optional"`
	ContentType      *string `parquet:"content_type,optional"`
	HeadEnrichedAt   *string `parquet:"head_enriched_at,optional"`
	FirstSeenRunID   string  `parquet:"first_seen_run_id"`
	FirstSeenAt      string  `parquet:"first_seen_at"`
	LastChangedRunID string  `parquet:"last_changed_run_id"`
	LastChangedAt    string  `parquet:"last_changed_at"`
	LastSeenRunID    string  `parquet:"last_seen_run_id"`
	LastSeenAt       string  `parquet:"last_seen_at"`
	DeletedAt        *string `parquet:"deleted_at,optional"`
}

func WriteSegmentSet(config SegmentWriterConfig, rows []CurrentObjectRow) (InternalManifest, error) {
	config = normalizeSegmentWriterConfig(config)
	if err := validateSegmentWriterConfig(config); err != nil {
		return InternalManifest{}, err
	}
	if err := validateManifestReferences(config.ParentManifests); err != nil {
		return InternalManifest{}, err
	}
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return InternalManifest{}, fmt.Errorf("create segment directory: %w", err)
	}

	sortedRows, err := normalizeAndSortSegmentRows(rows, config.IndexSetID)
	if err != nil {
		return InternalManifest{}, err
	}
	manifest := InternalManifest{
		Type:               ManifestType,
		Render:             ManifestRenderType,
		IndexSetID:         config.IndexSetID,
		RunID:              config.RunID,
		IndexSchemaVersion: IndexSchemaVersion,
		CreatedAt:          config.CreatedAt,
		ParentManifests:    normalizeManifestReferences(config.ParentManifests),
		Reachability:       DefaultManifestReachability(),
		Coverage:           copyCoverageAttestations(config.Coverage),
		SegmentSizing: SegmentSizing{
			TargetRowsPerSegment: config.TargetRowsPerSegment,
			Rationale:            SegmentSizingRationale,
		},
		Counts: manifestCounts(sortedRows),
	}

	for start := 0; start < len(sortedRows); start += config.TargetRowsPerSegment {
		end := start + config.TargetRowsPerSegment
		if end > len(sortedRows) {
			end = len(sortedRows)
		}
		descriptor, err := writeSegmentFile(config, len(manifest.Segments), sortedRows[start:end])
		if err != nil {
			return InternalManifest{}, err
		}
		manifest.Segments = append(manifest.Segments, descriptor)
	}
	return manifest, nil
}

func WriteInternalManifestFile(path string, manifest InternalManifest) error {
	return writeInternalManifestFile(path, manifest, false)
}

func writeInternalManifestFile(path string, manifest InternalManifest, allowExistingIdentical bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("manifest path is required")
	}
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create manifest directory: %w", err)
	}
	data, err := marshalIndentedJSON(manifest)
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".manifest-*.json.tmp")
	if err != nil {
		return fmt.Errorf("create temporary manifest: %w", err)
	}
	tempPath := temp.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	_, encodeErr := temp.Write(data)
	closeErr := temp.Close()
	if encodeErr != nil {
		return fmt.Errorf("write manifest: %w", encodeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close manifest: %w", closeErr)
	}
	if err := linkImmutableManifest(tempPath, filepath.Join(dir, name)); err != nil {
		if allowExistingIdentical && errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(filepath.Join(dir, name))
			if readErr != nil {
				return fmt.Errorf("read existing manifest: %w", readErr)
			}
			if string(existing) == string(data) {
				return nil
			}
		}
		return err
	}
	cleanupTemp = false
	_ = os.Remove(tempPath)
	return nil
}

func ReadInternalManifestFile(path string) (InternalManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return InternalManifest{}, fmt.Errorf("manifest path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return InternalManifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var manifest InternalManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return InternalManifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	return manifest, nil
}

func ReadSegmentFile(path string) ([]CurrentObjectRow, error) {
	out := []CurrentObjectRow{}
	if err := WalkSegmentFile(path, func(row CurrentObjectRow) error {
		out = append(out, row)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func WalkSegmentFile(path string, visit func(CurrentObjectRow) error) error {
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open segment directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	defer func() { _ = file.Close() }()
	reader := parquet.NewGenericReader[segmentParquetRow](file)
	defer func() { _ = reader.Close() }()
	rows := make([]segmentParquetRow, 64)
	for {
		n, err := reader.Read(rows)
		for _, row := range rows[:n] {
			current, convErr := currentRowFromSegmentParquet(row)
			if convErr != nil {
				return convErr
			}
			if err := visit(current); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read segment: %w", err)
		}
	}
	return nil
}

func ReadSegmentFileVerified(dir string, descriptor SegmentDescriptor) ([]CurrentObjectRow, error) {
	out := []CurrentObjectRow{}
	if err := WalkSegmentFileVerified(dir, descriptor, func(row CurrentObjectRow) error {
		out = append(out, row)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func WalkSegmentFileVerified(dir string, descriptor SegmentDescriptor, visit func(CurrentObjectRow) error) error {
	path, err := safeSegmentPath(dir, descriptor.Path)
	if err != nil {
		return err
	}
	if descriptor.Digest.Algorithm != "sha256" {
		return fmt.Errorf("unsupported segment digest algorithm %q", descriptor.Digest.Algorithm)
	}
	if strings.TrimSpace(descriptor.Digest.Hex) == "" {
		return fmt.Errorf("segment digest is required")
	}
	got, err := sha256HexFile(path)
	if err != nil {
		return err
	}
	if got != descriptor.Digest.Hex {
		return fmt.Errorf("segment digest mismatch for %s", descriptor.Path)
	}
	return WalkSegmentFile(path, visit)
}

func ReadManifestRows(segmentDir string, manifest InternalManifest) ([]CurrentObjectRow, error) {
	rows := make([]CurrentObjectRow, 0, manifest.Counts.Rows)
	err := WalkManifestRows(segmentDir, manifest, func(row CurrentObjectRow) error {
		rows = append(rows, row)
		return nil
	})
	return rows, err
}

func WalkManifestRows(segmentDir string, manifest InternalManifest, visit func(CurrentObjectRow) error) error {
	for _, segment := range manifest.Segments {
		if err := WalkSegmentFileVerified(segmentDir, segment, visit); err != nil {
			return err
		}
	}
	return nil
}

func writeSegmentFile(config SegmentWriterConfig, ordinal int, rows []CurrentObjectRow) (SegmentDescriptor, error) {
	temp, err := os.CreateTemp(config.Dir, ".segment-*.parquet.tmp")
	if err != nil {
		return SegmentDescriptor{}, fmt.Errorf("create temporary segment: %w", err)
	}
	tempPath := temp.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	digest := sha256.New()
	if err := writeParquetRows(io.MultiWriter(temp, digest), config, rows); err != nil {
		_ = temp.Close()
		return SegmentDescriptor{}, err
	}
	if err := temp.Close(); err != nil {
		return SegmentDescriptor{}, fmt.Errorf("close temporary segment: %w", err)
	}

	digestHex := hex.EncodeToString(digest.Sum(nil))
	descriptor := segmentDescriptor(config, ordinal, rows, digestHex)
	finalPath := filepath.Join(config.Dir, descriptor.Path)
	if err := linkImmutable(tempPath, finalPath); err != nil {
		if config.AllowExistingIdentical && errors.Is(err, os.ErrExist) {
			existingDigest, digestErr := sha256HexFile(finalPath)
			if digestErr != nil {
				return SegmentDescriptor{}, fmt.Errorf("hash existing segment: %w", digestErr)
			}
			if existingDigest == digestHex {
				info, statErr := os.Stat(finalPath)
				if statErr != nil {
					return SegmentDescriptor{}, fmt.Errorf("stat existing segment: %w", statErr)
				}
				descriptor.SizeBytes = info.Size()
				return descriptor, nil
			}
		}
		return SegmentDescriptor{}, err
	}
	cleanupTemp = false
	_ = os.Remove(tempPath)

	info, err := os.Stat(finalPath)
	if err != nil {
		return SegmentDescriptor{}, fmt.Errorf("stat segment: %w", err)
	}
	descriptor.SizeBytes = info.Size()
	return descriptor, nil
}

func writeParquetRows(output io.Writer, config SegmentWriterConfig, rows []CurrentObjectRow) error {
	writer := parquet.NewGenericWriter[segmentParquetRow](output,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(int64(config.TargetRowsPerSegment)),
		parquet.CreatedBy("gonimbus", "indexsubstrate", "v1"),
		parquet.KeyValueMetadata("gonimbus.index_set_id", config.IndexSetID),
		parquet.KeyValueMetadata("gonimbus.run_id", config.RunID),
		parquet.KeyValueMetadata("gonimbus.segment_format", SegmentFormatParquet),
	)
	parquetRows := make([]segmentParquetRow, len(rows))
	for i, row := range rows {
		parquetRows[i] = segmentParquetFromCurrentRow(row)
	}
	if _, err := writer.Write(parquetRows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write segment rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close segment writer: %w", err)
	}
	return nil
}

func segmentDescriptor(config SegmentWriterConfig, ordinal int, rows []CurrentObjectRow, digestHex string) SegmentDescriptor {
	segmentID := fmt.Sprintf("seg_%06d_%s", ordinal, digestHex[:16])
	descriptor := SegmentDescriptor{
		SegmentID:     segmentID,
		Path:          segmentID + ".parquet",
		Format:        SegmentFormatParquet,
		Compression:   "snappy",
		Rows:          len(rows),
		Tombstones:    tombstoneCount(rows),
		DistinctETags: distinctETagCount(rows),
		Digest: SegmentDigest{
			Algorithm: "sha256",
			Hex:       digestHex,
		},
	}
	if len(rows) > 0 {
		descriptor.MinRelKey = rows[0].RelKey
		descriptor.MaxRelKey = rows[len(rows)-1].RelKey
	}
	return descriptor
}

func linkImmutable(tempPath, finalPath string) error {
	if err := os.Link(tempPath, finalPath); err != nil {
		return fmt.Errorf("create immutable segment: %w", err)
	}
	return nil
}

func linkImmutableManifest(tempPath, finalPath string) error {
	if err := os.Link(tempPath, finalPath); err != nil {
		return fmt.Errorf("create immutable manifest: %w", err)
	}
	return nil
}

func marshalIndentedJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func sha256HexFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func safeSegmentPath(dir string, name string) (string, error) {
	dir = strings.TrimSpace(dir)
	name = filepath.Clean(strings.TrimSpace(name))
	if dir == "" {
		return "", fmt.Errorf("segment directory is required")
	}
	if name == "" || name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(filepath.Separator)) || name == ".." {
		return "", fmt.Errorf("invalid segment path")
	}
	return filepath.Join(dir, name), nil
}

func normalizeSegmentWriterConfig(config SegmentWriterConfig) SegmentWriterConfig {
	config.Dir = strings.TrimSpace(config.Dir)
	config.IndexSetID = strings.TrimSpace(config.IndexSetID)
	config.RunID = strings.TrimSpace(config.RunID)
	if config.CreatedAt.IsZero() {
		config.CreatedAt = time.Now().UTC()
	} else {
		config.CreatedAt = config.CreatedAt.UTC()
	}
	if config.TargetRowsPerSegment <= 0 {
		config.TargetRowsPerSegment = DefaultTargetRowsPerSegment
	}
	return config
}

func validateSegmentWriterConfig(config SegmentWriterConfig) error {
	switch {
	case config.Dir == "":
		return fmt.Errorf("segment directory is required")
	case config.IndexSetID == "":
		return fmt.Errorf("index_set_id is required")
	case config.RunID == "":
		return fmt.Errorf("run_id is required")
	case config.TargetRowsPerSegment <= 0:
		return fmt.Errorf("target rows per segment must be positive")
	default:
		return nil
	}
}

func validateManifestReferences(refs []ManifestReference) error {
	for _, ref := range normalizeManifestReferences(refs) {
		if ref.IndexSetID == "" || ref.RunID == "" {
			return fmt.Errorf("parent manifest index_set_id and run_id are required")
		}
	}
	return nil
}

func copyCoverageAttestations(in []CoverageAttestation) []CoverageAttestation {
	if len(in) == 0 {
		return nil
	}
	out := make([]CoverageAttestation, 0, len(in))
	for _, entry := range in {
		copied := entry
		if entry.Scope != nil {
			scope := *entry.Scope
			copied.Scope = &scope
		}
		if len(entry.Gaps) > 0 {
			copied.Gaps = append([]Scope(nil), entry.Gaps...)
		}
		out = append(out, copied)
	}
	return out
}

func normalizeAndSortSegmentRows(rows []CurrentObjectRow, indexSetID string) ([]CurrentObjectRow, error) {
	out := make([]CurrentObjectRow, 0, len(rows))
	for _, row := range rows {
		row = normalizeCurrentObjectRow(row)
		if row.RelKey == "" {
			return nil, fmt.Errorf("segment row rel_key is required")
		}
		if row.IndexSetID == "" {
			row.IndexSetID = indexSetID
		}
		if row.IndexSetID != indexSetID {
			return nil, fmt.Errorf("segment row index_set_id mismatch")
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RelKey < out[j].RelKey
	})
	return out, nil
}

func manifestCounts(rows []CurrentObjectRow) ManifestCounts {
	return ManifestCounts{
		Rows:          len(rows),
		ActiveRows:    len(rows) - tombstoneCount(rows),
		Tombstones:    tombstoneCount(rows),
		DistinctETags: distinctETagCount(rows),
	}
}

func tombstoneCount(rows []CurrentObjectRow) int {
	count := 0
	for _, row := range rows {
		if row.DeletedAt != nil {
			count++
		}
	}
	return count
}

func distinctETagCount(rows []CurrentObjectRow) int {
	seen := make(map[string]struct{})
	for _, row := range rows {
		if row.ETag == "" {
			continue
		}
		seen[row.ETag] = struct{}{}
	}
	return len(seen)
}

func segmentParquetFromCurrentRow(row CurrentObjectRow) segmentParquetRow {
	row = normalizeCurrentObjectRow(row)
	return segmentParquetRow{
		IndexSetID:       row.IndexSetID,
		RelKey:           row.RelKey,
		SizeBytes:        row.SizeBytes,
		LastModified:     timeStringPtr(row.LastModified),
		ETag:             row.ETag,
		StorageClass:     stringPtrCopy(row.StorageClass),
		ArchiveStatus:    stringPtrCopy(row.ArchiveStatus),
		RestoreState:     stringPtrCopy(row.RestoreState),
		RestoreExpiry:    timeStringPtr(row.RestoreExpiry),
		ContentType:      stringPtrCopy(row.ContentType),
		HeadEnrichedAt:   timeStringPtr(row.HeadEnrichedAt),
		FirstSeenRunID:   row.FirstSeenRunID,
		FirstSeenAt:      timeString(row.FirstSeenAt),
		LastChangedRunID: row.LastChangedRunID,
		LastChangedAt:    timeString(row.LastChangedAt),
		LastSeenRunID:    row.LastSeenRunID,
		LastSeenAt:       timeString(row.LastSeenAt),
		DeletedAt:        timeStringPtr(row.DeletedAt),
	}
}

func currentRowFromSegmentParquet(row segmentParquetRow) (CurrentObjectRow, error) {
	lastModified, err := parseOptionalTime(row.LastModified)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	restoreExpiry, err := parseOptionalTime(row.RestoreExpiry)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	headEnrichedAt, err := parseOptionalTime(row.HeadEnrichedAt)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	firstSeenAt, err := parseRequiredTime(row.FirstSeenAt)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	lastChangedAt, err := parseRequiredTime(row.LastChangedAt)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	lastSeenAt, err := parseRequiredTime(row.LastSeenAt)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	deletedAt, err := parseOptionalTime(row.DeletedAt)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	return normalizeCurrentObjectRow(CurrentObjectRow{
		IndexSetID:       row.IndexSetID,
		RelKey:           row.RelKey,
		SizeBytes:        row.SizeBytes,
		LastModified:     lastModified,
		ETag:             row.ETag,
		StorageClass:     stringPtrCopy(row.StorageClass),
		ArchiveStatus:    stringPtrCopy(row.ArchiveStatus),
		RestoreState:     stringPtrCopy(row.RestoreState),
		RestoreExpiry:    restoreExpiry,
		ContentType:      stringPtrCopy(row.ContentType),
		HeadEnrichedAt:   headEnrichedAt,
		FirstSeenRunID:   row.FirstSeenRunID,
		FirstSeenAt:      firstSeenAt,
		LastChangedRunID: row.LastChangedRunID,
		LastChangedAt:    lastChangedAt,
		LastSeenRunID:    row.LastSeenRunID,
		LastSeenAt:       lastSeenAt,
		DeletedAt:        deletedAt,
	}), nil
}

func timeStringPtr(value *time.Time) *string {
	if value == nil {
		return nil
	}
	out := timeString(*value)
	return &out
}

func timeString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseOptionalTime(value *string) (*time.Time, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := parseRequiredTime(*value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseRequiredTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse segment timestamp: %w", err)
	}
	return parsed.UTC(), nil
}
