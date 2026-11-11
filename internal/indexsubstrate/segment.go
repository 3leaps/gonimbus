package indexsubstrate

import (
	"crypto/sha256"
	"encoding/binary"
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
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"

	"github.com/3leaps/gonimbus/internal/buildinfo"
	"github.com/3leaps/gonimbus/pkg/durableindex"
)

const (
	ManifestType            = "gonimbus.index.manifest.v1"
	SegmentFormatParquet    = "parquet"
	ManifestRenderType      = "internal"
	DotSegmentPolicyInclude = "include"

	DefaultTargetRowsPerSegment = 500_000
	SegmentSizingRationale      = "real-corpus rebaseline for hub export and hydrate artifact counts"

	ManifestReachabilityModel = "retained_manifests_parent_chain_latest_pointers"
	SegmentNamespaceShared    = "shared_immutable_segments"
	RefcountModeDerivedAudit  = "derived_audit_plan_only"
	CompactOwnerIndexCompact  = "index compact"
)

// SegmentProgress is a sanitized, observational progress signal for segment
// writes. Counts/phase only — never object keys, rel_keys, or prefixes.
type SegmentProgress struct {
	Segment  int // 1-based index of the segment just written
	Total    int // planned segment count for this write
	Rows     int // rows in the segment just written
	RowsDone int // cumulative rows written across segments so far
}

// OnSegmentProgressFunc is optional and observational. Callers treat it as
// best-effort and never use its outcome as a publish failure vector.
type OnSegmentProgressFunc func(progress SegmentProgress)

type SegmentWriterConfig struct {
	Dir                    string
	IndexSetID             string
	RunID                  string
	GonimbusVersion        string
	CreatedAt              time.Time
	TargetRowsPerSegment   int
	AllowExistingIdentical bool
	ParentManifests        []ManifestReference
	Coverage               []CoverageAttestation
	// Continuity lineage inputs supplied by the durable build path: ordinary
	// builds set these from the verified parent capture (a first publication
	// emits a baseline lineage record with no state parent).
	RunStartedAt *time.Time
	StateParent  *StateParent
	Lineage      *LineageRecord
	// OnSegmentProgress is optional. Invoked after each segment file is written;
	// outside persisted artifact bytes and ignored for failure control.
	OnSegmentProgress OnSegmentProgressFunc
	// segmentOps is an optional per-invocation filesystem seam for hermetic
	// seal/cleanup tests. Nil selects production os operations. Unexported so
	// it is not part of the public config surface.
	segmentOps *segmentFileOps
}

type InternalManifest struct {
	Type               string    `json:"type"`
	Render             string    `json:"render"`
	IndexSetID         string    `json:"index_set_id"`
	RunID              string    `json:"run_id"`
	GonimbusVersion    string    `json:"gonimbus_version,omitempty"`
	DotSegmentPolicy   string    `json:"dot_segment_policy,omitempty"`
	IndexSchemaVersion int       `json:"index_schema_version"`
	CreatedAt          time.Time `json:"created_at"`
	// RunStartedAt is the optional authoritative run start.
	// Not an alias of CreatedAt, journal start, or complete-marker time.
	// Omitted on pre-continuity / legacy manifests; never invent from other
	// timestamps. Pointer so JSON omitempty works (time.Time zero is not omitted).
	// When Lineage is present, a non-zero UTC value is required.
	RunStartedAt *time.Time `json:"run_started_at,omitempty"`
	// StateParent is the optional exact single continuous-state parent binding
	// (set/run/digest). Distinct from ParentManifests (reachability/enrich
	// heritage; digest optional there). Requires Lineage when present.
	StateParent *StateParent `json:"state_parent,omitempty"`
	// Lineage is the optional all-or-nothing continuous-lineage record.
	// Absence means pre-continuity (verified state source only — not a delta boundary).
	Lineage *LineageRecord `json:"lineage,omitempty"`
	// ParentManifests remain segment-reachability / enrich heritage references.
	// Digests stay optional. Do not auto-translate into StateParent.
	ParentManifests []ManifestReference   `json:"parent_manifests"`
	Reachability    ManifestReachability  `json:"reachability"`
	SegmentSizing   SegmentSizing         `json:"segment_sizing"`
	Coverage        []CoverageAttestation `json:"coverage"`
	Counts          ManifestCounts        `json:"counts"`
	Segments        []SegmentDescriptor   `json:"segments"`
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
	Rows                   int `json:"rows"`
	ActiveRows             int `json:"active_rows"`
	Tombstones             int `json:"tombstones"`
	DistinctETags          int `json:"distinct_etags"`
	RetainedUnobservedRows int `json:"retained_unobserved_rows"`
	DotSegmentRows         int `json:"dot_segment_rows"`
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

// durableParquetRow implements the DDR-0004 parquet-inventory/v0 physical row schema.
type durableParquetRow struct {
	Namespace    string                     `parquet:"namespace,dict"`
	Key          string                     `parquet:"key,plain"`
	Revision     *durableParquetRevision    `parquet:"revision,optional"`
	SizeBytes    int64                      `parquet:"size_bytes,int(64),plain"`
	LastModified *string                    `parquet:"last_modified,optional,plain"`
	StorageClass *string                    `parquet:"storage_class,optional,dict"`
	FirstSeen    durableParquetStamp        `parquet:"first_seen"`
	LastSeen     durableParquetStamp        `parquet:"last_seen"`
	LastChanged  durableParquetStamp        `parquet:"last_changed"`
	Deleted      *durableParquetStamp       `parquet:"deleted,optional"`
	Enrichments  []durableParquetEnrichment `parquet:"enrichments,list"`
}

type durableParquetRevision struct {
	Kind  string `parquet:"kind,plain"`
	Value string `parquet:"value,plain"`
}

type durableParquetStamp struct {
	SnapshotID string `parquet:"snapshot_id,dict"`
	At         string `parquet:"at,dict"`
}

type durableParquetEnrichment struct {
	Name            string                 `parquet:"name,dict"`
	Value           *string                `parquet:"value,optional,plain"`
	Operation       string                 `parquet:"operation,dict"`
	ObservedAt      string                 `parquet:"observed_at,dict"`
	AgainstRevision durableParquetRevision `parquet:"against_revision"`
}

var durableParquetSchema = parquet.SchemaOf(new(durableParquetRow))

func isDurableParquetSchema(pf *parquet.File) bool {
	if pf == nil || pf.Schema() == nil {
		return false
	}
	for _, f := range pf.Schema().Fields() {
		if f.Name() == "namespace" {
			return true
		}
	}
	return false
}

// maxSegmentUncompressedPayloadBytes caps the estimated uncompressed byte footprint
// of a single segment at 64 MiB so the encoded Parquet file (including page headers,
// definition/repetition levels, and dictionary pages) stays well below the 128 MiB
// DefaultMaxParquetDecompressedBytes reader ceiling.
const maxSegmentUncompressedPayloadBytes int64 = 64 * 1024 * 1024

func estimatedDurableParquetRowBytes(pq durableParquetRow) int64 {
	const fixedPerRowOverhead = 256
	n := int64(fixedPerRowOverhead + len(pq.Namespace) + len(pq.Key))
	if pq.Revision != nil {
		n += int64(len(pq.Revision.Kind) + len(pq.Revision.Value))
	}
	if pq.LastModified != nil {
		n += int64(len(*pq.LastModified))
	}
	if pq.StorageClass != nil {
		n += int64(len(*pq.StorageClass))
	}
	n += int64(len(pq.FirstSeen.SnapshotID) + len(pq.FirstSeen.At))
	n += int64(len(pq.LastSeen.SnapshotID) + len(pq.LastSeen.At))
	n += int64(len(pq.LastChanged.SnapshotID) + len(pq.LastChanged.At))
	if pq.Deleted != nil {
		n += int64(len(pq.Deleted.SnapshotID) + len(pq.Deleted.At))
	}
	for _, e := range pq.Enrichments {
		n += int64(64 + len(e.Name) + len(e.Operation) + len(e.ObservedAt) + len(e.AgainstRevision.Kind) + len(e.AgainstRevision.Value))
		if e.Value != nil {
			n += int64(len(*e.Value))
		}
	}
	return n
}

func WriteSegmentSet(config SegmentWriterConfig, rows []CurrentObjectRow) (InternalManifest, error) {
	config, runStartedAt, err := prepareSegmentWriter(config)
	if err != nil {
		return InternalManifest{}, err
	}
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return InternalManifest{}, fmt.Errorf("create segment directory: %w", err)
	}

	sortedRows, err := normalizeAndSortSegmentRows(rows, config.IndexSetID)
	if err != nil {
		return InternalManifest{}, err
	}
	manifest := newSegmentManifestSkeleton(config, runStartedAt)
	manifest.Counts = manifestCountsForRun(sortedRows, config.RunID)

	var chunks [][]durableParquetRow
	var curChunk []durableParquetRow
	var curBytes int64
	for _, row := range sortedRows {
		pq := durableParquetFromCurrentRow(row, config.RunID)
		rowBytes := estimatedDurableParquetRowBytes(pq)
		if len(curChunk) > 0 && curBytes+rowBytes > maxSegmentUncompressedPayloadBytes {
			chunks = append(chunks, curChunk)
			curChunk = nil
			curBytes = 0
		}
		curChunk = append(curChunk, pq)
		curBytes += rowBytes
		if len(curChunk) >= config.TargetRowsPerSegment || curBytes >= maxSegmentUncompressedPayloadBytes {
			chunks = append(chunks, curChunk)
			curChunk = nil
			curBytes = 0
		}
	}
	if len(curChunk) > 0 {
		chunks = append(chunks, curChunk)
	}

	totalSegments := len(chunks)
	rowsDone := 0
	for _, chunk := range chunks {
		descriptor, _, err := writeParquetSegmentFileTracked(config, len(manifest.Segments), chunk)
		if err != nil {
			return InternalManifest{}, err
		}
		manifest.Segments = append(manifest.Segments, descriptor)
		rowsDone += len(chunk)
		if config.OnSegmentProgress != nil {
			// Observational only: never treat progress callback outcome as publish failure.
			config.OnSegmentProgress(SegmentProgress{
				Segment:  len(manifest.Segments),
				Total:    totalSegments,
				Rows:     len(chunk),
				RowsDone: rowsDone,
			})
		}
	}
	return manifest, nil
}

// prepareSegmentWriter normalizes config and validates lineage structure using
// the caller's raw RunStartedAt before any UTC clone. Does not create Dir.
func prepareSegmentWriter(config SegmentWriterConfig) (SegmentWriterConfig, *time.Time, error) {
	config = normalizeSegmentWriterConfig(config)
	if err := validateSegmentWriterConfig(config); err != nil {
		return SegmentWriterConfig{}, nil, err
	}
	if err := validateManifestReferences(config.ParentManifests); err != nil {
		return SegmentWriterConfig{}, nil, err
	}
	// Structural lineage validation before creating any segment artifacts.
	// Validate the caller's original RunStartedAt (including zone/offset) before
	// any UTC normalization — cloning first would silently accept non-UTC input.
	probe := InternalManifest{
		IndexSetID:   config.IndexSetID,
		RunID:        config.RunID,
		RunStartedAt: config.RunStartedAt,
		StateParent:  NormalizeStateParent(config.StateParent),
		Lineage:      NormalizeLineage(config.Lineage),
	}
	if err := ValidateManifestLineageStructure(probe); err != nil {
		return SegmentWriterConfig{}, nil, err
	}
	// Second check at emission: never rewrite a non-UTC offset into UTC.
	runStartedAt, err := cloneAcceptedRunStartedAt(config.RunStartedAt)
	if err != nil {
		return SegmentWriterConfig{}, nil, err
	}
	return config, runStartedAt, nil
}

func newSegmentManifestSkeleton(config SegmentWriterConfig, runStartedAt *time.Time) InternalManifest {
	return InternalManifest{
		Type:               ManifestType,
		Render:             ManifestRenderType,
		IndexSetID:         config.IndexSetID,
		RunID:              config.RunID,
		GonimbusVersion:    config.GonimbusVersion,
		DotSegmentPolicy:   DotSegmentPolicyInclude,
		IndexSchemaVersion: IndexSchemaVersion,
		CreatedAt:          config.CreatedAt,
		RunStartedAt:       runStartedAt,
		StateParent:        NormalizeStateParent(config.StateParent),
		Lineage:            NormalizeLineage(config.Lineage),
		ParentManifests:    normalizeManifestReferences(config.ParentManifests),
		Reachability:       DefaultManifestReachability(),
		Coverage:           copyCoverageAttestations(config.Coverage),
		SegmentSizing: SegmentSizing{
			TargetRowsPerSegment: config.TargetRowsPerSegment,
			Rationale:            SegmentSizingRationale,
		},
	}
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
	return ReadInternalManifestFileBounded(path, DefaultMaxPublishedManifestBytes)
}

// ReadInternalManifestFileBounded loads a manifest with an explicit size bound.
func ReadInternalManifestFileBounded(path string, maxBytes int64) (InternalManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return InternalManifest{}, fmt.Errorf("manifest path is required")
	}
	data, err := readFileBounded(path, maxBytes)
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
	file, root, err := openSegmentFile(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		_ = root.Close()
	}()
	return walkSegmentFile(file, visit)
}

// afterSegmentDigestVerifiedForTest is an optional test hook invoked after the
// segment digest has been verified against an open file descriptor and before
// that same descriptor is parsed. Tests replace the pathname contents to prove
// the parser never re-opens the path.
var afterSegmentDigestVerifiedForTest func(path string)

func openSegmentFile(path string) (*os.File, *os.Root, error) {
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return nil, nil, err
	}
	if name == "" || name == "." || name == ".." || filepath.Clean(name) != name || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return nil, nil, fmt.Errorf("invalid segment filename")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment directory: %w", err)
	}
	file, err := root.Open(name)
	if err != nil {
		_ = root.Close()
		return nil, nil, fmt.Errorf("open segment: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		_ = root.Close()
		return nil, nil, fmt.Errorf("stat segment: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		_ = root.Close()
		return nil, nil, fmt.Errorf("segment is not a regular file")
	}
	return file, root, nil
}

func openParquetSegmentFile(r io.ReaderAt, size int64) (pf *parquet.File, isDurable bool, err error) {
	if size < 8 || size > durableindex.DefaultMaxParquetDecompressedBytes {
		return nil, false, fmt.Errorf("read segment: invalid parquet segment byte length")
	}
	var head [4]byte
	var trailer [8]byte
	if _, err := r.ReadAt(head[:], 0); err != nil {
		return nil, false, fmt.Errorf("read segment: %w", err)
	}
	if _, err := r.ReadAt(trailer[:], size-8); err != nil {
		return nil, false, fmt.Errorf("read segment: %w", err)
	}
	if string(head[:]) != "PAR1" || string(trailer[4:8]) != "PAR1" {
		return nil, false, fmt.Errorf("read segment: invalid parquet magic bytes")
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerLen <= 0 || footerLen > size-8 || footerLen > durableindex.MaxMetadataBytes {
		return nil, false, fmt.Errorf("read segment: parquet footer length out of bounds")
	}
	dataEnd := size - 8 - footerLen

	defer func() {
		if rec := recover(); rec != nil {
			pf = nil
			isDurable = false
			err = fmt.Errorf("malformed parquet segment: %v", rec)
		}
	}()
	rawPF, openErr := parquet.OpenFile(r, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if openErr != nil {
		return nil, false, fmt.Errorf("read segment: %w", openErr)
	}
	if isDurableParquetSchema(rawPF) {
		validatedPF, valErr := durableindex.ValidateParquetInventoryReaderAt(r, size, durableindex.DefaultMaxParquetDecompressedBytes)
		if valErr != nil {
			return nil, false, fmt.Errorf("read segment: %w", valErr)
		}
		return validatedPF, true, nil
	}
	meta := rawPF.Metadata()
	if meta == nil {
		return nil, false, fmt.Errorf("read segment: missing parquet metadata")
	}
	if meta.EncryptionAlgorithm.Value != nil || len(meta.FooterSigningKeyMetadata) > 0 {
		return nil, false, fmt.Errorf("read segment: encrypted parquet files are unsupported")
	}
	if meta.NumRows < 0 || meta.NumRows > durableindex.MaxExactInteger {
		return nil, false, fmt.Errorf("read segment: invalid parquet num_rows")
	}
	var totalUncompressed int64
	for _, rg := range meta.RowGroups {
		if len(rg.Columns) == 0 {
			return nil, false, fmt.Errorf("read segment: empty row group columns")
		}
		for _, col := range rg.Columns {
			if col.FilePath != "" || col.CryptoMetadata.Value != nil || len(col.EncryptedColumnMetadata) > 0 {
				return nil, false, fmt.Errorf("read segment: external or encrypted column chunk is unsupported")
			}
			if col.MetaData.TotalUncompressedSize < 0 || col.MetaData.TotalCompressedSize < 0 {
				return nil, false, fmt.Errorf("read segment: negative column chunk size")
			}
			if col.ColumnIndexOffset < 0 || col.ColumnIndexLength < 0 || col.OffsetIndexOffset < 0 || col.OffsetIndexLength < 0 ||
				col.MetaData.BloomFilterOffset < 0 || col.MetaData.BloomFilterLength < 0 {
				return nil, false, fmt.Errorf("read segment: negative index or bloom filter bounds")
			}
			if (col.ColumnIndexOffset != 0 || col.ColumnIndexLength != 0) && (col.ColumnIndexOffset < 4 || col.ColumnIndexOffset > dataEnd || int64(col.ColumnIndexLength) > dataEnd-col.ColumnIndexOffset) {
				return nil, false, fmt.Errorf("read segment: column index out of bounds")
			}
			if (col.OffsetIndexOffset != 0 || col.OffsetIndexLength != 0) && (col.OffsetIndexOffset < 4 || col.OffsetIndexOffset > dataEnd || int64(col.OffsetIndexLength) > dataEnd-col.OffsetIndexOffset) {
				return nil, false, fmt.Errorf("read segment: offset index out of bounds")
			}
			if (col.MetaData.BloomFilterOffset != 0 || col.MetaData.BloomFilterLength != 0) && (col.MetaData.BloomFilterOffset < 4 || col.MetaData.BloomFilterOffset > dataEnd || int64(col.MetaData.BloomFilterLength) > dataEnd-col.MetaData.BloomFilterOffset) {
				return nil, false, fmt.Errorf("read segment: bloom filter out of bounds")
			}
			if totalUncompressed > durableindex.DefaultMaxParquetDecompressedBytes-col.MetaData.TotalUncompressedSize {
				return nil, false, fmt.Errorf("read segment: column chunk exceeds decompression ceiling")
			}
			totalUncompressed += col.MetaData.TotalUncompressedSize
		}
	}
	return rawPF, false, nil
}

func walkSegmentFile(file *os.File, visit func(CurrentObjectRow) error) (err error) {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat segment: %w", err)
	}
	pf, isDurable, err := openParquetSegmentFile(file, info.Size())
	if err != nil {
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("read segment panic: %v", rec)
		}
	}()
	if isDurable {
		indexSetID, _ := pf.Lookup("gonimbus.index_set_id")
		reader := parquet.NewGenericReader[durableParquetRow](pf)
		defer func() { _ = reader.Close() }()
		rows := make([]durableParquetRow, 64)
		for {
			n, err := reader.Read(rows)
			for _, row := range rows[:n] {
				current, convErr := currentRowFromDurableParquet(row, indexSetID)
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
	reader := parquet.NewGenericReader[segmentParquetRow](pf)
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
	// Same-open trust: open once under os.Root, digest that FD, seek, parse the
	// same FD. Never hash then re-open the pathname (TOCTOU).
	file, root, err := openSegmentFile(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		_ = root.Close()
	}()
	return WalkSegmentFileHandleVerified(file, descriptor, visit)
}

// WalkSegmentFileHandleVerified verifies and walks a segment through the exact
// already-bound file handle supplied by a retained filesystem capability.
func WalkSegmentFileHandleVerified(file *os.File, descriptor SegmentDescriptor, visit func(CurrentObjectRow) error) error {
	if file == nil {
		return fmt.Errorf("segment file handle is required")
	}
	if descriptor.Digest.Algorithm != "sha256" {
		return fmt.Errorf("unsupported segment digest algorithm %q", descriptor.Digest.Algorithm)
	}
	if strings.TrimSpace(descriptor.Digest.Hex) == "" {
		return fmt.Errorf("segment digest is required")
	}
	got, err := sha256HexFileHandle(file)
	if err != nil {
		return err
	}
	if got != descriptor.Digest.Hex {
		return fmt.Errorf("segment digest mismatch for %s", descriptor.Path)
	}
	if afterSegmentDigestVerifiedForTest != nil {
		afterSegmentDigestVerifiedForTest(file.Name())
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek segment after digest verify: %w", err)
	}
	return walkSegmentFile(file, visit)
}

func sha256HexFileHandle(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek segment for digest: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ReadManifestRows(segmentDir string, manifest InternalManifest) ([]CurrentObjectRow, error) {
	return ReadManifestRowsBounded(segmentDir, manifest, -1)
}

// ReadManifestRowsBounded materializes rows with an optional hard cap.
// maxRows < 0 means unlimited (after count-budget validation). When maxRows >= 0,
// manifest counts are pre-validated against the cap and materialization stops if
// actual rows would exceed maxRows (never allocates past cap+0).
func ReadManifestRowsBounded(segmentDir string, manifest InternalManifest, maxRows int) ([]CurrentObjectRow, error) {
	if err := validateManifestCountBudget(manifest, maxRows); err != nil {
		return nil, err
	}
	capHint := manifest.Counts.Rows
	if maxRows >= 0 && capHint > maxRows {
		// unreachable when validateManifestCountBudget enforces maxRows
		capHint = maxRows
	}
	if capHint < 0 {
		capHint = 0
	}
	rows := make([]CurrentObjectRow, 0, capHint)
	err := WalkManifestRows(segmentDir, manifest, func(row CurrentObjectRow) error {
		// Stop at the first row beyond declared counts.rows (do not grow to the
		// larger global cap when descriptors under-declare).
		if len(rows) >= manifest.Counts.Rows {
			return fmt.Errorf("manifest actual rows exceed declared counts.rows %d", manifest.Counts.Rows)
		}
		if maxRows >= 0 && len(rows) >= maxRows {
			return fmt.Errorf("manifest actual rows exceed limit %d", maxRows)
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(rows) != manifest.Counts.Rows {
		return nil, fmt.Errorf("manifest actual row count %d does not equal counts.rows %d", len(rows), manifest.Counts.Rows)
	}
	return rows, nil
}

func WalkManifestRows(segmentDir string, manifest InternalManifest, visit func(CurrentObjectRow) error) error {
	for _, segment := range manifest.Segments {
		if err := WalkSegmentFileVerified(segmentDir, segment, visit); err != nil {
			return err
		}
	}
	return nil
}

// segmentFileOps are per-invocation filesystem seams for segment seal.
// Production uses productionSegmentFileOps(); tests pass scoped fakes via
// SegmentWriterConfig.segmentOps (unexported) so package-global hooks are not required.
type segmentFileOps struct {
	remove   func(path string) error
	stat     func(path string) (os.FileInfo, error)
	postLink func(tempPath, finalPath string) error // optional; nil in production
}

func productionSegmentFileOps() segmentFileOps {
	return segmentFileOps{
		remove: func(path string) error {
			if path == "" {
				return nil
			}
			err := os.Remove(path)
			if err != nil && errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		},
		stat: os.Stat,
	}
}

func resolveSegmentFileOps(config SegmentWriterConfig) segmentFileOps {
	if config.segmentOps != nil {
		ops := *config.segmentOps
		if ops.remove == nil {
			ops.remove = productionSegmentFileOps().remove
		}
		if ops.stat == nil {
			ops.stat = productionSegmentFileOps().stat
		}
		return ops
	}
	return productionSegmentFileOps()
}

// retireTemp performs checked temp retirement and joins any failure into primary.
// There is no silent deferred unlink: callers must use this (or ops.remove) on
// every path that created a temp.
func retireTemp(ops segmentFileOps, tempPath string, primary error) error {
	if tempPath == "" {
		return primary
	}
	if err := ops.remove(tempPath); err != nil {
		cleanup := fmt.Errorf("remove temporary segment: %w", err)
		if primary != nil {
			return errors.Join(primary, cleanup)
		}
		return cleanup
	}
	return primary
}

// writeParquetSegmentFileTracked seals a segment from already-prepared parquet
// rows. created is true only when this call successfully links a new final.
// Once link succeeds, created stays true even if temp unlink or final stat fails
// so callers can roll back the new dentry; those cleanup/stat failures are
// returned rather than discarded. Pre-link temps are always retired via
// checked retireTemp (never a silent deferred Remove).
func writeParquetSegmentFileTracked(config SegmentWriterConfig, ordinal int, rows []durableParquetRow) (SegmentDescriptor, bool, error) {
	ops := resolveSegmentFileOps(config)
	temp, err := os.CreateTemp(config.Dir, ".segment-*.parquet.tmp")
	if err != nil {
		return SegmentDescriptor{}, false, fmt.Errorf("create temporary segment: %w", err)
	}
	tempPath := temp.Name()

	digest := sha256.New()
	if err := writePreparedParquetRows(io.MultiWriter(temp, digest), config, rows); err != nil {
		_ = temp.Close()
		return SegmentDescriptor{}, false, retireTemp(ops, tempPath, err)
	}
	if err := temp.Close(); err != nil {
		return SegmentDescriptor{}, false, retireTemp(ops, tempPath, fmt.Errorf("close temporary segment: %w", err))
	}

	digestHex := hex.EncodeToString(digest.Sum(nil))
	descriptor := segmentDescriptorFromParquet(config, ordinal, rows, digestHex)
	finalPath := filepath.Join(config.Dir, descriptor.Path)
	if err := linkImmutable(tempPath, finalPath); err != nil {
		if config.AllowExistingIdentical && errors.Is(err, os.ErrExist) {
			existingDigest, digestErr := sha256HexFile(finalPath)
			if digestErr != nil {
				return SegmentDescriptor{}, false, retireTemp(ops, tempPath, fmt.Errorf("hash existing segment: %w", digestErr))
			}
			if existingDigest == digestHex {
				info, statErr := ops.stat(finalPath)
				if statErr != nil {
					return SegmentDescriptor{}, false, retireTemp(ops, tempPath, fmt.Errorf("stat existing segment: %w", statErr))
				}
				descriptor.SizeBytes = info.Size()
				// Reused pre-existing identical final — not owned by this call.
				// Checked temp unlink; failure is returned with created=false.
				if unlinkErr := ops.remove(tempPath); unlinkErr != nil {
					return descriptor, false, fmt.Errorf("remove temporary segment: %w", unlinkErr)
				}
				return descriptor, false, nil
			}
			// Non-identical EEXIST (or AllowExisting with digest mismatch):
			// own only the temp, never the conflicting final.
			return SegmentDescriptor{}, false, retireTemp(ops, tempPath, err)
		}
		return SegmentDescriptor{}, false, retireTemp(ops, tempPath, err)
	}
	// Link succeeded: this invocation owns the final for rollback purposes.

	if ops.postLink != nil {
		if hookErr := ops.postLink(tempPath, finalPath); hookErr != nil {
			// Still attempt temp unlink; surface hook/unlink failures with ownership.
			return descriptor, true, retireTemp(ops, tempPath, hookErr)
		}
	}

	if err := ops.remove(tempPath); err != nil {
		// Owned final remains; outer rollback can remove it. Temp residue may
		// still exist when unlink fails — that failure is visible, not silent.
		return descriptor, true, fmt.Errorf("remove temporary segment: %w", err)
	}

	info, err := ops.stat(finalPath)
	if err != nil {
		// Do not silently drop ownership or discard cleanup of the final.
		return descriptor, true, fmt.Errorf("stat segment: %w", err)
	}
	descriptor.SizeBytes = info.Size()
	return descriptor, true, nil
}

func writePreparedParquetRows(output io.Writer, config SegmentWriterConfig, parquetRows []durableParquetRow) error {
	writer := parquet.NewGenericWriter[durableParquetRow](output,
		durableParquetSchema,
		parquet.Compression(&parquet.Zstd),
		parquet.DataPageVersion(2),
		parquet.MaxRowsPerRowGroup(int64(config.TargetRowsPerSegment)),
		parquet.CreatedBy("gonimbus", "indexsubstrate", "v1"),
		parquet.KeyValueMetadata("gonimbus.index_set_id", config.IndexSetID),
		parquet.KeyValueMetadata("gonimbus.run_id", config.RunID),
		parquet.KeyValueMetadata("gonimbus.segment_format", SegmentFormatParquet),
		parquet.KeyValueMetadata("gonimbus.encoding_profile", "parquet-inventory/v0"),
	)
	if _, err := writer.Write(parquetRows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write segment rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close segment writer: %w", err)
	}
	return nil
}

func segmentDescriptorFromParquet(config SegmentWriterConfig, ordinal int, rows []durableParquetRow, digestHex string) SegmentDescriptor {
	segmentID := fmt.Sprintf("seg_%06d_%s", ordinal, digestHex[:16])
	descriptor := SegmentDescriptor{
		SegmentID:     segmentID,
		Path:          segmentID + ".parquet",
		Format:        SegmentFormatParquet,
		Compression:   "zstd",
		Rows:          len(rows),
		Tombstones:    parquetTombstoneCount(rows),
		DistinctETags: parquetDistinctETagCount(rows),
		Digest: SegmentDigest{
			Algorithm: "sha256",
			Hex:       digestHex,
		},
	}
	if len(rows) > 0 {
		descriptor.MinRelKey = rows[0].Key
		descriptor.MaxRelKey = rows[len(rows)-1].Key
	}
	return descriptor
}

func parquetTombstoneCount(rows []durableParquetRow) int {
	count := 0
	for _, row := range rows {
		if row.Deleted != nil {
			count++
		}
	}
	return count
}

func parquetDistinctETagCount(rows []durableParquetRow) int {
	seen := make(map[string]struct{})
	for _, row := range rows {
		if row.Revision == nil || row.Revision.Value == "" {
			continue
		}
		seen[row.Revision.Value] = struct{}{}
	}
	return len(seen)
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
	name = strings.TrimSpace(name)
	if dir == "" {
		return "", fmt.Errorf("segment directory is required")
	}
	if name == "" || name == "." || name == ".." || filepath.IsAbs(name) || filepath.Clean(name) != name || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("invalid segment path")
	}
	return filepath.Join(dir, name), nil
}

func normalizeSegmentWriterConfig(config SegmentWriterConfig) SegmentWriterConfig {
	config.Dir = strings.TrimSpace(config.Dir)
	config.IndexSetID = strings.TrimSpace(config.IndexSetID)
	config.RunID = strings.TrimSpace(config.RunID)
	config.GonimbusVersion = strings.TrimSpace(config.GonimbusVersion)
	if config.GonimbusVersion == "" {
		config.GonimbusVersion = buildinfo.DefaultVersion()
	}
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

func validateCurrentRowInvariants(row CurrentObjectRow) error {
	hasAnyAttr := row.ArchiveStatus != nil || row.ContentType != nil || row.RestoreExpiry != nil || row.RestoreState != nil
	hasEnrichment := row.HeadEnrichedAt != nil || hasAnyAttr
	if row.DeletedAt != nil {
		if hasEnrichment {
			return fmt.Errorf("tombstone row cannot carry head enrichments")
		}
		if !row.LastChangedAt.IsZero() && !row.DeletedAt.Equal(row.LastChangedAt) {
			return fmt.Errorf("tombstone deleted_at must equal last_changed_at")
		}
	}
	if hasEnrichment && strings.TrimSpace(row.ETag) == "" {
		return fmt.Errorf("enriched row requires non-empty revision etag")
	}
	return nil
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
		if err := validateCurrentRowInvariants(row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RelKey < out[j].RelKey
	})
	return out, nil
}

func manifestCountsForRun(rows []CurrentObjectRow, runID string) ManifestCounts {
	return ManifestCounts{
		Rows:                   len(rows),
		ActiveRows:             len(rows) - tombstoneCount(rows),
		Tombstones:             tombstoneCount(rows),
		DistinctETags:          distinctETagCount(rows),
		RetainedUnobservedRows: retainedUnobservedCount(rows, runID),
		DotSegmentRows:         dotSegmentRowCount(rows),
	}
}

// HasDotPrefixedSegment reports whether any /-separated path segment of relKey
// starts with '.'. Marker-style names such as _SUCCESS are not dot-prefixed.
func HasDotPrefixedSegment(relKey string) bool {
	for _, seg := range strings.Split(relKey, "/") {
		if strings.HasPrefix(seg, ".") {
			return true
		}
	}
	return false
}

func dotSegmentRowCount(rows []CurrentObjectRow) int {
	count := 0
	for _, row := range rows {
		if row.DeletedAt != nil {
			continue
		}
		if HasDotPrefixedSegment(row.RelKey) {
			count++
		}
	}
	return count
}

func retainedUnobservedCount(rows []CurrentObjectRow, runID string) int {
	if runID == "" {
		return 0
	}
	count := 0
	for _, row := range rows {
		if row.LastSeenRunID != "" && row.LastSeenRunID != runID {
			count++
		}
	}
	return count
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

const durableInstantLayoutNanoUTC = "2006-01-02T15:04:05.000000000Z"

func durableTimeString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(durableInstantLayoutNanoUTC)
}

func durableTimeStringPtr(value *time.Time) *string {
	if value == nil {
		return nil
	}
	out := durableTimeString(*value)
	return &out
}

func durableParquetFromCurrentRow(row CurrentObjectRow, defaultRunID string) durableParquetRow {
	row = normalizeCurrentObjectRow(row)
	var rev *durableParquetRevision
	if row.ETag != "" {
		rev = &durableParquetRevision{
			Kind:  "opaque-etag",
			Value: row.ETag,
		}
	}
	var del *durableParquetStamp
	if row.DeletedAt != nil {
		snapID := row.LastChangedRunID
		if snapID == "" {
			snapID = defaultRunID
		}
		del = &durableParquetStamp{
			SnapshotID: snapID,
			At:         durableTimeString(*row.DeletedAt),
		}
	}
	enrichments := []durableParquetEnrichment{}
	hasAnyAttr := row.ArchiveStatus != nil || row.ContentType != nil || row.RestoreExpiry != nil || row.RestoreState != nil
	if row.HeadEnrichedAt != nil || hasAnyAttr {
		var observedAt string
		operation := "head-object"
		if row.HeadEnrichedAt != nil {
			observedAt = durableTimeString(*row.HeadEnrichedAt)
		} else {
			operation = "head-object-synthetic"
			observedAt = durableTimeString(row.LastSeenAt)
		}
		against := durableParquetRevision{
			Kind:  "opaque-etag",
			Value: row.ETag,
		}
		if hasAnyAttr {
			if row.ArchiveStatus != nil {
				enrichments = append(enrichments, durableParquetEnrichment{
					Name:            "archive_status",
					Value:           stringPtrCopy(row.ArchiveStatus),
					Operation:       operation,
					ObservedAt:      observedAt,
					AgainstRevision: against,
				})
			}
			if row.ContentType != nil {
				enrichments = append(enrichments, durableParquetEnrichment{
					Name:            "content_type",
					Value:           stringPtrCopy(row.ContentType),
					Operation:       operation,
					ObservedAt:      observedAt,
					AgainstRevision: against,
				})
			}
			if row.RestoreExpiry != nil {
				enrichments = append(enrichments, durableParquetEnrichment{
					Name:            "restore_expiry",
					Value:           durableTimeStringPtr(row.RestoreExpiry),
					Operation:       operation,
					ObservedAt:      observedAt,
					AgainstRevision: against,
				})
			}
			if row.RestoreState != nil {
				enrichments = append(enrichments, durableParquetEnrichment{
					Name:            "restore_state",
					Value:           stringPtrCopy(row.RestoreState),
					Operation:       operation,
					ObservedAt:      observedAt,
					AgainstRevision: against,
				})
			}
		} else {
			enrichments = append(enrichments, durableParquetEnrichment{
				Name:            "content_type",
				Value:           nil,
				Operation:       operation,
				ObservedAt:      observedAt,
				AgainstRevision: against,
			})
		}
	}
	return durableParquetRow{
		Namespace:    "objects",
		Key:          row.RelKey,
		Revision:     rev,
		SizeBytes:    row.SizeBytes,
		LastModified: durableTimeStringPtr(row.LastModified),
		StorageClass: stringPtrCopy(row.StorageClass),
		FirstSeen: durableParquetStamp{
			SnapshotID: row.FirstSeenRunID,
			At:         durableTimeString(row.FirstSeenAt),
		},
		LastSeen: durableParquetStamp{
			SnapshotID: row.LastSeenRunID,
			At:         durableTimeString(row.LastSeenAt),
		},
		LastChanged: durableParquetStamp{
			SnapshotID: row.LastChangedRunID,
			At:         durableTimeString(row.LastChangedAt),
		},
		Deleted:     del,
		Enrichments: enrichments,
	}
}

func logicalRowFromDurableParquet(row durableParquetRow) durableindex.LogicalRow {
	var rev *durableindex.Revision
	if row.Revision != nil {
		rev = &durableindex.Revision{
			Kind:  row.Revision.Kind,
			Value: row.Revision.Value,
		}
	}
	var del *durableindex.Stamp
	if row.Deleted != nil {
		del = &durableindex.Stamp{
			SnapshotID: row.Deleted.SnapshotID,
			At:         row.Deleted.At,
		}
	}
	enrichments := make([]durableindex.EnrichmentAttribute, len(row.Enrichments))
	for i, e := range row.Enrichments {
		enrichments[i] = durableindex.EnrichmentAttribute{
			Name:            e.Name,
			Value:           stringPtrCopy(e.Value),
			Operation:       e.Operation,
			ObservedAt:      e.ObservedAt,
			AgainstRevision: durableindex.Revision{Kind: e.AgainstRevision.Kind, Value: e.AgainstRevision.Value},
		}
	}
	return durableindex.LogicalRow{
		Namespace:    row.Namespace,
		Key:          row.Key,
		Revision:     rev,
		SizeBytes:    row.SizeBytes,
		LastModified: stringPtrCopy(row.LastModified),
		StorageClass: stringPtrCopy(row.StorageClass),
		FirstSeen:    durableindex.Stamp{SnapshotID: row.FirstSeen.SnapshotID, At: row.FirstSeen.At},
		LastSeen:     durableindex.Stamp{SnapshotID: row.LastSeen.SnapshotID, At: row.LastSeen.At},
		LastChanged:  durableindex.Stamp{SnapshotID: row.LastChanged.SnapshotID, At: row.LastChanged.At},
		Deleted:      del,
		Enrichments:  enrichments,
	}
}

func validateDurableParquetRowContract(row durableParquetRow) error {
	if row.Namespace == "" || !durableindex.ValidateIdentifier(row.Namespace) {
		return durableindex.NewRefusal("invalid_shape", "invalid durable parquet row namespace")
	}
	if row.Key == "" {
		return durableindex.NewRefusal("invalid_shape", "durable parquet row key is required")
	}
	if err := durableindex.ValidateTextUTF8(row.Key); err != nil {
		return err
	}
	if row.SizeBytes < 0 || row.SizeBytes > durableindex.MaxExactInteger {
		return durableindex.NewRefusal("invalid_shape", "invalid durable parquet row size_bytes")
	}
	if row.Revision != nil {
		if row.Revision.Kind == "" || row.Revision.Value == "" {
			return durableindex.NewRefusal("invalid_shape", "durable parquet row revision requires non-empty kind and value")
		}
		if err := durableindex.ValidateTextUTF8(row.Revision.Kind); err != nil {
			return err
		}
		if err := durableindex.ValidateTextUTF8(row.Revision.Value); err != nil {
			return err
		}
	}
	if row.StorageClass != nil {
		if err := durableindex.ValidateTextUTF8(*row.StorageClass); err != nil {
			return err
		}
	}
	if row.Deleted != nil {
		if len(row.Enrichments) > 0 {
			return durableindex.NewRefusal("enrichment_target", "tombstone row cannot carry enrichments")
		}
		if row.Deleted.At != row.LastChanged.At || row.Deleted.SnapshotID != row.LastChanged.SnapshotID {
			return durableindex.NewRefusal("lineage_order", "deleted stamp differs from last_changed")
		}
	}
	for j := 1; j < len(row.Enrichments); j++ {
		if strings.Compare(row.Enrichments[j-1].Name, row.Enrichments[j].Name) >= 0 {
			return durableindex.NewRefusal("enrichment_order", "enrichments are unsorted or duplicate")
		}
	}
	for _, e := range row.Enrichments {
		if e.Name == "" || e.Operation == "" {
			return durableindex.NewRefusal("invalid_shape", "enrichment name and operation are required")
		}
		if err := durableindex.ValidateTextUTF8(e.Name); err != nil {
			return err
		}
		if err := durableindex.ValidateTextUTF8(e.Operation); err != nil {
			return err
		}
		if err := durableindex.ValidateTextUTF8(e.AgainstRevision.Kind); err != nil {
			return err
		}
		if err := durableindex.ValidateTextUTF8(e.AgainstRevision.Value); err != nil {
			return err
		}
		if err := durableindex.ValidateInstant(e.ObservedAt); err != nil {
			return err
		}
		if e.Value != nil {
			if !utf8.ValidString(*e.Value) {
				return durableindex.NewRefusal("invalid_unicode", "enrichment value is not valid UTF-8")
			}
			if len(*e.Value) > durableindex.MaxEnrichmentBytes {
				return durableindex.NewRefusal("capacity", "enrichment value exceeds 65536 bytes")
			}
		}
		if row.Revision == nil || row.Revision.Kind != e.AgainstRevision.Kind || row.Revision.Value != e.AgainstRevision.Value {
			return durableindex.NewRefusal("enrichment_revision", "enrichment against_revision does not match active row revision")
		}
	}
	if row.FirstSeen.SnapshotID != "" && row.FirstSeen.At != "" &&
		row.LastSeen.SnapshotID != "" && row.LastSeen.At != "" &&
		row.LastChanged.SnapshotID != "" && row.LastChanged.At != "" {
		if err := durableindex.ValidateRows([]durableindex.LogicalRow{logicalRowFromDurableParquet(row)}, ""); err != nil {
			return err
		}
	}
	return nil
}

func currentRowFromDurableParquet(row durableParquetRow, indexSetID string) (CurrentObjectRow, error) {
	if err := validateDurableParquetRowContract(row); err != nil {
		return CurrentObjectRow{}, err
	}
	lastModified, err := parseOptionalTime(row.LastModified)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	firstSeenAt, err := parseRequiredTime(row.FirstSeen.At)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	lastChangedAt, err := parseRequiredTime(row.LastChanged.At)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	lastSeenAt, err := parseRequiredTime(row.LastSeen.At)
	if err != nil {
		return CurrentObjectRow{}, err
	}
	var deletedAt *time.Time
	if row.Deleted != nil {
		deletedAt, err = parseOptionalTime(&row.Deleted.At)
		if err != nil {
			return CurrentObjectRow{}, err
		}
	}
	var etag string
	if row.Revision != nil {
		etag = row.Revision.Value
	}
	var (
		archiveStatus  *string
		contentType    *string
		restoreExpiry  *time.Time
		restoreState   *string
		headEnrichedAt *time.Time
	)
	for _, e := range row.Enrichments {
		if e.Operation != "head-object-synthetic" && headEnrichedAt == nil && e.ObservedAt != "" {
			obsAt := e.ObservedAt
			headEnrichedAt, err = parseOptionalTime(&obsAt)
			if err != nil {
				return CurrentObjectRow{}, err
			}
		}
		switch e.Name {
		case "archive_status":
			archiveStatus = stringPtrCopy(e.Value)
		case "content_type":
			contentType = stringPtrCopy(e.Value)
		case "restore_expiry":
			restoreExpiry, err = parseOptionalTime(e.Value)
			if err != nil {
				return CurrentObjectRow{}, err
			}
		case "restore_state":
			restoreState = stringPtrCopy(e.Value)
		}
	}
	return normalizeCurrentObjectRow(CurrentObjectRow{
		IndexSetID:       indexSetID,
		RelKey:           row.Key,
		SizeBytes:        row.SizeBytes,
		LastModified:     lastModified,
		ETag:             etag,
		StorageClass:     stringPtrCopy(row.StorageClass),
		ArchiveStatus:    archiveStatus,
		RestoreState:     restoreState,
		RestoreExpiry:    restoreExpiry,
		ContentType:      contentType,
		HeadEnrichedAt:   headEnrichedAt,
		FirstSeenRunID:   row.FirstSeen.SnapshotID,
		FirstSeenAt:      firstSeenAt,
		LastChangedRunID: row.LastChanged.SnapshotID,
		LastChangedAt:    lastChangedAt,
		LastSeenRunID:    row.LastSeen.SnapshotID,
		LastSeenAt:       lastSeenAt,
		DeletedAt:        deletedAt,
	}), nil
}
