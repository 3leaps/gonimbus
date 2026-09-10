package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	indexQueryReceiptOutputFormat = "receipt-jsonl-v1"
	indexQueryReceiptType         = "gonimbus.index.query_receipt.v1"
	indexQueryReceiptVersion      = "1.0.0"
	indexQuerySpecType            = "gonimbus.index.query_spec.v1"
	maxJCSSafeInteger             = int64(1<<53 - 1)
)

type indexQueryReceiptRecord struct {
	Type                        string                    `json:"type"`
	SchemaVersion               string                    `json:"schema_version"`
	Outcome                     string                    `json:"outcome"`
	SourceKind                  string                    `json:"source_kind"`
	IndexSetID                  string                    `json:"index_set_id"`
	RunID                       string                    `json:"run_id"`
	RunStartedAt                string                    `json:"run_started_at"`
	SnapshotCompletedAt         string                    `json:"snapshot_completed_at"`
	SnapshotCompletionSemantics string                    `json:"snapshot_completion_semantics"`
	HubCommittedAt              string                    `json:"hub_committed_at,omitempty"`
	HubCompleteSHA256           string                    `json:"hub_complete_sha256,omitempty"`
	SourceIdentitySHA256        string                    `json:"source_identity_sha256"`
	SourceIdentitySchema        string                    `json:"source_identity_schema"`
	SourceIdentityProfile       string                    `json:"source_identity_profile"`
	ManifestSHA256              string                    `json:"manifest_sha256"`
	CoverageSHA256              string                    `json:"coverage_sha256"`
	Coverage                    indexQueryReceiptCoverage `json:"coverage"`
	Declared                    indexQueryReceiptDeclared `json:"declared"`
	Query                       indexQueryReceiptQuery    `json:"query"`
	Results                     indexQueryReceiptResults  `json:"results"`
	Segments                    indexQueryReceiptSegments `json:"segments"`
	Warnings                    []string                  `json:"warnings"`
	Errors                      []string                  `json:"errors"`
}

type indexQueryReceiptCoverage struct {
	Entries          int `json:"entries"`
	CompleteEntries  int `json:"complete_entries"`
	ConfirmedEntries int `json:"confirmed_entries"`
	InferredEntries  int `json:"inferred_entries"`
	GapCount         int `json:"gap_count"`
}

type indexQueryReceiptDeclared struct {
	Rows          int `json:"rows"`
	ActiveRows    int `json:"active_rows"`
	Tombstones    int `json:"tombstones"`
	DistinctETags int `json:"distinct_etags"`
	Segments      int `json:"segments"`
}

type indexQueryReceiptQuery struct {
	SpecSHA256        string   `json:"query_spec_sha256"`
	ResultMode        string   `json:"result_mode"`
	FilterKinds       []string `json:"filter_kinds"`
	FilterCount       int      `json:"filter_count"`
	StorageClassCount int      `json:"storage_class_count"`
	BaselineRunID     string   `json:"baseline_run_id,omitempty"`
	CanonicalTieBreak string   `json:"canonical_tie_break,omitempty"`
	IncludeAlternates bool     `json:"include_alternates"`
	EffectiveLimit    int      `json:"effective_limit"`
}

type indexQueryReceiptResults struct {
	Examined                int64 `json:"examined"`
	Matched                 int64 `json:"matched"`
	Emitted                 int64 `json:"emitted"`
	LogicalResults          int64 `json:"logical_results"`
	Truncated               bool  `json:"truncated"`
	TimestampParseAnomalies int64 `json:"timestamp_parse_anomalies"`
}

type indexQueryReceiptSegments struct {
	Declared       int `json:"declared"`
	Walked         int `json:"walked"`
	Verified       int `json:"verified"`
	ManifestPruned int `json:"manifest_pruned"`
}

type indexQueryReceiptRunOptions struct {
	BaseURI           string
	Params            indexstore.QueryParams
	CountOnly         bool
	CanonicalByETag   bool
	IncludeAlternates bool
	OutputURI         string
	OutputProfile     string
	OutputRegion      string
	OutputEndpoint    string
}

func runIndexQueryReceipt(ctx context.Context, reader indexreader.Reader, opts indexQueryReceiptRunOptions) (err error) {
	metadataReader, ok := reader.(indexreader.VerifiedSnapshotMetadataReader)
	if !ok {
		return fmt.Errorf("%s requires a reader with verified durable metadata: %w", indexQueryReceiptOutputFormat, indexreader.ErrVerifiedSnapshotMetadataUnavailable)
	}
	verified, err := metadataReader.VerifiedSnapshotMetadata()
	if err != nil {
		return fmt.Errorf("%s requires verified durable metadata: %w", indexQueryReceiptOutputFormat, err)
	}
	if verified.SourceKind != indexreader.SnapshotSourceLocalPublished &&
		verified.SourceKind != indexreader.SnapshotSourceAcquiredHub {
		return fmt.Errorf("%s does not yet support source kind %q", indexQueryReceiptOutputFormat, verified.SourceKind)
	}

	hubCommittedAt := ""
	if !verified.HubCommittedAt.IsZero() {
		hubCommittedAt = verified.HubCommittedAt.UTC().Format(time.RFC3339Nano)
	}
	querySummary, err := buildIndexQueryReceiptQuery(verified, opts)
	if err != nil {
		return err
	}
	if opts.CountOnly && opts.Params.Limit > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "warning: --limit is ignored with --count (use without --count to limit output)\n")
	}

	var (
		outputSpec   *outputDestSpec
		writer       io.Writer = os.Stdout
		outputFile   *os.File
		localStage   *indexQueryReceiptLocalStage
		tempPath     string
		outputClosed bool
	)
	if opts.OutputURI != "" {
		outputSpec, err = parseOutputDest(opts.OutputURI)
		if err != nil {
			return fmt.Errorf("invalid --output: %w", err)
		}
		outputFile, localStage, err = createIndexQueryReceiptOutputTemp(outputSpec)
		if err != nil {
			return err
		}
		writer = outputFile
		tempPath = outputFile.Name()
		defer func() {
			if !outputClosed {
				err = errors.Join(err, outputFile.Close())
			}
			if localStage != nil {
				err = errors.Join(err, localStage.Cleanup())
			} else {
				_ = os.Remove(tempPath)
			}
		}()
	}

	enc := json.NewEncoder(writer)
	recordTS := time.Now().UTC().Format(time.RFC3339Nano)
	var (
		stats          indexstore.QueryStats
		emitted        int64
		logicalResults int64
		truncated      bool
	)
	params := opts.Params

	switch {
	case opts.CountOnly && opts.CanonicalByETag:
		params.Limit = 0
		results, canonicalStats, queryErr := reader.QueryCanonicalObjects(ctx, params)
		if queryErr != nil {
			return fmt.Errorf("count query failed: %w", queryErr)
		}
		if canonicalStats.AvailablePassthroughRows > 0 {
			return fmt.Errorf("canonical receipt query requires non-empty ETags; %d matching row(s) cannot be represented", canonicalStats.AvailablePassthroughRows)
		}
		stats = canonicalStats.QueryStats
		logicalResults = int64(len(results))

	case opts.CountOnly:
		params.Limit = 0
		count := int64(0)
		stats, err = reader.WalkObjects(ctx, params, func(indexstore.QueryResult) error {
			count++
			return nil
		})
		if err != nil {
			return fmt.Errorf("count query failed: %w", err)
		}
		logicalResults = count

	case opts.CanonicalByETag:
		results, canonicalStats, queryErr := reader.QueryCanonicalObjects(ctx, params)
		if queryErr != nil {
			return fmt.Errorf("query failed: %w", queryErr)
		}
		if canonicalStats.AvailablePassthroughRows > 0 {
			return fmt.Errorf("canonical receipt query requires non-empty ETags; %d matching row(s) cannot be represented", canonicalStats.AvailablePassthroughRows)
		}
		stats = canonicalStats.QueryStats
		truncated = canonicalStats.Truncated
		for _, result := range results {
			switch {
			case result.Group != nil:
				if err := enc.Encode(newIndexCanonicalQueryRecord(opts.BaseURI, recordTS, *result.Group, params.CanonicalTieBreak, opts.IncludeAlternates)); err != nil {
					return fmt.Errorf("encode record: %w", err)
				}
				emitted++
			}
		}
		logicalResults = emitted

	case params.Limit > 0:
		probeParams := params
		if probeParams.Limit < math.MaxInt {
			probeParams.Limit++
		}
		results, queryStats, queryErr := reader.QueryObjects(ctx, probeParams)
		if queryErr != nil {
			return fmt.Errorf("query failed: %w", queryErr)
		}
		stats = queryStats
		if len(results) > params.Limit {
			results = results[:params.Limit]
			truncated = true
		}
		for _, result := range results {
			if err := enc.Encode(newIndexQueryRecord(opts.BaseURI, recordTS, result)); err != nil {
				return fmt.Errorf("encode record: %w", err)
			}
			emitted++
		}
		logicalResults = emitted

	default:
		stats, err = reader.WalkObjects(ctx, params, func(result indexstore.QueryResult) error {
			if encodeErr := enc.Encode(newIndexQueryRecord(opts.BaseURI, recordTS, result)); encodeErr != nil {
				return fmt.Errorf("encode record: %w", encodeErr)
			}
			emitted++
			return nil
		})
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
		logicalResults = emitted
	}

	warnings := []string{}
	if stats.TimestampParseErrors > 0 {
		warnings = append(warnings, "timestamp_parse_anomaly")
	}
	receipt := indexQueryReceiptRecord{
		Type:                        indexQueryReceiptType,
		SchemaVersion:               indexQueryReceiptVersion,
		Outcome:                     "success",
		SourceKind:                  string(verified.SourceKind),
		IndexSetID:                  verified.IndexSetID,
		RunID:                       verified.RunID,
		RunStartedAt:                verified.RunStartedAt.UTC().Format(time.RFC3339Nano),
		SnapshotCompletedAt:         verified.SnapshotCompletedAt.UTC().Format(time.RFC3339Nano),
		SnapshotCompletionSemantics: verified.SnapshotCompletionSemantics,
		HubCommittedAt:              hubCommittedAt,
		HubCompleteSHA256:           verified.HubCompleteSHA256,
		SourceIdentitySHA256:        verified.SourceIdentitySHA256,
		SourceIdentitySchema:        verified.SourceIdentitySchema,
		SourceIdentityProfile:       verified.SourceIdentityProfile,
		ManifestSHA256:              verified.ManifestSHA256,
		CoverageSHA256:              verified.CoverageSHA256,
		Coverage: indexQueryReceiptCoverage{
			Entries:          verified.Coverage.Entries,
			CompleteEntries:  verified.Coverage.CompleteEntries,
			ConfirmedEntries: verified.Coverage.ConfirmedEntries,
			InferredEntries:  verified.Coverage.InferredEntries,
			GapCount:         verified.Coverage.GapCount,
		},
		Declared: indexQueryReceiptDeclared{
			Rows:          verified.Declared.Rows,
			ActiveRows:    verified.Declared.ActiveRows,
			Tombstones:    verified.Declared.Tombstones,
			DistinctETags: verified.Declared.DistinctETags,
			Segments:      verified.Declared.Segments,
		},
		Query: querySummary,
		Results: indexQueryReceiptResults{
			Examined:                stats.Examined,
			Matched:                 stats.Matched,
			Emitted:                 emitted,
			LogicalResults:          logicalResults,
			Truncated:               truncated,
			TimestampParseAnomalies: stats.TimestampParseErrors,
		},
		Segments: indexQueryReceiptSegments{
			Declared:       verified.Declared.Segments,
			Walked:         stats.SegmentsWalked,
			Verified:       stats.SegmentsVerified,
			ManifestPruned: stats.SegmentsManifestPruned,
		},
		Warnings: warnings,
		Errors:   []string{},
	}
	if !verified.HubCommittedAt.IsZero() {
		receipt.HubCommittedAt = verified.HubCommittedAt.UTC().Format(time.RFC3339Nano)
	}
	if err := validateIndexQueryReceipt(receipt); err != nil {
		return fmt.Errorf("query receipt invalid: %w", err)
	}
	if err := enc.Encode(receipt); err != nil {
		return fmt.Errorf("encode terminal receipt: %w", err)
	}

	if outputFile != nil {
		if err := outputFile.Sync(); err != nil {
			return fmt.Errorf("sync staged output: %w", err)
		}
		info, statErr := outputFile.Stat()
		if statErr != nil {
			return fmt.Errorf("inspect staged output: %w", statErr)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("staged output is not a regular file")
		}
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("close temp file: %w", err)
		}
		outputClosed = true
		if outputSpec.Provider == string(provider.ProviderFile) {
			publicationWarnings, err := localStage.Publish()
			if err != nil {
				return err
			}
			for _, warning := range publicationWarnings {
				_, _ = fmt.Fprintln(os.Stderr, warning)
			}
			_, _ = fmt.Fprintf(os.Stderr, "Wrote %d records plus terminal receipt to %s\n", emitted, opts.OutputURI)
			return nil
		}
		outputSpec.Profile = opts.OutputProfile
		outputSpec.Region = opts.OutputRegion
		outputSpec.Endpoint = opts.OutputEndpoint
		if outputSpec.Endpoint != "" {
			outputSpec.ForcePathStyle = true
		}
		putter, err := newOutputProvider(ctx, outputSpec)
		if err != nil {
			return fmt.Errorf("output provider: %w", err)
		}
		if closer, ok := putter.(interface{ Close() error }); ok {
			defer func() { err = errors.Join(err, closer.Close()) }()
		}
		if err := uploadConditionallyToOutputDest(ctx, putter, outputSpec.Key, tempPath, provider.PutPrecondition{IfAbsent: true}); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stderr, "Wrote %d records plus terminal receipt to %s\n", emitted, opts.OutputURI)
	}
	return nil
}

func buildIndexQueryReceiptQuery(meta indexreader.VerifiedSnapshotMetadata, opts indexQueryReceiptRunOptions) (indexQueryReceiptQuery, error) {
	params := opts.Params
	if params.Limit < 0 {
		return indexQueryReceiptQuery{}, fmt.Errorf("--limit must not be negative with --output-format %s", indexQueryReceiptOutputFormat)
	}
	if params.MinSize < 0 || params.MaxSize < 0 {
		return indexQueryReceiptQuery{}, fmt.Errorf("size filters must not be negative with --output-format %s", indexQueryReceiptOutputFormat)
	}
	storageClasses := sortedUniqueStrings(params.StorageClasses)
	params.StorageClasses = storageClasses
	mode := indexQueryResultMode(opts.CountOnly, opts.CanonicalByETag)
	effectiveLimit := params.Limit
	if opts.CountOnly {
		effectiveLimit = 0
	}
	canonicalTieBreak := ""
	includeAlternates := false
	if opts.CanonicalByETag && !opts.CountOnly {
		canonicalTieBreak = string(params.CanonicalTieBreak)
		includeAlternates = opts.IncludeAlternates
	}
	baselineRunID := ""
	if params.SinceRun != nil {
		baselineRunID = params.SinceRun.RunID
	}
	spec := map[string]any{
		"base_uri":            opts.BaseURI,
		"canonical_tie_break": canonicalTieBreak,
		"effective_limit":     effectiveLimit,
		"enriched_after":      canonicalQueryTime(params.EnrichedAfter),
		"include_alternates":  includeAlternates,
		"include_deleted":     params.IncludeDeleted,
		"index_set_id":        meta.IndexSetID,
		"key_regex":           params.KeyRegex,
		"max_size_bytes":      params.MaxSize,
		"min_size_bytes":      params.MinSize,
		"modified_after":      canonicalQueryTime(params.ModifiedAfter),
		"modified_before":     canonicalQueryTime(params.ModifiedBefore),
		"output_format":       indexQueryReceiptOutputFormat,
		"pattern":             params.Pattern,
		"result_mode":         mode,
		"run_id":              meta.RunID,
		"since_run":           nil,
		"storage_classes":     storageClasses,
		"type":                indexQuerySpecType,
	}
	if baselineRunID != "" {
		spec["since_run"] = baselineRunID
	}
	canonical, err := marshalJCSSubset(spec)
	if err != nil {
		return indexQueryReceiptQuery{}, fmt.Errorf("canonicalize query spec: %w", err)
	}
	sum := sha256.Sum256(canonical)
	filterKinds := indexQueryFilterKinds(params)
	return indexQueryReceiptQuery{
		SpecSHA256:        hex.EncodeToString(sum[:]),
		ResultMode:        mode,
		FilterKinds:       filterKinds,
		FilterCount:       len(filterKinds),
		StorageClassCount: len(storageClasses),
		BaselineRunID:     baselineRunID,
		CanonicalTieBreak: canonicalTieBreak,
		IncludeAlternates: includeAlternates,
		EffectiveLimit:    effectiveLimit,
	}, nil
}

func indexQueryResultMode(countOnly, canonical bool) string {
	switch {
	case countOnly && canonical:
		return "canonical_count"
	case countOnly:
		return "count"
	case canonical:
		return "canonical_objects"
	default:
		return "objects"
	}
}

func indexQueryFilterKinds(params indexstore.QueryParams) []string {
	kinds := make([]string, 0, 10)
	if params.Pattern != "" {
		kinds = append(kinds, "pattern")
	}
	if params.KeyRegex != "" {
		kinds = append(kinds, "key_regex")
	}
	if params.MinSize > 0 {
		kinds = append(kinds, "min_size")
	}
	if params.MaxSize > 0 {
		kinds = append(kinds, "max_size")
	}
	if !params.ModifiedAfter.IsZero() {
		kinds = append(kinds, "modified_after")
	}
	if !params.ModifiedBefore.IsZero() {
		kinds = append(kinds, "modified_before")
	}
	if len(params.StorageClasses) > 0 {
		kinds = append(kinds, "storage_class")
	}
	if !params.EnrichedAfter.IsZero() {
		kinds = append(kinds, "enriched_after")
	}
	if params.IncludeDeleted {
		kinds = append(kinds, "include_deleted")
	}
	if params.SinceRun != nil {
		kinds = append(kinds, "since_run")
	}
	sort.Strings(kinds)
	return kinds
}

func canonicalQueryTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func sortedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// marshalJCSSubset emits RFC 8785-compatible bytes for the closed query-spec
// value domain used above: objects with ASCII keys, arrays, strings, booleans,
// null, and signed integer types. Floats and arbitrary structs are refused.
func marshalJCSSubset(value any) ([]byte, error) {
	var out []byte
	var err error
	out, err = appendJCSValue(out, value)
	return out, err
}

func appendJCSValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case string:
		return appendJCSString(dst, v)
	case int:
		if int64(v) < -maxJCSSafeInteger || int64(v) > maxJCSSafeInteger {
			return nil, fmt.Errorf("integer %d exceeds the RFC 8785 interoperable range", v)
		}
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int64:
		if v < -maxJCSSafeInteger || v > maxJCSSafeInteger {
			return nil, fmt.Errorf("integer %d exceeds the RFC 8785 interoperable range", v)
		}
		return strconv.AppendInt(dst, v, 10), nil
	case []string:
		dst = append(dst, '[')
		for i, item := range v {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendJCSString(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst = append(dst, '{')
		for i, key := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendJCSString(dst, key)
			if err != nil {
				return nil, err
			}
			dst = append(dst, ':')
			dst, err = appendJCSValue(dst, v[key])
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	default:
		return nil, fmt.Errorf("unsupported canonical JSON value %T", value)
	}
}

func appendJCSString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, fmt.Errorf("string is not valid UTF-8")
	}
	dst = append(dst, '"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\r':
			dst = append(dst, '\\', 'r')
		default:
			if r >= 0 && r <= 0x1f {
				dst = append(dst, '\\', 'u', '0', '0', "0123456789abcdef"[byte(r)>>4], "0123456789abcdef"[byte(r)&0xf])
				continue
			}
			dst = utf8.AppendRune(dst, r)
		}
	}
	return append(dst, '"'), nil
}

func validateIndexQueryReceipt(receipt indexQueryReceiptRecord) error {
	switch {
	case receipt.Type != indexQueryReceiptType:
		return fmt.Errorf("type must be %s", indexQueryReceiptType)
	case receipt.SchemaVersion != indexQueryReceiptVersion:
		return fmt.Errorf("schema_version must be %s", indexQueryReceiptVersion)
	case receipt.Outcome != "success":
		return fmt.Errorf("authoritative receipt outcome must be success")
	case receipt.SourceKind != string(indexreader.SnapshotSourceLocalPublished) &&
		receipt.SourceKind != string(indexreader.SnapshotSourceAcquiredHub):
		return fmt.Errorf("unsupported source_kind %q", receipt.SourceKind)
	case !validFullIndexSetID(receipt.IndexSetID) || receipt.IndexSetID != strings.ToLower(receipt.IndexSetID):
		return fmt.Errorf("index_set_id must be a full lowercase ID")
	case strings.TrimSpace(receipt.RunID) == "":
		return fmt.Errorf("run_id is required")
	case !validSHA256Hex(receipt.SourceIdentitySHA256):
		return fmt.Errorf("source_identity_sha256 must be lowercase SHA-256")
	case receipt.SourceIdentitySchema != indexreader.SourceIdentitySchemaV1:
		return fmt.Errorf("source_identity_schema must be %s", indexreader.SourceIdentitySchemaV1)
	case receipt.SourceIdentityProfile != indexreader.SourceIdentityProfileV1:
		return fmt.Errorf("source_identity_profile must be %s", indexreader.SourceIdentityProfileV1)
	case !validSHA256Hex(receipt.ManifestSHA256):
		return fmt.Errorf("manifest_sha256 must be lowercase SHA-256")
	case !validSHA256Hex(receipt.CoverageSHA256):
		return fmt.Errorf("coverage_sha256 must be lowercase SHA-256")
	case !validSHA256Hex(receipt.Query.SpecSHA256):
		return fmt.Errorf("query_spec_sha256 must be lowercase SHA-256")
	case receipt.Results.Examined < 0 || receipt.Results.Matched < 0 ||
		receipt.Results.Emitted < 0 || receipt.Results.LogicalResults < 0 ||
		receipt.Results.TimestampParseAnomalies < 0:
		return fmt.Errorf("result counters must be non-negative")
	case receipt.Results.Examined > maxJCSSafeInteger || receipt.Results.Matched > maxJCSSafeInteger ||
		receipt.Results.Emitted > maxJCSSafeInteger || receipt.Results.LogicalResults > maxJCSSafeInteger ||
		receipt.Results.TimestampParseAnomalies > maxJCSSafeInteger:
		return fmt.Errorf("result counters exceed the I-JSON interoperable range")
	case receipt.Results.Emitted > receipt.Results.Matched:
		return fmt.Errorf("emitted must not exceed matched")
	case receipt.Results.Matched > receipt.Results.Examined:
		return fmt.Errorf("matched must not exceed examined")
	case receipt.Declared.Rows < 0 || receipt.Declared.ActiveRows < 0 ||
		receipt.Declared.Tombstones < 0 || receipt.Declared.DistinctETags < 0 ||
		receipt.Declared.Segments < 0:
		return fmt.Errorf("declared counters must be non-negative")
	case int64(receipt.Declared.Rows) > maxJCSSafeInteger ||
		int64(receipt.Declared.ActiveRows) > maxJCSSafeInteger ||
		int64(receipt.Declared.Tombstones) > maxJCSSafeInteger ||
		int64(receipt.Declared.DistinctETags) > maxJCSSafeInteger ||
		int64(receipt.Declared.Segments) > maxJCSSafeInteger:
		return fmt.Errorf("declared counters exceed the I-JSON interoperable range")
	case receipt.Declared.ActiveRows+receipt.Declared.Tombstones != receipt.Declared.Rows:
		return fmt.Errorf("declared active_rows plus tombstones must equal rows")
	case receipt.Coverage.Entries < 0 || receipt.Coverage.CompleteEntries < 0 ||
		receipt.Coverage.ConfirmedEntries < 0 || receipt.Coverage.InferredEntries < 0 ||
		receipt.Coverage.GapCount < 0:
		return fmt.Errorf("coverage counters must be non-negative")
	case int64(receipt.Coverage.Entries) > maxJCSSafeInteger ||
		int64(receipt.Coverage.CompleteEntries) > maxJCSSafeInteger ||
		int64(receipt.Coverage.ConfirmedEntries) > maxJCSSafeInteger ||
		int64(receipt.Coverage.InferredEntries) > maxJCSSafeInteger ||
		int64(receipt.Coverage.GapCount) > maxJCSSafeInteger:
		return fmt.Errorf("coverage counters exceed the I-JSON interoperable range")
	case receipt.Coverage.CompleteEntries > receipt.Coverage.Entries ||
		receipt.Coverage.ConfirmedEntries+receipt.Coverage.InferredEntries != receipt.Coverage.Entries:
		return fmt.Errorf("coverage summary counters are inconsistent")
	case receipt.Segments.Declared < 0 || receipt.Segments.Walked < 0 ||
		receipt.Segments.Verified < 0 || receipt.Segments.ManifestPruned < 0:
		return fmt.Errorf("segment counters must be non-negative")
	case int64(receipt.Segments.Declared) > maxJCSSafeInteger ||
		int64(receipt.Segments.Walked) > maxJCSSafeInteger ||
		int64(receipt.Segments.Verified) > maxJCSSafeInteger ||
		int64(receipt.Segments.ManifestPruned) > maxJCSSafeInteger:
		return fmt.Errorf("segment counters exceed the I-JSON interoperable range")
	case receipt.Segments.Walked > receipt.Segments.Verified:
		return fmt.Errorf("walked segments must not exceed verified segments")
	case receipt.Segments.Verified+receipt.Segments.ManifestPruned > receipt.Segments.Declared:
		return fmt.Errorf("verified and pruned segments exceed declared segments")
	case receipt.Segments.Declared != receipt.Declared.Segments:
		return fmt.Errorf("segment declared counters disagree")
	case receipt.Query.FilterCount != len(receipt.Query.FilterKinds):
		return fmt.Errorf("filter_count does not match filter_kinds")
	case receipt.Query.FilterCount < 0 || receipt.Query.StorageClassCount < 0 ||
		receipt.Query.EffectiveLimit < 0:
		return fmt.Errorf("query counters must be non-negative")
	case int64(receipt.Query.FilterCount) > maxJCSSafeInteger ||
		int64(receipt.Query.StorageClassCount) > maxJCSSafeInteger ||
		int64(receipt.Query.EffectiveLimit) > maxJCSSafeInteger:
		return fmt.Errorf("query counters exceed the I-JSON interoperable range")
	case receipt.Warnings == nil || receipt.Errors == nil:
		return fmt.Errorf("warnings and errors must be arrays")
	case len(receipt.Errors) != 0:
		return fmt.Errorf("success receipt errors must be empty")
	}
	runStartedAt, err := indexsubstrate.ParseCanonicalUTCTime(receipt.RunStartedAt)
	if err != nil {
		return fmt.Errorf("run_started_at must be canonical UTC: %w", err)
	}
	snapshotCompletedAt, err := indexsubstrate.ParseCanonicalUTCTime(receipt.SnapshotCompletedAt)
	if err != nil {
		return fmt.Errorf("snapshot_completed_at must be canonical UTC: %w", err)
	}
	if receipt.Results.LogicalResults > receipt.Results.Matched {
		return fmt.Errorf("logical_results must not exceed matched")
	}
	switch receipt.Query.ResultMode {
	case "count", "canonical_count":
		if receipt.Results.Emitted != 0 || receipt.Results.Truncated || receipt.Query.EffectiveLimit != 0 {
			return fmt.Errorf("count mode must emit no object records, never truncate, and have zero effective_limit")
		}
	case "objects", "canonical_objects":
		if receipt.Results.LogicalResults != receipt.Results.Emitted {
			return fmt.Errorf("enumeration logical_results must equal emitted")
		}
		if receipt.Results.Truncated && receipt.Query.EffectiveLimit == 0 {
			return fmt.Errorf("unlimited enumeration must not be truncated")
		}
	default:
		return fmt.Errorf("unsupported result_mode %q", receipt.Query.ResultMode)
	}
	if receipt.Query.ResultMode == "canonical_objects" {
		if receipt.Query.CanonicalTieBreak == "" {
			return fmt.Errorf("canonical_objects requires canonical_tie_break")
		}
	} else if receipt.Query.CanonicalTieBreak != "" || receipt.Query.IncludeAlternates {
		return fmt.Errorf("non-canonical enumeration must not claim canonical options")
	}
	var hubCommittedAt time.Time
	if receipt.SourceKind == string(indexreader.SnapshotSourceLocalPublished) {
		if receipt.HubCommittedAt != "" || receipt.HubCompleteSHA256 != "" {
			return fmt.Errorf("local_published receipt must not claim hub metadata")
		}
	} else {
		hubCommittedAt, err = indexsubstrate.ParseCanonicalUTCTime(receipt.HubCommittedAt)
		if err != nil {
			return fmt.Errorf("hub_committed_at must be canonical UTC: %w", err)
		}
		if !validSHA256Hex(receipt.HubCompleteSHA256) {
			return fmt.Errorf("acquired_hub receipt requires UTC hub_committed_at and lowercase hub_complete_sha256")
		}
	}
	if err := indexsubstrate.ValidateExactSnapshotCompletion(
		receipt.SnapshotCompletionSemantics,
		runStartedAt,
		snapshotCompletedAt,
		hubCommittedAt,
	); err != nil {
		return fmt.Errorf("snapshot completion is not exact-time eligible: %w", err)
	}
	return nil
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
