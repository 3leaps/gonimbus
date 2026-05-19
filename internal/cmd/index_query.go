package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/match"
)

var indexQueryCmd = &cobra.Command{
	Use:   "query [base-uri]",
	Short: "Query indexed objects by pattern",
	Long: `Query objects from a local index using glob patterns and filters.

The query searches the objects_current table for objects matching
the specified patterns and filters. Results are emitted as JSONL
records to stdout (default) or written to a destination via --output.

Pattern matching uses doublestar semantics (same as crawl):
  **      matches any path segments
  *       matches any characters except /
  ?       matches any single character except /
  [...]   matches character class

Examples:
  # Find all XML files
  gonimbus index query s3://bucket/prefix/ --pattern "**/*.xml"

  # Find files with specific path segment
  gonimbus index query s3://bucket/ --pattern "**/71234/**/*PJR.xml"

  # Find files matching regex
  gonimbus index query s3://bucket/ --key-regex "2025-01.*\.json$"

  # Find files larger than 1MB modified in last 30 days
  gonimbus index query s3://bucket/ --min-size 1MB --after 2025-01-01

  # Write results to a local file
  gonimbus index query s3://bucket/prefix/ --pattern "**/*.xml" \
    --output file:///tmp/results.jsonl

  # Write results to S3
  gonimbus index query s3://bucket/prefix/ --pattern "**/*.xml" \
    --output s3://output-bucket/queries/results.jsonl

  # Query a specific index (skip auto-selection; base-uri is optional)
  gonimbus index query --index-set idx_da038d8171b4a9ba --pattern "**/*.xml"

  # Query a specific index with explicit base-uri override
  gonimbus index query s3://bucket/prefix/ --index-set idx_da038d8171b4a9ba --pattern "**/*.xml"

  # Emit one canonical object per non-empty ETag group
  gonimbus index query s3://bucket/prefix/ --canonical-by-etag

  ETag caveat: ETag is a provider version/fingerprint hint, not a universal
  content hash. See docs/user-guide/index-build-mental-model.md.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runIndexQuery,
}

func init() {
	indexCmd.AddCommand(indexQueryCmd)

	// Pattern filters
	indexQueryCmd.Flags().StringP("pattern", "p", "", "Doublestar glob pattern to match keys")
	indexQueryCmd.Flags().String("key-regex", "", "Regex pattern to match keys")

	// Size filters
	indexQueryCmd.Flags().String("min-size", "", "Minimum object size (e.g., 1KB, 1MB)")
	indexQueryCmd.Flags().String("max-size", "", "Maximum object size (e.g., 100MB, 1GB)")

	// Date filters
	indexQueryCmd.Flags().String("after", "", "Objects modified after this date (YYYY-MM-DD or RFC3339)")
	indexQueryCmd.Flags().String("before", "", "Objects modified before this date (YYYY-MM-DD or RFC3339)")

	// Output options
	indexQueryCmd.Flags().Int("limit", 0, "Maximum number of results (0 = no limit)")
	indexQueryCmd.Flags().Bool("include-deleted", false, "Include soft-deleted objects")
	indexQueryCmd.Flags().Bool("count", false, "Only output count of matching objects")
	indexQueryCmd.Flags().Bool("canonical-by-etag", false, "Emit one canonical record per non-empty ETag group; empty ETags pass through as standard records")
	indexQueryCmd.Flags().String("canonical-tie-break", string(indexstore.CanonicalTieBreakMinKey), "Canonical selection rule for --canonical-by-etag: min-key, min-modified, max-modified")
	indexQueryCmd.Flags().Bool("include-alternates", false, "Populate alternates[] on canonical ETag records")

	// Index selection
	indexQueryCmd.Flags().String("index-set", "", "Explicit index set ID (e.g., idx_da038d8171b4a9ba); skips auto-selection")

	// Output destination
	indexQueryCmd.Flags().String("output", "", "Output destination URI (s3://bucket/key.jsonl or file:///path/file.jsonl)")
	indexQueryCmd.Flags().String("output-profile", "", "AWS profile for output destination")
	indexQueryCmd.Flags().String("output-region", "", "AWS region for output destination")
	indexQueryCmd.Flags().String("output-endpoint", "", "Custom endpoint for output destination")
}

// indexQueryRecord is the JSONL output format for query results.
type indexQueryRecord struct {
	Type string               `json:"type"`
	TS   string               `json:"ts"`
	Data indexQueryRecordData `json:"data"`
}

type indexQueryRecordData struct {
	BaseURI      string  `json:"base_uri"`
	RelKey       string  `json:"rel_key"`
	Key          string  `json:"key"`
	SizeBytes    int64   `json:"size_bytes"`
	LastModified *string `json:"last_modified,omitempty"`
	ETag         string  `json:"etag,omitempty"`
	DeletedAt    *string `json:"deleted_at,omitempty"`
}

type indexCanonicalQueryRecord struct {
	Type string                        `json:"type"`
	TS   string                        `json:"ts"`
	Data indexCanonicalQueryRecordData `json:"data"`
}

type indexCanonicalQueryRecordData struct {
	BaseURI         string                        `json:"base_uri"`
	ETag            string                        `json:"etag"`
	Canonical       indexCanonicalObjectData      `json:"canonical"`
	TieBreakRule    string                        `json:"tie_break_rule"`
	AlternatesCount int                           `json:"alternates_count"`
	Alternates      []indexCanonicalAlternateData `json:"alternates,omitempty"`
}

type indexCanonicalObjectData struct {
	RelKey       string  `json:"rel_key"`
	Key          string  `json:"key"`
	SizeBytes    int64   `json:"size_bytes"`
	LastModified *string `json:"last_modified,omitempty"`
	DeletedAt    *string `json:"deleted_at"`
}

type indexCanonicalAlternateData struct {
	RelKey       string  `json:"rel_key"`
	SizeBytes    int64   `json:"size_bytes"`
	LastModified *string `json:"last_modified,omitempty"`
	DeletedAt    *string `json:"deleted_at"`
}

func runIndexQuery(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	indexSetFlag, _ := cmd.Flags().GetString("index-set")

	// Resolve base URI: required unless --index-set is provided.
	var baseURI string
	if len(args) > 0 {
		baseURI = normalizeQueryBaseURI(args[0])
	} else if indexSetFlag == "" {
		return fmt.Errorf("<base-uri> is required unless --index-set is provided")
	}

	// Get flags
	pattern, _ := cmd.Flags().GetString("pattern")
	keyRegex, _ := cmd.Flags().GetString("key-regex")
	minSizeStr, _ := cmd.Flags().GetString("min-size")
	maxSizeStr, _ := cmd.Flags().GetString("max-size")
	afterStr, _ := cmd.Flags().GetString("after")
	beforeStr, _ := cmd.Flags().GetString("before")
	limit, _ := cmd.Flags().GetInt("limit")
	includeDeleted, _ := cmd.Flags().GetBool("include-deleted")
	countOnly, _ := cmd.Flags().GetBool("count")
	canonicalByETag, _ := cmd.Flags().GetBool("canonical-by-etag")
	canonicalTieBreakRaw, _ := cmd.Flags().GetString("canonical-tie-break")
	includeAlternates, _ := cmd.Flags().GetBool("include-alternates")
	outputURI, _ := cmd.Flags().GetString("output")
	outputProfile, _ := cmd.Flags().GetString("output-profile")
	outputRegion, _ := cmd.Flags().GetString("output-region")
	outputEndpoint, _ := cmd.Flags().GetString("output-endpoint")

	// Open index database: explicit --index-set or auto-select by base URI
	var (
		db       *sql.DB
		indexSet *indexstore.IndexSet
		err      error
	)
	if indexSetFlag != "" {
		db, indexSet, err = openIndexDBByID(ctx, indexSetFlag)
	} else {
		db, indexSet, err = openIndexDBForBaseURI(ctx, baseURI)
	}
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// When --index-set is provided, use the DB's authoritative base_uri.
	// A positional base-uri arg is accepted but ignored with a warning if it differs.
	if indexSetFlag != "" {
		if baseURI != "" && baseURI != indexSet.BaseURI {
			_, _ = fmt.Fprintf(os.Stderr, "warning: positional base-uri %s differs from index base_uri %s; using index value\n", baseURI, indexSet.BaseURI)
		}
		baseURI = indexSet.BaseURI
	}

	// Build query params
	params := indexstore.QueryParams{
		IndexSetID:     indexSet.IndexSetID,
		Pattern:        pattern,
		KeyRegex:       keyRegex,
		IncludeDeleted: includeDeleted,
		Limit:          limit,
	}
	params.CanonicalTieBreak = indexstore.CanonicalTieBreak(canonicalTieBreakRaw)

	// Parse size filters using match package
	if minSizeStr != "" {
		minSize, err := match.ParseSize(minSizeStr)
		if err != nil {
			return fmt.Errorf("invalid --min-size: %w", err)
		}
		params.MinSize = minSize
	}
	if maxSizeStr != "" {
		maxSize, err := match.ParseSize(maxSizeStr)
		if err != nil {
			return fmt.Errorf("invalid --max-size: %w", err)
		}
		params.MaxSize = maxSize
	}

	// Parse date filters using match package
	if afterStr != "" {
		t, err := match.ParseDate(afterStr)
		if err != nil {
			return fmt.Errorf("invalid --after: %w", err)
		}
		params.ModifiedAfter = t
	}
	if beforeStr != "" {
		t, err := match.ParseDate(beforeStr)
		if err != nil {
			return fmt.Errorf("invalid --before: %w", err)
		}
		params.ModifiedBefore = t
	}

	// Handle count-only mode with optimized path
	// Note: --count ignores --limit (count returns total matches, not capped)
	if countOnly {
		if limit > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "warning: --limit is ignored with --count (use without --count to limit output)\n")
		}
		// Clear limit for count - we want total matches
		countParams := params
		countParams.Limit = 0
		if canonicalByETag {
			_, stats, err := indexstore.QueryCanonicalObjects(ctx, db, countParams)
			if err != nil {
				return fmt.Errorf("count query failed: %w", err)
			}
			_, _ = fmt.Fprintf(os.Stdout, "%d\n", stats.TotalRecords)
			_, _ = fmt.Fprintf(os.Stderr, "%d canonical groups, %d ungrouped empty-ETag rows, %d total records\n", stats.CanonicalGroups, stats.PassthroughRows, stats.TotalRecords)
			if stats.TimestampParseErrors > 0 {
				_, _ = fmt.Fprintf(os.Stderr, "warning: %d rows had unparseable timestamps (fields set to null)\n", stats.TimestampParseErrors)
			}
			return nil
		}
		count, err := indexstore.QueryObjectCount(ctx, db, countParams)
		if err != nil {
			return fmt.Errorf("count query failed: %w", err)
		}
		_, _ = fmt.Fprintf(os.Stdout, "%d\n", count)
		return nil
	}

	var (
		results          []indexstore.QueryResult
		canonicalResults []indexstore.CanonicalOutputRecord
		stats            indexstore.QueryStats
		canonicalStats   indexstore.CanonicalQueryStats
	)
	if canonicalByETag {
		canonicalResults, canonicalStats, err = indexstore.QueryCanonicalObjects(ctx, db, params)
		stats = canonicalStats.QueryStats
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
	} else {
		results, stats, err = indexstore.QueryObjects(ctx, db, params)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
	}

	// Set up output writer: temp file when --output is set, stdout otherwise.
	var (
		writer   *os.File
		tempPath string
	)
	if outputURI != "" {
		tmpFile, err := os.CreateTemp("", "gonimbus-query-*.jsonl")
		if err != nil {
			return fmt.Errorf("create temp file: %w", err)
		}
		tempPath = tmpFile.Name()
		writer = tmpFile
		defer func() { _ = os.Remove(tempPath) }()
	} else {
		writer = os.Stdout
	}

	// Emit JSONL records
	now := time.Now().UTC().Format(time.RFC3339Nano)
	enc := json.NewEncoder(writer)

	if canonicalByETag {
		for _, r := range canonicalResults {
			if r.Passthrough != nil {
				if err := enc.Encode(newIndexQueryRecord(baseURI, now, *r.Passthrough)); err != nil {
					return fmt.Errorf("encode record: %w", err)
				}
				continue
			}
			if r.Group == nil {
				continue
			}
			if err := enc.Encode(newIndexCanonicalQueryRecord(baseURI, now, *r.Group, indexstore.CanonicalTieBreak(canonicalTieBreakRaw), includeAlternates)); err != nil {
				return fmt.Errorf("encode record: %w", err)
			}
		}
	} else {
		for _, r := range results {
			if err := enc.Encode(newIndexQueryRecord(baseURI, now, r)); err != nil {
				return fmt.Errorf("encode record: %w", err)
			}
		}
	}

	// Upload to output destination if --output is set.
	if outputURI != "" {
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close temp file: %w", err)
		}

		spec, err := parseOutputDest(outputURI)
		if err != nil {
			return fmt.Errorf("invalid --output: %w", err)
		}
		spec.Profile = outputProfile
		spec.Region = outputRegion
		spec.Endpoint = outputEndpoint
		if spec.Endpoint != "" {
			spec.ForcePathStyle = true
		}

		putter, err := newOutputProvider(ctx, spec)
		if err != nil {
			return fmt.Errorf("output provider: %w", err)
		}
		if closer, ok := putter.(interface{ Close() error }); ok {
			defer func() { _ = closer.Close() }()
		}

		if err := uploadToOutputDest(ctx, putter, spec.Key, tempPath); err != nil {
			return err
		}

		_, _ = fmt.Fprintf(os.Stderr, "Wrote %d records to %s\n", outputRecordCount(results, canonicalResults, canonicalByETag), outputURI)
	}

	// Summary to stderr
	if canonicalByETag {
		_, _ = fmt.Fprintf(os.Stderr, "%d canonical groups, %d ungrouped empty-ETag rows, %d total records\n", canonicalStats.CanonicalGroups, canonicalStats.PassthroughRows, canonicalStats.TotalRecords)
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "Matched %d objects\n", len(results))
	}
	if stats.TimestampParseErrors > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "warning: %d rows had unparseable timestamps (fields set to null)\n", stats.TimestampParseErrors)
	}

	return nil
}

func outputRecordCount(results []indexstore.QueryResult, canonicalResults []indexstore.CanonicalOutputRecord, canonicalByETag bool) int {
	if canonicalByETag {
		return len(canonicalResults)
	}
	return len(results)
}

func newIndexQueryRecord(baseURI string, ts string, r indexstore.QueryResult) indexQueryRecord {
	record := indexQueryRecord{
		Type: "gonimbus.index.object.v1",
		TS:   ts,
		Data: indexQueryRecordData{
			BaseURI:   baseURI,
			RelKey:    r.RelKey,
			Key:       reconstructFullKey(baseURI, r.RelKey),
			SizeBytes: r.SizeBytes,
			ETag:      r.ETag,
		},
	}

	if r.LastModified != nil {
		lastModified := r.LastModified.Format(time.RFC3339)
		record.Data.LastModified = &lastModified
	}
	if r.DeletedAt != nil {
		deletedAt := r.DeletedAt.Format(time.RFC3339)
		record.Data.DeletedAt = &deletedAt
	}
	return record
}

func newIndexCanonicalQueryRecord(baseURI string, ts string, group indexstore.CanonicalObjectGroup, rule indexstore.CanonicalTieBreak, includeAlternates bool) indexCanonicalQueryRecord {
	record := indexCanonicalQueryRecord{
		Type: "gonimbus.index.object.canonical.v1",
		TS:   ts,
		Data: indexCanonicalQueryRecordData{
			BaseURI:         baseURI,
			ETag:            group.ETag,
			Canonical:       newIndexCanonicalObjectData(baseURI, group.Canonical),
			TieBreakRule:    string(rule),
			AlternatesCount: len(group.Alternates),
		},
	}
	if includeAlternates {
		record.Data.Alternates = make([]indexCanonicalAlternateData, 0, len(group.Alternates))
		for _, alternate := range group.Alternates {
			record.Data.Alternates = append(record.Data.Alternates, newIndexCanonicalAlternateData(alternate))
		}
	}
	return record
}

func newIndexCanonicalObjectData(baseURI string, r indexstore.QueryResult) indexCanonicalObjectData {
	record := indexCanonicalObjectData{
		RelKey:    r.RelKey,
		Key:       reconstructFullKey(baseURI, r.RelKey),
		SizeBytes: r.SizeBytes,
	}
	if r.LastModified != nil {
		lastModified := r.LastModified.Format(time.RFC3339)
		record.LastModified = &lastModified
	}
	if r.DeletedAt != nil {
		deletedAt := r.DeletedAt.Format(time.RFC3339)
		record.DeletedAt = &deletedAt
	}
	return record
}

func newIndexCanonicalAlternateData(r indexstore.QueryResult) indexCanonicalAlternateData {
	record := indexCanonicalAlternateData{
		RelKey:    r.RelKey,
		SizeBytes: r.SizeBytes,
	}
	if r.LastModified != nil {
		lastModified := r.LastModified.Format(time.RFC3339)
		record.LastModified = &lastModified
	}
	if r.DeletedAt != nil {
		deletedAt := r.DeletedAt.Format(time.RFC3339)
		record.DeletedAt = &deletedAt
	}
	return record
}

type indexDBEntry struct {
	Path string
	Dir  string
	Info indexstore.IndexListEntry
}

// openIndexDB opens a local index database for reading.
func openIndexDB(ctx context.Context, path string) (*sql.DB, error) {
	cfg := indexstore.Config{Path: path}
	if strings.HasPrefix(path, "libsql://") || strings.HasPrefix(path, "https://") {
		cfg = indexstore.Config{URL: path}
	}
	return indexstore.Open(ctx, cfg)
}

func indexDataDir() (string, error) {
	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return "", fmt.Errorf("app identity is not available")
	}
	return gfconfig.GetAppDataDir(identity.ConfigName), nil
}

func indexRootDir() (string, error) {
	dataDir, err := indexDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "indexes"), nil
}

func listIndexDBPaths() ([]string, error) {
	rootDir, err := indexRootDir()
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read index directory: %w", err)
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dbPath := filepath.Join(rootDir, entry.Name(), "index.db")
		if _, err := os.Stat(dbPath); err != nil {
			continue
		}
		paths = append(paths, dbPath)
	}

	return paths, nil
}

func loadIndexEntriesWithPaths(ctx context.Context) ([]indexDBEntry, error) {
	paths, err := listIndexDBPaths()
	if err != nil {
		return nil, err
	}

	entries := make([]indexDBEntry, 0, len(paths))
	for _, path := range paths {
		db, err := openIndexDB(ctx, path)
		if err != nil {
			warnIndexDB("skip index db %s: %v", path, err)
			continue
		}

		info, err := indexstore.ListIndexSetsWithStats(ctx, db)
		_ = db.Close()
		if err != nil {
			warnIndexDB("skip index db %s: %v", path, err)
			continue
		}
		if len(info) == 0 {
			continue
		}
		if len(info) > 1 {
			warnIndexDB("skip index db %s: multiple index sets found", path)
			continue
		}

		entries = append(entries, indexDBEntry{
			Path: path,
			Dir:  filepath.Dir(path),
			Info: info[0],
		})
	}

	return entries, nil
}

type indexDBCandidate struct {
	Path          string
	Dir           string
	IndexSet      *indexstore.IndexSet
	LatestRun     *indexstore.IndexRun
	LatestSuccess *indexstore.IndexRun
}

// openIndexDBByID opens a local index database by its index set ID or directory name.
// Accepts either a full index_set_id (idx_<64hex>) or directory name (idx_<16hex>).
func openIndexDBByID(ctx context.Context, id string) (*sql.DB, *indexstore.IndexSet, error) {
	rootDir, err := indexRootDir()
	if err != nil {
		return nil, nil, err
	}
	return openIndexDBByIDInRoot(ctx, rootDir, id)
}

// validHexPattern matches lowercase hex strings (1-64 chars).
var validHexPattern = regexp.MustCompile(`^[0-9a-f]{1,64}$`)

// indexDirMatch holds the resolved directory path and name for an index set.
type indexDirMatch struct {
	DBPath  string
	DirName string
}

// resolveIndexDirInRoot finds the index directory matching the given ID in the root.
// This is the shared directory-matching logic used by both index query and index export.
func resolveIndexDirInRoot(rootDir, id string) (*indexDirMatch, error) {
	cleanID := strings.TrimPrefix(id, "idx_")
	if cleanID == "" {
		return nil, fmt.Errorf("invalid index set ID: %s", id)
	}
	if !validHexPattern.MatchString(cleanID) {
		return nil, fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", id)
	}

	entries, err := os.ReadDir(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no indexes found (index root does not exist)")
		}
		return nil, fmt.Errorf("read index directory: %w", err)
	}

	var matches []indexDirMatch
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirHex := strings.TrimPrefix(entry.Name(), "idx_")
		// Match when the stored dir hex starts with the user input (short prefix lookup),
		// or when the user provided a full 64-char hash and the dir is a truncated prefix of it.
		// Only allow reverse matching for exact full-length hashes to prevent partial-suffix matches.
		if strings.HasPrefix(dirHex, cleanID) || (len(cleanID) == 64 && strings.HasPrefix(cleanID, dirHex)) {
			dbPath := filepath.Join(rootDir, entry.Name(), "index.db")
			if _, statErr := os.Stat(dbPath); statErr == nil {
				matches = append(matches, indexDirMatch{DBPath: dbPath, DirName: entry.Name()})
			}
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no index found matching ID: %s", id)
	}
	if len(matches) > 1 {
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.DirName
		}
		return nil, fmt.Errorf("ambiguous index ID %s matches %d directories: %s", id, len(matches), strings.Join(names, ", "))
	}

	return &matches[0], nil
}

// openIndexDBByIDInRoot is the testable core of openIndexDBByID.
func openIndexDBByIDInRoot(ctx context.Context, rootDir, id string) (*sql.DB, *indexstore.IndexSet, error) {
	match, err := resolveIndexDirInRoot(rootDir, id)
	if err != nil {
		return nil, nil, err
	}

	db, err := openIndexDB(ctx, match.DBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open index database: %w", err)
	}

	cleanID := strings.TrimPrefix(id, "idx_")
	sets, err := indexstore.ListIndexSets(ctx, db, "")
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("list index sets: %w", err)
	}
	if len(sets) == 0 {
		_ = db.Close()
		return nil, nil, fmt.Errorf("no index sets in database: %s", match.DBPath)
	}

	// Match the user-provided prefix against the full index_set_id in the DB.
	for i := range sets {
		setHex := strings.TrimPrefix(sets[i].IndexSetID, "idx_")
		if strings.HasPrefix(setHex, cleanID) {
			return db, &sets[i], nil
		}
	}

	// Fallback: single-set-per-DB convention.
	return db, &sets[0], nil
}

func openIndexDBForBaseURI(ctx context.Context, baseURI string) (*sql.DB, *indexstore.IndexSet, error) {
	paths, err := listIndexDBPaths()
	if err != nil {
		return nil, nil, err
	}

	var candidates []indexDBCandidate
	for _, path := range paths {
		candidate, err := inspectIndexDBForBaseURI(ctx, path, baseURI)
		if err != nil {
			warnIndexDB("skip index db %s: %v", path, err)
			continue
		}
		if candidate == nil {
			continue
		}
		candidates = append(candidates, *candidate)
	}

	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("no index found for base URI: %s", baseURI)
	}

	best, reason := selectBestCandidate(candidates)
	if len(candidates) > 1 {
		warnIndexSelection(baseURI, best, candidates, reason)
	}

	db, err := openIndexDB(ctx, best.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("open index database: %w", err)
	}

	return db, best.IndexSet, nil
}

func inspectIndexDBForBaseURI(ctx context.Context, path string, baseURI string) (*indexDBCandidate, error) {
	db, err := openIndexDB(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	indexSet, err := indexstore.GetIndexSetByBaseURI(ctx, db, baseURI)
	if err != nil {
		return nil, err
	}
	if indexSet == nil {
		return nil, nil
	}

	runs, err := indexstore.ListIndexRuns(ctx, db, indexSet.IndexSetID)
	if err != nil {
		warnIndexDB("index runs unavailable for %s: %v", path, err)
	}

	var latestRun *indexstore.IndexRun
	var latestSuccess *indexstore.IndexRun
	if len(runs) > 0 {
		latestRun = &runs[0]
		for i := range runs {
			if runs[i].Status == indexstore.RunStatusSuccess {
				latestSuccess = &runs[i]
				break
			}
		}
	}

	return &indexDBCandidate{
		Path:          path,
		Dir:           filepath.Dir(path),
		IndexSet:      indexSet,
		LatestRun:     latestRun,
		LatestSuccess: latestSuccess,
	}, nil
}

func selectBestCandidate(candidates []indexDBCandidate) (indexDBCandidate, string) {
	best := candidates[0]
	bestScore := scoreCandidate(best)

	for i := 1; i < len(candidates); i++ {
		score := scoreCandidate(candidates[i])
		if compareCandidateScore(score, bestScore) > 0 {
			best = candidates[i]
			bestScore = score
		}
	}

	switch {
	case bestScore.HasSuccess:
		return best, "latest successful run"
	case bestScore.HasRun:
		return best, "latest run"
	default:
		return best, "latest index set"
	}
}

type candidateScore struct {
	HasSuccess bool
	SuccessAt  time.Time
	HasRun     bool
	RunAt      time.Time
	CreatedAt  time.Time
}

func scoreCandidate(candidate indexDBCandidate) candidateScore {
	score := candidateScore{CreatedAt: candidate.IndexSet.CreatedAt}
	if candidate.LatestSuccess != nil {
		score.HasSuccess = true
		score.SuccessAt = runTimestamp(candidate.LatestSuccess)
	}
	if candidate.LatestRun != nil {
		score.HasRun = true
		score.RunAt = runTimestamp(candidate.LatestRun)
	}
	return score
}

func compareCandidateScore(a candidateScore, b candidateScore) int {
	if a.HasSuccess || b.HasSuccess {
		if a.HasSuccess && b.HasSuccess {
			return compareTime(a.SuccessAt, b.SuccessAt)
		}
		if a.HasSuccess {
			return 1
		}
		return -1
	}
	if a.HasRun || b.HasRun {
		if a.HasRun && b.HasRun {
			return compareTime(a.RunAt, b.RunAt)
		}
		if a.HasRun {
			return 1
		}
		return -1
	}
	return compareTime(a.CreatedAt, b.CreatedAt)
}

func compareTime(a time.Time, b time.Time) int {
	switch {
	case a.After(b):
		return 1
	case b.After(a):
		return -1
	default:
		return 0
	}
}

func runTimestamp(run *indexstore.IndexRun) time.Time {
	if run == nil {
		return time.Time{}
	}
	if run.EndedAt != nil {
		return *run.EndedAt
	}
	return run.StartedAt
}

func warnIndexDB(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "warning: "+format+"\n", args...)
}

func warnIndexSelection(baseURI string, selected indexDBCandidate, candidates []indexDBCandidate, reason string) {
	_, _ = fmt.Fprintf(os.Stderr, "warning: multiple indexes match base URI %s\n", baseURI)
	_, _ = fmt.Fprintf(os.Stderr, "  selected: %s (reason: %s)\n", selected.IndexSet.IndexSetID, reason)
	_, _ = fmt.Fprintf(os.Stderr, "  details: %s\n", formatCandidateDetails(selected))
	_, _ = fmt.Fprintln(os.Stderr, "  alternatives:")
	for _, candidate := range candidates {
		if candidate.IndexSet.IndexSetID == selected.IndexSet.IndexSetID {
			continue
		}
		_, _ = fmt.Fprintf(os.Stderr, "    - %s\n", formatCandidateDetails(candidate))
	}
}

func formatCandidateDetails(candidate indexDBCandidate) string {
	latestRunAt, latestStatus := "-", "-"
	if candidate.LatestRun != nil {
		latestRunAt = runTimestamp(candidate.LatestRun).Format(time.RFC3339)
		latestStatus = string(candidate.LatestRun.Status)
	}
	return fmt.Sprintf("index_set_id=%s created_at=%s latest_run=%s status=%s", candidate.IndexSet.IndexSetID, candidate.IndexSet.CreatedAt.Format(time.RFC3339), latestRunAt, latestStatus)
}

// normalizeQueryBaseURI normalizes a base URI for query lookup.
// Ensures trailing slash for prefix URIs.
func normalizeQueryBaseURI(uri string) string {
	// Ensure trailing slash for consistency with how we store base URIs
	if !strings.HasSuffix(uri, "/") {
		return uri + "/"
	}
	return uri
}

// reconstructFullKey builds the full object key from base URI and relative key.
// The key is the path portion after the bucket in the base URI.
func reconstructFullKey(baseURI, relKey string) string {
	// baseURI is like "s3://bucket/prefix/"
	// Extract the prefix part (everything after s3://bucket/)
	// For now, since rel_key is stored relative to base_uri prefix, return
	// the prefix + rel_key
	//
	// Example:
	//   base_uri: s3://mybucket/data/
	//   rel_key: 2025/01/file.json
	//   full_key: data/2025/01/file.json

	// Parse the base URI to extract the prefix
	if strings.HasPrefix(baseURI, "s3://") {
		parts := strings.SplitN(strings.TrimPrefix(baseURI, "s3://"), "/", 2)
		if len(parts) == 2 {
			prefix := parts[1] // includes trailing slash
			return prefix + relKey
		}
	}

	// Fallback: just return rel_key
	return relKey
}
