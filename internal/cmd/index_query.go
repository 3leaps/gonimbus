package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/match"
)

var indexQueryCmd = &cobra.Command{
	Use:   "query <base-uri>",
	Short: "Query indexed objects by pattern",
	Long: `Query objects from a local index using glob patterns and filters.

The query searches the objects_current table for objects matching
the specified patterns and filters. Results are emitted as JSONL
records to stdout.

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
  gonimbus index query s3://bucket/ --min-size 1MB --after 2025-01-01`,
	Args: cobra.ExactArgs(1),
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

func runIndexQuery(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	baseURI := args[0]

	// Normalize base URI (ensure trailing slash for prefix URIs)
	baseURI = normalizeQueryBaseURI(baseURI)

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

	// Open index database
	db, err := openQueryIndexDB(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Find index set by base URI
	indexSet, err := indexstore.GetIndexSetByBaseURI(ctx, db, baseURI)
	if err != nil {
		return fmt.Errorf("lookup index set: %w", err)
	}
	if indexSet == nil {
		return fmt.Errorf("no index found for base URI: %s", baseURI)
	}

	// Build query params
	params := indexstore.QueryParams{
		IndexSetID:     indexSet.IndexSetID,
		Pattern:        pattern,
		KeyRegex:       keyRegex,
		IncludeDeleted: includeDeleted,
		Limit:          limit,
	}

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
		count, err := indexstore.QueryObjectCount(ctx, db, countParams)
		if err != nil {
			return fmt.Errorf("count query failed: %w", err)
		}
		_, _ = fmt.Fprintf(os.Stdout, "%d\n", count)
		return nil
	}

	// Execute full query
	results, stats, err := indexstore.QueryObjects(ctx, db, params)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	// Emit JSONL records
	now := time.Now().UTC().Format(time.RFC3339Nano)
	enc := json.NewEncoder(os.Stdout)

	for _, r := range results {
		// Reconstruct full key from base URI + rel_key
		fullKey := reconstructFullKey(baseURI, r.RelKey)

		record := indexQueryRecord{
			Type: "gonimbus.index.object.v1",
			TS:   now,
			Data: indexQueryRecordData{
				BaseURI:   baseURI,
				RelKey:    r.RelKey,
				Key:       fullKey,
				SizeBytes: r.SizeBytes,
				ETag:      r.ETag,
			},
		}

		if r.LastModified != nil {
			ts := r.LastModified.Format(time.RFC3339)
			record.Data.LastModified = &ts
		}

		if r.DeletedAt != nil {
			ts := r.DeletedAt.Format(time.RFC3339)
			record.Data.DeletedAt = &ts
		}

		if err := enc.Encode(record); err != nil {
			return fmt.Errorf("encode record: %w", err)
		}
	}

	// Summary to stderr
	_, _ = fmt.Fprintf(os.Stderr, "Matched %d objects\n", len(results))
	if stats.TimestampParseErrors > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "warning: %d rows had unparseable timestamps (fields set to null)\n", stats.TimestampParseErrors)
	}

	return nil
}

// openQueryIndexDB opens the local index database for querying.
func openQueryIndexDB(ctx context.Context) (*sql.DB, error) {
	// Get index database path using same logic as index build
	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return nil, fmt.Errorf("app identity is not available")
	}

	dataDir := gfconfig.GetAppDataDir(identity.ConfigName)
	indexDBPath := filepath.Join(dataDir, "indexes", "gonimbus-index.db")

	// Check if database exists
	if _, err := os.Stat(indexDBPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("index database not found at %s (run 'gonimbus index init' first)", indexDBPath)
	}

	cfg := indexstore.Config{Path: indexDBPath}
	db, err := indexstore.Open(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open index database: %w", err)
	}

	return db, nil
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
