package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect <uri>",
	Short: "Quick inspection of object or prefix",
	Long: `Inspect objects in cloud storage without a manifest file.

Supports S3 URIs with optional glob patterns for filtering.

Examples:
  gonimbus inspect s3://bucket/path/to/object.txt
  gonimbus inspect s3://bucket/prefix/
  gonimbus inspect s3://bucket/data/**/*.parquet
  gonimbus inspect s3://bucket/prefix/ --limit 10
  gonimbus inspect s3://bucket/prefix/ --json`,
	Args: cobra.ExactArgs(1),
	RunE: runInspect,
}

var (
	inspectRegion   string
	inspectProfile  string
	inspectEndpoint string
	inspectLimit    int
	inspectJSON     bool
	// Filter flags
	inspectMinSize  string
	inspectMaxSize  string
	inspectAfter    string
	inspectBefore   string
	inspectKeyRegex string
)

func init() {
	rootCmd.AddCommand(inspectCmd)

	inspectCmd.Flags().StringVarP(&inspectRegion, "region", "r", "", "AWS region")
	inspectCmd.Flags().StringVarP(&inspectProfile, "profile", "p", "", "AWS profile")
	inspectCmd.Flags().StringVar(&inspectEndpoint, "endpoint", "", "Custom S3 endpoint")
	inspectCmd.Flags().IntVarP(&inspectLimit, "limit", "n", 100, "Max objects to list")
	inspectCmd.Flags().BoolVar(&inspectJSON, "json", false, "Output as JSON")

	// Filter flags
	inspectCmd.Flags().StringVar(&inspectMinSize, "min-size", "", "Minimum object size (e.g., 1KB, 100MiB)")
	inspectCmd.Flags().StringVar(&inspectMaxSize, "max-size", "", "Maximum object size (e.g., 100MB, 1GiB)")
	inspectCmd.Flags().StringVar(&inspectAfter, "after", "", "Only objects modified after date (ISO 8601: 2024-01-15)")
	inspectCmd.Flags().StringVar(&inspectBefore, "before", "", "Only objects modified before date (ISO 8601: 2024-06-30)")
	inspectCmd.Flags().StringVar(&inspectKeyRegex, "key-regex", "", "Regex pattern for object keys")
}

func runInspect(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	rawURI := args[0]

	// Parse URI
	parsed, err := uri.ParseURI(rawURI)
	if err != nil {
		observability.CLILogger.Error("Invalid URI", zap.String("uri", rawURI), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}

	observability.CLILogger.Debug("Parsed URI",
		zap.String("provider", parsed.Provider),
		zap.String("bucket", parsed.Bucket),
		zap.String("key", parsed.Key),
		zap.String("pattern", parsed.Pattern))

	// Build filter from CLI flags
	filter, err := buildInspectFilter()
	if err != nil {
		observability.CLILogger.Error("Invalid filter", zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid filter", err)
	}

	target := commandSourceTargetForRead(parsed)

	// Create provider
	prov, err := createInspectProvider(ctx, target.ProviderURI)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}

	// List objects
	result, err := listObjectsDetailed(ctx, prov, target.QueryURI, filter)
	if err != nil {
		observability.CLILogger.Error("Failed to list objects", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to list objects", err)
	}

	// Output results
	if inspectJSON {
		return outputJSONWithSummary(result.Objects, result.Summary)
	}
	return outputTableWithSummary(result.Objects, result.Summary)
}

// buildInspectFilter creates a composite filter from CLI flags.
func buildInspectFilter() (*match.CompositeFilter, error) {
	cfg := &match.FilterConfig{}
	hasFilter := false

	// Size filter
	if inspectMinSize != "" || inspectMaxSize != "" {
		cfg.Size = &match.SizeFilterConfig{
			Min: inspectMinSize,
			Max: inspectMaxSize,
		}
		hasFilter = true
	}

	// Date filter
	if inspectAfter != "" || inspectBefore != "" {
		cfg.Modified = &match.DateFilterConfig{
			After:  inspectAfter,
			Before: inspectBefore,
		}
		hasFilter = true
	}

	// Regex filter
	if inspectKeyRegex != "" {
		cfg.KeyRegex = inspectKeyRegex
		hasFilter = true
	}

	if !hasFilter {
		return nil, nil
	}

	return match.NewFilterFromConfig(cfg)
}

// createInspectProvider creates a provider for inspect command.
func createInspectProvider(ctx context.Context, objURI *uri.ObjectURI) (provider.Provider, error) {
	return newCommandSourceProvider(ctx, objURI, "inspect", inspectRegion, inspectProfile, inspectEndpoint)
}

type inspectListResult struct {
	Objects []provider.ObjectSummary
	Summary *inspectSummary
}

type inspectSummary struct {
	Type string             `json:"type"`
	Data inspectSummaryData `json:"data"`
}

type inspectSummaryData struct {
	ObjectsEmitted int    `json:"objects_emitted"`
	Limit          int    `json:"limit"`
	Truncated      bool   `json:"truncated"`
	Reason         string `json:"reason,omitempty"`
	MayHaveMore    bool   `json:"may_have_more"`
}

const inspectSummaryType = "gonimbus.inspect.summary.v1"

// listObjects lists objects matching the URI and optional filter.
func listObjects(ctx context.Context, prov provider.Provider, uri *uri.ObjectURI, filter *match.CompositeFilter) ([]provider.ObjectSummary, error) {
	result, err := listObjectsDetailed(ctx, prov, uri, filter)
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

// listObjectsDetailed lists objects and reports whether output was capped by --limit.
func listObjectsDetailed(ctx context.Context, prov provider.Provider, uri *uri.ObjectURI, filter *match.CompositeFilter) (*inspectListResult, error) {
	// If URI is an exact object key (not a pattern, not a prefix), use Head
	// for precise lookup. This avoids prefix-based listing which could return
	// unrelated objects (e.g., "object.txt" vs "object.txt.bak").
	if !uri.IsPattern() && !uri.IsPrefix() {
		meta, err := prov.Head(ctx, uri.Key)
		if err != nil {
			return nil, err
		}
		// Apply filter to single object
		if filter != nil && !filter.Match(&meta.ObjectSummary) {
			return &inspectListResult{}, nil
		}
		return &inspectListResult{Objects: []provider.ObjectSummary{meta.ObjectSummary}}, nil
	}

	var objects []provider.ObjectSummary
	var continuationToken string
	var matcher *match.Matcher

	// Create matcher if pattern specified
	if uri.IsPattern() {
		cfg := match.Config{
			Includes: []string{uri.Pattern},
		}
		var err error
		matcher, err = match.New(cfg)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern: %w", err)
		}
	}

	// List with pagination
	for len(objects) < inspectLimit {
		result, err := prov.List(ctx, provider.ListOptions{
			Prefix:            uri.Key,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			// Apply glob pattern filter if specified
			if matcher != nil && !matcher.Match(obj.Key) {
				continue
			}

			// Apply metadata filter if specified
			if filter != nil && !filter.Match(&obj) {
				continue
			}

			if len(objects) >= inspectLimit {
				return &inspectListResult{
					Objects: objects,
					Summary: newInspectLimitSummary(len(objects)),
				}, nil
			}
			objects = append(objects, obj)
		}

		if len(objects) >= inspectLimit {
			if result.IsTruncated && result.ContinuationToken != "" {
				return &inspectListResult{
					Objects: objects,
					Summary: newInspectLimitSummary(len(objects)),
				}, nil
			}
			break
		}

		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		continuationToken = result.ContinuationToken
	}

	return &inspectListResult{Objects: objects}, nil
}

func newInspectLimitSummary(objectsEmitted int) *inspectSummary {
	return &inspectSummary{
		Type: inspectSummaryType,
		Data: inspectSummaryData{
			ObjectsEmitted: objectsEmitted,
			Limit:          inspectLimit,
			Truncated:      true,
			Reason:         "limit_reached",
			MayHaveMore:    true,
		},
	}
}

// objectOutput is the JSON output structure for inspect.
type objectOutput struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag,omitempty"`
}

// outputJSON writes objects as JSONL to stdout.
func outputJSON(objects []provider.ObjectSummary) error {
	return outputJSONWithSummary(objects, nil)
}

// outputJSONWithSummary writes objects as JSONL to stdout, followed by a
// stream-level summary record when inspect output was capped.
func outputJSONWithSummary(objects []provider.ObjectSummary, summary *inspectSummary) error {
	enc := json.NewEncoder(os.Stdout)
	for _, obj := range objects {
		out := objectOutput{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("failed to encode object: %w", err)
		}
	}
	if summary != nil {
		if err := enc.Encode(summary); err != nil {
			return fmt.Errorf("failed to encode summary: %w", err)
		}
	}
	return nil
}

// outputTable writes objects as a formatted table to stdout.
func outputTable(objects []provider.ObjectSummary) error {
	return outputTableWithSummary(objects, nil)
}

// outputTableWithSummary writes objects as a formatted table to stdout.
func outputTableWithSummary(objects []provider.ObjectSummary, summary *inspectSummary) error {
	if len(objects) == 0 {
		fmt.Println("No objects found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Header
	if _, err := fmt.Fprintln(w, "KEY\tSIZE\tMODIFIED"); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	var totalSize int64
	for _, obj := range objects {
		totalSize += obj.Size
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\n",
			obj.Key,
			formatSize(obj.Size),
			obj.LastModified.Format("2006-01-02 15:04:05")); err != nil {
			return fmt.Errorf("failed to write object: %w", err)
		}
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush output: %w", err)
	}

	fmt.Println()
	fmt.Printf("Found %d object(s) (%s total)\n", len(objects), formatSize(totalSize))
	if summary != nil && summary.Data.Truncated {
		fmt.Printf("Warning: output limited to %d object(s); more matching objects may exist. Increase --limit to inspect more.\n", summary.Data.Limit)
	}

	return nil
}

// formatSize formats bytes as human-readable size.
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.1f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
