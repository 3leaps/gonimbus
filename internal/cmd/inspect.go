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
	"github.com/3leaps/gonimbus/pkg/provider/s3"
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
)

func init() {
	rootCmd.AddCommand(inspectCmd)

	inspectCmd.Flags().StringVarP(&inspectRegion, "region", "r", "", "AWS region")
	inspectCmd.Flags().StringVarP(&inspectProfile, "profile", "p", "", "AWS profile")
	inspectCmd.Flags().StringVar(&inspectEndpoint, "endpoint", "", "Custom S3 endpoint")
	inspectCmd.Flags().IntVarP(&inspectLimit, "limit", "n", 100, "Max objects to list")
	inspectCmd.Flags().BoolVar(&inspectJSON, "json", false, "Output as JSON")
}

func runInspect(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	uri := args[0]

	// Parse URI
	parsed, err := ParseURI(uri)
	if err != nil {
		observability.CLILogger.Error("Invalid URI", zap.String("uri", uri), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}

	observability.CLILogger.Debug("Parsed URI",
		zap.String("provider", parsed.Provider),
		zap.String("bucket", parsed.Bucket),
		zap.String("key", parsed.Key),
		zap.String("pattern", parsed.Pattern))

	// Create provider
	prov, err := createInspectProvider(ctx, parsed)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}

	// List objects
	objects, err := listObjects(ctx, prov, parsed)
	if err != nil {
		observability.CLILogger.Error("Failed to list objects", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to list objects", err)
	}

	// Output results
	if inspectJSON {
		return outputJSON(objects)
	}
	return outputTable(objects)
}

// createInspectProvider creates an S3 provider for inspect command.
func createInspectProvider(ctx context.Context, uri *ObjectURI) (*s3.Provider, error) {
	cfg := s3.Config{
		Bucket:   uri.Bucket,
		Region:   inspectRegion,
		Endpoint: inspectEndpoint,
		Profile:  inspectProfile,
		// Force path-style URLs when custom endpoint is set.
		// S3-compatible services (moto, MinIO, etc.) require this.
		ForcePathStyle: inspectEndpoint != "",
	}
	return s3.New(ctx, cfg)
}

// listObjects lists objects matching the URI.
func listObjects(ctx context.Context, prov provider.Provider, uri *ObjectURI) ([]provider.ObjectSummary, error) {
	// If URI is an exact object key (not a pattern, not a prefix), use Head
	// for precise lookup. This avoids prefix-based listing which could return
	// unrelated objects (e.g., "object.txt" vs "object.txt.bak").
	if !uri.IsPattern() && !uri.IsPrefix() {
		meta, err := prov.Head(ctx, uri.Key)
		if err != nil {
			return nil, err
		}
		return []provider.ObjectSummary{meta.ObjectSummary}, nil
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
			// Apply pattern filter if specified
			if matcher != nil && !matcher.Match(obj.Key) {
				continue
			}

			objects = append(objects, obj)
			if len(objects) >= inspectLimit {
				break
			}
		}

		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		continuationToken = result.ContinuationToken
	}

	return objects, nil
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
	return nil
}

// outputTable writes objects as a formatted table to stdout.
func outputTable(objects []provider.ObjectSummary) error {
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
