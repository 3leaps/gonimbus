package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var crawlCmd = &cobra.Command{
	Use:   "crawl",
	Short: "Run a crawl job from manifest",
	Long: `Run a crawl job as defined in a YAML or JSON manifest file.

The manifest specifies the provider connection, pattern matching rules,
crawl behavior, and output configuration.

Example:
  gonimbus crawl --job crawl.yaml
  gonimbus crawl --job crawl.yaml --output results.jsonl
  gonimbus crawl --job crawl.yaml --quiet
  gonimbus crawl --job crawl.yaml --dry-run`,
	RunE: runCrawl,
}

var (
	crawlJobPath       string
	crawlOutput        string
	crawlQuiet         bool
	crawlDryRun        bool
	crawlPlan          bool
	crawlPreflightMode string
)

func init() {
	rootCmd.AddCommand(crawlCmd)

	crawlCmd.Flags().StringVarP(&crawlJobPath, "job", "j", "", "Path to job manifest (required)")
	crawlCmd.Flags().StringVarP(&crawlOutput, "output", "o", "", "Override output destination")
	crawlCmd.Flags().BoolVarP(&crawlQuiet, "quiet", "q", false, "Suppress progress records")
	crawlCmd.Flags().BoolVar(&crawlDryRun, "dry-run", false, "Validate manifest and show plan without executing")
	crawlCmd.Flags().BoolVar(&crawlPlan, "plan", false, "Alias for --dry-run")
	crawlCmd.Flags().StringVar(&crawlPreflightMode, "preflight", "", "Override preflight mode (plan-only|read-safe|write-probe)")

	_ = crawlCmd.MarkFlagRequired("job")
}

func runCrawl(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Load and validate manifest
	m, err := manifest.Load(crawlJobPath)
	if err != nil {
		observability.CLILogger.Error("Failed to load manifest",
			zap.String("path", crawlJobPath),
			zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid manifest", err)
	}

	observability.CLILogger.Debug("Loaded manifest",
		zap.String("path", crawlJobPath),
		zap.String("provider", m.Connection.Provider),
		zap.String("bucket", m.Connection.Bucket),
		zap.Strings("includes", m.Match.Includes))

	// Apply output override if specified
	if crawlOutput != "" {
		m.Output.Destination = crawlOutput
	}

	// Apply preflight override if specified
	if crawlPreflightMode != "" {
		switch crawlPreflightMode {
		case "plan-only", "read-safe", "write-probe":
			m.Crawl.Preflight.Mode = crawlPreflightMode
		default:
			return exitError(foundry.ExitInvalidArgument, "Invalid --preflight value", fmt.Errorf("unsupported preflight mode: %s", crawlPreflightMode))
		}
	}

	// Apply quiet flag
	if crawlQuiet {
		enabled := false
		m.Output.Progress = &enabled
	}

	// Plan mode: show plan and exit
	if crawlPlan || crawlDryRun {
		return showCrawlPlan(m)
	}

	// Execute crawl
	return executeCrawl(ctx, m)
}

// showCrawlPlan displays what would be crawled without executing.
func showCrawlPlan(m *manifest.Manifest) error {
	fmt.Println("=== Crawl Plan (dry-run) ===")
	fmt.Println()
	fmt.Printf("Provider:    %s\n", m.Connection.Provider)
	fmt.Printf("Bucket:      %s\n", m.Connection.Bucket)
	if m.Connection.Region != "" {
		fmt.Printf("Region:      %s\n", m.Connection.Region)
	}
	if m.Connection.Endpoint != "" {
		fmt.Printf("Endpoint:    %s\n", m.Connection.Endpoint)
	}
	fmt.Println()
	fmt.Println("Patterns:")
	fmt.Println("  Include:")
	for _, p := range m.Match.Includes {
		fmt.Printf("    - %s\n", p)
	}
	if len(m.Match.Excludes) > 0 {
		fmt.Println("  Exclude:")
		for _, p := range m.Match.Excludes {
			fmt.Printf("    - %s\n", p)
		}
	}
	fmt.Println()

	if m.Match.Filters != nil {
		fmt.Println("Filters:")
		if m.Match.Filters.Size != nil {
			fmt.Printf("  Size:      min=%s max=%s\n", m.Match.Filters.Size.Min, m.Match.Filters.Size.Max)
		}
		if m.Match.Filters.Modified != nil {
			fmt.Printf("  Modified:  after=%s before=%s\n", m.Match.Filters.Modified.After, m.Match.Filters.Modified.Before)
		}
		if m.Match.Filters.KeyRegex != "" {
			fmt.Printf("  Key Regex: %s\n", m.Match.Filters.KeyRegex)
		}
		fmt.Println()
	}

	fmt.Printf("Concurrency: %d\n", m.Crawl.Concurrency)
	if m.Crawl.RateLimit > 0 {
		fmt.Printf("Rate Limit:  %.1f req/s\n", m.Crawl.RateLimit)
	}
	if m.Crawl.Preflight.Mode != "" {
		fmt.Printf("Preflight:   %s\n", m.Crawl.Preflight.Mode)
	}
	fmt.Printf("Output:      %s\n", m.Output.Destination)
	fmt.Printf("Progress:    %v\n", m.Output.ProgressEnabled())
	fmt.Println()
	fmt.Println("Manifest validated successfully. Remove --dry-run to execute.")
	return nil
}

// executeCrawl runs the actual crawl job.
func executeCrawl(ctx context.Context, m *manifest.Manifest) error {
	// Generate job ID early so we can use it in writer
	jobID := uuid.New().String()

	// Create provider
	prov, err := createProvider(ctx, m)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}

	// Create matcher
	matchCfg := match.Config{
		Includes:      m.Match.Includes,
		Excludes:      m.Match.Excludes,
		IncludeHidden: m.Match.IncludeHidden,
	}
	matcher, err := match.New(matchCfg)
	if err != nil {
		observability.CLILogger.Error("Failed to create matcher", zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid match patterns", err)
	}

	filter, err := buildCrawlFilter(m)
	if err != nil {
		observability.CLILogger.Error("Invalid filters", zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid filters", err)
	}

	// Create output writer
	writer, cleanup, err := createWriter(m, jobID)
	if err != nil {
		observability.CLILogger.Error("Failed to create writer", zap.Error(err))
		return exitError(foundry.ExitFileWriteError, "Failed to create output", err)
	}
	defer cleanup()

	// Preflight checks (plan-only/read-safe/write-probe)
	pfSpec := preflight.Spec{
		Mode:          preflight.Mode(m.Crawl.Preflight.Mode),
		ProbeStrategy: preflight.ProbeStrategy(m.Crawl.Preflight.ProbeStrategy),
		ProbePrefix:   m.Crawl.Preflight.ProbePrefix,
	}
	pfRec, pfErr := preflight.Crawl(ctx, prov, matcher.Prefixes(), pfSpec)
	if err := writer.WritePreflight(ctx, pfRec); err != nil {
		observability.CLILogger.Warn("Failed to write preflight record", zap.Error(err))
	}
	if pfErr != nil {
		observability.CLILogger.Error("Preflight failed", zap.Error(pfErr))
		return exitError(foundry.ExitExternalServiceUnavailable, "Preflight failed", pfErr)
	}

	// Create crawler config
	cfg := crawler.Config{
		Concurrency:   m.Crawl.Concurrency,
		RateLimit:     m.Crawl.RateLimit,
		ProgressEvery: m.Crawl.ProgressEvery,
	}

	// Create and run crawler
	c := crawler.New(prov, matcher, writer, jobID, cfg)
	if filter != nil {
		c.WithFilter(filter)
	}

	observability.CLILogger.Info("Starting crawl",
		zap.String("job_id", jobID),
		zap.String("bucket", m.Connection.Bucket),
		zap.Int("concurrency", cfg.Concurrency))

	summary, err := c.Run(ctx)
	if err != nil {
		if ctx.Err() != nil {
			observability.CLILogger.Warn("Crawl cancelled",
				zap.String("job_id", jobID),
				zap.Int64("objects_matched", summary.ObjectsMatched))
			return exitError(foundry.ExitSignalInt, "Crawl cancelled", err)
		}
		observability.CLILogger.Error("Crawl failed",
			zap.String("job_id", jobID),
			zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Crawl failed", err)
	}

	observability.CLILogger.Info("Crawl completed",
		zap.String("job_id", jobID),
		zap.Int64("objects_listed", summary.ObjectsListed),
		zap.Int64("objects_matched", summary.ObjectsMatched),
		zap.Int64("bytes_total", summary.BytesTotal),
		zap.Duration("duration", summary.Duration))

	return nil
}

func buildCrawlFilter(m *manifest.Manifest) (*match.CompositeFilter, error) {
	if m.Match.Filters == nil {
		return nil, nil
	}

	cfg := &match.FilterConfig{
		KeyRegex:    m.Match.Filters.KeyRegex,
		ContentType: m.Match.Filters.ContentType,
	}

	if m.Match.Filters.Size != nil {
		cfg.Size = &match.SizeFilterConfig{
			Min: m.Match.Filters.Size.Min,
			Max: m.Match.Filters.Size.Max,
		}
	}

	if m.Match.Filters.Modified != nil {
		cfg.Modified = &match.DateFilterConfig{
			After:  m.Match.Filters.Modified.After,
			Before: m.Match.Filters.Modified.Before,
		}
	}

	return match.NewFilterFromConfig(cfg)
}

// createProvider creates a storage provider from manifest configuration.
func createProvider(ctx context.Context, m *manifest.Manifest) (*s3.Provider, error) {
	cfg := s3.Config{
		Bucket:   m.Connection.Bucket,
		Region:   m.Connection.Region,
		Endpoint: m.Connection.Endpoint,
		Profile:  m.Connection.Profile,
		// Force path-style URLs when custom endpoint is set.
		// S3-compatible services (moto, MinIO, etc.) require this.
		ForcePathStyle: m.Connection.Endpoint != "",
	}
	return s3.New(ctx, cfg)
}

// createWriter creates an output writer from manifest configuration.
// Returns the writer, a cleanup function, and any error.
func createWriter(m *manifest.Manifest, jobID string) (output.Writer, func(), error) {
	dest := m.Output.Destination
	provider := m.Connection.Provider

	// Parse destination
	if dest == "" || dest == "stdout" {
		w := output.NewJSONLWriter(os.Stdout, jobID, provider)
		return w, func() { _ = w.Close() }, nil
	}

	// Handle file: prefix
	path := dest
	if strings.HasPrefix(dest, "file:") {
		path = strings.TrimPrefix(dest, "file:")
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create output file %s: %w", path, err)
	}

	w := output.NewJSONLWriter(f, jobID, provider)
	cleanup := func() {
		_ = w.Close()
		_ = f.Close()
	}
	return w, cleanup, nil
}

// exitError creates an error that will cause the CLI to exit with the given code.
func exitError(code int, message string, err error) error {
	return fmt.Errorf("%s: %w (exit code %d)", message, err, code)
}
