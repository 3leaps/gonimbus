package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
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
	crawlEmit          string
	crawlSelectionSum  string
	crawlMinObjects    int64
	crawlMaxBytes      int64
)

const (
	crawlEmitObject       = "object"
	crawlEmitReflowInput  = "reflow-input"
	crawlReflowInputType  = "gonimbus.reflow.input.v1"
	crawlSelectionSumType = "gonimbus.selection.summary.v1"
)

func init() {
	rootCmd.AddCommand(crawlCmd)

	crawlCmd.Flags().StringVarP(&crawlJobPath, "job", "j", "", "Path to job manifest (required)")
	crawlCmd.Flags().StringVarP(&crawlOutput, "output", "o", "", "Override output destination")
	crawlCmd.Flags().BoolVarP(&crawlQuiet, "quiet", "q", false, "Suppress progress records")
	crawlCmd.Flags().BoolVar(&crawlDryRun, "dry-run", false, "Validate manifest and show plan without executing")
	crawlCmd.Flags().BoolVar(&crawlPlan, "plan", false, "Alias for --dry-run")
	crawlCmd.Flags().StringVar(&crawlPreflightMode, "preflight", "", "Override preflight mode (plan-only|read-safe|write-probe)")
	crawlCmd.Flags().StringVar(&crawlEmit, "emit", crawlEmitObject, "Output mode: object|reflow-input")
	crawlCmd.Flags().StringVar(&crawlSelectionSum, "selection-summary", "", "Write gonimbus.selection.summary.v1 JSONL side output for --emit reflow-input")
	crawlCmd.Flags().Int64Var(&crawlMinObjects, "min-objects", 0, "Fail before streaming reflow input if selected object count is below this value")
	crawlCmd.Flags().Int64Var(&crawlMaxBytes, "max-bytes", 0, "Fail before streaming reflow input if selected bytes exceed this value")

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
	if err := validateCrawlEmitFlags(); err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid crawl output mode", err)
	}

	// Plan mode: show plan and exit
	if crawlPlan || crawlDryRun {
		return showCrawlPlan(m)
	}

	// Execute crawl
	return executeCrawl(ctx, m)
}

func validateCrawlEmitFlags() error {
	switch crawlEmit {
	case "", crawlEmitObject:
		crawlEmit = crawlEmitObject
	case crawlEmitReflowInput:
	default:
		return fmt.Errorf("--emit must be one of: object, reflow-input")
	}
	if crawlMinObjects < 0 {
		return fmt.Errorf("--min-objects must be >= 0")
	}
	if crawlMaxBytes < 0 {
		return fmt.Errorf("--max-bytes must be >= 0")
	}
	if crawlEmit != crawlEmitReflowInput {
		if crawlSelectionSum != "" {
			return fmt.Errorf("--selection-summary requires --emit reflow-input")
		}
		if crawlMinObjects > 0 {
			return fmt.Errorf("--min-objects requires --emit reflow-input")
		}
		if crawlMaxBytes > 0 {
			return fmt.Errorf("--max-bytes requires --emit reflow-input")
		}
	}
	return nil
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
	if IsReadOnly() && preflight.Mode(m.Crawl.Preflight.Mode) == preflight.ModeWriteProbe {
		return exitError(foundry.ExitInvalidArgument, "readonly mode enabled: refusing write-probe preflight", fmt.Errorf("set crawl.preflight.mode=read-safe or disable --readonly"))
	}

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

	if crawlEmit == crawlEmitReflowInput {
		return executeCrawlReflowInput(ctx, m, prov, matcher, filter, writer, jobID)
	}

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

type crawlSelectionSummary struct {
	ObjectsSelected int64  `json:"objects_selected"`
	BytesTotal      int64  `json:"bytes_total"`
	MinObjects      int64  `json:"min_objects,omitempty"`
	MaxBytes        int64  `json:"max_bytes,omitempty"`
	Status          string `json:"status"`
	Reason          string `json:"reason,omitempty"`
}

func executeCrawlReflowInput(ctx context.Context, m *manifest.Manifest, prov provider.Provider, matcher *match.Matcher, filter *match.CompositeFilter, writer output.Writer, jobID string) error {
	spool, err := os.CreateTemp("", "gonimbus-selection-*.jsonl")
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to create selection spool", err)
	}
	spoolPath := spool.Name()
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spoolPath)
	}()

	summary, err := spoolCrawlReflowSelection(ctx, m, prov, matcher, filter, spool)
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Crawl selection failed", err)
	}

	summary.Status = "ok"
	if crawlMinObjects > 0 && summary.ObjectsSelected < crawlMinObjects {
		summary.Status = "failed"
		summary.Reason = "min_objects"
		_ = writeCrawlSelectionSummary(ctx, m, jobID, summary)
		return exitError(foundry.ExitInvalidArgument, "Crawl selection below minimum object threshold", fmt.Errorf("selected_objects=%d min_objects=%d", summary.ObjectsSelected, crawlMinObjects))
	}
	if crawlMaxBytes > 0 && summary.BytesTotal > crawlMaxBytes {
		summary.Status = "failed"
		summary.Reason = "max_bytes"
		_ = writeCrawlSelectionSummary(ctx, m, jobID, summary)
		return exitError(foundry.ExitInvalidArgument, "Crawl selection exceeds maximum byte threshold", fmt.Errorf("selected_bytes=%d max_bytes=%d", summary.BytesTotal, crawlMaxBytes))
	}
	if err := writeCrawlSelectionSummary(ctx, m, jobID, summary); err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to write selection summary", err)
	}

	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return exitError(foundry.ExitFileReadError, "Failed to read selection spool", err)
	}
	scanner := bufio.NewScanner(spool)
	for scanner.Scan() {
		var rec reflowInputRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return exitError(foundry.ExitFailure, "Failed to decode selection spool", err)
		}
		jsonl, ok := writer.(*output.JSONLWriter)
		if !ok {
			return exitError(foundry.ExitFailure, "Unsupported crawl writer", fmt.Errorf("writer does not support custom records"))
		}
		if err := jsonl.WriteAny(ctx, crawlReflowInputType, &rec); err != nil {
			return exitError(foundry.ExitFileWriteError, "Failed to write reflow input", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return exitError(foundry.ExitFileReadError, "Failed to read selection spool", err)
	}
	return nil
}

func spoolCrawlReflowSelection(ctx context.Context, m *manifest.Manifest, prov provider.Provider, matcher *match.Matcher, filter *match.CompositeFilter, spool io.Writer) (crawlSelectionSummary, error) {
	summary := crawlSelectionSummary{MinObjects: crawlMinObjects, MaxBytes: crawlMaxBytes}
	enc := json.NewEncoder(spool)
	sourceBaseDir := strings.TrimSpace(m.Connection.BaseDir)
	if m.Connection.Provider == string(provider.ProviderFile) {
		resolved, err := filepath.EvalSymlinks(sourceBaseDir)
		if err != nil {
			return summary, fmt.Errorf("resolve file source base_dir: %w", err)
		}
		sourceBaseDir = filepath.Clean(resolved)
	}
	for _, prefix := range matcher.Prefixes() {
		var token string
		for {
			res, err := prov.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
			if err != nil {
				return summary, err
			}
			for _, obj := range res.Objects {
				if !matcher.Match(obj.Key) {
					continue
				}
				if filter != nil && !filter.Match(&obj) {
					continue
				}
				rec := crawlReflowRecordForObject(m, sourceBaseDir, obj)
				if err := enc.Encode(&rec); err != nil {
					return summary, err
				}
				summary.ObjectsSelected++
				summary.BytesTotal += obj.Size
			}
			if !res.IsTruncated {
				break
			}
			token = res.ContinuationToken
			if token == "" {
				break
			}
			if err := ctx.Err(); err != nil {
				return summary, err
			}
		}
	}
	return summary, nil
}

func crawlReflowRecordForObject(m *manifest.Manifest, sourceBaseDir string, obj provider.ObjectSummary) reflowInputRecord {
	key := strings.TrimPrefix(obj.Key, "/")
	sourceURI := fmt.Sprintf("%s://%s/%s", m.Connection.Provider, m.Connection.Bucket, key)
	if m.Connection.Provider == string(provider.ProviderFile) {
		sourceURI = fileURI(filepath.Join(sourceBaseDir, filepath.FromSlash(key)))
	}
	var lastMod *time.Time
	if !obj.LastModified.IsZero() {
		t := obj.LastModified.UTC()
		lastMod = &t
	}
	return reflowInputRecord{
		SourceURI:     sourceURI,
		SourceKey:     key,
		SourceETag:    obj.ETag,
		SourceSize:    obj.Size,
		SourceLastMod: lastMod,
		DestRelKey:    key,
	}
}

func writeCrawlSelectionSummary(ctx context.Context, m *manifest.Manifest, jobID string, summary crawlSelectionSummary) error {
	if crawlSelectionSum == "" {
		return nil
	}
	path := strings.TrimPrefix(crawlSelectionSum, "file:")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := output.NewJSONLWriter(f, jobID, m.Connection.Provider)
	defer func() { _ = w.Close() }()
	return w.WriteAny(ctx, crawlSelectionSumType, &summary)
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
func createProvider(ctx context.Context, m *manifest.Manifest) (provider.Provider, error) {
	src := &uri.ObjectURI{
		Provider: m.Connection.Provider,
		Bucket:   m.Connection.Bucket,
	}
	opts := providerdispatch.SourceOptions{
		Command: "crawl",
		S3: providerdispatch.S3Options{
			Region:         m.Connection.Region,
			Endpoint:       m.Connection.Endpoint,
			Profile:        m.Connection.Profile,
			ForcePathStyle: m.Connection.Endpoint != "",
		},
	}
	if m.Connection.Provider == string(provider.ProviderFile) {
		baseDir := filepath.Clean(m.Connection.BaseDir)
		src.Bucket = "local"
		src.Key = baseDir
		opts.FileBaseDir = baseDir
	}
	return providerdispatch.NewSource(ctx, src, opts)
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
