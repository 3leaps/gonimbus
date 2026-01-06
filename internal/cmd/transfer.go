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
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

var transferCmd = &cobra.Command{
	Use:   "transfer",
	Short: "Run a transfer job from manifest",
	Long: `Run a transfer (copy/move) job as defined in a YAML or JSON manifest file.

This is the multi-step engine for bucket-to-bucket operations. Preflight is run
first to fail fast on missing permissions.

Examples:
  gonimbus transfer --job transfer.yaml
  gonimbus transfer --job transfer.yaml --plan
  gonimbus transfer --job transfer.yaml --dry-run`,
	RunE: runTransfer,
}

var (
	transferJobPath string
	transferOutput  string
	transferPlan    bool
	transferDryRun  bool
)

func init() {
	rootCmd.AddCommand(transferCmd)

	transferCmd.Flags().StringVarP(&transferJobPath, "job", "j", "", "Path to transfer manifest (required)")
	transferCmd.Flags().StringVarP(&transferOutput, "output", "o", "", "Override output destination")
	transferCmd.Flags().BoolVar(&transferPlan, "plan", false, "Validate manifest and show plan without executing")
	transferCmd.Flags().BoolVar(&transferDryRun, "dry-run", false, "Run preflight and show plan without executing")

	_ = transferCmd.MarkFlagRequired("job")
}

func runTransfer(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	data, err := os.ReadFile(transferJobPath)
	if err != nil {
		observability.CLILogger.Error("Failed to read transfer manifest", zap.String("path", transferJobPath), zap.Error(err))
		return exitError(foundry.ExitFileNotFound, "Failed to read transfer manifest", err)
	}

	m, err := manifest.LoadTransferFromBytes(data, transferJobPath)
	if err != nil {
		observability.CLILogger.Error("Invalid transfer manifest", zap.String("path", transferJobPath), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid transfer manifest", err)
	}

	if transferOutput != "" {
		m.Output.Destination = transferOutput
	}

	if transferPlan {
		return showTransferPlan(m)
	}

	return executeTransfer(ctx, m, transferDryRun)
}

func showTransferPlan(m *manifest.TransferManifest) error {
	fmt.Println("=== Transfer Plan ===")
	fmt.Println()
	fmt.Printf("Source:   %s://%s\n", m.Source.Provider, m.Source.Bucket)
	fmt.Printf("Target:   %s://%s\n", m.Target.Provider, m.Target.Bucket)
	fmt.Printf("Mode:     %s\n", m.Transfer.Mode)
	fmt.Printf("Includes: %s\n", strings.Join(m.Match.Includes, ", "))
	if len(m.Match.Excludes) > 0 {
		fmt.Printf("Excludes: %s\n", strings.Join(m.Match.Excludes, ", "))
	}
	fmt.Printf("Workers:  %d\n", m.Transfer.Concurrency)
	fmt.Printf("OnExists: %s\n", m.Transfer.OnExists)
	if m.Transfer.Sharding.Enabled {
		fmt.Printf("Sharding: enabled=true depth=%d max_shards=%d list_concurrency=%d delimiter=%q\n", m.Transfer.Sharding.Depth, m.Transfer.Sharding.MaxShards, m.Transfer.Sharding.ListConcurrency, m.Transfer.Sharding.Delimiter)
	}
	fmt.Printf("Dedup:    enabled=%v strategy=%s\n", m.Transfer.Dedup.DedupEnabled(), m.Transfer.Dedup.Strategy)
	fmt.Printf("Preflight: mode=%s probe_strategy=%s probe_prefix=%s\n", m.Transfer.Preflight.Mode, m.Transfer.Preflight.ProbeStrategy, m.Transfer.Preflight.ProbePrefix)
	if m.Transfer.PathTemplate != "" {
		fmt.Printf("Template: %s\n", m.Transfer.PathTemplate)
	}
	fmt.Printf("Output:   %s\n", m.Output.Destination)
	fmt.Println()
	fmt.Println("Manifest validated successfully. Remove --plan to execute.")
	return nil
}

func executeTransfer(ctx context.Context, m *manifest.TransferManifest, dryRun bool) error {
	jobID := uuid.New().String()

	writer, cleanup, err := createTransferWriter(m, jobID)
	if err != nil {
		observability.CLILogger.Error("Failed to create writer", zap.Error(err))
		return exitError(foundry.ExitFileWriteError, "Failed to create output", err)
	}
	defer cleanup()

	matcher, err := match.New(match.Config{Includes: m.Match.Includes, Excludes: m.Match.Excludes, IncludeHidden: m.Match.IncludeHidden})
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid match patterns", err)
	}

	if m.Transfer.PathTemplate != "" {
		if _, err := transfer.CompilePathTemplate(m.Transfer.PathTemplate); err != nil {
			return exitError(foundry.ExitInvalidArgument, "Invalid path_template", err)
		}
	}

	// In plan-only mode we never hit provider endpoints. Use --plan, or use
	// --dry-run with plan-only to validate config without side effects.
	if m.Transfer.Preflight.Mode == string(preflight.ModePlanOnly) {
		if dryRun {
			return showTransferPlan(m)
		}
		return exitError(foundry.ExitInvalidArgument, "preflight.mode=plan-only cannot execute transfers", fmt.Errorf("set transfer.preflight.mode to read-safe or write-probe"))
	}

	srcProv, err := createTransferProvider(ctx, m.Source)
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to source", err)
	}
	defer func() { _ = srcProv.Close() }()

	dstProv, err := createTransferProvider(ctx, m.Target)
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to target", err)
	}
	defer func() { _ = dstProv.Close() }()
	// Preflight: fail fast before heavy listing.
	spec := preflight.Spec{
		Mode:          preflight.Mode(m.Transfer.Preflight.Mode),
		ProbeStrategy: preflight.ProbeStrategy(m.Transfer.Preflight.ProbeStrategy),
		ProbePrefix:   m.Transfer.Preflight.ProbePrefix,
	}

	opts := preflight.TransferOptions{
		RequireSourceRead:       true,
		RequireTargetHead:       m.Transfer.OnExists != "overwrite",
		RequireTargetWriteProbe: spec.Mode == preflight.ModeWriteProbe,
	}

	pfRec, pfErr := preflight.Transfer(ctx, srcProv, dstProv, matcher.Prefixes(), spec, opts)
	_ = writer.WritePreflight(ctx, pfRec)
	if pfErr != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Preflight failed", pfErr)
	}

	if dryRun {
		return showTransferPlan(m)
	}

	cfg := transfer.Config{
		Concurrency:  m.Transfer.Concurrency,
		OnExists:     m.Transfer.OnExists,
		Mode:         m.Transfer.Mode,
		PathTemplate: m.Transfer.PathTemplate,
		Sharding: transfer.ShardingConfig{
			Enabled:         m.Transfer.Sharding.Enabled,
			Depth:           m.Transfer.Sharding.Depth,
			MaxShards:       m.Transfer.Sharding.MaxShards,
			ListConcurrency: m.Transfer.Sharding.ListConcurrency,
			Delimiter:       m.Transfer.Sharding.Delimiter,
		},
		Dedup: transfer.DedupConfig{
			Enabled:  m.Transfer.Dedup.DedupEnabled(),
			Strategy: m.Transfer.Dedup.Strategy,
		},
	}

	tx := transfer.New(srcProv, dstProv, matcher, writer, jobID, cfg)
	sum, err := tx.Run(ctx)
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Transfer failed", err)
	}

	_ = writer.WriteSummary(ctx, &output.SummaryRecord{
		ObjectsFound:   sum.ObjectsListed,
		ObjectsMatched: sum.ObjectsTransferred,
		BytesTotal:     sum.BytesTransferred,
		Duration:       sum.Duration,
		DurationHuman:  sum.Duration.Round(0).String(),
		Errors:         sum.Errors,
	})

	return nil
}

func createTransferWriter(m *manifest.TransferManifest, jobID string) (output.Writer, func(), error) {
	dest := m.Output.Destination
	providerName := m.Source.Provider

	if dest == "" || dest == "stdout" {
		w := output.NewJSONLWriter(os.Stdout, jobID, providerName)
		return w, func() { _ = w.Close() }, nil
	}

	path := dest
	if strings.HasPrefix(dest, "file:") {
		path = strings.TrimPrefix(dest, "file:")
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create output file %s: %w", path, err)
	}

	w := output.NewJSONLWriter(f, jobID, providerName)
	cleanup := func() {
		_ = w.Close()
		_ = f.Close()
	}
	return w, cleanup, nil
}

func createTransferProvider(ctx context.Context, conn manifest.ConnectionConfig) (*s3.Provider, error) {
	cfg := s3.Config{
		Bucket:   conn.Bucket,
		Region:   conn.Region,
		Endpoint: conn.Endpoint,
		Profile:  conn.Profile,
		// Force path-style URLs when custom endpoint is set.
		ForcePathStyle: conn.Endpoint != "",
	}
	return s3.New(ctx, cfg)
}
