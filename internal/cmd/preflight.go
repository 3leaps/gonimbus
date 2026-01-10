package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var preflightCmd = &cobra.Command{
	Use:   "preflight",
	Short: "Probe permissions and capabilities",
	Long: `Probe permissions and capabilities for cloud object store operations.

This command is intended for dogfooding and operational validation before running
long jobs. It emits a JSONL preflight record (gonimbus.preflight.v1).

Examples:
  # Plan-only: no provider calls
  gonimbus preflight crawl s3://bucket/data/**/*.parquet --mode plan-only

  # Read-safe: minimal non-mutating calls
  gonimbus preflight crawl s3://bucket/data/**/*.parquet --mode read-safe

  # Write-probe: minimal opt-in side effects under probe prefix
  gonimbus preflight write s3://bucket/ --mode write-probe --probe-strategy multipart-abort

  # Safety latch: disable all provider-side mutations
  gonimbus preflight crawl s3://bucket/data/**/*.parquet --mode read-safe --readonly`,
}

var preflightCrawlCmd = &cobra.Command{
	Use:   "crawl <uri>",
	Short: "Preflight checks for crawl/list/head",
	Args:  cobra.ExactArgs(1),
	RunE:  runPreflightCrawl,
}

var preflightWriteCmd = &cobra.Command{
	Use:   "write <uri>",
	Short: "Preflight write-probe checks for target buckets",
	Args:  cobra.ExactArgs(1),
	RunE:  runPreflightWrite,
}

var (
	preflightRegion        string
	preflightProfile       string
	preflightEndpoint      string
	preflightMode          string
	preflightProbeStrategy string
	preflightProbePrefix   string
)

func init() {
	rootCmd.AddCommand(preflightCmd)
	preflightCmd.AddCommand(preflightCrawlCmd)
	preflightCmd.AddCommand(preflightWriteCmd)

	preflightCmd.Long += "\n\nSafety:\n- --readonly (or GONIMBUS_READONLY=1) disables write-probe preflight and other provider-side mutations."

	for _, c := range []*cobra.Command{preflightCrawlCmd, preflightWriteCmd} {
		c.Flags().StringVarP(&preflightRegion, "region", "r", "", "AWS region")
		c.Flags().StringVarP(&preflightProfile, "profile", "p", "", "AWS profile")
		c.Flags().StringVar(&preflightEndpoint, "endpoint", "", "Custom S3 endpoint")
		c.Flags().StringVar(&preflightMode, "mode", "read-safe", "Preflight mode (plan-only|read-safe|write-probe)")
		c.Flags().StringVar(&preflightProbeStrategy, "probe-strategy", "multipart-abort", "Write probe strategy (multipart-abort|put-delete)")
		c.Flags().StringVar(&preflightProbePrefix, "probe-prefix", "_gonimbus/probe/", "Probe prefix for write probes")
	}
}

func runPreflightCrawl(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	uriStr := args[0]

	parsed, err := ParseURI(uriStr)
	if err != nil {
		observability.CLILogger.Error("Invalid URI", zap.String("uri", uriStr), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(os.Stdout, jobID, parsed.Provider)
	defer func() { _ = w.Close() }()

	// Plan-only should not create providers or hit endpoints.
	spec := preflight.Spec{
		Mode:          preflight.Mode(preflightMode),
		ProbeStrategy: preflight.ProbeStrategy(preflightProbeStrategy),
		ProbePrefix:   preflightProbePrefix,
	}
	switch spec.Mode {
	case preflight.ModePlanOnly, preflight.ModeReadSafe, preflight.ModeWriteProbe:
		// ok
	default:
		return exitError(foundry.ExitInvalidArgument, "Invalid --mode value", fmt.Errorf("unsupported preflight mode: %s", preflightMode))
	}
	if IsReadOnly() && spec.Mode == preflight.ModeWriteProbe {
		return exitError(foundry.ExitInvalidArgument, "readonly mode enabled: refusing write-probe preflight", fmt.Errorf("use --mode read-safe or disable --readonly"))
	}
	if spec.Mode == preflight.ModePlanOnly {
		rec := &output.PreflightRecord{
			Mode:          string(spec.Mode),
			ProbeStrategy: string(spec.ProbeStrategy),
			ProbePrefix:   spec.ProbePrefix,
			Results:       []output.PreflightCheckResult{},
		}
		return w.WritePreflight(ctx, rec)
	}

	prov, err := createPreflightProvider(ctx, parsed)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	defer func() { _ = prov.Close() }()

	prefixes := []string{parsed.Key}
	if parsed.IsPattern() {
		m, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			return exitError(foundry.ExitInvalidArgument, "Invalid match pattern", err)
		}
		prefixes = m.Prefixes()
	}

	rec, pfErr := preflight.Crawl(ctx, prov, prefixes, spec)
	if err := w.WritePreflight(ctx, rec); err != nil {
		return err
	}

	// For exact object URIs, also probe Head.
	if !parsed.IsPattern() && !parsed.IsPrefix() {
		_, err := prov.Head(ctx, parsed.Key)
		if err != nil {
			rec.Results = append(rec.Results, output.PreflightCheckResult{
				Capability: preflight.CapSourceHead,
				Allowed:    false,
				Method:     fmt.Sprintf("Head(key=%q)", parsed.Key),
				ErrorCode:  preflightErrorCode(err),
				Detail:     err.Error(),
			})
			_ = w.WritePreflight(ctx, rec)
			return exitError(foundry.ExitExternalServiceUnavailable, "Preflight head probe failed", err)
		}
		rec.Results = append(rec.Results, output.PreflightCheckResult{
			Capability: preflight.CapSourceHead,
			Allowed:    true,
			Method:     fmt.Sprintf("Head(key=%q)", parsed.Key),
		})
		_ = w.WritePreflight(ctx, rec)
	}

	if pfErr != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Preflight failed", pfErr)
	}

	return nil
}

func runPreflightWrite(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	uriStr := args[0]

	parsed, err := ParseURI(uriStr)
	if err != nil {
		observability.CLILogger.Error("Invalid URI", zap.String("uri", uriStr), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(os.Stdout, jobID, parsed.Provider)
	defer func() { _ = w.Close() }()

	spec := preflight.Spec{
		Mode:          preflight.Mode(preflightMode),
		ProbeStrategy: preflight.ProbeStrategy(preflightProbeStrategy),
		ProbePrefix:   preflightProbePrefix,
	}
	switch spec.Mode {
	case preflight.ModePlanOnly, preflight.ModeWriteProbe:
		// ok
	default:
		return exitError(foundry.ExitInvalidArgument, "Invalid --mode for preflight write", fmt.Errorf("use --mode write-probe or plan-only"))
	}
	if IsReadOnly() && spec.Mode == preflight.ModeWriteProbe {
		return exitError(foundry.ExitInvalidArgument, "readonly mode enabled: refusing write-probe preflight", fmt.Errorf("disable --readonly or unset GONIMBUS_READONLY"))
	}
	if spec.Mode == preflight.ModePlanOnly {
		rec := &output.PreflightRecord{
			Mode:          string(spec.Mode),
			ProbeStrategy: string(spec.ProbeStrategy),
			ProbePrefix:   spec.ProbePrefix,
			Results:       []output.PreflightCheckResult{},
		}
		return w.WritePreflight(ctx, rec)
	}

	prov, err := createPreflightProvider(ctx, parsed)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	defer func() { _ = prov.Close() }()

	rec, pfErr := preflight.WriteProbe(ctx, prov, spec)
	if err := w.WritePreflight(ctx, rec); err != nil {
		return err
	}
	if pfErr != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Write probe failed", pfErr)
	}
	return nil
}

func createPreflightProvider(ctx context.Context, uri *ObjectURI) (*s3.Provider, error) {
	cfg := s3.Config{
		Bucket:   uri.Bucket,
		Region:   preflightRegion,
		Endpoint: preflightEndpoint,
		Profile:  preflightProfile,
		// Force path-style URLs when custom endpoint is set.
		ForcePathStyle: preflightEndpoint != "",
	}
	return s3.New(ctx, cfg)
}

func preflightErrorCode(err error) string {
	switch {
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsBucketNotFound(err), provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	default:
		return output.ErrCodeInternal
	}
}
