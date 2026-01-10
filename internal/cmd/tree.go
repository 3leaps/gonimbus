package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var treeCmd = &cobra.Command{
	Use:   "tree <uri>",
	Short: "Direct-only prefix summary (safe tree)",
	Long: `Summarize an object-store prefix as a directory-like tree.

Wave 1 is direct-only (non-recursive): it reports counts/bytes for objects directly
under the given prefix and counts the immediate child prefixes (common prefixes).

This command is intended to be safe on massive buckets; it does not traverse into
child prefixes.

Examples:
  gonimbus tree s3://bucket/prefix/
  gonimbus tree s3://bucket/prefix/ --max-objects 100000 --max-pages 500
  gonimbus tree s3://bucket/prefix/ --output table`,
	Args: cobra.ExactArgs(1),
	RunE: runTree,
}

var (
	treeRegion     string
	treeProfile    string
	treeEndpoint   string
	treeDelimiter  string
	treeMaxObjects int
	treeMaxPages   int
	treeOutput     string
)

func init() {
	rootCmd.AddCommand(treeCmd)

	treeCmd.Flags().StringVarP(&treeRegion, "region", "r", "", "AWS region")
	treeCmd.Flags().StringVarP(&treeProfile, "profile", "p", "", "AWS profile")
	treeCmd.Flags().StringVar(&treeEndpoint, "endpoint", "", "Custom S3 endpoint")
	treeCmd.Flags().StringVar(&treeDelimiter, "delimiter", "/", "Delimiter for common prefixes")
	treeCmd.Flags().IntVar(&treeMaxObjects, "max-objects", 2_000_000, "Max direct objects to count before truncating")
	treeCmd.Flags().IntVar(&treeMaxPages, "max-pages", 10_000, "Max listing pages before truncating")
	treeCmd.Flags().StringVar(&treeOutput, "output", "jsonl", "Output format (jsonl|table)")
}

func runTree(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	uri := args[0]

	parsed, err := ParseURI(uri)
	if err != nil {
		observability.CLILogger.Error("Invalid URI", zap.String("uri", uri), zap.Error(err))
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return exitError(foundry.ExitInvalidArgument, "Unsupported provider for tree", fmt.Errorf("provider %q is not supported", parsed.Provider))
	}
	if parsed.IsPattern() {
		return exitError(foundry.ExitInvalidArgument, "tree requires a prefix URI (no glob pattern)", fmt.Errorf("patterns are not supported in wave 1"))
	}
	if !parsed.IsPrefix() {
		return exitError(foundry.ExitInvalidArgument, "tree requires a prefix URI", fmt.Errorf("append '/' to treat the URI as a prefix"))
	}

	prov, err := createTreeProvider(ctx, parsed)
	if err != nil {
		observability.CLILogger.Error("Failed to create provider", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	defer func() { _ = prov.Close() }()

	lister, ok := interface{}(prov).(provider.DelimiterLister)
	if !ok {
		return exitError(foundry.ExitInvalidArgument, "Provider does not support delimiter listing", fmt.Errorf("missing delimiter listing support"))
	}

	rec, err := summarizeDirectPrefix(ctx, lister, parsed.Key, treeDelimiter, treeMaxObjects, treeMaxPages)
	if err != nil {
		observability.CLILogger.Error("Failed to summarize prefix", zap.Error(err))
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to summarize prefix", err)
	}

	if treeOutput == "table" {
		return outputTreeTable(rec)
	}
	if treeOutput != "jsonl" {
		return exitError(foundry.ExitInvalidArgument, "Invalid --output value", fmt.Errorf("expected jsonl or table"))
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(os.Stdout, jobID, parsed.Provider)
	if err := w.WritePrefix(ctx, rec); err != nil {
		return err
	}
	if err := w.WriteSummary(ctx, &output.SummaryRecord{
		ObjectsFound:   rec.ObjectsDirect,
		ObjectsMatched: rec.ObjectsDirect,
		BytesTotal:     rec.BytesDirect,
		Prefixes:       []string{rec.Prefix},
	}); err != nil {
		return err
	}
	return nil
}

func summarizeDirectPrefix(ctx context.Context, lister provider.DelimiterLister, prefix, delimiter string, maxObjects, maxPages int) (*output.PrefixRecord, error) {
	var (
		objects         int64
		bytes           int64
		commonPrefixes  int64
		pages           int64
		token           string
		truncated       bool
		truncatedReason string
	)

	for {
		if maxPages > 0 && int(pages) >= maxPages {
			truncated = true
			truncatedReason = "max-pages"
			break
		}

		res, err := lister.ListWithDelimiter(ctx, provider.ListWithDelimiterOptions{
			Prefix:            prefix,
			Delimiter:         delimiter,
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		pages++
		commonPrefixes += int64(len(res.CommonPrefixes))

		for _, obj := range res.Objects {
			if maxObjects > 0 && int(objects) >= maxObjects {
				truncated = true
				truncatedReason = "max-objects"
				break
			}
			objects++
			bytes += obj.Size
		}

		if truncated {
			break
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}

	return &output.PrefixRecord{
		Prefix:          prefix,
		Delimiter:       delimiter,
		ObjectsDirect:   objects,
		BytesDirect:     bytes,
		CommonPrefixes:  commonPrefixes,
		Pages:           pages,
		Truncated:       truncated,
		TruncatedReason: truncatedReason,
	}, nil
}

func createTreeProvider(ctx context.Context, uri *ObjectURI) (*s3.Provider, error) {
	cfg := s3.Config{
		Bucket:         uri.Bucket,
		Region:         treeRegion,
		Endpoint:       treeEndpoint,
		Profile:        treeProfile,
		ForcePathStyle: treeEndpoint != "",
	}
	return s3.New(ctx, cfg)
}

func outputTreeTable(rec *output.PrefixRecord) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PREFIX\tOBJECTS\tBYTES\tCOMMON_PREFIXES\tPAGES\tTRUNCATED"); err != nil {
		return err
	}

	trunc := "no"
	if rec.Truncated {
		trunc = "yes"
		if rec.TruncatedReason != "" {
			trunc = trunc + " (" + rec.TruncatedReason + ")"
		}
	}

	if _, err := fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%s\n",
		rec.Prefix,
		rec.ObjectsDirect,
		formatSize(rec.BytesDirect),
		rec.CommonPrefixes,
		rec.Pages,
		trunc,
	); err != nil {
		return err
	}

	return tw.Flush()
}
