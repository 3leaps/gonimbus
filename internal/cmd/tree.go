package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
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
	Short: "Prefix summary (tree/du building block)",
	Long: `Summarize an object-store prefix as a directory-like tree.

Wave 1 (default) is direct-only (non-recursive): it reports counts/bytes for objects
directly under the given prefix and counts the immediate child prefixes (common prefixes).

Wave 2 enables depth-limited traversal via --depth N.

Examples:
  gonimbus tree s3://bucket/prefix/
  gonimbus tree s3://bucket/prefix/ --max-objects 100000 --max-pages 500
  gonimbus tree s3://bucket/prefix/ --output table

  # Traverse two levels deep (bounded)
  gonimbus tree s3://bucket/prefix/ --depth 2 --max-prefixes 50000 --timeout 10m`,
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

	// Wave 2 traversal flags
	treeDepth         int
	treeMaxPrefixes   int
	treeTimeout       time.Duration
	treeParallel      int
	treeIncludes      []string
	treeExcludes      []string
	treeProgressEvery int
	treeNoProgress    bool
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

	treeCmd.Flags().IntVar(&treeDepth, "depth", 0, "Traversal depth (0=direct-only)")
	treeCmd.Flags().IntVar(&treeMaxPrefixes, "max-prefixes", 50_000, "Max prefixes to traverse before stopping (wave 2)")
	treeCmd.Flags().DurationVar(&treeTimeout, "timeout", 10*time.Minute, "Traversal timeout (wave 2)")
	treeCmd.Flags().IntVar(&treeParallel, "parallel", 8, "Max concurrent prefix listings (wave 2)")
	treeCmd.Flags().StringArrayVar(&treeIncludes, "include", nil, "Include glob pattern for traversal scope (repeatable; wave 2)")
	treeCmd.Flags().StringArrayVar(&treeExcludes, "exclude", nil, "Exclude glob pattern for traversal scope (repeatable; wave 2)")
	treeCmd.Flags().IntVar(&treeProgressEvery, "progress-every", 500, "Emit progress logs every N prefixes (0=disable; wave 2)")
	treeCmd.Flags().BoolVar(&treeNoProgress, "no-progress", false, "Disable progress logs (wave 2)")
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
		return exitError(foundry.ExitInvalidArgument, "tree requires a prefix URI (no glob pattern)", fmt.Errorf("patterns are not supported; append '/' and use --include/--exclude for traversal scoping"))
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

	if treeDepth <= 0 {
		start := time.Now()
		rec, _, err := summarizeDirectPrefix(ctx, lister, parsed.Key, treeDelimiter, treeMaxObjects, treeMaxPages, false)
		if err != nil {
			observability.CLILogger.Error("Failed to summarize prefix", zap.Error(err))
			return exitError(foundry.ExitExternalServiceUnavailable, "Failed to summarize prefix", err)
		}
		rec.Depth = 0

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

		dur := time.Since(start)
		if err := w.WriteSummary(ctx, &output.SummaryRecord{
			ObjectsFound:   rec.ObjectsDirect,
			ObjectsMatched: rec.ObjectsDirect,
			BytesTotal:     rec.BytesDirect,
			Duration:       dur,
			DurationHuman:  formatDuration(dur),
			Prefixes:       []string{rec.Prefix},
		}); err != nil {
			return err
		}
		return nil
	}

	return runTreeTraversal(ctx, parsed, lister)
}

func runTreeTraversal(ctx context.Context, uri *ObjectURI, lister provider.DelimiterLister) error {
	if treeParallel < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --parallel value", fmt.Errorf("parallel must be >= 1"))
	}
	if treeMaxPrefixes < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --max-prefixes value", fmt.Errorf("max-prefixes must be >= 1"))
	}

	ctx2 := ctx
	cancel := func() {}
	if treeTimeout > 0 {
		ctx2, cancel = context.WithTimeout(ctx, treeTimeout)
	}
	defer cancel()

	allowPrefix, err := buildTreeScopeFilter(treeIncludes, treeExcludes)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid include/exclude patterns", err)
	}

	start := time.Now()
	jobID := uuid.New().String()

	var (
		totalObjects int64
		totalBytes   int64
		partial      atomic.Bool
		partialMu    sync.Mutex
		reasonsSet   = map[string]struct{}{}
		pagesTotal   atomic.Int64
		processed    atomic.Int64
		discovered   atomic.Int64
	)

	markPartial := func(reason string) {
		partial.Store(true)
		partialMu.Lock()
		reasonsSet[reason] = struct{}{}
		partialMu.Unlock()
	}

	if treeOutput == "table" {
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if err := outputTreeTableHeader(tw); err != nil {
			return err
		}

		err = traversePrefixes(ctx2, lister, uri.Key, treeDelimiter, treeDepth, treeMaxObjects, treeMaxPages, treeMaxPrefixes, treeParallel, allowPrefix,
			func(rec *output.PrefixRecord) error {

				if err := outputTreeTableRow(tw, rec); err != nil {
					return err
				}

				pagesTotal.Add(rec.Pages)
				processed.Add(1)
				atomic.AddInt64(&totalObjects, rec.ObjectsDirect)
				atomic.AddInt64(&totalBytes, rec.BytesDirect)

				if rec.Truncated {
					markPartial(rec.TruncatedReason)
				}
				return nil
			},
			func(newPrefixes int) {
				discovered.Add(int64(newPrefixes))
			},
			markPartial,
		)
		if err != nil {
			if errorsIsContext(err) {
				markPartial("timeout")
			}
			return err
		}

		if err := tw.Flush(); err != nil {
			return err
		}

		dur := time.Since(start)
		if partial.Load() {
			reasons := sortedKeys(reasonsSet)
			fmt.Fprintf(os.Stderr, "tree: partial results (%s)\n", strings.Join(reasons, ","))
		}
		fmt.Fprintf(os.Stderr, "tree: processed=%d prefixes, objects=%d, bytes=%s, duration=%s\n",
			processed.Load(), totalObjects, formatSize(totalBytes), formatDuration(dur),
		)
		return nil
	}

	if treeOutput != "jsonl" {
		return exitError(foundry.ExitInvalidArgument, "Invalid --output value", fmt.Errorf("expected jsonl or table"))
	}

	w := output.NewJSONLWriter(os.Stdout, jobID, uri.Provider)

	lastProgress := time.Now()
	progressEvery := treeProgressEvery
	if treeNoProgress {
		progressEvery = 0
	}

	err = traversePrefixes(ctx2, lister, uri.Key, treeDelimiter, treeDepth, treeMaxObjects, treeMaxPages, treeMaxPrefixes, treeParallel, allowPrefix,
		func(rec *output.PrefixRecord) error {
			if err := w.WritePrefix(ctx2, rec); err != nil {
				return err
			}

			pagesTotal.Add(rec.Pages)
			processed.Add(1)
			atomic.AddInt64(&totalObjects, rec.ObjectsDirect)
			atomic.AddInt64(&totalBytes, rec.BytesDirect)

			if rec.Truncated {
				markPartial(rec.TruncatedReason)
			}

			if progressEvery > 0 {
				p := processed.Load()
				if p%int64(progressEvery) == 0 || time.Since(lastProgress) > 10*time.Second {
					lastProgress = time.Now()
					observability.CLILogger.Info("Tree progress",
						zap.String("prefix", rec.Prefix),
						zap.Int64("prefixes_processed", p),
						zap.Int64("prefixes_discovered", discovered.Load()),
						zap.Int64("pages", pagesTotal.Load()),
					)
				}
			}

			return nil
		},
		func(newPrefixes int) {
			discovered.Add(int64(newPrefixes))
		},
		markPartial,
	)
	if err != nil {
		if errorsIsContext(err) {
			markPartial("timeout")
		}
		return err
	}

	dur := time.Since(start)

	if partial.Load() {
		reasons := sortedKeys(reasonsSet)
		_ = w.WriteError(ctx2, &output.ErrorRecord{
			Code:    output.ErrCodeInternal,
			Message: "tree traversal produced partial results due to safety limits",
			Prefix:  uri.Key,
			Details: map[string]any{"reasons": reasons},
		})
	}

	sum := &output.SummaryRecord{
		ObjectsFound:   totalObjects,
		ObjectsMatched: totalObjects,
		BytesTotal:     totalBytes,
		Duration:       dur,
		DurationHuman:  formatDuration(dur),
		Errors:         0,
		Prefixes:       []string{uri.Key},
	}
	if partial.Load() {
		sum.Errors = 1
	}

	return w.WriteSummary(ctx2, sum)
}

func buildTreeScopeFilter(includes, excludes []string) (func(prefix string) bool, error) {
	if len(includes) == 0 && len(excludes) == 0 {
		return func(prefix string) bool { return true }, nil
	}

	cfg := match.Config{
		Includes:      includes,
		Excludes:      excludes,
		IncludeHidden: true,
	}
	if len(cfg.Includes) == 0 {
		cfg.Includes = []string{"**"}
	}
	m, err := match.New(cfg)
	if err != nil {
		return nil, err
	}

	return func(prefix string) bool {
		return m.Match(prefix)
	}, nil

}

func traversePrefixes(
	ctx context.Context,
	lister provider.DelimiterLister,
	rootPrefix string,
	delimiter string,
	maxDepth int,
	maxObjects int,
	maxPages int,
	maxPrefixes int,
	parallel int,
	allowPrefix func(prefix string) bool,
	onPrefix func(rec *output.PrefixRecord) error,
	onDiscover func(n int),
	onPartial func(reason string),
) error {
	seen := map[string]struct{}{rootPrefix: {}}
	seenMu := sync.Mutex{}

	current := []string{rootPrefix}
	onDiscover(1)

	for depth := 0; depth <= maxDepth; depth++ {
		if len(current) == 0 {
			break
		}

		var (
			next     []string
			nextMu   sync.Mutex
			wg       sync.WaitGroup
			errMu    sync.Mutex
			firstErr error
		)

		sem := make(chan struct{}, parallel)

		for _, pfx := range current {
			prefix := pfx
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()

				rec, children, err := summarizeDirectPrefix(ctx, lister, prefix, delimiter, maxObjects, maxPages, depth < maxDepth)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				rec.Depth = depth

				if err := onPrefix(rec); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}

				if depth >= maxDepth {
					return
				}

				// Enqueue children.
				for _, child := range children {
					if !allowPrefix(child) {
						continue
					}

					seenMu.Lock()
					_, exists := seen[child]
					if !exists {
						if len(seen) >= maxPrefixes {
							seenMu.Unlock()
							onPartial("max-prefixes")
							continue
						}
						seen[child] = struct{}{}
					}
					seenMu.Unlock()
					if exists {
						continue
					}

					nextMu.Lock()
					next = append(next, child)
					nextMu.Unlock()
				}
			}()
		}

		wg.Wait()
		errMu.Lock()
		err := firstErr
		errMu.Unlock()
		if err != nil {
			return err
		}

		sort.Strings(next)
		if len(next) > 0 {
			onDiscover(len(next))
		}
		current = next

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return ctx.Err()
}

func summarizeDirectPrefix(
	ctx context.Context,
	lister provider.DelimiterLister,
	prefix string,
	delimiter string,
	maxObjects int,
	maxPages int,
	collectChildren bool,
) (*output.PrefixRecord, []string, error) {
	var (
		objects         int64
		bytes           int64
		pages           int64
		token           string
		truncated       bool
		truncatedReason string
	)

	childrenSet := map[string]struct{}{}

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
			return nil, nil, err
		}

		pages++

		for _, cp := range res.CommonPrefixes {
			childrenSet[cp] = struct{}{}
		}

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

	children := make([]string, 0, len(childrenSet))
	if collectChildren {
		for cp := range childrenSet {
			children = append(children, cp)
		}
		sort.Strings(children)
	}

	return &output.PrefixRecord{
		Prefix:          prefix,
		Delimiter:       delimiter,
		ObjectsDirect:   objects,
		BytesDirect:     bytes,
		CommonPrefixes:  int64(len(childrenSet)),
		Pages:           pages,
		Truncated:       truncated,
		TruncatedReason: truncatedReason,
	}, children, nil
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
	if err := outputTreeTableHeader(tw); err != nil {
		return err
	}
	if err := outputTreeTableRow(tw, rec); err != nil {
		return err
	}
	return tw.Flush()
}

func outputTreeTableHeader(tw *tabwriter.Writer) error {
	if _, err := fmt.Fprintln(tw, "PREFIX\tDEPTH\tOBJECTS\tBYTES\tCOMMON_PREFIXES\tPAGES\tTRUNCATED"); err != nil {
		return err
	}
	return nil
}

func outputTreeTableRow(tw *tabwriter.Writer, rec *output.PrefixRecord) error {
	trunc := "no"
	if rec.Truncated {
		trunc = "yes"
		if rec.TruncatedReason != "" {
			trunc = trunc + " (" + rec.TruncatedReason + ")"
		}
	}

	if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%d\t%d\t%s\n",
		rec.Prefix,
		rec.Depth,
		rec.ObjectsDirect,
		formatSize(rec.BytesDirect),
		rec.CommonPrefixes,
		rec.Pages,
		trunc,
	); err != nil {
		return err
	}
	return nil
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func errorsIsContext(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
