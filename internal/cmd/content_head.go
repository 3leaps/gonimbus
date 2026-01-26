package cmd

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/content"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var contentHeadCmd = &cobra.Command{
	Use:   "head <uri>",
	Short: "Read the first N bytes (JSONL)",
	Long: `Read the first N bytes of one or more objects.

Supports:
- Exact object URIs (single key)
- Prefix URIs (trailing '/') to enumerate keys
- Glob patterns to enumerate matching keys
- Bulk input via --stdin (one URI per line)

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.
`,
	Args: validateContentHeadArgs,
	RunE: runContentHead,
}

var (
	contentHeadBytes       int64
	contentHeadStdin       bool
	contentHeadConcurrency int
	contentHeadRegion      string
	contentHeadProfile     string
	contentHeadEndpoint    string
)

const contentHeadMaxBytes = 10 * 1024 * 1024

func init() {
	contentCmd.AddCommand(contentHeadCmd)
	contentHeadCmd.Flags().Int64Var(&contentHeadBytes, "bytes", 4096, fmt.Sprintf("Number of bytes to read (max %d)", contentHeadMaxBytes))
	contentHeadCmd.Flags().BoolVar(&contentHeadStdin, "stdin", false, "Read URIs from stdin (one per line)")
	contentHeadCmd.Flags().IntVar(&contentHeadConcurrency, "concurrency", 16, "Max concurrent head operations")
	contentHeadCmd.Flags().StringVarP(&contentHeadRegion, "region", "r", "", "AWS region")
	contentHeadCmd.Flags().StringVarP(&contentHeadProfile, "profile", "p", "", "AWS profile")
	contentHeadCmd.Flags().StringVar(&contentHeadEndpoint, "endpoint", "", "Custom S3 endpoint")
}

func validateContentHeadArgs(cmd *cobra.Command, args []string) error {
	stdin, _ := cmd.Flags().GetBool("stdin")
	if stdin {
		if len(args) != 0 {
			return fmt.Errorf("when using --stdin, do not provide <uri> arguments")
		}
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("requires exactly 1 argument: <uri> (or use --stdin)")
	}
	return nil
}

func validateContentHeadBytes(n int64) error {
	if n < 0 {
		return fmt.Errorf("bytes must be >= 0")
	}
	if n > contentHeadMaxBytes {
		return fmt.Errorf("bytes must be <= %d", contentHeadMaxBytes)
	}
	return nil
}

type contentHeadTask struct {
	// URI is the resolved exact object URI for this task.
	// BaseURI is the original input URI that produced this task (stdin line, prefix URI, or glob URI).
	Bucket  string
	Key     string
	URI     string
	BaseURI string
}

func runContentHead(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if err := validateContentHeadBytes(contentHeadBytes); err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --bytes value", err)
	}
	if contentHeadConcurrency < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --concurrency value", fmt.Errorf("concurrency must be >= 1"))
	}

	inputs := []string{}
	if contentHeadStdin {
		uris, err := readURILines(cmd.InOrStdin())
		if err != nil {
			return exitError(foundry.ExitInvalidArgument, "Failed to read stdin", err)
		}
		inputs = append(inputs, uris...)
	} else {
		inputs = append(inputs, args[0])
	}
	if len(inputs) == 0 {
		return nil
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, string(provider.ProviderS3))
	defer func() { _ = w.Close() }()

	var (
		invalidCount    atomic.Int64
		serviceErrCount atomic.Int64
	)

	provMu := sync.Mutex{}
	providers := map[string]*s3.Provider{}
	getProvider := func(bucket string) (*s3.Provider, error) {
		provMu.Lock()
		if p, ok := providers[bucket]; ok {
			provMu.Unlock()
			return p, nil
		}
		provMu.Unlock()

		pNew, err := s3.New(ctx, s3.Config{
			Bucket:         bucket,
			Region:         contentHeadRegion,
			Endpoint:       contentHeadEndpoint,
			Profile:        contentHeadProfile,
			ForcePathStyle: contentHeadEndpoint != "",
		})
		if err != nil {
			return nil, err
		}

		provMu.Lock()
		if p, ok := providers[bucket]; ok {
			provMu.Unlock()
			_ = pNew.Close()
			return p, nil
		}
		providers[bucket] = pNew
		provMu.Unlock()
		return pNew, nil
	}
	defer func() {
		provMu.Lock()
		toClose := make([]*s3.Provider, 0, len(providers))
		for _, p := range providers {
			toClose = append(toClose, p)
		}
		provMu.Unlock()

		for _, p := range toClose {
			_ = p.Close()
		}
	}()

	tasks := make(chan contentHeadTask, contentHeadConcurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < contentHeadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}
				p, err := getProvider(task.Bucket)
				if err != nil {
					serviceErrCount.Add(1)
					_ = emitContentHeadError(context.Background(), w, task.Key, "failed to connect to storage provider", err, map[string]any{"uri": task.URI, "base_uri": task.BaseURI})
					continue
				}

				b, meta, err := content.HeadBytes(ctx, p, task.Key, contentHeadBytes)
				if err != nil {
					serviceErrCount.Add(1)
					_ = emitContentHeadError(context.Background(), w, task.Key, "content head failed", err, map[string]any{"uri": task.URI, "base_uri": task.BaseURI})
					continue
				}

				payload := map[string]any{
					"uri":             task.URI,
					"key":             task.Key,
					"bytes_requested": contentHeadBytes,
					"bytes_returned":  int64(len(b)),
					"content_b64":     base64.StdEncoding.EncodeToString(b),
					"etag":            meta.ETag,
					"size":            meta.Size,
					"last_modified":   meta.LastModified,
					"content_type":    meta.ContentType,
				}
				if err := w.WriteAny(ctx, "gonimbus.content.head.v1", payload); err != nil {
					observability.CLILogger.Error("Failed to write record", zap.Error(err))
					serviceErrCount.Add(1)
					return
				}
			}
		}()
	}

	for _, in := range inputs {
		if ctx.Err() != nil {
			break
		}
		if err := enqueueContentHeadInput(ctx, in, tasks, w, getProvider, &invalidCount, &serviceErrCount); err != nil {
			observability.CLILogger.Error("Failed to enqueue input", zap.String("input", in), zap.Error(err))
			serviceErrCount.Add(1)
			_ = emitContentHeadError(context.Background(), w, "", "failed to enqueue input", err, map[string]any{"uri": strings.TrimSpace(in)})
		}
	}
	close(tasks)
	wg.Wait()

	if ctx.Err() != nil {
		return exitError(foundry.ExitSignalInt, "content head cancelled", ctx.Err())
	}

	if invalidCount.Load() > 0 {
		return exitError(foundry.ExitInvalidArgument, "content head completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", invalidCount.Load()))
	}
	if serviceErrCount.Load() > 0 {
		return exitError(foundry.ExitExternalServiceUnavailable, "content head completed with errors", fmt.Errorf("errors=%d", serviceErrCount.Load()))
	}
	return nil
}

func readURILines(r io.Reader) ([]string, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 1024*1024)

	var out []string
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func enqueueContentHeadInput(
	ctx context.Context,
	input string,
	ch chan<- contentHeadTask,
	w output.Writer,
	getProvider func(bucket string) (*s3.Provider, error),
	invalidCount *atomic.Int64,
	serviceErrCount *atomic.Int64,
) error {
	uriStr := strings.TrimSpace(input)
	if uriStr == "" {
		return nil
	}

	parsed, err := ParseURI(uriStr)
	if err != nil {
		invalidCount.Add(1)
		_ = emitContentHeadError(context.Background(), w, "", "invalid URI", err, map[string]any{"uri": uriStr})
		return nil
	}
	if parsed.Provider != string(provider.ProviderS3) {
		invalidCount.Add(1)
		_ = emitContentHeadError(context.Background(), w, "", "unsupported provider", fmt.Errorf("provider %q is not supported", parsed.Provider), map[string]any{"uri": uriStr})
		return nil
	}

	// Exact key: schedule directly.
	if !parsed.IsPattern() && !parsed.IsPrefix() {
		select {
		case ch <- contentHeadTask{Bucket: parsed.Bucket, Key: parsed.Key, URI: parsed.String(), BaseURI: uriStr}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Prefix or glob: enumerate via list.
	p, err := getProvider(parsed.Bucket)
	if err != nil {
		serviceErrCount.Add(1)
		_ = emitContentHeadError(context.Background(), w, "", "failed to connect to storage provider", err, map[string]any{"uri": uriStr, "base_uri": parsed.String()})
		return nil
	}

	var matcher *match.Matcher
	if parsed.IsPattern() {
		m, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			invalidCount.Add(1)
			_ = emitContentHeadError(context.Background(), w, "", "invalid pattern", err, map[string]any{"uri": uriStr})
			return nil
		}
		matcher = m
	}

	var token string
	for {
		res, err := p.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			serviceErrCount.Add(1)
			_ = emitContentHeadListError(context.Background(), w, parsed.Key, "list failed", err, map[string]any{"uri": uriStr, "base_uri": parsed.String()})
			return nil
		}

		for _, obj := range res.Objects {
			if matcher != nil && !matcher.Match(obj.Key) {
				continue
			}
			objURI := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case ch <- contentHeadTask{Bucket: parsed.Bucket, Key: obj.Key, URI: objURI, BaseURI: uriStr}:
				// ok
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}

	return nil
}

func emitContentHeadError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
	code := contentHeadErrorCode(err)
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "content_head"

	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Key: key, Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit content head error record", zap.Error(werr))
	}
	return nil
}

func emitContentHeadListError(ctx context.Context, w output.Writer, prefix, msg string, err error, details map[string]any) error {
	code := contentHeadErrorCode(err)
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "content_head"

	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Prefix: prefix, Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit content head list error record", zap.Error(werr))
	}
	return nil
}

func contentHeadErrorCode(err error) string {
	code := output.ErrCodeInternal
	switch {
	case provider.IsNotFound(err):
		code = output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		code = output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		code = output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		code = output.ErrCodeProviderUnavailable
	}
	return code
}
