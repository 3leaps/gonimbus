package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/content"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

const contentProbeMaxBytes = 10 * 1024 * 1024

var contentProbeCmd = &cobra.Command{
	Use:   "probe [uri]",
	Short: "Probe object content for derived fields (JSONL)",
	Long: `Probe object content within a fixed byte window and extract derived fields.

Inputs:
- Exact object URIs
- Prefix URIs (trailing '/') to enumerate keys
- Glob patterns to enumerate matching keys
- Bulk input via --stdin (one input per line)
- JSONL index objects (type=gonimbus.index.object.v1)

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.
`,
	Args: validateContentProbeArgs,
	RunE: runContentProbe,
}

var (
	contentProbeBytes       int64
	contentProbeStdin       bool
	contentProbeConcurrency int
	contentProbeConfigPath  string
	contentProbeEmit        string
	contentProbeRegion      string
	contentProbeProfile     string
	contentProbeEndpoint    string
)

func init() {
	contentCmd.AddCommand(contentProbeCmd)
	contentProbeCmd.Flags().Int64Var(&contentProbeBytes, "bytes", 4096, fmt.Sprintf("Number of bytes to read (max %d)", contentProbeMaxBytes))
	contentProbeCmd.Flags().BoolVar(&contentProbeStdin, "stdin", false, "Read inputs from stdin (one per line)")
	contentProbeCmd.Flags().IntVar(&contentProbeConcurrency, "concurrency", 16, "Max concurrent probe operations")
	contentProbeCmd.Flags().StringVar(&contentProbeConfigPath, "config", "", "Probe config file (YAML or JSON)")
	contentProbeCmd.Flags().StringVar(&contentProbeEmit, "emit", "reflow-input", "Output record type: probe|reflow-input|both")
	contentProbeCmd.Flags().StringVarP(&contentProbeRegion, "region", "r", "", "AWS region")
	contentProbeCmd.Flags().StringVarP(&contentProbeProfile, "profile", "p", "", "AWS profile")
	contentProbeCmd.Flags().StringVar(&contentProbeEndpoint, "endpoint", "", "Custom S3 endpoint")
	_ = contentProbeCmd.MarkFlagRequired("config")
}

func validateContentProbeArgs(cmd *cobra.Command, args []string) error {
	stdin, _ := cmd.Flags().GetBool("stdin")
	if stdin {
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("requires exactly 1 argument: [uri] (or use --stdin)")
	}
	return nil
}

type probeTask struct {
	Bucket    string
	Key       string
	URI       string
	BaseInput string
	ETag      string
	Size      int64
}

type contentProbeRecord struct {
	URI            string            `json:"uri"`
	Key            string            `json:"key"`
	BytesRequested int64             `json:"bytes_requested"`
	BytesReturned  int64             `json:"bytes_returned"`
	Vars           map[string]string `json:"vars"`
	ETag           string            `json:"etag,omitempty"`
	Size           int64             `json:"size,omitempty"`
}

type reflowInputRecord struct {
	SourceURI  string            `json:"source_uri"`
	SourceKey  string            `json:"source_key"`
	SourceETag string            `json:"source_etag,omitempty"`
	SourceSize int64             `json:"source_size_bytes,omitempty"`
	Vars       map[string]string `json:"vars,omitempty"`
}

func runContentProbe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if contentProbeConcurrency < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --concurrency value", fmt.Errorf("concurrency must be >= 1"))
	}
	if err := validateContentProbeBytes(contentProbeBytes); err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --bytes value", err)
	}
	switch contentProbeEmit {
	case "probe", "reflow-input", "both":
		// ok
	default:
		return exitError(foundry.ExitInvalidArgument, "Invalid --emit value", fmt.Errorf("emit must be one of: probe, reflow-input, both"))
	}

	cfgBytes, err := os.ReadFile(contentProbeConfigPath)
	if err != nil {
		return exitError(foundry.ExitFileReadError, "Failed to read probe config", err)
	}
	probeCfg, err := loadProbeConfig(cfgBytes, contentProbeConfigPath)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid probe config", err)
	}
	prober, err := probe.New(*probeCfg)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid probe config", err)
	}

	inputs := []string{}
	if contentProbeStdin {
		lines, err := readLines(cmd.InOrStdin())
		if err != nil {
			return exitError(foundry.ExitInvalidArgument, "Failed to read stdin", err)
		}
		inputs = append(inputs, lines...)
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
		invalidCount atomic.Int64
		errorCount   atomic.Int64
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
			Region:         contentProbeRegion,
			Endpoint:       contentProbeEndpoint,
			Profile:        contentProbeProfile,
			ForcePathStyle: contentProbeEndpoint != "",
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

	tasks := make(chan probeTask, contentProbeConcurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < contentProbeConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}
				prov, err := getProvider(task.Bucket)
				if err != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "failed to connect to provider", err, map[string]any{"uri": task.URI, "base_input": task.BaseInput})
					continue
				}

				b, meta, err := content.HeadBytes(ctx, prov, task.Key, contentProbeBytes)
				if err != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "content probe read failed", err, map[string]any{"uri": task.URI, "base_input": task.BaseInput})
					continue
				}

				vars, err := prober.Probe(b)
				if err != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "content probe extract failed", err, map[string]any{"uri": task.URI, "base_input": task.BaseInput})
					continue
				}

				if contentProbeEmit == "probe" || contentProbeEmit == "both" {
					_ = w.WriteAny(ctx, "gonimbus.content.probe.v1", &contentProbeRecord{
						URI:            task.URI,
						Key:            task.Key,
						BytesRequested: contentProbeBytes,
						BytesReturned:  int64(len(b)),
						Vars:           vars,
						ETag:           meta.ETag,
						Size:           meta.Size,
					})
				}
				if contentProbeEmit == "reflow-input" || contentProbeEmit == "both" {
					_ = w.WriteAny(ctx, "gonimbus.reflow.input.v1", &reflowInputRecord{
						SourceURI:  task.URI,
						SourceKey:  task.Key,
						SourceETag: meta.ETag,
						SourceSize: meta.Size,
						Vars:       vars,
					})
				}
			}
		}()
	}

	for _, in := range inputs {
		if ctx.Err() != nil {
			break
		}
		if err := enqueueContentProbeInput(ctx, in, tasks, w, getProvider, &invalidCount, &errorCount); err != nil {
			errorCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "failed to enqueue input", err, map[string]any{"input": strings.TrimSpace(in)})
		}
	}
	close(tasks)
	wg.Wait()

	if ctx.Err() != nil {
		return exitError(foundry.ExitSignalInt, "content probe cancelled", ctx.Err())
	}
	if invalidCount.Load() > 0 {
		return exitError(foundry.ExitInvalidArgument, "content probe completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", invalidCount.Load()))
	}
	if errorCount.Load() > 0 {
		return exitError(foundry.ExitExternalServiceUnavailable, "content probe completed with errors", fmt.Errorf("errors=%d", errorCount.Load()))
	}
	return nil
}

func validateContentProbeBytes(n int64) error {
	if n < 0 {
		return fmt.Errorf("bytes must be >= 0")
	}
	if n > contentProbeMaxBytes {
		return fmt.Errorf("bytes must be <= %d", contentProbeMaxBytes)
	}
	return nil
}

func loadProbeConfig(data []byte, path string) (*probe.Config, error) {
	var cfg probe.Config
	// Heuristic: yaml.v3 can parse JSON as well; use extension only for better errors later.
	_ = path
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func readLines(r io.Reader) ([]string, error) {
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

func enqueueContentProbeInput(
	ctx context.Context,
	input string,
	ch chan<- probeTask,
	w output.Writer,
	getProvider func(bucket string) (*s3.Provider, error),
	invalidCount *atomic.Int64,
	errorCount *atomic.Int64,
) error {
	line := strings.TrimSpace(input)
	if line == "" {
		return nil
	}

	// JSONL index objects.
	if strings.HasPrefix(line, "{") {
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "invalid json input", err, map[string]any{"input": line})
			return nil
		}
		if env.Type != "gonimbus.index.object.v1" {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "unsupported json input", fmt.Errorf("unsupported json record type %q", env.Type), map[string]any{"input": line})
			return nil
		}
		var data struct {
			BaseURI   string  `json:"base_uri"`
			Key       string  `json:"key"`
			ETag      string  `json:"etag"`
			SizeBytes int64   `json:"size_bytes"`
			RelKey    string  `json:"rel_key"`
			DeletedAt *string `json:"deleted_at"`
		}
		if err := json.Unmarshal(env.Data, &data); err != nil {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "invalid json input", err, map[string]any{"input": line})
			return nil
		}
		if data.DeletedAt != nil {
			return nil
		}
		base, err := ParseURI(data.BaseURI)
		if err != nil {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "invalid base_uri", err, map[string]any{"base_uri": data.BaseURI})
			return nil
		}
		if base.Provider != string(provider.ProviderS3) {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "unsupported provider", fmt.Errorf("provider %q is not supported", base.Provider), map[string]any{"base_uri": data.BaseURI})
			return nil
		}
		key := strings.TrimPrefix(data.Key, "/")
		if key == "" {
			key = strings.TrimPrefix(data.RelKey, "/")
		}
		if key == "" {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "missing key", fmt.Errorf("missing key in index record"), map[string]any{"base_uri": data.BaseURI})
			return nil
		}
		uri := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
		select {
		case ch <- probeTask{Bucket: base.Bucket, Key: key, URI: uri, BaseInput: "jsonl", ETag: data.ETag, Size: data.SizeBytes}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	parsed, err := ParseURI(line)
	if err != nil {
		invalidCount.Add(1)
		_ = emitContentProbeError(context.Background(), w, "", "invalid URI", err, map[string]any{"uri": line})
		return nil
	}
	if parsed.Provider != string(provider.ProviderS3) {
		invalidCount.Add(1)
		_ = emitContentProbeError(context.Background(), w, "", "unsupported provider", fmt.Errorf("provider %q is not supported", parsed.Provider), map[string]any{"uri": line})
		return nil
	}

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case ch <- probeTask{Bucket: parsed.Bucket, Key: parsed.Key, URI: parsed.String(), BaseInput: line}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	prov, err := getProvider(parsed.Bucket)
	if err != nil {
		errorCount.Add(1)
		_ = emitContentProbeError(context.Background(), w, "", "failed to connect to provider", err, map[string]any{"uri": line})
		return nil
	}

	var matcher *match.Matcher
	if parsed.IsPattern() {
		m, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "invalid pattern", err, map[string]any{"uri": line})
			return nil
		}
		matcher = m
	}

	var token string
	for {
		res, err := prov.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			errorCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, parsed.Key, "list failed", err, map[string]any{"uri": line})
			return nil
		}
		for _, obj := range res.Objects {
			if matcher != nil && !matcher.Match(obj.Key) {
				continue
			}
			uri := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case ch <- probeTask{Bucket: parsed.Bucket, Key: obj.Key, URI: uri, BaseInput: line, ETag: obj.ETag, Size: obj.Size}:
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

func emitContentProbeError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
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
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "content_probe"
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Key: key, Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit content probe error record", zap.Error(werr))
	}
	return nil
}
