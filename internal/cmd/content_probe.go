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
	"time"

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
	"github.com/3leaps/gonimbus/pkg/uri"
)

const contentProbeMaxBytes = 10 * 1024 * 1024

type contentProbeProvider interface {
	provider.Provider
}

var newContentProbeProvider = func(ctx context.Context, cfg s3.Config) (contentProbeProvider, error) {
	return s3.New(ctx, cfg)
}

var contentProbeCmd = &cobra.Command{
	Use:   "probe [uri]",
	Short: "Probe object content for derived fields (JSONL)",
	Long: `Probe object content and extract derived fields.

Inputs:
- Exact object URIs
- Prefix URIs (trailing '/') to enumerate keys
- Glob patterns to enumerate matching keys
- Bulk input via --stdin (one input per line)
- JSONL index objects (type=gonimbus.index.object.v1)

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.

Read strategies:
- fixed_window (default): read --bytes from the object head
- until_resolved: configure read_strategy.mode in probe config with max_bytes
  and optional chunk_bytes; streams monotonic ranges until required extractors
  resolve, max_bytes is reached, or the stream is exhausted

until_resolved supports xml_xpath and regex extractors in this release.
json_path remains available in fixed_window mode and is rejected under
until_resolved. on_missing supports fail and quarantine; fallback is deferred.
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
	URI              string            `json:"uri"`
	Key              string            `json:"key"`
	BytesRequested   int64             `json:"bytes_requested"`
	BytesReturned    int64             `json:"bytes_returned"`
	Vars             map[string]string `json:"vars"`
	ETag             string            `json:"etag,omitempty"`
	Size             int64             `json:"size,omitempty"`
	RoutingClass     string            `json:"routing_class,omitempty"`
	QuarantinePrefix string            `json:"quarantine_prefix,omitempty"`
	Probe            *probe.ProbeAudit `json:"probe,omitempty"`
}

type reflowInputRecord struct {
	SourceURI        string            `json:"source_uri"`
	SourceKey        string            `json:"source_key"`
	SourceETag       string            `json:"source_etag,omitempty"`
	SourceSize       int64             `json:"source_size_bytes,omitempty"`
	SourceLastMod    *time.Time        `json:"source_last_modified,omitempty"`
	Vars             map[string]string `json:"vars,omitempty"`
	RoutingClass     string            `json:"routing_class,omitempty"`
	QuarantinePrefix string            `json:"quarantine_prefix,omitempty"`
	Probe            *probe.ProbeAudit `json:"probe,omitempty"`
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
	providers := map[string]contentProbeProvider{}
	getProvider := func(bucket string) (contentProbeProvider, error) {
		provMu.Lock()
		if p, ok := providers[bucket]; ok {
			provMu.Unlock()
			return p, nil
		}
		provMu.Unlock()

		pNew, err := newContentProbeProvider(ctx, s3.Config{
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
		toClose := make([]contentProbeProvider, 0, len(providers))
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

				result, err := runContentProbeTask(ctx, prov, task, prober, probeCfg)
				if err != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "content probe read failed", err, map[string]any{"uri": task.URI, "base_input": task.BaseInput})
					continue
				}
				if result.extractErr != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "content probe extract failed", result.extractErr, map[string]any{"uri": task.URI, "base_input": task.BaseInput, "probe": result.audit})
					continue
				}

				if contentProbeEmit == "probe" || contentProbeEmit == "both" {
					_ = w.WriteAny(ctx, "gonimbus.content.probe.v1", &contentProbeRecord{
						URI:              task.URI,
						Key:              task.Key,
						BytesRequested:   result.bytesRequested,
						BytesReturned:    result.bytesRead,
						Vars:             result.vars,
						ETag:             result.meta.ETag,
						Size:             result.meta.Size,
						RoutingClass:     omitNormalRoutingClass(result.routingClass),
						QuarantinePrefix: result.quarantinePrefix,
						Probe:            result.audit,
					})
				}
				if contentProbeEmit == "reflow-input" || contentProbeEmit == "both" {
					var sourceLastMod *time.Time
					if result.meta != nil && !result.meta.LastModified.IsZero() {
						t := result.meta.LastModified.UTC()
						sourceLastMod = &t
					}
					_ = w.WriteAny(ctx, "gonimbus.reflow.input.v1", &reflowInputRecord{
						SourceURI:        task.URI,
						SourceKey:        task.Key,
						SourceETag:       result.meta.ETag,
						SourceSize:       result.meta.Size,
						SourceLastMod:    sourceLastMod,
						Vars:             result.vars,
						RoutingClass:     omitNormalRoutingClass(result.routingClass),
						QuarantinePrefix: result.quarantinePrefix,
						Probe:            result.audit,
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

type contentProbeTaskResult struct {
	vars             map[string]string
	audit            *probe.ProbeAudit
	meta             *provider.ObjectMeta
	bytesRequested   int64
	bytesRead        int64
	routingClass     string
	quarantinePrefix string
	extractErr       error
}

func runContentProbeTask(ctx context.Context, prov contentProbeProvider, task probeTask, prober *probe.Prober, cfg *probe.Config) (*contentProbeTaskResult, error) {
	if cfg.ReadStrategy.Mode == probe.ReadStrategyUntilResolved {
		return runContentProbeUntilResolved(ctx, prov, task.Key, prober, cfg)
	}

	b, meta, err := content.HeadBytes(ctx, prov, task.Key, contentProbeBytes)
	if err != nil {
		return nil, err
	}
	res, err := prober.ProbeDetailed(b, int64(len(b)), probe.TerminationAllRequiredResolved)
	if err != nil {
		return &contentProbeTaskResult{meta: meta, bytesRequested: contentProbeBytes, bytesRead: int64(len(b)), extractErr: err}, nil
	}
	routingClass, requiredFailed, failureErr := prober.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)
	if requiredFailed {
		if failureErr == nil {
			failureErr = fmt.Errorf("required extractors unresolved")
		}
		return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: contentProbeBytes, bytesRead: int64(len(b)), routingClass: routingClass, extractErr: failureErr}, nil
	}
	return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: contentProbeBytes, bytesRead: int64(len(b)), routingClass: routingClass, quarantinePrefix: quarantinePrefixForRouting(routingClass, cfg)}, nil
}

func runContentProbeUntilResolved(ctx context.Context, prov contentProbeProvider, key string, prober *probe.Prober, cfg *probe.Config) (*contentProbeTaskResult, error) {
	ranger, ok := prov.(provider.ObjectRanger)
	if !ok {
		return nil, fmt.Errorf("provider does not support range reads")
	}
	meta, err := prov.Head(ctx, key)
	if err != nil {
		return nil, err
	}
	readLimit := cfg.ReadStrategy.MaxBytesValue
	exhaustsObject := false
	if meta.Size > 0 && meta.Size < readLimit {
		readLimit = meta.Size
		exhaustsObject = true
	}
	chunkBytes := cfg.ReadStrategy.ChunkBytesValue
	if chunkBytes <= 0 {
		chunkBytes = probe.DefaultChunkBytes
	}

	var (
		buf          []byte
		bytesRead    int64
		lastErr      error
		resolvedAt   = map[string]int64{}
		lastProbeRes *probe.Result
	)
	termination := probe.TerminationStreamExhausted
	for start := int64(0); start < readLimit; {
		end := start + chunkBytes - 1
		if end >= readLimit {
			end = readLimit - 1
		}
		body, _, err := ranger.GetRange(ctx, key, start, end)
		if err != nil {
			return nil, err
		}
		chunk, readErr := io.ReadAll(body)
		_ = body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(chunk) == 0 {
			termination = probe.TerminationStreamExhausted
			break
		}
		buf = append(buf, chunk...)
		bytesRead += int64(len(chunk))
		atReadLimit := bytesRead >= readLimit

		res, err := prober.ProbeDetailedAllowIncomplete(buf, bytesRead, "", func(err error) bool {
			return !atReadLimit && isIncompleteProbeParseError(err)
		})
		if err != nil {
			lastErr = err
			if isIncompleteProbeParseError(err) && atReadLimit && !exhaustsObject {
				termination = probe.TerminationMaxBytesReached
			} else {
				termination = probe.TerminationParseError
			}
			break
		}
		rememberBytesAtResolution(res, resolvedAt)
		lastProbeRes = res
		if prober.AllRequiredResolved(res.Vars) {
			res.Audit.TerminationReason = probe.TerminationAllRequiredResolved
			applyBytesAtResolution(&res.Audit, resolvedAt)
			routingClass, requiredFailed, failureErr := prober.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)
			if requiredFailed {
				if failureErr == nil {
					failureErr = fmt.Errorf("required extractors unresolved")
				}
				return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, extractErr: failureErr}, nil
			}
			return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, quarantinePrefix: quarantinePrefixForRouting(routingClass, cfg)}, nil
		}
		if atReadLimit {
			if exhaustsObject {
				termination = probe.TerminationStreamExhausted
			} else {
				termination = probe.TerminationMaxBytesReached
			}
			break
		}
		start += int64(len(chunk))
	}

	if bytesRead >= cfg.ReadStrategy.MaxBytesValue {
		termination = probe.TerminationMaxBytesReached
	}
	res, err := prober.ProbeDetailed(buf, bytesRead, termination)
	if err != nil {
		if termination != probe.TerminationMaxBytesReached && termination != probe.TerminationParseError && isIncompleteProbeParseError(err) {
			termination = probe.TerminationParseError
		}
		if lastProbeRes != nil {
			res = lastProbeRes
			res.Audit.BytesRead = bytesRead
		} else {
			res = prober.UnresolvedResult(bytesRead, termination)
		}
		lastErr = err
	}
	res.Audit.TerminationReason = termination
	applyBytesAtResolution(&res.Audit, resolvedAt)
	routingClass, requiredFailed, failureErr := prober.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)
	if requiredFailed {
		if failureErr == nil {
			failureErr = lastErr
		}
		if failureErr == nil {
			failureErr = fmt.Errorf("required extractors unresolved")
		}
		if lastErr != nil {
			return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, extractErr: failureErr}, nil
		}
		return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, extractErr: failureErr}, nil
	}
	if lastErr != nil && routingClass != "quarantine" {
		return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, extractErr: lastErr}, nil
	}
	return &contentProbeTaskResult{vars: res.Vars, audit: &res.Audit, meta: meta, bytesRequested: cfg.ReadStrategy.MaxBytesValue, bytesRead: bytesRead, routingClass: routingClass, quarantinePrefix: quarantinePrefixForRouting(routingClass, cfg)}, nil
}

func rememberBytesAtResolution(res *probe.Result, resolvedAt map[string]int64) {
	if res == nil {
		return
	}
	for _, item := range res.Audit.Extractors {
		if !item.Resolved || item.BytesAtResolution == nil {
			continue
		}
		if _, ok := resolvedAt[item.Name]; !ok {
			resolvedAt[item.Name] = *item.BytesAtResolution
		}
	}
}

func applyBytesAtResolution(audit *probe.ProbeAudit, resolvedAt map[string]int64) {
	if audit == nil {
		return
	}
	for i := range audit.Extractors {
		at, ok := resolvedAt[audit.Extractors[i].Name]
		if !ok {
			continue
		}
		audit.Extractors[i].BytesAtResolution = &at
	}
}

func quarantinePrefixForRouting(routingClass string, cfg *probe.Config) string {
	if routingClass != "quarantine" || cfg == nil {
		return ""
	}
	return strings.Trim(cfg.QuarantinePrefix, "/") + "/"
}

func omitNormalRoutingClass(routingClass string) string {
	if routingClass == "normal" {
		return ""
	}
	return routingClass
}

func isIncompleteProbeParseError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "unexpected end") ||
		strings.Contains(msg, "eof")
}

func loadProbeConfig(data []byte, path string) (*probe.Config, error) {
	var cfg probe.Config
	// Heuristic: yaml.v3 can parse JSON as well; use extension only for better errors later.
	_ = path
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
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
	getProvider func(bucket string) (contentProbeProvider, error),
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
		base, err := uri.ParseURI(data.BaseURI)
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

	parsed, err := uri.ParseURI(line)
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
