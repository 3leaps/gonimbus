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
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const contentProbeMaxBytes = 10 * 1024 * 1024

type contentProbeProvider interface {
	provider.Provider
}

var newContentProbeProvider = func(ctx context.Context, src *uri.ObjectURI) (contentProbeProvider, error) {
	return newCommandSourceProvider(ctx, src, "content probe", contentProbeRegion, contentProbeProfile, contentProbeEndpoint)
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
xml_xpath extractors may use xpath_priority for ordered fallback paths. json_path
remains available in fixed_window mode and is rejected under until_resolved.
on_missing supports fail and quarantine.

Use --rewrite-from when derived fields reference source-key captures. The
template is applied to the parsed object key, not the full URI, and its captures
seed the variable map before content extraction and derived evaluation.
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
	contentProbeRewriteFrom string
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
	contentProbeCmd.Flags().StringVar(&contentProbeRewriteFrom, "rewrite-from", "", "Rewrite source template used to seed path captures before derived evaluation")
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
	ProviderURI *uri.ObjectURI
	Key         string
	URI         string
	BaseInput   string
	ETag        string
	Size        int64
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
	DestRelKey       string            `json:"dest_rel_key,omitempty"`
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

	cfgBytes, err := os.ReadFile(contentProbeConfigPath) // #nosec G304 -- operator-supplied probe config path is the CLI input being read.
	if err != nil {
		return exitError(foundry.ExitFileReadError, "Failed to read probe config", err)
	}
	probeCfg, err := loadProbeConfig(cfgBytes, contentProbeConfigPath)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid probe config", err)
	}
	rewriteCapture, err := compileContentProbeRewriteCapture(contentProbeRewriteFrom)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --rewrite-from value", err)
	}
	var rewriteCaptureNames []string
	if rewriteCapture != nil {
		rewriteCaptureNames = rewriteCapture.CaptureNames()
	}
	prober, err := newContentProbeProber(probeCfg, rewriteCaptureNames)
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
	getProvider := func(src *uri.ObjectURI) (contentProbeProvider, error) {
		id := commandSourceProviderID(src)
		provMu.Lock()
		if p, ok := providers[id]; ok {
			provMu.Unlock()
			return p, nil
		}
		provMu.Unlock()

		pNew, err := newContentProbeProvider(ctx, src)
		if err != nil {
			return nil, err
		}

		provMu.Lock()
		if p, ok := providers[id]; ok {
			provMu.Unlock()
			_ = pNew.Close()
			return p, nil
		}
		providers[id] = pNew
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
				prov, err := getProvider(task.ProviderURI)
				if err != nil {
					errorCount.Add(1)
					_ = emitContentProbeError(context.Background(), w, task.Key, "failed to connect to provider", err, map[string]any{"uri": task.URI, "base_input": task.BaseInput})
					continue
				}

				result, err := runContentProbeTask(ctx, prov, task, prober, probeCfg, rewriteCapture)
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

func compileContentProbeRewriteCapture(raw string) (*transfer.ReflowCapture, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	return transfer.CompileReflowCapture(raw)
}

func contentProbeInitialVars(task probeTask, rewriteCapture *transfer.ReflowCapture) (map[string]string, error) {
	if rewriteCapture == nil {
		return nil, nil
	}
	vars, err := rewriteCapture.Apply(task.Key)
	if err != nil {
		return nil, fmt.Errorf("rewriteFrom capture failed: %w", err)
	}
	return vars, nil
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

func runContentProbeTask(ctx context.Context, prov contentProbeProvider, task probeTask, prober *probe.Prober, cfg *probe.Config, rewriteCapture *transfer.ReflowCapture) (*contentProbeTaskResult, error) {
	initialVars, err := contentProbeInitialVars(task, rewriteCapture)
	if err != nil {
		return &contentProbeTaskResult{meta: &provider.ObjectMeta{}, bytesRequested: contentProbeBytes, routingClass: "normal", extractErr: err}, nil
	}
	if cfg.ReadStrategy.Mode == probe.ReadStrategyUntilResolved {
		return runContentProbeUntilResolved(ctx, prov, task.Key, prober, cfg, initialVars)
	}

	b, meta, err := content.HeadBytes(ctx, prov, task.Key, contentProbeBytes)
	if err != nil {
		return nil, err
	}
	termination := probe.TerminationAllRequiredResolved
	if prober.HasPriorityExtractors() && fixedWindowHitBoundary(b, meta, contentProbeBytes) {
		termination = probe.TerminationFixedWindow
	}
	res, err := prober.ProbeDetailedWithVars(b, int64(len(b)), termination, initialVars)
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

func runContentProbeUntilResolved(ctx context.Context, prov contentProbeProvider, key string, prober *probe.Prober, cfg *probe.Config, initialVars map[string]string) (*contentProbeTaskResult, error) {
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
	if meta.Size > 0 && meta.Size <= readLimit {
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

		res, err := prober.ProbeDetailedAllowIncompleteWithVars(buf, bytesRead, "", initialVars, func(err error) bool {
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
		if prober.RequiredResolvedForTermination(res) {
			res.Audit.TerminationReason = probe.TerminationAllRequiredResolved
			probe.FinalizeAuditForTermination(&res.Audit)
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

	if bytesRead >= cfg.ReadStrategy.MaxBytesValue && !exhaustsObject {
		termination = probe.TerminationMaxBytesReached
	}
	res, err := prober.ProbeDetailedWithVars(buf, bytesRead, termination, initialVars)
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
	probe.FinalizeAuditForTermination(&res.Audit)
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
		key := auditResolutionKey(item)
		if _, ok := resolvedAt[key]; !ok {
			resolvedAt[key] = *item.BytesAtResolution
		}
	}
}

func applyBytesAtResolution(audit *probe.ProbeAudit, resolvedAt map[string]int64) {
	if audit == nil {
		return
	}
	for i := range audit.Extractors {
		at, ok := resolvedAt[auditResolutionKey(audit.Extractors[i])]
		if !ok {
			continue
		}
		audit.Extractors[i].BytesAtResolution = &at
	}
}

func fixedWindowHitBoundary(data []byte, meta *provider.ObjectMeta, requested int64) bool {
	bytesRead := int64(len(data))
	if meta != nil && meta.Size > 0 {
		return meta.Size > bytesRead
	}
	return requested > 0 && bytesRead >= requested
}

func auditResolutionKey(item probe.ExtractorAudit) string {
	if item.ResolvedPriority != nil {
		return fmt.Sprintf("%s#%d#%s", item.Name, *item.ResolvedPriority, item.ResolvedXPath)
	}
	return item.Name
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
	return &cfg, nil
}

func newContentProbeProber(cfg *probe.Config, rewriteCaptureNames []string) (*probe.Prober, error) {
	return probe.NewNormalizedWithRewriteCaptures(cfg, rewriteCaptureNames)
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
	getProvider func(src *uri.ObjectURI) (contentProbeProvider, error),
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
		key := strings.TrimPrefix(data.Key, "/")
		if key == "" {
			key = strings.TrimPrefix(data.RelKey, "/")
		}
		if key == "" {
			invalidCount.Add(1)
			_ = emitContentProbeError(context.Background(), w, "", "missing key", fmt.Errorf("missing key in index record"), map[string]any{"base_uri": data.BaseURI})
			return nil
		}
		target := commandSourceTargetForRead(base)
		uri := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
		if base.Provider == string(provider.ProviderFile) {
			uri = fileURI(filepath.Join(target.ProviderURI.Key, filepath.FromSlash(key)))
		}
		select {
		case ch <- probeTask{ProviderURI: target.ProviderURI, Key: key, URI: uri, BaseInput: "jsonl", ETag: data.ETag, Size: data.SizeBytes}:
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
	target := commandSourceTargetForRead(parsed)

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case ch <- probeTask{ProviderURI: target.ProviderURI, Key: target.QueryURI.Key, URI: parsed.String(), BaseInput: line}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	prov, err := getProvider(target.ProviderURI)
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
		res, err := prov.List(ctx, provider.ListOptions{Prefix: target.QueryURI.Key, ContinuationToken: token})
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
			if parsed.Provider == string(provider.ProviderFile) {
				uri = fileURI(filepath.Join(target.ProviderURI.Key, filepath.FromSlash(obj.Key)))
			}
			select {
			case ch <- probeTask{ProviderURI: target.ProviderURI, Key: obj.Key, URI: uri, BaseInput: line, ETag: obj.ETag, Size: obj.Size}:
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
