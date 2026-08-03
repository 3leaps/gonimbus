package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/internal/connpolicy"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// errConnPoolAdmission is a typed pre-construction failure from admitted-N
// formula resolution. Callers must not treat construction/provider errors as
// admission by matching error text.
type errConnPoolAdmission struct {
	op  string
	err error
}

func (e *errConnPoolAdmission) Error() string {
	if e == nil {
		return "connection pool admission failed"
	}
	if e.op == "" {
		return fmt.Sprintf("connection pool admission: %v", e.err)
	}
	return fmt.Sprintf("connection pool admission (%s): %v", e.op, e.err)
}

func (e *errConnPoolAdmission) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func isConnPoolAdmission(err error) bool {
	var e *errConnPoolAdmission
	return errors.As(err, &e)
}

func wrapConnPoolAdmission(op string, err error) error {
	if err == nil {
		return nil
	}
	if isConnPoolAdmission(err) {
		return err
	}
	return &errConnPoolAdmission{op: op, err: err}
}

// applySourceConnectionPool exact-assigns HTTP pool knobs on source construction
// options from ResolveConnectionPool(admittedN). Callers must pre-resolve
// admitted N (engine defaults/clamps) before calling.
func applySourceConnectionPool(opts *providerdispatch.SourceOptions, admittedN int) error {
	if opts == nil {
		return fmt.Errorf("source options is nil")
	}
	pool, err := provider.ResolveConnectionPool(admittedN)
	if err != nil {
		return err
	}
	opts.S3.MaxIdleConnsPerHost = pool.MaxIdleConnsPerHost
	opts.S3.MaxConnsPerHost = pool.MaxConnsPerHost
	opts.GCS.MaxIdleConnsPerHost = pool.MaxIdleConnsPerHost
	opts.GCS.MaxConnsPerHost = pool.MaxConnsPerHost
	return nil
}

// resolvedCrawlAdmittedN returns crawl LIST concurrency after defaulting zero/negative
// to the crawler default (matches engine RequestBudget sizing).
func resolvedCrawlAdmittedN(concurrency int) int {
	if concurrency <= 0 {
		return crawler.DefaultConfig().Concurrency
	}
	return concurrency
}

// indexBuildSourceOptions is the single production boundary for index-build
// source construction pool policy (engine both-format, durable, and SQLite crawl).
func indexBuildSourceOptions(m *manifest.IndexManifest) (providerdispatch.SourceOptions, error) {
	if m == nil {
		return providerdispatch.SourceOptions{}, fmt.Errorf("index manifest is nil")
	}
	opts := providerdispatch.SourceOptions{
		Command: operationIndexBuild,
		S3: providerdispatch.S3Options{
			Region:         m.Connection.Region,
			Endpoint:       m.Connection.Endpoint,
			Profile:        m.Connection.Profile,
			ForcePathStyle: m.Connection.Endpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: m.Connection.Project,
		},
	}
	if err := applySourceConnectionPool(&opts, resolvedCrawlAdmittedN(indexBuildEngineCrawlConfig(m).Concurrency)); err != nil {
		return providerdispatch.SourceOptions{}, err
	}
	return opts, nil
}

// transferSourceAdmittedN resolves non-reflow transfer source composite N
// (C+L, or C+2L when sharding is enabled). Defaults must already be applied
// on the manifest (LoadTransferFromBytes); zero values are still guarded.
func transferSourceAdmittedN(m *manifest.TransferManifest) (int, error) {
	if m == nil {
		return 0, fmt.Errorf("transfer manifest is nil")
	}
	c := m.Transfer.Concurrency
	if c <= 0 {
		c = manifest.DefaultTransferConcurrency
	}
	l := m.Transfer.Sharding.ListConcurrency
	if l <= 0 {
		l = manifest.DefaultShardListConcurrency
	}
	return connpolicy.TransferSourceAdmittedN(c, l, m.Transfer.Sharding.Enabled)
}

// transferDestAdmittedN resolves non-reflow transfer destination N (workers only).
func transferDestAdmittedN(m *manifest.TransferManifest) (int, error) {
	if m == nil {
		return 0, fmt.Errorf("transfer manifest is nil")
	}
	c := m.Transfer.Concurrency
	if c <= 0 {
		c = manifest.DefaultTransferConcurrency
	}
	return connpolicy.TransferDestAdmittedN(c)
}

// contentEnumeratorByClient maps commandSourceProviderID → true when any input
// for that HTTP client identity is a prefix/glob that may LIST while workers
// HEAD on the same client. Classification is per client (S0 lock), not global.
func contentEnumeratorByClient(inputs []string) map[string]bool {
	out := make(map[string]bool)
	for _, raw := range inputs {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "{") {
			// Empty or JSONL index/error records are key-based, not LIST enumerators.
			continue
		}
		parsed, err := uri.ParseURI(line)
		if err != nil {
			continue
		}
		if !parsed.IsPrefix() && !parsed.IsPattern() {
			continue
		}
		target := commandSourceTargetForRead(parsed)
		id := commandSourceProviderID(target.ProviderURI)
		if id == "" {
			continue
		}
		out[id] = true
	}
	return out
}

// contentAdmittedNForClient resolves content head/probe pool N for one
// provider-identity client from worker concurrency and that client's enumerator bit.
func contentAdmittedNForClient(workers int, clientID string, enumByClient map[string]bool) (int, error) {
	return connpolicy.ContentWorkerPlusEnumerator(workers, enumByClient[clientID])
}

// contentClientIdentitiesFromInputs collects every provider identity that may
// construct from the input set (exact keys, prefix/glob, JSONL base_uri).
func contentClientIdentitiesFromInputs(inputs []string) map[string]struct{} {
	out := make(map[string]struct{})
	add := func(src *uri.ObjectURI) {
		id := commandSourceProviderID(src)
		if id != "" {
			out[id] = struct{}{}
		}
	}
	for _, raw := range inputs {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "{") {
			var env struct {
				Type string          `json:"type"`
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal([]byte(line), &env); err != nil {
				continue
			}
			if env.Type != "gonimbus.index.object.v1" {
				continue
			}
			var data struct {
				BaseURI string `json:"base_uri"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil || strings.TrimSpace(data.BaseURI) == "" {
				continue
			}
			parsed, err := uri.ParseURI(data.BaseURI)
			if err != nil {
				continue
			}
			add(commandSourceTargetForRead(parsed).ProviderURI)
			continue
		}
		parsed, err := uri.ParseURI(line)
		if err != nil {
			continue
		}
		add(commandSourceTargetForRead(parsed).ProviderURI)
	}
	return out
}

// resolveContentAdmittedByClient pre-resolves admitted N for every client
// identity in inputs before any provider construction. Returns typed
// errConnPoolAdmission on formula failure (no construction attempted).
func resolveContentAdmittedByClient(workers int, inputs []string) (map[string]int, error) {
	enum := contentEnumeratorByClient(inputs)
	ids := contentClientIdentitiesFromInputs(inputs)
	for id := range enum {
		ids[id] = struct{}{}
	}
	out := make(map[string]int, len(ids))
	for id := range ids {
		n, err := contentAdmittedNForClient(workers, id, enum)
		if err != nil {
			return nil, wrapConnPoolAdmission("content", err)
		}
		out[id] = n
	}
	return out, nil
}

// openContentHeadProvider constructs a content-head provider from a
// pre-resolved per-client admitted-N map (no formula work at open time).
func openContentHeadProvider(ctx context.Context, src *uri.ObjectURI, admittedByClient map[string]int) (provider.Provider, error) {
	id := commandSourceProviderID(src)
	n, ok := admittedByClient[id]
	if !ok {
		return nil, wrapConnPoolAdmission("content", fmt.Errorf("no pre-resolved admitted N for client %q", id))
	}
	return newContentHeadProvider(ctx, src, n)
}

// openContentProbeProvider constructs a content-probe provider from a
// pre-resolved per-client admitted-N map.
func openContentProbeProvider(ctx context.Context, src *uri.ObjectURI, admittedByClient map[string]int) (contentProbeProvider, error) {
	id := commandSourceProviderID(src)
	n, ok := admittedByClient[id]
	if !ok {
		return nil, wrapConnPoolAdmission("content", fmt.Errorf("no pre-resolved admitted N for client %q", id))
	}
	return newContentProbeProvider(ctx, src, n)
}

// resolveTransferAdmittedNs pre-resolves source and dest admitted N without
// constructing providers. Returns typed errConnPoolAdmission on formula failure.
func resolveTransferAdmittedNs(m *manifest.TransferManifest) (srcN, dstN int, err error) {
	srcN, err = transferSourceAdmittedN(m)
	if err != nil {
		return 0, 0, wrapConnPoolAdmission("transfer-source", err)
	}
	dstN, err = transferDestAdmittedN(m)
	if err != nil {
		return 0, 0, wrapConnPoolAdmission("transfer-dest", err)
	}
	return srcN, dstN, nil
}

// openTransferProviders is the production construction boundary for non-reflow
// transfer: resolve composite admitted N first (typed admission error, no
// construction), then open source and dest. Construction errors are plain
// provider errors (not reclassified by message text).
func openTransferProviders(ctx context.Context, m *manifest.TransferManifest) (src, dst provider.Provider, err error) {
	srcN, dstN, err := resolveTransferAdmittedNs(m)
	if err != nil {
		return nil, nil, err
	}
	src, err = createTransferProvider(ctx, m.Source, srcN)
	if err != nil {
		return nil, nil, err
	}
	dst, err = createTransferProvider(ctx, m.Target, dstN)
	if err != nil {
		_ = src.Close()
		return nil, nil, err
	}
	return src, dst, nil
}

// openTreeProvider is the production construction boundary for tree: resolve
// admitted N from depth/parallel flags (typed admission), then construct.
func openTreeProvider(ctx context.Context, objURI *uri.ObjectURI, depth, parallel int) (provider.Provider, error) {
	admittedN, err := treeAdmittedN(depth, parallel)
	if err != nil {
		return nil, wrapConnPoolAdmission("tree", err)
	}
	return createTreeProvider(ctx, objURI, admittedN)
}

// treeAdmittedN returns pool admitted N for tree: depth<=0 → 1 (SDK-default
// zero fields); depth>0 → resolved treeParallel (must already be >= 1).
func treeAdmittedN(depth, parallel int) (int, error) {
	if depth <= 0 {
		return 1, nil
	}
	if parallel < 1 {
		return 0, fmt.Errorf("tree parallel must be >= 1 when depth > 0, got %d", parallel)
	}
	return parallel, nil
}
