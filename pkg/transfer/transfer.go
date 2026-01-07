// Package transfer implements streaming copy/move operations for cloud object storage.
package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/shard"
)

type Config struct {
	Concurrency  int
	OnExists     string // skip | overwrite | fail
	Dedup        DedupConfig
	Mode         string // copy | move
	PathTemplate string

	// RetryBufferMaxMemoryBytes controls how large an object we buffer in memory to
	// make the PUT request body seekable for SDK retries.
	// Larger objects are spooled to a temp file.
	RetryBufferMaxMemoryBytes int64

	Sharding ShardingConfig
}

type ShardingConfig struct {
	Enabled         bool
	Depth           int
	MaxShards       int
	ListConcurrency int
	Delimiter       string
}

type DedupConfig struct {
	Enabled  bool
	Strategy string // etag | key | none
}

func DefaultConfig() Config {
	return Config{
		Concurrency: 16,
		OnExists:    "skip",
		Mode:        "copy",
		Dedup: DedupConfig{
			Enabled:  true,
			Strategy: "etag",
		},
	}
}

type Summary struct {
	ObjectsListed      int64
	ObjectsMatched     int64
	ObjectsTransferred int64
	BytesTransferred   int64
	Errors             int64
	Duration           time.Duration
}

type Transfer struct {
	src     provider.Provider
	dst     provider.Provider
	matcher *match.Matcher
	writer  output.Writer
	jobID   string
	cfg     Config
	mapper  *PathTemplate

	listed      atomic.Int64
	matched     atomic.Int64
	transferred atomic.Int64
	bytes       atomic.Int64
	errors      atomic.Int64
}

func New(src provider.Provider, dst provider.Provider, matcher *match.Matcher, writer output.Writer, jobID string, cfg Config) *Transfer {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConfig().Concurrency
	}
	if cfg.OnExists == "" {
		cfg.OnExists = DefaultConfig().OnExists
	}
	if cfg.Mode == "" {
		cfg.Mode = DefaultConfig().Mode
	}
	if cfg.Dedup.Strategy == "" {
		cfg.Dedup.Strategy = DefaultConfig().Dedup.Strategy
	}

	if cfg.RetryBufferMaxMemoryBytes == 0 {
		cfg.RetryBufferMaxMemoryBytes = DefaultRetryBufferMaxMemoryBytes
	}

	if cfg.Sharding.Depth == 0 {
		cfg.Sharding.Depth = 1
	}
	if cfg.Sharding.ListConcurrency == 0 {
		cfg.Sharding.ListConcurrency = 16
	}
	if cfg.Sharding.Delimiter == "" {
		cfg.Sharding.Delimiter = "/"
	}

	var mapper *PathTemplate
	if cfg.PathTemplate != "" {
		tpl, err := CompilePathTemplate(cfg.PathTemplate)
		if err != nil {
			// Keep constructor non-fatal by deferring to runtime errors.
			// Callers should validate templates before executing.
			mapper = nil
		} else {
			mapper = tpl
		}
	}

	return &Transfer{src: src, dst: dst, matcher: matcher, writer: writer, jobID: jobID, cfg: cfg, mapper: mapper}
}

func (t *Transfer) Run(ctx context.Context) (*Summary, error) {
	start := time.Now()

	prefixes := t.matcher.Prefixes()
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}

	listCh := make(chan objectItem, 1000)
	workCh := make(chan objectItem, 1000)
	errCh := make(chan error, 1)

	// Listing stage: bounded prefix enumeration.
	prefixCh := make(chan string, 1024)
	var listWg sync.WaitGroup
	for i := 0; i < t.cfg.Sharding.ListConcurrency; i++ {
		listWg.Add(1)
		go func() {
			defer listWg.Done()
			for prefix := range prefixCh {
				if err := t.listPrefix(ctx, prefix, listCh); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(prefixCh)
		for _, pfx := range prefixes {
			for _, shardPrefix := range t.expandShardPrefixes(ctx, pfx) {
				prefixCh <- shardPrefix
			}
		}
	}()

	go func() {
		listWg.Wait()
		close(listCh)
	}()

	// Matcher stage
	var matchWg sync.WaitGroup
	matchWg.Add(1)
	go func() {
		defer matchWg.Done()
		for it := range listCh {
			if t.matcher.Match(it.summary.Key) {
				t.matched.Add(1)
				workCh <- it
			}
		}
		close(workCh)
	}()

	// Transfer workers
	var xferWg sync.WaitGroup
	for i := 0; i < t.cfg.Concurrency; i++ {
		xferWg.Add(1)
		go func() {
			defer xferWg.Done()
			for it := range workCh {
				if err := t.transferOne(ctx, it.summary); err != nil {
					t.errors.Add(1)
					_ = t.writer.WriteError(ctx, &output.ErrorRecord{Code: output.ErrCodeInternal, Message: err.Error(), Key: it.summary.Key})
				}
			}
		}()
	}

	// Wait for completion or fatal error
	done := make(chan struct{})
	go func() {
		matchWg.Wait()
		xferWg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		return t.summary(time.Since(start)), err
	case <-done:
		return t.summary(time.Since(start)), nil
	case <-ctx.Done():
		return t.summary(time.Since(start)), ctx.Err()
	}
}

type objectItem struct {
	summary provider.ObjectSummary
	prefix  string
}

func (t *Transfer) expandShardPrefixes(ctx context.Context, basePrefix string) []string {
	if !t.cfg.Sharding.Enabled {
		return []string{basePrefix}
	}

	shards, err := shard.Discover(ctx, t.src, basePrefix, shard.Config{
		Enabled:         true,
		Depth:           t.cfg.Sharding.Depth,
		MaxShards:       t.cfg.Sharding.MaxShards,
		ListConcurrency: t.cfg.Sharding.ListConcurrency,
		Delimiter:       t.cfg.Sharding.Delimiter,
	})
	if err != nil {
		// Best-effort: fall back to base prefix to avoid hard-failing.
		return []string{basePrefix}
	}
	return shards
}

func (t *Transfer) listPrefix(ctx context.Context, prefix string, out chan<- objectItem) error {
	var token string
	for {
		res, err := t.src.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			t.listed.Add(1)
			out <- objectItem{summary: obj, prefix: prefix}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			return nil
		}
		token = res.ContinuationToken
	}
}

func (t *Transfer) transferOne(ctx context.Context, obj provider.ObjectSummary) error {
	srcKey := obj.Key
	dstKey := obj.Key
	if t.mapper != nil {
		mapped, err := t.mapper.Apply(srcKey)
		if err != nil {
			return err
		}
		dstKey = mapped
	}

	// on_exists / dedup
	if t.cfg.OnExists != "overwrite" {
		dstMeta, err := t.dst.Head(ctx, dstKey)
		if err == nil {
			// Exists
			switch t.cfg.OnExists {
			case "fail":
				return fmt.Errorf("target exists: %s", dstKey)
			case "skip":
				if t.cfg.Dedup.Enabled && t.cfg.Dedup.Strategy == "etag" && dstMeta.ETag == obj.ETag {
					return t.writer.WriteSkip(ctx, &output.SkipRecord{SourceKey: srcKey, TargetKey: dstKey, Reason: "dedup.etag"})
				}
				if t.cfg.Dedup.Enabled && t.cfg.Dedup.Strategy == "key" {
					return t.writer.WriteSkip(ctx, &output.SkipRecord{SourceKey: srcKey, TargetKey: dstKey, Reason: "dedup.key"})
				}
				return t.writer.WriteSkip(ctx, &output.SkipRecord{SourceKey: srcKey, TargetKey: dstKey, Reason: "on_exists.skip"})
			}
		}
		if err != nil && !provider.IsNotFound(err) {
			return err
		}
	}

	getter, ok := t.src.(provider.ObjectGetter)
	if !ok {
		return errors.New("source provider does not support GetObject")
	}
	putter, ok := t.dst.(provider.ObjectPutter)
	if !ok {
		return errors.New("target provider does not support PutObject")
	}

	body, size, err := getter.GetObject(ctx, srcKey)
	if err != nil {
		return err
	}

	retryBody, err := newRetryableBody(ctx, body, size, t.cfg.RetryBufferMaxMemoryBytes)
	if err != nil {
		return err
	}
	defer func() { _ = retryBody.Close() }()

	if err := putter.PutObject(ctx, dstKey, retryBody.Reader(), size); err != nil {
		return err
	}

	if err := t.writer.WriteTransfer(ctx, &output.TransferRecord{SourceKey: srcKey, TargetKey: dstKey, Bytes: size}); err != nil {
		return err
	}
	if size > 0 {
		t.bytes.Add(size)
	}
	t.transferred.Add(1)

	if t.cfg.Mode == "move" {
		deleter, ok := t.src.(provider.ObjectDeleter)
		if !ok {
			return errors.New("source provider does not support DeleteObject")
		}
		if err := deleter.DeleteObject(ctx, srcKey); err != nil {
			return err
		}
	}

	return nil
}

func (t *Transfer) summary(d time.Duration) *Summary {
	return &Summary{
		ObjectsListed:      t.listed.Load(),
		ObjectsMatched:     t.matched.Load(),
		ObjectsTransferred: t.transferred.Load(),
		BytesTransferred:   t.bytes.Load(),
		Errors:             t.errors.Load(),
		Duration:           d,
	}
}

// DrainReader ensures io.Reader is fully consumed when needed.
func DrainReader(r io.Reader) {
	_, _ = io.Copy(io.Discard, r)
}
