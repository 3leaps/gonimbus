package crawler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider implements provider.Provider for testing.
type mockProvider struct {
	objects   map[string][]provider.ObjectSummary // prefix -> objects
	listDelay time.Duration
	listErr   error
	headErr   error
	mu        sync.Mutex
	listCalls int
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		objects: make(map[string][]provider.ObjectSummary),
	}
}

func (m *mockProvider) addObjects(prefix string, objs ...provider.ObjectSummary) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[prefix] = append(m.objects[prefix], objs...)
}

func (m *mockProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	m.mu.Lock()
	m.listCalls++
	delay := m.listDelay
	err := m.listErr
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Find objects matching prefix
	var result []provider.ObjectSummary
	for p, objs := range m.objects {
		if opts.Prefix == "" || p == opts.Prefix || len(p) >= len(opts.Prefix) && p[:len(opts.Prefix)] == opts.Prefix {
			result = append(result, objs...)
		}
	}

	return &provider.ListResult{
		Objects:     result,
		IsTruncated: false,
	}, nil
}

func (m *mockProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	return nil, provider.ErrNotFound
}

func (m *mockProvider) Close() error {
	return nil
}

// mockWriter implements output.Writer for testing.
type mockWriter struct {
	mu       sync.Mutex
	objects  []*output.ObjectRecord
	errors   []*output.ErrorRecord
	progress []*output.ProgressRecord
	summary  *output.SummaryRecord

	writeDelay time.Duration
	writeErr   error

	objectCount atomic.Int64
}

func newMockWriter() *mockWriter {
	return &mockWriter{}
}

func (w *mockWriter) WriteObject(ctx context.Context, obj *output.ObjectRecord) error {
	if w.writeDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.writeDelay):
		}
	}

	if w.writeErr != nil {
		return w.writeErr
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.objects = append(w.objects, obj)
	w.objectCount.Add(1)
	return nil
}

func (w *mockWriter) WriteError(ctx context.Context, err *output.ErrorRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errors = append(w.errors, err)
	return nil
}

func (w *mockWriter) WriteProgress(ctx context.Context, prog *output.ProgressRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.progress = append(w.progress, prog)
	return nil
}

func (w *mockWriter) WriteSummary(ctx context.Context, sum *output.SummaryRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.summary = sum
	return nil
}

func (w *mockWriter) WritePreflight(ctx context.Context, preflight *output.PreflightRecord) error {
	// Crawler package doesn't emit preflight records; ignore.
	return nil
}

func (w *mockWriter) WriteTransfer(ctx context.Context, transfer *output.TransferRecord) error {
	return nil
}

func (w *mockWriter) WriteSkip(ctx context.Context, skip *output.SkipRecord) error {
	return nil
}

func (w *mockWriter) Close() error {
	return nil
}

func (w *mockWriter) getObjects() []*output.ObjectRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]*output.ObjectRecord, len(w.objects))
	copy(result, w.objects)
	return result
}

func (w *mockWriter) getProgress() []*output.ProgressRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]*output.ProgressRecord, len(w.progress))
	copy(result, w.progress)
	return result
}

func TestNew(t *testing.T) {
	p := newMockProvider()
	m, _ := match.New(match.Config{Includes: []string{"**"}})
	w := newMockWriter()

	c := New(p, m, w, "job-123", DefaultConfig())

	assert.NotNil(t, c)
	assert.Equal(t, 4, c.config.Concurrency)
	assert.Equal(t, 1000, c.config.ChannelBuffer)
	assert.Equal(t, 1000, c.config.ProgressEvery)
	assert.Nil(t, c.limiter) // No rate limit by default
}

func TestNew_WithRateLimit(t *testing.T) {
	p := newMockProvider()
	m, _ := match.New(match.Config{Includes: []string{"**"}})
	w := newMockWriter()

	cfg := DefaultConfig()
	cfg.RateLimit = 10.0

	c := New(p, m, w, "job-123", cfg)

	assert.NotNil(t, c.limiter)
}

func TestCrawler_Run_BasicCrawl(t *testing.T) {
	p := newMockProvider()
	p.addObjects("data/",
		provider.ObjectSummary{Key: "data/file1.txt", Size: 100, ETag: "abc"},
		provider.ObjectSummary{Key: "data/file2.txt", Size: 200, ETag: "def"},
	)

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(2), summary.ObjectsListed)
	assert.Equal(t, int64(2), summary.ObjectsMatched)
	assert.Equal(t, int64(300), summary.BytesTotal)
	assert.Equal(t, int64(0), summary.Errors)

	objects := w.getObjects()
	assert.Len(t, objects, 2)
}

func TestCrawler_Run_PatternFiltering(t *testing.T) {
	p := newMockProvider()
	// Objects must be under the derived prefix "data/" for the matcher
	p.addObjects("data/",
		provider.ObjectSummary{Key: "data/file.txt", Size: 100},
		provider.ObjectSummary{Key: "data/file.json", Size: 200},
		provider.ObjectSummary{Key: "data/subdir/file.txt", Size: 300},
	)

	m, err := match.New(match.Config{Includes: []string{"data/**/*.txt"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(3), summary.ObjectsListed)
	assert.Equal(t, int64(2), summary.ObjectsMatched) // file.txt and subdir/file.txt
	assert.Equal(t, int64(400), summary.BytesTotal)

	objects := w.getObjects()
	assert.Len(t, objects, 2)
}

func TestCrawler_Run_MetadataFiltering(t *testing.T) {
	now := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	p := newMockProvider()
	p.addObjects("data/",
		provider.ObjectSummary{Key: "data/small.txt", Size: 100, LastModified: now},
		provider.ObjectSummary{Key: "data/big.txt", Size: 2000, LastModified: now},
	)

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	f, err := match.NewFilterFromConfig(&match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1KB"},
	})
	require.NoError(t, err)
	require.NotNil(t, f)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig()).WithFilter(f)

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(2), summary.ObjectsListed)
	assert.Equal(t, int64(1), summary.ObjectsMatched)
	assert.Equal(t, int64(2000), summary.BytesTotal)

	objects := w.getObjects()
	assert.Len(t, objects, 1)
	assert.Equal(t, "data/big.txt", objects[0].Key)
}

func TestCrawler_Run_HiddenFilesExcluded(t *testing.T) {
	p := newMockProvider()
	// Pattern ** with empty prefix means full bucket listing
	p.addObjects("",
		provider.ObjectSummary{Key: "data/file.txt", Size: 100},
		provider.ObjectSummary{Key: "data/.hidden", Size: 200},
		provider.ObjectSummary{Key: ".git/config", Size: 300},
	)

	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(3), summary.ObjectsListed)
	assert.Equal(t, int64(1), summary.ObjectsMatched) // Only data/file.txt (hidden excluded)

	objects := w.getObjects()
	assert.Len(t, objects, 1)
	assert.Equal(t, "data/file.txt", objects[0].Key)
}

func TestCrawler_Run_ContextCancellation(t *testing.T) {
	p := newMockProvider()
	p.listDelay = 100 * time.Millisecond
	p.addObjects("data/",
		provider.ObjectSummary{Key: "data/file1.txt", Size: 100},
	)

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = c.Run(ctx)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}

func TestCrawler_Run_ProgressEmission(t *testing.T) {
	p := newMockProvider()

	// Add enough objects to trigger progress
	for i := 0; i < 15; i++ {
		p.addObjects("data/", provider.ObjectSummary{
			Key:  "data/file" + string(rune('a'+i)) + ".txt",
			Size: int64(100 * (i + 1)),
		})
	}

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()

	cfg := DefaultConfig()
	cfg.ProgressEvery = 5 // Emit progress every 5 objects

	c := New(p, m, w, "job-123", cfg)

	_, err = c.Run(context.Background())
	require.NoError(t, err)

	progress := w.getProgress()
	// Should have: starting + at least 2 progress (at 5 and 10) + complete
	assert.GreaterOrEqual(t, len(progress), 4)

	// First should be starting
	assert.Equal(t, output.PhaseStarting, progress[0].Phase)

	// Last should be complete
	assert.Equal(t, output.PhaseComplete, progress[len(progress)-1].Phase)
}

func TestCrawler_Run_AccessDeniedError(t *testing.T) {
	p := newMockProvider()
	p.listErr = provider.ErrAccessDenied

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err) // Access denied is non-fatal

	assert.Equal(t, int64(1), summary.Errors)

	w.mu.Lock()
	assert.Len(t, w.errors, 1)
	assert.Equal(t, output.ErrCodeAccessDenied, w.errors[0].Code)
	w.mu.Unlock()
}

func TestCrawler_Run_ThrottledError(t *testing.T) {
	p := newMockProvider()
	p.listErr = provider.ErrThrottled

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err) // Throttling is non-fatal

	assert.Equal(t, int64(1), summary.Errors)

	w.mu.Lock()
	assert.Len(t, w.errors, 1)
	assert.Equal(t, output.ErrCodeThrottled, w.errors[0].Code)
	w.mu.Unlock()
}

func TestCrawler_Run_MultiplePrefixes(t *testing.T) {
	p := newMockProvider()
	p.addObjects("data/2024/",
		provider.ObjectSummary{Key: "data/2024/file1.txt", Size: 100},
	)
	p.addObjects("data/2025/",
		provider.ObjectSummary{Key: "data/2025/file2.txt", Size: 200},
	)

	m, err := match.New(match.Config{Includes: []string{"data/2024/**", "data/2025/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(2), summary.ObjectsMatched)
	assert.Equal(t, int64(300), summary.BytesTotal)
	assert.Len(t, summary.Prefixes, 2)
}

func TestCrawler_Run_Summary(t *testing.T) {
	p := newMockProvider()
	p.addObjects("data/",
		provider.ObjectSummary{Key: "data/file.txt", Size: 1000},
	)

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	// Verify summary was written
	w.mu.Lock()
	defer w.mu.Unlock()

	assert.NotNil(t, w.summary)
	assert.Equal(t, int64(1), w.summary.ObjectsMatched)
	assert.Equal(t, int64(1000), w.summary.BytesTotal)
	assert.NotEmpty(t, w.summary.DurationHuman)
	assert.Greater(t, summary.Duration, time.Duration(0))
}

func TestCrawler_Run_Concurrency(t *testing.T) {
	p := newMockProvider()
	p.listDelay = 50 * time.Millisecond

	// Add objects under multiple prefixes
	for i := 0; i < 10; i++ {
		prefix := "prefix" + string(rune('0'+i)) + "/"
		p.addObjects(prefix, provider.ObjectSummary{
			Key:  prefix + "file.txt",
			Size: 100,
		})
	}

	// Create matcher with multiple prefixes
	includes := make([]string, 10)
	for i := 0; i < 10; i++ {
		includes[i] = "prefix" + string(rune('0'+i)) + "/**"
	}
	m, err := match.New(match.Config{Includes: includes})
	require.NoError(t, err)

	w := newMockWriter()

	cfg := DefaultConfig()
	cfg.Concurrency = 5 // Run 5 concurrent list operations

	c := New(p, m, w, "job-123", cfg)

	start := time.Now()
	summary, err := c.Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)

	assert.Equal(t, int64(10), summary.ObjectsMatched)

	// With concurrency=5 and 10 prefixes at 50ms each, should complete in ~100-200ms.
	// Without concurrency it would take ~500ms.
	// Use a generous upper bound (500ms) to avoid flakiness on loaded CI machines,
	// while still verifying concurrency provides meaningful speedup.
	assert.Less(t, elapsed, 500*time.Millisecond, "concurrent crawl should be faster than sequential")

	// Also verify we're actually getting speedup (should be < 400ms with any concurrency)
	// This is a sanity check that concurrency is working at all.
	if elapsed > 400*time.Millisecond {
		t.Logf("Warning: elapsed time %v is slower than expected, may indicate concurrency issues", elapsed)
	}
}

func TestCrawler_Run_EmptyBucket(t *testing.T) {
	p := newMockProvider()

	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)

	w := newMockWriter()
	c := New(p, m, w, "job-123", DefaultConfig())

	summary, err := c.Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(0), summary.ObjectsListed)
	assert.Equal(t, int64(0), summary.ObjectsMatched)
	assert.Equal(t, int64(0), summary.BytesTotal)
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, 4, cfg.Concurrency)
	assert.Equal(t, 1000, cfg.ChannelBuffer)
	assert.Equal(t, float64(0), cfg.RateLimit)
	assert.Equal(t, 1000, cfg.ProgressEvery)
}

// Benchmark for crawler throughput
func BenchmarkCrawler_Run(b *testing.B) {
	p := newMockProvider()

	// Add 10000 objects
	for i := 0; i < 10000; i++ {
		p.addObjects("data/", provider.ObjectSummary{
			Key:  "data/file" + string(rune(i)) + ".txt",
			Size: 1000,
		})
	}

	m, _ := match.New(match.Config{Includes: []string{"data/**"}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := newMockWriter()
		c := New(p, m, w, "job-123", DefaultConfig())
		_, _ = c.Run(context.Background())
	}
}
