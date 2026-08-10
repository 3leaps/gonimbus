package reflowthroughput

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Controls for concurrent corpus staging. Each drives the real uploadCorpus
// through an injected stager, so reverting the implementation to its serial
// form fails them rather than leaving them green against a copy of the loop.

// fakeStager stands in for the object store, recording observed concurrency and
// the order keys were staged in.
type fakeStager struct {
	mu        sync.Mutex
	inFlight  int
	maxSeen   int
	staged    []string
	delay     time.Duration
	failOnKey string
}

func (f *fakeStager) Stage(ctx context.Context, key string, body []byte) (int64, string, error) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxSeen {
		f.maxSeen = f.inFlight
	}
	f.staged = append(f.staged, key)
	f.mu.Unlock()

	// Hold the slot so genuine overlap is observable. A serial implementation
	// cannot raise maxSeen above 1 no matter how long this sleeps.
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
		}
	}

	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()

	if f.failOnKey != "" && strings.HasSuffix(key, f.failOnKey) {
		return 0, "", fmt.Errorf("synthetic stage failure on %s", key)
	}
	return int64(len(body)), "etag-" + key, nil
}

func (f *fakeStager) snapshot() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.staged), f.maxSeen
}

// buildCorpus materializes a real on-disk corpus so uploadCorpus exercises its
// actual file reads and manifest ordering.
func buildCorpus(t *testing.T, n int) GeneratedCorpus {
	t.Helper()
	root := t.TempDir()
	entries := make([]ManifestEntry, 0, n)
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("entity=%04d/device=%04d/date=2026-01-17/object-%06d.xml", i%8, i%3, i)
		abs := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o750); err != nil {
			t.Fatal(err)
		}
		body := []byte(fmt.Sprintf("<r id=\"%d\"/>", i))
		if err := os.WriteFile(abs, body, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, ManifestEntry{RelativeKey: rel, SizeBytes: int64(len(body))})
	}
	return GeneratedCorpus{Root: root, Manifest: Manifest{Entries: entries}}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path) // #nosec G304 -- test-owned path
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
}

// The load-bearing control: staging must actually overlap. The serial Put+Head
// loop this replaced is bounded by 1/(2*RTT) and cannot stage a large corpus
// inside any sane run budget.
func TestCorpusUploadRunsConcurrently(t *testing.T) {
	corpus := buildCorpus(t, 64)
	stager := &fakeStager{delay: 3 * time.Millisecond}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	if err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 16); err != nil {
		t.Fatalf("uploadCorpus: %v", err)
	}

	staged, maxSeen := stager.snapshot()
	if maxSeen < 2 {
		t.Fatalf("observed max concurrency %d: staging is serialized", maxSeen)
	}
	if staged != 64 {
		t.Fatalf("staged %d objects, want 64", staged)
	}
}

// Concurrency must not leak completion order into the emitted fixture: the
// measured child must receive the same input the serial form produced, or the
// A/B across this change is not comparable.
func TestCorpusUploadPreservesManifestOrder(t *testing.T) {
	corpus := buildCorpus(t, 64)
	stager := &fakeStager{delay: 2 * time.Millisecond}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	if err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 16); err != nil {
		t.Fatalf("uploadCorpus: %v", err)
	}

	lines := readLines(t, out)
	if len(lines) != len(corpus.Manifest.Entries) {
		t.Fatalf("emitted %d lines, want %d", len(lines), len(corpus.Manifest.Entries))
	}
	for i, e := range corpus.Manifest.Entries {
		if !strings.Contains(lines[i], e.RelativeKey) {
			t.Fatalf("line %d does not carry %q: completion order leaked into the emitted input", i, e.RelativeKey)
		}
	}

	// Guard the control itself. If nothing raced, ordering held trivially and
	// this test proved nothing about the concurrent path.
	if _, maxSeen := stager.snapshot(); maxSeen < 2 {
		t.Fatalf("control is vacuous: no overlap occurred (max concurrency %d)", maxSeen)
	}
}

// A failure must stop the fan-out rather than let siblings keep writing objects
// that then have to be cleaned up.
func TestCorpusUploadFailureStopsSiblings(t *testing.T) {
	corpus := buildCorpus(t, 512)
	stager := &fakeStager{delay: time.Millisecond, failOnKey: "object-000005.xml"}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 8)
	if err == nil {
		t.Fatal("expected the synthetic failure to propagate")
	}

	staged, _ := stager.snapshot()
	if staged == len(corpus.Manifest.Entries) {
		t.Fatalf("all %d objects staged after a failure: cancellation did not reach the siblings", staged)
	}
	// A failed run must not leave a half-written input file behind for the
	// child to consume.
	if _, statErr := os.Stat(out); statErr == nil {
		t.Fatal("input file written despite a staging failure")
	}
}

// CorpusUploadConcurrency must fall back rather than return a value that would
// serialize the fan-out or panic it.
func TestCorpusUploadConcurrencyOverride(t *testing.T) {
	t.Setenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY", "")
	if got := CorpusUploadConcurrency(); got != DefaultCorpusUploadConcurrency {
		t.Fatalf("unset = %d, want default %d", got, DefaultCorpusUploadConcurrency)
	}

	t.Setenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY", "4")
	if got := CorpusUploadConcurrency(); got != 4 {
		t.Fatalf("override = %d, want 4", got)
	}

	for _, bad := range []string{"0", "-3", "banana"} {
		t.Setenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY", bad)
		if got := CorpusUploadConcurrency(); got != DefaultCorpusUploadConcurrency {
			t.Fatalf("%q = %d, want default %d", bad, got, DefaultCorpusUploadConcurrency)
		}
	}
}

// The provider config must size the connection pool through the shared product
// resolver (provider.ResolveConnectionPool). Left at zero the S3 provider hands
// the SDK a nil HTTP client and it falls back to Go's default transport, which
// keeps 2 idle connections per host -- a 16-way fan-out would then be mostly
// TLS handshakes and the concurrency above would not deliver.
func TestProviderConfigSizesConnectionPool(t *testing.T) {
	t.Setenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY", "12")
	cfg := BYOS3Config{Bucket: "b"}.ProviderConfig()

	want, err := provider.ResolveConnectionPool(12)
	if err != nil {
		t.Fatalf("ResolveConnectionPool(12): %v", err)
	}
	if cfg.MaxIdleConnsPerHost != want.MaxIdleConnsPerHost || cfg.MaxConnsPerHost != want.MaxConnsPerHost {
		t.Fatalf("pool knobs = idle=%d max=%d, want idle=%d max=%d (single authority: ResolveConnectionPool)",
			cfg.MaxIdleConnsPerHost, cfg.MaxConnsPerHost, want.MaxIdleConnsPerHost, want.MaxConnsPerHost)
	}
	if cfg.MaxIdleConnsPerHost <= 2 {
		t.Fatalf("pool of %d is at or below Go's default of 2: staging would be handshake-bound", cfg.MaxIdleConnsPerHost)
	}
}

// N < 2 must leave SDK defaults (zero policy), matching product
// ResolveConnectionPool semantics — not invent 1/1 knobs.
func TestProviderConfigPoolUsesSharedResolverForN1(t *testing.T) {
	t.Setenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY", "1")
	cfg := BYOS3Config{Bucket: "b"}.ProviderConfig()
	if cfg.MaxIdleConnsPerHost != 0 || cfg.MaxConnsPerHost != 0 {
		t.Fatalf("N=1 pool knobs = idle=%d max=%d, want 0/0 (SDK defaults via ResolveConnectionPool)",
			cfg.MaxIdleConnsPerHost, cfg.MaxConnsPerHost)
	}
}

// A corpus smaller than the fan-out must still stage every object exactly once.
func TestCorpusUploadTinyCorpus(t *testing.T) {
	corpus := buildCorpus(t, 3)
	stager := &fakeStager{}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	if err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 16); err != nil {
		t.Fatalf("uploadCorpus: %v", err)
	}
	staged, _ := stager.snapshot()
	if staged != 3 {
		t.Fatalf("staged %d, want 3", staged)
	}
	if got := len(readLines(t, out)); got != 3 {
		t.Fatalf("emitted %d lines, want 3", got)
	}
}

// Concurrency 1 must remain correct — the override allows it for gentle lanes.
func TestCorpusUploadSerialOverrideStillCorrect(t *testing.T) {
	corpus := buildCorpus(t, 8)
	stager := &fakeStager{}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	if err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 1); err != nil {
		t.Fatalf("uploadCorpus: %v", err)
	}
	lines := readLines(t, out)
	for i, e := range corpus.Manifest.Entries {
		if !strings.Contains(lines[i], e.RelativeKey) {
			t.Fatalf("line %d does not carry %q", i, e.RelativeKey)
		}
	}
}

// Cancellation that does not originate from a staging failure — a parent run
// budget expiring mid-staging — must not be reported as success. Before this
// was fixed the dispatch loop's select skipped the entry and kept looping, so
// the function drained the range without staging, left empty slots, and
// returned nil: a truncated fixture published as a complete one, silently
// changing what the measured child consumed.
func TestCorpusUploadParentCancellationIsNotSuccess(t *testing.T) {
	corpus := buildCorpus(t, 256)
	stager := &fakeStager{delay: 2 * time.Millisecond}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel once staging is demonstrably underway but far from finished.
	go func() {
		for {
			if n, _ := stager.snapshot(); n >= 8 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	err := uploadCorpus(ctx, stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 4)
	if err == nil {
		t.Fatal("parent cancellation reported as success: a truncated fixture would be handed to the measured child")
	}

	staged, _ := stager.snapshot()
	if staged >= len(corpus.Manifest.Entries) {
		t.Fatalf("control is vacuous: staging completed (%d objects) before cancellation landed", staged)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Fatal("input file published despite interrupted staging")
	}
}

// The published fixture must never contain a gap, whatever path produced it.
func TestCorpusUploadRejectsIncompleteFixture(t *testing.T) {
	corpus := buildCorpus(t, 16)
	stager := &fakeStager{}
	out := filepath.Join(t.TempDir(), "reflow.input.jsonl")

	if err := uploadCorpus(context.Background(), stager, BYOS3Config{Bucket: "b"}, corpus, "src/", out, 4); err != nil {
		t.Fatalf("uploadCorpus: %v", err)
	}
	for i, l := range readLines(t, out) {
		if strings.TrimSpace(l) == "" {
			t.Fatalf("line %d is empty: a gap in the fixture changes the measurement silently", i)
		}
	}
}
