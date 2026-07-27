package reflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	prefixSelectorURI  = "s3://source-bucket/a/"
	patternSelectorURI = "s3://source-bucket/a/*.xml"
)

// errListPageBudget is returned once a control's scripted provider has been
// asked for more pages than the control scripted, whether by walking past the
// last page or by re-listing an earlier one. It converts a producer that never
// terminates into an assertion failure instead of a hung test — the difference
// between a control that reports a defect and one that stalls CI.
var errListPageBudget = errors.New("listing provider exceeded its scripted page budget")

// listCallBudget bounds total List calls per control. Every control here
// scripts at most a handful of pages, so any run approaching this ceiling is
// re-listing rather than paginating.
const listCallBudget = 32

// errUnknownContinuationToken is returned for a continuation token the provider
// never issued, so a producer that fabricates or drops tokens fails loudly.
var errUnknownContinuationToken = errors.New("listing provider received an unissued continuation token")

// listPage is one scripted List response: the objects it returns, the two
// termination signals it reports, and an optional failure served in place of it.
type listPage struct {
	objects   []provider.ObjectSummary
	truncated bool
	// nextToken is the continuation token this page hands back. The provider
	// serves the following page only when asked with exactly this token.
	nextToken string
	err       error
	// gate, when non-nil, must be closed before this page is served. It is how a
	// control proves the producer requested a later page only after copy workers
	// were already running on an earlier one.
	gate <-chan struct{}
	// gateTimeout bounds that wait so an enumerate-then-copy implementation fails
	// this control rather than deadlocking the suite.
	gateTimeout time.Duration
}

// listingProvider serves a scripted page sequence and counts every source-side
// operation, so a control can pin both what enumeration asked for and what
// planning did NOT do.
type listingProvider struct {
	*countingSourceProvider

	mu        sync.Mutex
	pages     []listPage
	byToken   map[string]int
	listOpts  []provider.ListOptions
	listCalls int

	// onList observes each List call from inside the provider, so a control can
	// sample planning progress at the exact moment a page is requested.
	onList func()
	// onGet observes each object read, which is how a control detects that a copy
	// worker has actually started.
	onGet func(key string)
	// headErr denies the engine's tolerated metadata-recovery HEAD, so a control
	// can hold the producer's own planned values decisive.
	headErr error
}

func (p *listingProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	if p.headErr != nil {
		// Still counted: the control asserts the recovery was attempted and denied.
		_, _ = p.countingSourceProvider.Head(ctx, key)
		return nil, p.headErr
	}
	return p.countingSourceProvider.Head(ctx, key)
}

// headCount and getCount split what countingSourceProvider totals, so a control
// can pin "planning issued no HEAD" separately from "a dry run read no bodies".
func (p *countingSourceProvider) headCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heads
}

func (p *countingSourceProvider) getCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gets
}

func newListingProvider() *listingProvider {
	return &listingProvider{
		countingSourceProvider: newCountingSourceProvider(),
		byToken:                map[string]int{"": 0},
	}
}

// addPage appends a page and stores each object's body, so the same fixture
// serves both enumeration and the copies it feeds.
func (p *listingProvider) addPage(page listPage) *listingProvider {
	for _, obj := range page.objects {
		p.putFixtureAt(obj.Key, strings.Repeat("x", int(obj.Size)), obj.ETag, obj.LastModified)
	}
	p.pages = append(p.pages, page)
	if page.nextToken != "" {
		p.byToken[page.nextToken] = len(p.pages)
	}
	return p
}

func (p *listingProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if p.onGet != nil {
		p.onGet(key)
	}
	return p.countingSourceProvider.GetObject(ctx, key)
}

func (p *listingProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	if p.onList != nil {
		p.onList()
	}
	p.mu.Lock()
	p.listOpts = append(p.listOpts, opts)
	p.listCalls++
	idx, known := p.byToken[opts.ContinuationToken]
	pageCount := len(p.pages)
	overBudget := p.listCalls > listCallBudget
	var page listPage
	if known && idx < pageCount {
		page = p.pages[idx]
	}
	p.mu.Unlock()

	if overBudget {
		return nil, errListPageBudget
	}
	if !known {
		return nil, errUnknownContinuationToken
	}
	if idx >= pageCount {
		return nil, errListPageBudget
	}
	if page.gate != nil {
		timeout := page.gateTimeout
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		select {
		case <-page.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, errEnumerateThenCopy
		}
	}
	if page.err != nil {
		return nil, page.err
	}
	return &provider.ListResult{
		Objects:           page.objects,
		IsTruncated:       page.truncated,
		ContinuationToken: page.nextToken,
	}, nil
}

func (p *listingProvider) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listCalls
}

func (p *listingProvider) options() []provider.ListOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provider.ListOptions(nil), p.listOpts...)
}

func obj(key, etag string, size int64) provider.ObjectSummary {
	return provider.ObjectSummary{Key: key, ETag: etag, Size: size}
}

func objAt(key, etag string, size int64, lastMod time.Time) provider.ObjectSummary {
	return provider.ObjectSummary{Key: key, ETag: etag, Size: size, LastModified: lastMod}
}

// singlePage scripts one terminal page.
func singlePage(objects ...provider.ObjectSummary) *listingProvider {
	return newListingProvider().addPage(listPage{objects: objects})
}

func prefixDryRunConfig(sink EventSink) Config {
	return prefixDryRunConfigWithRewrite(sink, "{dir}/{file}", "{dir}/{file}")
}

// prefixDryRunConfigWithRewrite is for fixtures whose keys are deeper than two
// segments: the rewrite matches segment-for-segment, so a nested key needs a
// template of matching depth.
func prefixDryRunConfigWithRewrite(sink EventSink, from, to string) Config {
	cfg := dryRunConfig(sink)
	cfg.Rewrite = RewriteConfig{From: from, To: to}
	return cfg
}

func recordKeys(records []Record) []string {
	keys := make([]string, 0, len(records))
	for _, rec := range records {
		keys = append(keys, rec.SourceKey)
	}
	sort.Strings(keys)
	return keys
}

// TestPrefixSourceSelectorAndItemAuthorityAreDistinct is the cardinality control
// secrev made mandatory (N2), and the one ObjectSource structurally could not
// provide: for an exact object the selector and the item authority hold the same
// string, so only a prefix can show them diverging.
//
// One selector fans out to N objects. The run reports exactly one source record
// and writes exactly one run-level selector, while each object takes its own
// checkpoint authority keyed by its EXACT object URI — and no item is keyed by
// the selector. The risk class is resume false-skip: an item keyed by the
// selector would make one completed object answer for every object under it.
func TestPrefixSourceSelectorAndItemAuthorityAreDistinct(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := singlePage(obj("a/one.xml", "etag-1", 3), obj("a/two.xml", "etag-2", 3), obj("a/three.xml", "etag-3", 3))
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Len(t, sink.sources, 1, "one selector, one source record")
	require.Equal(t, prefixSelectorURI, sink.sources[0].URI)

	metas := ckpt.written()
	require.Len(t, metas, 1, "one selector, one run-level metadata write")
	require.Equal(t, prefixSelectorURI, metas[0].URI)
	require.Equal(t, "s3", metas[0].Provider)
	require.Equal(t, "source-bucket", metas[0].Bucket)
	require.Empty(t, metas[0].Root, "an object-store selector carries no local root")

	require.Len(t, ckpt.items, 3, "one item authority per enumerated object")
	itemURIs := make([]string, 0, len(ckpt.items))
	for _, item := range ckpt.items {
		require.NotEqual(t, prefixSelectorURI, item.SourceURI,
			"an item keyed by the selector would let one object answer for every object under it")
		itemURIs = append(itemURIs, item.SourceURI)
	}
	sort.Strings(itemURIs)
	require.Equal(t, []string{
		"s3://source-bucket/a/one.xml",
		"s3://source-bucket/a/three.xml",
		"s3://source-bucket/a/two.xml",
	}, itemURIs, "each item authority is its own exact object URI")
}

// TestPrefixSourceItemAuthorityIsLosslessForLegalKeys extends the cardinality
// contract to keys that a generic URL rewrite would damage.
//
// The first control proves the SHAPE (one selector, N items). This one proves
// the VALUES survive: `?` is key syntax under the gonimbus grammar, and a space
// or a non-ASCII byte is an ordinary key character, so an authority routed
// through a display sanitizer would percent-encode or blank them. Two keys
// differing only after a `?` would then share one authority — the same
// false-skip class, reached without any resume bug of its own.
func TestPrefixSourceItemAuthorityIsLosslessForLegalKeys(t *testing.T) {
	keys := []string{
		"a/file?version=one",
		"a/file?version=two",
		"a/lit*eral.xml",
		"a/[backup].xml",
		"a/file with space.xml",
		"a/café.xml",
	}
	objects := make([]provider.ObjectSummary, 0, len(keys))
	for i, key := range keys {
		objects = append(objects, obj(key, fmt.Sprintf("etag-%d", i), 3))
	}

	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := singlePage(objects...)
	dst := newCopyMemoryProvider()

	cfg := positionalCopyConfig(t, dst, sink, ckpt)
	// A flat rewrite keeps every key mapping to its own destination, so this row
	// isolates SOURCE identity from destination arithmetic.
	cfg.Rewrite = RewriteConfig{From: "{_}/{file}", To: "{file}"}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Len(t, ckpt.items, len(keys))
	got := make([]string, 0, len(ckpt.items))
	for _, item := range ckpt.items {
		require.NotEqual(t, prefixSelectorURI, item.SourceURI)
		got = append(got, item.SourceURI)
	}
	want := make([]string, 0, len(keys))
	for _, key := range keys {
		want = append(want, "s3://source-bucket/"+key)
	}
	sort.Strings(got)
	sort.Strings(want)
	require.Equal(t, want, got, "each authority is the listed key, byte for byte")
	require.Len(t, uniqueStrings(got), len(keys), "no two listed objects may share an authority")
}

// TestPrefixSourceFanInKeepsDistinctSourceAuthorities is the same-destination
// row. The checkpoint primary key is (source_uri, dest_uri), so a destination
// collision is exactly where a lossy source authority stops being visible as a
// duplicate row and starts being a silent overwrite of one object's terminal by
// another's.
func TestPrefixSourceFanInKeepsDistinctSourceAuthorities(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := singlePage(
		obj("a/file?version=one", "etag-1", 3),
		obj("a/file?version=two", "etag-2", 3),
	)
	dst := newCopyMemoryProvider()

	cfg := positionalCopyConfig(t, dst, sink, ckpt)
	// Both keys collapse onto ONE destination key by construction.
	cfg.Rewrite = RewriteConfig{From: "{_}/{_}", To: "merged.xml"}
	cfg.Collision = CollisionPolicy{Mode: CollisionOverwrite}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Len(t, ckpt.items, 2)
	authorities := []string{ckpt.items[0].SourceURI, ckpt.items[1].SourceURI}
	sort.Strings(authorities)
	require.Equal(t, []string{
		"s3://source-bucket/a/file?version=one",
		"s3://source-bucket/a/file?version=two",
	}, authorities)
	require.Equal(t, ckpt.items[0].DestURI, ckpt.items[1].DestURI,
		"the fixture is only meaningful if the two objects really share a destination")
}

// TestPrefixSourceResumeSkipsOnlyTheCheckpointedObject is the consequence proof
// for the two controls above, stated where it actually costs data: resume.
//
// One of two question-mark keys is recorded complete. A resumed run must skip
// exactly that object and still copy its sibling. Under a lossy authority both
// keys answer to the same checkpoint row, so the sibling is skipped too and is
// silently never transferred.
func TestPrefixSourceResumeSkipsOnlyTheCheckpointedObject(t *testing.T) {
	const (
		doneKey    = "a/file?version=one"
		pendingKey = "a/file?version=two"
	)
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := singlePage(obj(doneKey, "etag-1", 3), obj(pendingKey, "etag-2", 3))
	dst := newCopyMemoryProvider()

	cfg := positionalCopyConfig(t, dst, sink, ckpt)
	cfg.Rewrite = RewriteConfig{From: "{_}/{file}", To: "{file}"}
	ckpt.markDone("s3://source-bucket/"+doneKey, "s3://dest-bucket/data/file?version=one", "complete")

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Equal(t, 0, dst.writeCount("data/file?version=one"),
		"the checkpointed object must be skipped")
	require.Equal(t, 1, dst.writeCount("data/file?version=two"),
		"its sibling must still be copied; one authority answering for both is the false-skip class")
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// TestItemAuthorityIsLosslessAcrossSourceForms pins the identity boundary where
// it actually lives: the shared copy path, not the prefix producer.
//
// PrefixSource surfaced the defect because it enumerates whatever the bucket
// holds, but ObjectSource and RecordStreamSource construct item authority
// through the same step. An escaped `\?` is a literal key character, so all
// three forms must record the same exact key — a fix that only covered the
// prefix would leave the other two recording a rewritten object.
func TestItemAuthorityIsLosslessAcrossSourceForms(t *testing.T) {
	const (
		spelled = `s3://source-bucket/a/file\?.xml`
		literal = "a/file?.xml"
		wantURI = "s3://source-bucket/a/file?.xml"
	)

	newRun := func(t *testing.T) (*copyMemoryProvider, *copyMemoryProvider, *capabilityCheckpoint, Config) {
		t.Helper()
		log := &eventLog{}
		sink := &orderSink{log: log}
		ckpt := newCapabilityCheckpoint(log)
		src := newCopyMemoryProvider()
		src.putFixture(literal, "payload", "etag-a")
		dst := newCopyMemoryProvider()
		return src, dst, ckpt, positionalCopyConfig(t, dst, sink, ckpt)
	}

	t.Run("ObjectSource", func(t *testing.T) {
		src, dst, ckpt, cfg := newRun(t)
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: spelled})
		require.NoError(t, err)

		require.Len(t, ckpt.items, 1)
		require.Equal(t, wantURI, ckpt.items[0].SourceURI,
			"the escaped literal is a key character; the authority keeps it")
		require.Equal(t, literal, ckpt.items[0].SourceKey)
		require.Equal(t, 1, dst.writeCount("data/"+literal))
	})

	t.Run("PrefixSource", func(t *testing.T) {
		_, dst, ckpt, cfg := newRun(t)
		src := singlePage(obj(literal, "etag-a", 7))
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
		require.NoError(t, err)

		require.Len(t, ckpt.items, 1)
		require.Equal(t, wantURI, ckpt.items[0].SourceURI)
		require.Equal(t, 1, dst.writeCount("data/"+literal))
	})

	t.Run("RecordStreamSource", func(t *testing.T) {
		src, dst, ckpt, cfg := newRun(t)
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), RecordStreamSource{
			Records: strings.NewReader(spelled + "\n"),
			Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
		})
		require.NoError(t, err)

		require.Len(t, ckpt.items, 1)
		require.Equal(t, wantURI, ckpt.items[0].SourceURI)
		require.Equal(t, 1, dst.writeCount("data/"+literal))
	})
}

// TestPrefixSourceCancellationIsNotAnEnumerationFailure pins the precedence the
// producer seam already establishes: a List that fails BECAUSE the context was
// cancelled is that cancellation, not an unlistable source.
//
// The distinction is observable and consequential. A producer-returned error
// outranks ctx.Err() at the seam, so wrapping every List error as a
// SourceEnumerationError would make a cancelled run carry the enumeration-failure
// type — and the command maps that type to external-service-unavailable rather
// than the cancellation disposition.
func TestPrefixSourceCancellationIsNotAnEnumerationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &collectSink{}
	src := newListingProvider()
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)}, truncated: true, nextToken: "token-0"})
	// Page 2 is gated on a channel nothing ever closes, so the only way out of the
	// provider is the cancellation this control triggers on that same call.
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)}, gate: make(chan struct{})})
	calls := 0
	src.onList = func() {
		calls++
		if calls == 2 {
			cancel()
		}
	}

	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, runErr := runner.Run(ctx, PrefixSource{Provider: src, URI: prefixSelectorURI})

	require.ErrorIs(t, runErr, context.Canceled)
	var enumErr *SourceEnumerationError
	require.False(t, errors.As(runErr, &enumErr),
		"a cancelled run must not be reported as a source that could not be enumerated")
	require.NotErrorIs(t, runErr, errEnumerateThenCopy)
}

// TestPrefixSourceEnumerationFailureSelectorIsFaithful pins that the typed
// failure names the selector that actually failed, including a selector whose
// spelling a display sanitizer would rewrite. An operator cannot act on a
// selector reported as something they never typed.
func TestPrefixSourceEnumerationFailureSelectorIsFaithful(t *testing.T) {
	listErr := errors.New("provider refused the listing")
	sink := &collectSink{}
	src := newListingProvider()
	src.addPage(listPage{err: listErr})

	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	const selector = `s3://source-bucket/a/file?.xml`
	_, runErr := runner.Run(context.Background(), PrefixSource{Provider: src, URI: selector})

	var enumErr *SourceEnumerationError
	require.True(t, errors.As(runErr, &enumErr))
	require.Equal(t, selector, enumErr.Selector,
		"a question mark is glob syntax here, not a query to blank out")
	require.Contains(t, enumErr.Error(), selector)
}

// TestPrefixSourcePositionalEventOrder pins that the prefix form reports its run
// through the same ordered sequence ObjectSource established: the run record,
// the resolved-source event, the source-run-metadata setup write, and only then
// enumeration and per-object records.
func TestPrefixSourcePositionalEventOrder(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := singlePage(obj("a/one.xml", "etag-1", 3))
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Equal(t, []string{
		"run",
		"source",
		"source_run_metadata",
		"record:in_progress",
		"record:complete",
		"summary",
	}, log.snapshot())
}

// TestPrefixSourceProcessesEachPageBeforeRequestingTheNext pins the streaming
// shape open item C asks for, stated as what is observable: at the moment the
// producer asks for page N+1, every object from every earlier page has already
// been planned and handed on. The producer therefore holds one page at a time
// and never accumulates the listing across pages.
//
// It is a claim about the enumerate/plan interleaving, not a measurement of heap
// bytes; the later-page barrier control adds the live-copy half.
func TestPrefixSourceProcessesEachPageBeforeRequestingTheNext(t *testing.T) {
	const pages, perPage = 6, 4
	sink := &collectSink{}
	src := newListingProvider()

	// plannedAtListCall[i] is how many records had been emitted when List call i
	// was served. Recorded through the sink, which the provider reads.
	var mu sync.Mutex
	plannedAtListCall := []int{}
	countingSink := &recordCountSink{collectSink: sink}

	for page := 0; page < pages; page++ {
		objects := make([]provider.ObjectSummary, 0, perPage)
		for i := 0; i < perPage; i++ {
			objects = append(objects, obj(fmt.Sprintf("a/p%d-%d.xml", page, i), "etag", 3))
		}
		last := page == pages-1
		src.addPage(listPage{objects: objects, truncated: !last, nextToken: tokenFor(page, last)})
	}
	src.onList = func() {
		mu.Lock()
		plannedAtListCall = append(plannedAtListCall, countingSink.count())
		mu.Unlock()
	}

	runner, err := NewRunner(prefixDryRunConfig(countingSink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Equal(t, pages, src.calls())
	require.Len(t, sink.records, pages*perPage)

	mu.Lock()
	observed := append([]int(nil), plannedAtListCall...)
	mu.Unlock()
	want := make([]int, 0, pages)
	for page := 0; page < pages; page++ {
		want = append(want, page*perPage)
	}
	require.Equal(t, want, observed,
		"page N+1 must be requested only after every object of page N is already planned")
}

func tokenFor(page int, last bool) string {
	if last {
		return ""
	}
	return fmt.Sprintf("token-%d", page)
}

// recordCountSink exposes a running count of emitted records so a provider can
// observe planning progress from inside a List call.
type recordCountSink struct {
	*collectSink
	mu sync.Mutex
	n  int
}

func (s *recordCountSink) OnRecord(ctx context.Context, rec Record) error {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	return s.collectSink.OnRecord(ctx, rec)
}

func (s *recordCountSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// errEnumerateThenCopy is served when a scripted later page waits past its
// deadline for copy workers that never started — the signature of a producer
// that enumerated the whole selector before dispatching anything.
var errEnumerateThenCopy = errors.New("later page was requested before any copy worker became active")

// TestPrefixSourceRequestsLaterPageOnlyWhileCopiesRun is the live-copy half of
// the streaming contract: page 2 is not served until a copy worker is
// demonstrably active on a page-1 object.
//
// The control is built so the wrong implementation FAILS rather than passes
// slowly. An enumerate-then-copy producer would sit in List waiting for a worker
// that cannot start until enumeration finishes; that wait is bounded and
// surfaces as errEnumerateThenCopy, which this control asserts never happens.
func TestPrefixSourceRequestsLaterPageOnlyWhileCopiesRun(t *testing.T) {
	workerActive := make(chan struct{})
	src := newListingProvider()
	src.onGet = func(string) {
		select {
		case <-workerActive:
		default:
			close(workerActive)
		}
	}
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)}, truncated: true, nextToken: "token-0"})
	src.addPage(listPage{
		objects: []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)},
		gate:    workerActive,
	})

	sink := &collectSink{}
	dst := newCopyMemoryProvider()
	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, newMemCheckpoint()))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})

	require.NotErrorIs(t, err, errEnumerateThenCopy,
		"the producer must dispatch from inside the page loop, not after enumeration completes")
	require.NoError(t, err)
	require.Equal(t, 2, src.calls())
	require.Equal(t, 1, dst.writeCount("data/a/one.xml"))
	require.Equal(t, 1, dst.writeCount("data/a/two.xml"))
}

// TestPrefixSourceMatcherParity pins the matcher construction and application
// the CLI pool performs: a matcher exists ONLY for a pattern, is built from the
// full glob, and is applied to the FULL object key.
//
// The subset case discriminates on that last point. Listing runs under the
// DERIVED prefix `a/`, so a key made relative to it would be `one.xml`, which
// the pattern `a/*.xml` does not match; the subset survives only because the
// whole key is tested.
func TestPrefixSourceMatcherParity(t *testing.T) {
	objects := []provider.ObjectSummary{
		obj("a/one.xml", "etag-1", 3),
		obj("a/two.json", "etag-2", 3),
	}

	for name, tc := range map[string]struct {
		uri      string
		wantKeys []string
	}{
		"prefix admits every listed object": {
			uri:      prefixSelectorURI,
			wantKeys: []string{"a/one.xml", "a/two.json"},
		},
		"pattern filters a subset of the full keys": {
			uri:      patternSelectorURI,
			wantKeys: []string{"a/one.xml"},
		},
		"pattern matching nothing is an empty, successful run": {
			uri:      "s3://source-bucket/a/*.parquet",
			wantKeys: []string{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &collectSink{}
			src := singlePage(objects...)
			runner, err := NewRunner(prefixDryRunConfig(sink))
			require.NoError(t, err)
			_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: tc.uri})
			require.NoError(t, err, "a selector matching nothing is still a successful run")

			require.Equal(t, tc.wantKeys, recordKeys(sink.records))
			opts := src.options()
			require.Len(t, opts, 1)
			require.Equal(t, "a/", opts[0].Prefix,
				"listing uses the derived prefix; the glob is applied to the returned keys")
			require.Len(t, sink.summaries, 1, "an empty match still reports a terminal summary")
		})
	}
}

// TestPrefixSourcePatternMatchesAcrossSegments extends the full-key claim over a
// multi-segment glob: `a/**/*.xml` selects `a/deep/three.xml` only when the key
// is matched whole.
func TestPrefixSourcePatternMatchesAcrossSegments(t *testing.T) {
	sink := &collectSink{}
	src := singlePage(
		obj("a/deep/three.xml", "etag-3", 3),
		obj("a/deep/four.json", "etag-4", 3),
	)
	runner, err := NewRunner(prefixDryRunConfigWithRewrite(sink, "{_}/{dir}/{file}", "{dir}/{file}"))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: "s3://source-bucket/a/**/*.xml"})
	require.NoError(t, err)

	require.Equal(t, []string{"a/deep/three.xml"}, recordKeys(sink.records))
	opts := src.options()
	require.Len(t, opts, 1)
	require.Equal(t, "a/", opts[0].Prefix)
}

// TestPrefixSourceCarriesListingMetadata pins that etag and size reach the
// record from the LISTING, so planning enriches without a HEAD.
func TestPrefixSourceCarriesListingMetadata(t *testing.T) {
	sink := &collectSink{}
	src := singlePage(obj("a/one.xml", "etag-listed", 7))
	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Len(t, sink.records, 1)
	require.Equal(t, "etag-listed", sink.records[0].SourceETag)
	require.Equal(t, int64(7), sink.records[0].SourceSize)
	require.Equal(t, 0, src.operations(), "listing metadata must not be re-derived by a probe")
}

// TestPrefixSourceListingLastModifiedReachesTheCollisionDecision observes the
// listing's last-modified through a consumer that actually uses it, rather than
// asserting a field the public Record does not carry.
//
// overwrite-if-source-newer compares the planned input's last-modified against
// the destination's. A listed source NEWER than the destination overwrites; if
// the producer dropped last-modified, the zero value would not be after the
// destination and the object would be skipped instead.
func TestPrefixSourceListingLastModifiedReachesTheCollisionDecision(t *testing.T) {
	destMod := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srcMod := destMod.Add(time.Hour)

	sink := &collectSink{}
	src := singlePage(objAt("a/one.xml", "etag-src", 3, srcMod))
	dst := newCopyMemoryProvider()
	dst.putFixtureAt("data/a/one.xml", "old", "etag-dst", destMod)

	cfg := positionalCopyConfig(t, dst, sink, newMemCheckpoint())
	cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	terminal := terminalRecord(t, sink.records, "a/one.xml")
	require.Equal(t, "complete", terminal.Status)
	require.NotNil(t, terminal.Collision)
	require.Equal(t, reasonSrcNewer, terminal.Collision.DecisionReason,
		"the decision must be reached from the listing's last-modified")
	require.NotNil(t, terminal.Collision.SrcLastModified)
	require.True(t, srcMod.Equal(*terminal.Collision.SrcLastModified),
		"the compared timestamp is the one the listing reported")
	require.Equal(t, 1, dst.writeCount("data/a/one.xml"))
}

// TestPrefixSourceListedSizeIsKnown pins SourceSizeKnown on a listed object
// through the one consumer that distinguishes it: at EQUAL timestamps, only a
// KNOWN size difference authorizes an overwrite, and an unknown size refuses
// fail-closed with collision.source_size_unavailable.
//
// The zero-byte subcase needs a failing source HEAD to mean anything, and that
// is a statement about the engine rather than about the fixture. A listed
// zero-byte object trips the optional size-recovery HEAD (an absent size and a
// measured zero are the same number on the input), and a SUCCESSFUL recovery
// marks the size known regardless of what the producer set — masking the very
// flag under test. Denying the recovery is what leaves the producer's value
// decisive. The non-zero subcase needs no such setup: it never heads at all.
func TestPrefixSourceListedSizeIsKnown(t *testing.T) {
	sameMod := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("listed non-zero size decides the equal-timestamp tie", func(t *testing.T) {
		sink := &collectSink{}
		src := singlePage(objAt("a/one.xml", "etag-src", 3, sameMod))
		dst := newCopyMemoryProvider()
		dst.putFixtureAt("data/a/one.xml", "12345", "etag-dst", sameMod)

		cfg := positionalCopyConfig(t, dst, sink, newMemCheckpoint())
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
		require.NoError(t, err)

		terminal := terminalRecord(t, sink.records, "a/one.xml")
		require.Equal(t, "complete", terminal.Status)
		require.NotNil(t, terminal.Collision)
		require.Equal(t, reasonEqualSizeDiffers, terminal.Collision.DecisionReason)
		require.Empty(t, sink.errs, "a listed size must not read as unavailable")
		require.Equal(t, 0, src.headCount(), "a listing that carries etag and size needs no recovery head")
		require.Equal(t, 1, dst.writeCount("data/a/one.xml"))
	})

	t.Run("listed zero size is a measured zero when no head can recover it", func(t *testing.T) {
		sink := &collectSink{}
		src := singlePage(objAt("a/empty.xml", "etag-src", 0, sameMod))
		src.headErr = errors.New("source head unavailable")
		dst := newCopyMemoryProvider()
		dst.putFixtureAt("data/a/empty.xml", "12345", "etag-dst", sameMod)

		cfg := positionalCopyConfig(t, dst, sink, newMemCheckpoint())
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
		require.NoError(t, err)

		terminal := terminalRecord(t, sink.records, "a/empty.xml")
		require.Equal(t, "complete", terminal.Status)
		require.NotNil(t, terminal.Collision)
		require.Equal(t, reasonEqualSizeDiffers, terminal.Collision.DecisionReason)
		require.Empty(t, sink.errs,
			"a measured zero must not refuse as collision.source_size_unavailable")
		require.Positive(t, src.headCount(), "the tolerated recovery head was attempted and denied")
		require.Equal(t, 1, dst.writeCount("data/a/empty.xml"))
	})
}

func terminalRecord(t *testing.T, records []Record, sourceKey string) Record {
	t.Helper()
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].SourceKey == sourceKey && records[i].Status != "in_progress" {
			return records[i]
		}
	}
	t.Fatalf("no terminal record for %q in %d records", sourceKey, len(records))
	return Record{}
}

// TestPrefixSourcePaginationTerminatesOnBothSignals pins that either stop signal
// ends enumeration. The scripted provider serves a bounded page budget, so a
// producer that honored neither signal would re-list and fail with
// errListPageBudget rather than spinning forever.
func TestPrefixSourcePaginationTerminatesOnBothSignals(t *testing.T) {
	for name, last := range map[string]listPage{
		"not truncated, token still present": {
			objects:   []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)},
			truncated: false,
			nextToken: "token-would-continue",
		},
		"truncated, token empty": {
			objects:   []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)},
			truncated: true,
			nextToken: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &collectSink{}
			src := newListingProvider()
			src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)}, truncated: true, nextToken: "token-0"})
			src.addPage(last)

			runner, err := NewRunner(prefixDryRunConfig(sink))
			require.NoError(t, err)
			_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
			require.NoError(t, err)
			require.NotErrorIs(t, err, errListPageBudget)

			require.Equal(t, 2, src.calls(), "enumeration must stop at the terminal page")
			require.Equal(t, []string{"a/one.xml", "a/two.xml"}, recordKeys(sink.records))
		})
	}
}

// TestPrefixSourceFollowsIssuedContinuationTokens pins that later pages are
// fetched with the token the previous page issued.
func TestPrefixSourceFollowsIssuedContinuationTokens(t *testing.T) {
	sink := &collectSink{}
	src := newListingProvider()
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)}, truncated: true, nextToken: "token-0"})
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)}, truncated: true, nextToken: "token-1"})
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/three.xml", "etag-3", 3)}})

	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)
	require.NotErrorIs(t, err, errUnknownContinuationToken)

	opts := src.options()
	require.Len(t, opts, 3)
	require.Equal(t, []string{"", "token-0", "token-1"}, []string{
		opts[0].ContinuationToken, opts[1].ContinuationToken, opts[2].ContinuationToken,
	})
	for _, o := range opts {
		require.Equal(t, "a/", o.Prefix, "every page lists under the same derived prefix")
	}
}

// TestPrefixSourceEscapedLiteralSpellingIsPreserved re-pins the 8a defect class
// at the prefix boundary: whether a metacharacter is a glob or a literal key
// character is decided by the SPELLING, so the selector must arrive as spelled.
//
// The two rows are the same characters with and without an escape, and they must
// produce different listings. A caller that canonicalized the escaped row first
// would hand over the unescaped spelling and get the pattern row's behavior —
// a wider listing prefix and a matcher that was never asked for.
func TestPrefixSourceEscapedLiteralSpellingIsPreserved(t *testing.T) {
	objects := []provider.ObjectSummary{
		obj("a/lit*eral/one.xml", "etag-1", 3),
		obj("a/other/two.xml", "etag-2", 3),
	}

	t.Run("escaped metacharacter is a literal prefix", func(t *testing.T) {
		sink := &collectSink{}
		src := singlePage(objects...)
		runner, err := NewRunner(prefixDryRunConfigWithRewrite(sink, "{_}/{dir}/{file}", "{dir}/{file}"))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: `s3://source-bucket/a/lit\*eral/`})
		require.NoError(t, err)

		opts := src.options()
		require.Len(t, opts, 1)
		require.Equal(t, "a/lit*eral/", opts[0].Prefix,
			"an escaped metacharacter lists under the unescaped literal, with no glob derivation")
	})

	t.Run("unescaped metacharacter derives a pattern", func(t *testing.T) {
		sink := &collectSink{}
		src := singlePage(objects...)
		runner, err := NewRunner(prefixDryRunConfigWithRewrite(sink, "{_}/{dir}/{file}", "{dir}/{file}"))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: `s3://source-bucket/a/*/one.xml`})
		require.NoError(t, err)

		opts := src.options()
		require.Len(t, opts, 1)
		require.Equal(t, "a/", opts[0].Prefix, "an unescaped glob lists under the derived prefix")
		require.Equal(t, []string{"a/lit*eral/one.xml"}, recordKeys(sink.records),
			"and filters the listing by the glob")
	})
}

// TestPrefixSourceEnumerationFailureDrainsAndWithholdsTheSummary pins entarch's
// D3 ruling in full.
//
// A List failure partway through is a RUN-level failure, not an object terminal.
// Enumeration stops (no later page is requested), work already admitted drains
// to its own terminals and checkpoint items, the original cause stays
// unwrap-visible, and NO terminal summary is emitted — a summary accounts for a
// whole selector, and this run never enumerated one.
//
// This deliberately diverges from the CLI pool, which writes a summary after the
// same drain. Success and zero-match parity are unaffected.
func TestPrefixSourceEnumerationFailureDrainsAndWithholdsTheSummary(t *testing.T) {
	listErr := errors.New("provider refused page two")
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := newListingProvider()
	src.addPage(listPage{
		objects:   []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3), obj("a/two.xml", "etag-2", 3)},
		truncated: true,
		nextToken: "token-0",
	})
	src.addPage(listPage{err: listErr})
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, runErr := runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})

	var enumErr *SourceEnumerationError
	require.True(t, errors.As(runErr, &enumErr), fmt.Sprintf("want SourceEnumerationError, got %v", runErr))
	require.Equal(t, prefixSelectorURI, enumErr.Selector, "the failure names the selector, not an object")
	require.ErrorIs(t, runErr, listErr, "the provider's own cause must stay unwrap-visible")

	require.Equal(t, 2, src.calls(), "no page may be requested after the failure")

	steps := log.snapshot()
	require.NotContains(t, steps, "summary",
		"a partial enumeration has no whole-selector accounting to report")
	require.NotContains(t, steps, "error:"+ErrCodeInvalidInput,
		"an enumeration failure is not a per-object error event")

	require.Equal(t, 1, dst.writeCount("data/a/one.xml"), "admitted work must drain")
	require.Equal(t, 1, dst.writeCount("data/a/two.xml"), "admitted work must drain")
	require.Len(t, ckpt.items, 2, "drained work still takes its checkpoint authority")
}

// TestPrefixSourceDryRunEnumerationFailureWithholdsTheSummary is the dry-run
// subcase of D3: records already planned may remain, but the summary is still
// withheld.
func TestPrefixSourceDryRunEnumerationFailureWithholdsTheSummary(t *testing.T) {
	listErr := errors.New("provider refused page two")
	sink := &collectSink{}
	src := newListingProvider()
	src.addPage(listPage{
		objects:   []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)},
		truncated: true,
		nextToken: "token-0",
	})
	src.addPage(listPage{err: listErr})

	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, runErr := runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})

	var enumErr *SourceEnumerationError
	require.True(t, errors.As(runErr, &enumErr))
	require.ErrorIs(t, runErr, listErr)
	require.Len(t, sink.records, 1, "records planned before the failure may remain")
	require.Empty(t, sink.summaries, "the summary is withheld on either path")
}

// TestPrefixSourceRequiresProviderOnBothPaths pins entarch's D4 confirmation:
// unlike ObjectSource, a prefix needs a provider in dry-run too, because List is
// the planning operation. The refusal precedes every event.
func TestPrefixSourceRequiresProviderOnBothPaths(t *testing.T) {
	t.Run("dry run without provider is refused", func(t *testing.T) {
		log := &eventLog{}
		sink := &orderSink{log: log}
		cfg := prefixDryRunConfig(sink)
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{URI: prefixSelectorURI})
		require.ErrorContains(t, err, "PrefixSource.Provider is required")
		require.Empty(t, log.snapshot(), "a source-form refusal must precede every event")
	})

	t.Run("copy without provider is refused", func(t *testing.T) {
		log := &eventLog{}
		sink := &orderSink{log: log}
		dst := newCopyMemoryProvider()
		runner, err := NewRunner(positionalCopyConfig(t, dst, sink, newCapabilityCheckpoint(log)))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), PrefixSource{URI: prefixSelectorURI})
		require.ErrorContains(t, err, "PrefixSource.Provider is required")
		require.Empty(t, log.snapshot())
		require.Empty(t, dst.objects)
	})
}

// TestPrefixSourcePlanningIssuesNoHead pins that enumeration enriches from List
// alone. It is the counterpart of ObjectSource's no-HEAD control, differing only
// in metadata richness: both plan without a HEAD, and only the prefix form gets
// etag, size, and last-modified for free.
func TestPrefixSourcePlanningIssuesNoHead(t *testing.T) {
	sink := &collectSink{}
	src := newListingProvider()
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/one.xml", "etag-1", 3)}, truncated: true, nextToken: "token-0"})
	src.addPage(listPage{objects: []provider.ObjectSummary{obj("a/two.xml", "etag-2", 3)}})

	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: prefixSelectorURI})
	require.NoError(t, err)

	require.Positive(t, src.calls(), "enumeration lists")
	require.Equal(t, 0, src.headCount(), "planning must not HEAD a listed object")
	require.Equal(t, 0, src.getCount(), "a dry run must not read object bodies")
}

// TestPrefixSourceRefusesNonPrefixAndUnsupportedURIs pins the source-form
// refusals before any event. An exact object is refused because ObjectSource is
// its form: listing under a whole object key would also admit its prefix
// siblings.
func TestPrefixSourceRefusesNonPrefixAndUnsupportedURIs(t *testing.T) {
	for name, tc := range map[string]struct{ uri, wants string }{
		"exact object": {"s3://source-bucket/a/b.xml", "must be a prefix or pattern URI"},
		"escaped-literal exact object": {
			`s3://source-bucket/a/file\*.xml`, "must be a prefix or pattern URI",
		},
		"file source": {"file:///tmp/x", "unsupported provider"},
		"empty":       {"   ", "PrefixSource.URI is required"},
		"not a uri":   {"not-a-uri", "missing scheme"},
	} {
		t.Run(name, func(t *testing.T) {
			log := &eventLog{}
			sink := &orderSink{log: log}
			ckpt := newCapabilityCheckpoint(log)
			src := newListingProvider()
			dst := newCopyMemoryProvider()

			runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
			require.NoError(t, err)
			_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: tc.uri})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wants)

			require.Empty(t, log.snapshot(), "a source-form refusal must precede every event")
			require.Empty(t, ckpt.written())
			require.Equal(t, 0, src.calls(), "no listing may precede the refusal")
		})
	}
}

// TestPrefixSourceBareBucketSelectorListsEverything pins that a bucket-root
// selector is a prefix (its key is empty), listed with an empty prefix.
func TestPrefixSourceBareBucketSelectorListsEverything(t *testing.T) {
	sink := &collectSink{}
	src := singlePage(obj("a/one.xml", "etag-1", 3), obj("b/two.xml", "etag-2", 3))
	runner, err := NewRunner(prefixDryRunConfig(sink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: src, URI: "s3://source-bucket/"})
	require.NoError(t, err)

	opts := src.options()
	require.Len(t, opts, 1)
	require.Empty(t, opts[0].Prefix)
	require.Equal(t, []string{"a/one.xml", "b/two.xml"}, recordKeys(sink.records))
}

// TestPrefixSourceReadOnlyRequiresDryRun pins the read-only refusal in the
// prefix form's own terms rather than borrowed from another source's message.
func TestPrefixSourceReadOnlyRequiresDryRun(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	dst := newCopyMemoryProvider()
	cfg := positionalCopyConfig(t, dst, sink, newCapabilityCheckpoint(log))
	cfg.ReadOnly = true
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), PrefixSource{Provider: singlePage(), URI: prefixSelectorURI})
	require.ErrorContains(t, err, "PrefixSource copy execution")
	require.Empty(t, log.snapshot())
}

// TestPrefixSourceRedactionUnchanged guards the String/GoString contract while
// PrefixSource becomes executable: the injected provider handle may hold
// credential material and must never be formatted by value.
func TestPrefixSourceRedactionUnchanged(t *testing.T) {
	s := PrefixSource{Provider: newCopyMemoryProvider(), URI: prefixSelectorURI}
	rendered := fmt.Sprintf("%v %#v", s, s)
	require.Contains(t, rendered, prefixSelectorURI)
	require.Contains(t, rendered, "<redacted>")
	require.False(t, strings.Contains(rendered, "copyMemoryProvider{"),
		"the provider handle must not be formatted by value")
}
