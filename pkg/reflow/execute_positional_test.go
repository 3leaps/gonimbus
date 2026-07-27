package reflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// eventLog is a single ordered trace shared by an EventSink and a checkpoint
// store, so a test can assert the ORDER of engine events against the
// source-run-metadata setup write rather than only their presence.
type eventLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *eventLog) add(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.steps = append(l.steps, step)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.steps...)
}

// orderSink traces engine events into a shared log and can fail a chosen event.
type orderSink struct {
	log       *eventLog
	sources   []SourceRunRecord
	sourceErr error
	mu        sync.Mutex
}

func (s *orderSink) OnRun(context.Context, RunRecord) error { s.log.add("run"); return nil }

func (s *orderSink) OnSource(_ context.Context, rec SourceRunRecord) error {
	s.log.add("source")
	s.mu.Lock()
	s.sources = append(s.sources, rec)
	s.mu.Unlock()
	return s.sourceErr
}

func (s *orderSink) OnRecord(_ context.Context, rec Record) error {
	s.log.add("record:" + rec.Status)
	return nil
}
func (s *orderSink) OnWarning(_ context.Context, w Warning) error {
	s.log.add("warning:" + w.Code)
	return nil
}
func (s *orderSink) OnError(_ context.Context, e ErrorEvent) error {
	s.log.add("error:" + e.Code)
	return nil
}
func (s *orderSink) OnSummary(context.Context, SummaryRecord) error {
	s.log.add("summary")
	return nil
}

// capabilityCheckpoint is a memCheckpoint that also implements the optional
// SourceRunMetadataStore capability, tracing the setup write into the shared log.
type capabilityCheckpoint struct {
	*memCheckpoint
	log   *eventLog
	mu    sync.Mutex
	metas []SourceRunMetadata
	err   error
}

func newCapabilityCheckpoint(log *eventLog) *capabilityCheckpoint {
	return &capabilityCheckpoint{memCheckpoint: newMemCheckpoint(), log: log}
}

func (c *capabilityCheckpoint) SetSourceRunMetadata(_ context.Context, meta SourceRunMetadata) error {
	c.log.add("source_run_metadata")
	c.mu.Lock()
	c.metas = append(c.metas, meta)
	c.mu.Unlock()
	return c.err
}

func (c *capabilityCheckpoint) written() []SourceRunMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]SourceRunMetadata(nil), c.metas...)
}

// countingSourceProvider counts every source-side operation, so a test can prove
// an abort happened before ANY source I/O rather than merely before a copy.
type countingSourceProvider struct {
	*copyMemoryProvider
	mu    sync.Mutex
	heads int
	gets  int
}

func newCountingSourceProvider() *countingSourceProvider {
	return &countingSourceProvider{copyMemoryProvider: newCopyMemoryProvider()}
}

func (p *countingSourceProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	p.mu.Lock()
	p.heads++
	p.mu.Unlock()
	return p.copyMemoryProvider.Head(ctx, key)
}

func (p *countingSourceProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	p.gets++
	p.mu.Unlock()
	return p.copyMemoryProvider.GetObject(ctx, key)
}

func (p *countingSourceProvider) operations() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heads + p.gets
}

const positionalObjectURI = "s3://source-bucket/a/b.xml"

func positionalCopyConfig(t *testing.T, dst *copyMemoryProvider, sink EventSink, ckpt CheckpointStore) Config {
	t.Helper()
	cfg := copyConfig(dst, sink)
	cfg.Rewrite = RewriteConfig{From: "{dir}/{file}", To: "{dir}/{file}"}
	cfg.Checkpoint = ckpt
	return cfg
}

// TestObjectSourcePositionalEventOrder pins the positional run order the command
// path establishes: the run record, then the resolved-source event, then the
// source-run-metadata setup write through the coordinator, and only then any
// enumeration or per-object record.
func TestObjectSourcePositionalEventOrder(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := newCountingSourceProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.NoError(t, err)

	steps := log.snapshot()
	require.Equal(t, []string{
		"run",
		"source",
		"source_run_metadata",
		"record:in_progress",
		"record:complete",
		"summary",
	}, steps)

	// The setup write precedes every per-item authority write.
	require.Equal(t, 1, len(ckpt.written()))
	require.NotEmpty(t, ckpt.items, "the object should still have taken its item authority write")
}

// TestRecordStreamEmitsNoSourceEventOrRunMetadata is the negative half of the
// positional contract: a record stream has no single positional selector, so it
// must emit neither the resolved-source event nor the source-run-metadata setup
// write. The command path emits a source record only for positional arguments.
func TestRecordStreamEmitsNoSourceEventOrRunMetadata(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	cfg := positionalCopyConfig(t, dst, sink, ckpt)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), copySource(src, s3DryRunLine))
	require.NoError(t, err)

	steps := log.snapshot()
	require.NotContains(t, steps, "source", "a record stream must not emit a resolved-source event")
	require.NotContains(t, steps, "source_run_metadata", "a record stream must not write source-run metadata")
	require.Empty(t, ckpt.written())
	require.Contains(t, steps, "record:complete", "the stream itself must still have executed")
}

// TestObjectSourceOnSourceFailureAbortsBeforeAnyEffect pins the fatality of the
// resolved-source event: a sink that accepts OnRun and fails OnSource aborts the
// run with that exact error, before the source-run-metadata write, before any
// enumeration, and before any source provider operation. No warning is
// synthesized in place of the failure.
func TestObjectSourceOnSourceFailureAbortsBeforeAnyEffect(t *testing.T) {
	log := &eventLog{}
	sinkErr := errors.New("sink refused the source event")
	sink := &orderSink{log: log, sourceErr: sinkErr}
	ckpt := newCapabilityCheckpoint(log)
	src := newCountingSourceProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.ErrorIs(t, err, sinkErr)

	require.Equal(t, []string{"run", "source"}, log.snapshot(),
		"nothing may follow a failed source event")
	require.Empty(t, ckpt.written(), "metadata setup must not run after a failed source event")
	require.Equal(t, 0, src.operations(), "no source provider operation may precede the abort")
	require.Empty(t, dst.objects, "no destination write may precede the abort")
	require.Empty(t, ckpt.items, "no item authority write may precede the abort")
}

// TestObjectSourceWithoutSourceRunMetadataCapability pins the wired absence
// case: a checkpoint store that does not implement the optional capability is a
// clean no-op — the run completes and copies, nothing panics, and no work is
// skipped. memCheckpoint deliberately does not implement the capability.
func TestObjectSourceWithoutSourceRunMetadataCapability(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newMemCheckpoint()
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	var store CheckpointStore = ckpt
	_, capable := store.(SourceRunMetadataStore)
	require.False(t, capable, "fixture must lack the capability for this control to mean anything")

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, store))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.NoError(t, err)

	steps := log.snapshot()
	require.Equal(t, []string{"run", "source", "record:in_progress", "record:complete", "summary"}, steps)
	require.Equal(t, 1, dst.writeCount("data/a/b.xml"), "the object must still land")
}

// TestObjectSourceRunMetadataErrorDoesNotFailRunImmediately pins the swallow at
// exactly its scope: the runner does not fail SOLELY AND IMMEDIATELY on the
// capability's returned error, and no record changes because of it.
//
// It makes no stronger claim. The coordinator is globally fail-closed, so a real
// statement failure may still surface at a later strict-authority request; this
// control does not assert isolation from that state.
func TestObjectSourceRunMetadataErrorDoesNotFailRunImmediately(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	ckpt.err = errors.New("injected source-run-metadata failure")
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.NoError(t, err)

	require.Equal(t, []string{
		"run", "source", "source_run_metadata", "record:in_progress", "record:complete", "summary",
	}, log.snapshot())
	require.Equal(t, 1, dst.writeCount("data/a/b.xml"))
}

// TestObjectSourceSelectorAndItemAuthorityAreSeparatelySourced pins that the
// run-level selector and the per-item checkpoint authority are populated from
// their own paths.
//
// For an exact object the two VALUES coincide by definition, so this control
// cannot prove they diverge — that is the prefix source's control, where one
// selector fans out to many exact object URIs. What it does pin here is that the
// item authority is the exact object URI rather than anything reconstructed, so
// the prefix case has a fixed reference point to diverge from.
func TestObjectSourceSelectorAndItemAuthorityAreSeparatelySourced(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	ckpt := newCapabilityCheckpoint(log)
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newCopyMemoryProvider()

	runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.NoError(t, err)

	metas := ckpt.written()
	require.Len(t, metas, 1)
	require.Equal(t, positionalObjectURI, metas[0].URI, "run-level selector")
	require.Equal(t, "s3", metas[0].Provider)
	require.Equal(t, "source-bucket", metas[0].Bucket)
	require.Empty(t, metas[0].Root, "an object-store selector carries no local root")

	require.Len(t, sink.sources, 1)
	require.Equal(t, positionalObjectURI, sink.sources[0].URI)

	require.Len(t, ckpt.items, 1)
	require.Equal(t, positionalObjectURI, ckpt.items[0].SourceURI, "item authority is the exact object URI")
	require.Equal(t, "a/b.xml", ckpt.items[0].SourceKey)
}

// TestObjectSourceRefusesNonExactAndMalformedURIs pins the source-form refusals
// before any event: a prefix or pattern reaching ObjectSource is refused rather
// than reflowing a single object named like a prefix, and a non-s3 source is
// refused rather than silently mishandled.
func TestObjectSourceRefusesNonExactAndMalformedURIs(t *testing.T) {
	for name, tc := range map[string]struct{ uri, wants string }{
		"prefix":      {"s3://source-bucket/a/", "exact object URI"},
		"pattern":     {"s3://source-bucket/a/*.xml", "exact object URI"},
		"file source": {"file:///tmp/x", "unsupported provider"},
		"empty":       {"   ", "ObjectSource.URI is required"},
		"missing key": {"s3://source-bucket/", "exact object URI"},
		"not a uri":   {"not-a-uri", "missing scheme"},
	} {
		t.Run(name, func(t *testing.T) {
			log := &eventLog{}
			sink := &orderSink{log: log}
			ckpt := newCapabilityCheckpoint(log)
			src := newCountingSourceProvider()
			dst := newCopyMemoryProvider()

			runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
			require.NoError(t, err)
			_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: tc.uri})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wants)

			require.Empty(t, log.snapshot(), "a source-form refusal must precede every event")
			require.Empty(t, ckpt.written())
			require.Equal(t, 0, src.operations())
		})
	}
}

// TestObjectSourceEscapedLiteralMetacharacterIsExact pins escape-aware parsing
// at the engine boundary: an escaped metacharacter is a LITERAL object-key
// character, so the source stays exact and the planned lookup key is the
// unescaped literal. A caller that canonicalized the URI first would strip the
// escape and have a supported exact object refused as a pattern.
func TestObjectSourceEscapedLiteralMetacharacterIsExact(t *testing.T) {
	for name, tc := range map[string]struct{ uri, wantKey string }{
		"escaped asterisk":      {`s3://source-bucket/a/file\*.xml`, "a/file*.xml"},
		"escaped question mark": {`s3://source-bucket/a/file\?.xml`, "a/file?.xml"},
		"escaped brackets":      {`s3://source-bucket/\[backup\]/file.xml`, "[backup]/file.xml"},
	} {
		t.Run(name, func(t *testing.T) {
			log := &eventLog{}
			sink := &orderSink{log: log}
			ckpt := newCapabilityCheckpoint(log)
			src := newCountingSourceProvider()
			src.putFixture(tc.wantKey, "payload", "etag-a")
			dst := newCopyMemoryProvider()

			runner, err := NewRunner(positionalCopyConfig(t, dst, sink, ckpt))
			require.NoError(t, err)
			_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: tc.uri})
			require.NoError(t, err, "an escaped literal metacharacter is an exact object")

			require.Equal(t, []string{
				"run", "source", "source_run_metadata", "record:in_progress", "record:complete", "summary",
			}, log.snapshot())
			require.Len(t, ckpt.items, 1)
			require.Equal(t, tc.wantKey, ckpt.items[0].SourceKey,
				"the planned lookup key is the unescaped literal")
			require.Equal(t, 1, dst.writeCount("data/"+tc.wantKey))
		})
	}
}

// TestObjectSourceRequiresProviderForCopy pins that a live copy without an
// injected provider is refused before any event, while dry-run needs none.
func TestObjectSourceRequiresProviderForCopy(t *testing.T) {
	t.Run("copy without provider is refused", func(t *testing.T) {
		log := &eventLog{}
		sink := &orderSink{log: log}
		dst := newCopyMemoryProvider()
		runner, err := NewRunner(positionalCopyConfig(t, dst, sink, newCapabilityCheckpoint(log)))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), ObjectSource{URI: positionalObjectURI})
		require.ErrorContains(t, err, "ObjectSource.Provider is required")
		require.Empty(t, log.snapshot())
	})

	t.Run("dry run plans without a provider", func(t *testing.T) {
		log := &eventLog{}
		sink := &orderSink{log: log}
		cfg := dryRunConfig(sink)
		cfg.Rewrite = RewriteConfig{From: "{dir}/{file}", To: "{dir}/{file}"}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), ObjectSource{URI: positionalObjectURI})
		require.NoError(t, err)
		require.Equal(t, []string{"run", "warning:REFLOW_IFABSENT_FALLBACK_ACTIVE", "source", "record:planned", "summary"}, log.snapshot())
	})
}

// TestObjectSourceReadOnlyRequiresDryRun pins the read-only refusal for the
// positional form, stated in its own terms rather than borrowed from the record
// stream's message.
func TestObjectSourceReadOnlyRequiresDryRun(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	dst := newCopyMemoryProvider()
	cfg := positionalCopyConfig(t, dst, sink, newCapabilityCheckpoint(log))
	cfg.ReadOnly = true
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: newCopyMemoryProvider(), URI: positionalObjectURI})
	require.ErrorContains(t, err, "ObjectSource copy execution")
	require.Empty(t, log.snapshot())
}

// TestObjectSourcePlansWithoutHeadingTheSource pins the parity detail that the
// command path establishes for an exact-object positional argument: the task is
// enqueued from the URI alone, carrying no etag, size, or last-modified. A head
// happens only when a later policy needs one, not to enrich the plan.
func TestObjectSourcePlansWithoutHeadingTheSource(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	cfg := dryRunConfig(sink)
	cfg.Rewrite = RewriteConfig{From: "{dir}/{file}", To: "{dir}/{file}"}
	src := newCountingSourceProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{Provider: src, URI: positionalObjectURI})
	require.NoError(t, err)
	require.Equal(t, 0, src.operations(), "planning an exact object must not probe the source")
}

// TestObjectSourceInvalidDestinationMappingIsPerRecord pins that an unmappable
// destination surfaces as an INVALID_INPUT event and a non-zero run, matching
// the record-stream path, rather than aborting before the summary.
func TestObjectSourceInvalidDestinationMappingIsPerRecord(t *testing.T) {
	log := &eventLog{}
	sink := &orderSink{log: log}
	cfg := dryRunConfig(sink)
	cfg.Rewrite = RewriteConfig{From: "unmatched/{dir}/{file}", To: "{file}"}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), ObjectSource{URI: positionalObjectURI})

	var invalid *InvalidInputsError
	require.True(t, errors.As(err, &invalid), fmt.Sprintf("want InvalidInputsError, got %v", err))
	steps := log.snapshot()
	require.Equal(t, []string{"run", "warning:REFLOW_IFABSENT_FALLBACK_ACTIVE", "source", "error:" + ErrCodeInvalidInput, "summary"}, steps)
}

// TestObjectSourceRedactionUnchanged guards the String/GoString contract while
// ObjectSource becomes executable: the injected provider handle may hold
// credential material and must never be formatted by value.
func TestObjectSourceRedactionUnchanged(t *testing.T) {
	s := ObjectSource{Provider: newCopyMemoryProvider(), URI: positionalObjectURI}
	rendered := fmt.Sprintf("%v %#v", s, s)
	require.Contains(t, rendered, positionalObjectURI)
	require.Contains(t, rendered, "<redacted>")
	require.False(t, strings.Contains(rendered, "copyMemoryProvider{"),
		"the provider handle must not be formatted by value")
}
