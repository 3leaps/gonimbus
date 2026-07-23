package reflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/reflowprobe"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

// collectSink captures emitted events for direct engine assertions.
type collectSink struct {
	runs      []RunRecord
	records   []Record
	warnings  []Warning
	errs      []ErrorEvent
	summaries []SummaryRecord
}

func (s *collectSink) OnRun(_ context.Context, rec RunRecord) error {
	s.runs = append(s.runs, rec)
	return nil
}
func (s *collectSink) OnSource(context.Context, SourceRunRecord) error { return nil }
func (s *collectSink) OnRecord(_ context.Context, rec Record) error {
	s.records = append(s.records, rec)
	return nil
}
func (s *collectSink) OnWarning(_ context.Context, w Warning) error {
	s.warnings = append(s.warnings, w)
	return nil
}
func (s *collectSink) OnError(_ context.Context, e ErrorEvent) error {
	s.errs = append(s.errs, e)
	return nil
}
func (s *collectSink) OnSummary(_ context.Context, rec SummaryRecord) error {
	s.summaries = append(s.summaries, rec)
	return nil
}

func dryRunConfig(sink EventSink) Config {
	return Config{
		Destination: Destination{Provider: sentinelProvider{}, ProviderID: "s3", BaseURI: "s3://dest-bucket/data/"},
		Collision:   CollisionPolicy{Mode: CollisionSkipIfDuplicate},
		Concurrency: ResolveConcurrency(1, true, DefaultResourceProbe()),
		DryRun:      true,
		Events:      sink,
	}
}

type copyMemoryProvider struct {
	mu           sync.Mutex
	providerType provider.ProviderType
	objects      map[string][]byte
	meta         map[string]provider.ObjectMeta
	preconds     []provider.PutPrecondition
	putErrByKey  map[string]error
	// putObjectErrByKey fails the unconditional PutObject for a specific key,
	// modelling a sidecar write failure (sidecars are written via PutObject) while
	// leaving the conditional main-object land unaffected.
	putObjectErrByKey      map[string]error
	throttleHeadsRemaining int
	throttleGetsRemaining  int
	// condCaps overrides the providerType-derived conditional-write capability
	// declaration when set — used to model an IfAbsent-only S3-shaped adapter.
	condCaps *provider.ConditionalWriteCapabilities
	// mutateBeforeIfMatch, when true, mutates the stored object once before the
	// next IfMatch evaluation so the compare-and-swap observes a changed ETag —
	// modelling a destination mutated between the head and the conditional PUT.
	mutateBeforeIfMatch bool
	// writeCounts counts successful object writes (lands) per key across every put
	// path, so a test can assert the exact number of data-object lands — e.g. a
	// resume that re-lands under overwrite adds one.
	writeCounts map[string]int
}

func newCopyMemoryProvider() *copyMemoryProvider {
	return &copyMemoryProvider{
		providerType:      provider.ProviderS3,
		objects:           map[string][]byte{},
		meta:              map[string]provider.ObjectMeta{},
		putErrByKey:       map[string]error{},
		putObjectErrByKey: map[string]error{},
		writeCounts:       map[string]int{},
	}
}

// writeCount reports how many successful writes landed at key.
func (p *copyMemoryProvider) writeCount(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeCounts[key]
}

func newGCSCapabilityProvider() *copyMemoryProvider {
	p := newCopyMemoryProvider()
	p.providerType = provider.ProviderGCS
	return p
}

func (p *copyMemoryProvider) putFixture(key, body, etag string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = []byte(body)
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(body)), ETag: etag}}
}

func (p *copyMemoryProvider) putFixtureAt(key, body, etag string, lastMod time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = []byte(body)
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(body)), ETag: etag, LastModified: lastMod}}
}

func (p *copyMemoryProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *copyMemoryProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.throttleHeadsRemaining > 0 {
		p.throttleHeadsRemaining--
		return nil, &provider.ProviderError{Op: "Head", Provider: p.providerType, Key: key, Err: provider.ErrThrottled}
	}
	meta, ok := p.meta[key]
	if !ok {
		return nil, &provider.ProviderError{Op: "Head", Provider: p.providerType, Key: key, Err: provider.ErrNotFound}
	}
	return &meta, nil
}

func (p *copyMemoryProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.throttleGetsRemaining > 0 {
		p.throttleGetsRemaining--
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: p.providerType, Key: key, Err: provider.ErrThrottled}
	}
	body, ok := p.objects[key]
	if !ok {
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: p.providerType, Key: key, Err: provider.ErrNotFound}
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (p *copyMemoryProvider) PutObject(_ context.Context, key string, body io.Reader, contentLength int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return fmt.Errorf("content length mismatch")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if perr := p.putObjectErrByKey[key]; perr != nil {
		return &provider.ProviderError{Op: "PutObject", Provider: p.providerType, Key: key, Err: perr}
	}
	p.objects[key] = data
	p.writeCounts[key]++
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: "dest-" + key}}
	return nil
}

func (p *copyMemoryProvider) PutObjectConditional(_ context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	if err := precond.Validate(); err != nil {
		return provider.PutResult{}, err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return provider.PutResult{}, err
	}
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return provider.PutResult{}, fmt.Errorf("content length mismatch")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.preconds = append(p.preconds, precond)
	if err := p.putErrByKey[key]; err != nil {
		return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: p.providerType, Key: key, Err: err}
	}
	if precond.IfAbsent {
		if _, ok := p.objects[key]; ok {
			return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: p.providerType, Key: key, Err: provider.ErrAlreadyExists}
		}
		etag := "dest-" + key
		p.objects[key] = data
		p.writeCounts[key]++
		p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: etag}}
		return provider.PutResult{ETag: etag}, nil
	}
	if precond.IfMatchETag != nil {
		if p.providerType == provider.ProviderGCS {
			return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: p.providerType, Key: key, Err: provider.ErrUnsupportedPrecondition}
		}
		if p.mutateBeforeIfMatch {
			p.objects[key] = []byte("concurrent mutation")
			p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len("concurrent mutation")), ETag: "mutated-" + key, LastModified: time.Now().UTC()}}
			p.mutateBeforeIfMatch = false
		}
		meta, ok := p.meta[key]
		if !ok || meta.ETag != *precond.IfMatchETag {
			return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: p.providerType, Key: key, Err: provider.ErrPreconditionFailed}
		}
		etag := "dest-" + key + "-v2"
		p.objects[key] = data
		p.writeCounts[key]++
		p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: etag, LastModified: time.Now().UTC()}}
		return provider.PutResult{ETag: etag}, nil
	}
	return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: p.providerType, Key: key, Err: provider.ErrUnsupportedPrecondition}
}

// ConditionalWriteCapabilities declares the fixture's honored conditional-write
// predicates. A GCS-typed fixture honors IfAbsent only; an S3-typed fixture also
// honors IfMatch compare-and-swap. condCaps overrides this to model an
// IfAbsent-only S3-shaped adapter whose ConditionalPutter presence must not be
// mistaken for If-Match support.
func (p *copyMemoryProvider) ConditionalWriteCapabilities() provider.ConditionalWriteCapabilities {
	if p.condCaps != nil {
		return *p.condCaps
	}
	if p.providerType == provider.ProviderGCS {
		return provider.ConditionalWriteCapabilities{IfAbsent: true}
	}
	return provider.ConditionalWriteCapabilities{IfAbsent: true, IfMatchETag: true}
}

func (p *copyMemoryProvider) PutObjectWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, opts provider.PutOptions) error {
	if err := p.PutObject(ctx, key, body, contentLength); err != nil {
		return err
	}
	p.applyOptions(key, opts)
	return nil
}

func (p *copyMemoryProvider) PutObjectConditionalWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition, opts provider.PutOptions) (provider.PutResult, error) {
	result, err := p.PutObjectConditional(ctx, key, body, contentLength, precond)
	if err != nil {
		return provider.PutResult{}, err
	}
	p.applyOptions(key, opts)
	return result, nil
}

func (p *copyMemoryProvider) DeleteObject(_ context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.objects, key)
	delete(p.meta, key)
	return nil
}

func (p *copyMemoryProvider) applyOptions(key string, opts provider.PutOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta := p.meta[key]
	meta.Metadata = cloneStringMap(opts.UserMetadata)
	meta.ContentType = opts.ContentType
	meta.StorageClass = opts.StorageClass
	p.meta[key] = meta
}

func (p *copyMemoryProvider) Close() error { return nil }

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (p *copyMemoryProvider) body(key string) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]byte, len(p.objects[key]))
	copy(out, p.objects[key])
	return out
}

func (p *copyMemoryProvider) preconditions() []provider.PutPrecondition {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provider.PutPrecondition(nil), p.preconds...)
}

func copyConfig(dst *copyMemoryProvider, sink EventSink) Config {
	cfg := dryRunConfig(sink)
	cfg.Destination.Provider = dst
	cfg.DryRun = false
	return cfg
}

func gcsCopyConfig(dst *copyMemoryProvider, sink EventSink) Config {
	cfg := copyConfig(dst, sink)
	cfg.Destination.ProviderID = string(provider.ProviderGCS)
	cfg.Destination.BaseURI = "gs://dest-bucket/data/"
	return cfg
}

func copySource(src *copyMemoryProvider, line string) RecordStreamSource {
	return RecordStreamSource{
		Records: strings.NewReader(line),
		Resolve: func(context.Context, string) (provider.Provider, error) {
			return src, nil
		},
	}
}

func TestRunnerDryRunRecordStream(t *testing.T) {
	sink := &collectSink{}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line)})
	require.NoError(t, err)

	require.Len(t, sink.runs, 1)
	require.True(t, sink.runs[0].DryRun)
	require.Equal(t, "s3://dest-bucket/data/", sink.runs[0].DestURI)
	require.Equal(t, 1, sink.runs[0].Parallel)

	// Dry-run over a skip-if-duplicate object store reports the head-compare fallback.
	require.Len(t, sink.warnings, 1)
	require.Equal(t, ifAbsentFallbackWarningCode, sink.warnings[0].Code)

	require.Len(t, sink.records, 1)
	require.Equal(t, "planned", sink.records[0].Status)
	require.Equal(t, "data/a/b.xml", sink.records[0].DestKey)
	require.Equal(t, "s3://dest-bucket/data/a/b.xml", sink.records[0].DestURI)
	require.Equal(t, "s3://source-bucket/a/b.xml", sink.records[0].SourceURI)

	require.Len(t, sink.summaries, 1)
	require.Equal(t, int64(1), summary.Statuses["planned"])
	require.True(t, summary.FallbackActive)
	require.Equal(t, "inconclusive", summary.DestIfAbsentProbeStatus)
}

func TestRunnerCopyRecordStreamConditionalPut(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	runner, err := NewRunner(copyConfig(dst, sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), dst.body("data/a/b.xml"))
	require.Len(t, sink.records, 2)
	require.Equal(t, "in_progress", sink.records[0].Status)
	require.Equal(t, "complete", sink.records[1].Status)
	require.Equal(t, int64(7), sink.records[1].Bytes)
	require.Equal(t, int64(1), summary.Statuses["complete"])
	require.Equal(t, "honored", summary.DestIfAbsentProbeStatus)
	require.NotNil(t, summary.DestIfAbsentHonored)
	require.True(t, *summary.DestIfAbsentHonored)
	require.Equal(t, 1, summary.ConcurrencyMaxActive)
	require.Len(t, dst.preconditions(), 3, "two preflight puts plus the object conditional put")
	require.True(t, dst.preconditions()[2].IfAbsent)
}

func TestRunnerGCSCapabilityMatrixIfAbsentHonoredNoFallback(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newGCSCapabilityProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	runner, err := NewRunner(gcsCopyConfig(dst, sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), dst.body("data/a/b.xml"))
	require.Empty(t, sink.warnings, "GCS-shaped IfAbsent-honored destination must not activate the head fallback")
	require.Len(t, sink.records, 2)
	require.Equal(t, "gs://dest-bucket/data/a/b.xml", sink.records[1].DestURI)
	require.Equal(t, "honored", summary.DestIfAbsentProbeStatus)
	require.NotNil(t, summary.DestIfAbsentHonored)
	require.True(t, *summary.DestIfAbsentHonored)
	require.False(t, summary.FallbackActive)
	require.Zero(t, summary.IfAbsentFallbackObjects)
	require.Len(t, dst.preconditions(), 3, "two preflight puts plus the object conditional put")
	require.True(t, dst.preconditions()[2].IfAbsent)
}

func TestGCSCapabilityFixtureRejectsIfMatchETag(t *testing.T) {
	dst := newGCSCapabilityProvider()
	etag := "etag-a"
	_, err := dst.PutObjectConditional(context.Background(), "data/a.xml", strings.NewReader("payload"), int64(len("payload")), provider.PutPrecondition{IfMatchETag: &etag})
	require.ErrorIs(t, err, provider.ErrUnsupportedPrecondition)

	var providerErr *provider.ProviderError
	require.ErrorAs(t, err, &providerErr)
	require.Equal(t, provider.ProviderGCS, providerErr.Provider)
	require.NotContains(t, err.Error(), "x-goog-signature")
	require.NotContains(t, err.Error(), "x-goog-credential")
	require.NotContains(t, strings.ToLower(err.Error()), "bearer")
}

func TestRunnerGCSCapabilityMatrixClassifiesProviderSignals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantCode   string
		wantBack   int64
		wantFreeze int64
	}{
		{
			name:     "quota throttled",
			err:      provider.ErrThrottled,
			wantCode: ErrCodeThrottled,
			wantBack: 1,
		},
		{
			name:       "service unavailable",
			err:        provider.ErrProviderUnavailable,
			wantCode:   ErrCodeProviderUnavailable,
			wantFreeze: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, dst := newCopyMemoryProvider(), newGCSCapabilityProvider()
			src.putFixture("a/b.xml", "payload", "etag-a")
			dst.putErrByKey["data/a/b.xml"] = tc.err
			sink := &collectSink{}
			cfg := gcsCopyConfig(dst, sink)
			cfg.Concurrency = ConcurrencyConfig{
				RequestedCeiling: 4,
				EffectiveCeiling: 4,
				CeilingReason:    "test",
				AdaptiveEnabled:  true,
				Floor:            1,
				Initial:          4,
			}
			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
			summary, err := runner.Run(context.Background(), copySource(src, line))
			var objectErr *ObjectErrorsError
			require.ErrorAs(t, err, &objectErr)
			require.Equal(t, int64(1), objectErr.Count)

			require.Len(t, sink.errs, 1)
			require.Equal(t, tc.wantCode, sink.errs[0].Code)
			require.Equal(t, int64(1), summary.Errors)
			require.Equal(t, int64(1), summary.Statuses["failed"])
			require.Equal(t, "honored", summary.DestIfAbsentProbeStatus)
			require.False(t, summary.FallbackActive)
			require.Equal(t, tc.wantBack, summary.ConcurrencyThrottleBackoffs)
			require.Equal(t, tc.wantFreeze, summary.ConcurrencyConnectionErrorFreezes)
		})
	}
}

func TestRunnerCopyValidatesMetadataBudgetBeforePut(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.Metadata = MetadataPlan{
		Policy: MetadataPolicyClear,
		Set:    map[string]string{"oversized": strings.Repeat("x", metadataMaxTotalBytes)},
	}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, err, &objectErr)
	require.Equal(t, int64(1), objectErr.Count)

	require.Empty(t, dst.body("data/a/b.xml"), "oversized metadata must fail before destination PUT")
	require.Len(t, sink.errs, 1)
	require.Equal(t, ErrCodeInvalidInput, sink.errs[0].Code)
	require.Contains(t, sink.errs[0].Details, "metadata_total_bytes")
	require.Equal(t, int64(1), summary.Errors)
	require.Equal(t, int64(1), summary.Statuses["failed"])
}

func TestRunnerCopyCollisionFailReturnsObjectError(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "new payload", "src-etag")
	dst.putFixture("data/a/b.xml", "old payload", "old-etag")
	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.Collision = CollisionPolicy{Mode: CollisionFail}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":11,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, err, &objectErr)
	require.Equal(t, int64(1), objectErr.Count)

	require.Equal(t, []byte("old payload"), dst.body("data/a/b.xml"))
	require.Len(t, sink.errs, 1)
	require.Equal(t, int64(1), summary.Errors)
	require.Equal(t, int64(1), summary.Statuses["failed"])
	require.Equal(t, int64(1), summary.Collisions["conflict"])
}

// TestEngineFailedCollisionReasonConvergesAcrossSurfaces pins that a failed
// collision reports the SAME specific reason on all three failure surfaces — the
// emitted record, the error event details, and the durable checkpoint — so a
// resume reading the checkpoint sees the reason a consumer saw on the record, not
// the coarse error class. error_code still carries the class.
func TestEngineFailedCollisionReasonConvergesAcrossSurfaces(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "new payload", "src-etag")
	dst.putFixture("data/a/b.xml", "old payload", "old-etag")
	ckpt := newMemCheckpoint()
	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.Collision = CollisionPolicy{Mode: CollisionFail}
	cfg.Checkpoint = ckpt
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":11,"dest_rel_key":"a/b.xml"}}`
	_, runErr := runner.Run(context.Background(), copySource(src, line))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, runErr, &objectErr)

	const wantReason = "collision.exists.conflict"
	require.Equal(t, wantReason, lastRecord(t, sink).Reason, "emitted record reason")
	require.Len(t, sink.errs, 1)
	require.Equal(t, wantReason, sink.errs[0].Details["reason"], "error event details.reason")

	item := lastCheckpointItem(t, ckpt, "data/a/b.xml")
	require.Equal(t, "failed", item.Status)
	require.Equal(t, wantReason, item.Reason, "durable checkpoint reason converges with the emitted reason")
	require.Equal(t, "INTERNAL", item.ErrorCode, "durable error_code keeps the class")
	require.NotEmpty(t, item.ErrorMessage, "durable checkpoint keeps the sanitized cause")
}

// TestRecordObjectErrorReasonIsAuthoritativeOverCallerDetails is the adversarial
// guardrail for the single-reason invariant: even when a caller pre-populates
// details["reason"] with a stale value, the derived collision reason wins on all
// three surfaces (event details, durable checkpoint, record). No production caller
// supplies that key today; this pins the invariant locally so a later caller or
// refactor cannot silently reopen the split.
func TestRecordObjectErrorReasonIsAuthoritativeOverCallerDetails(t *testing.T) {
	dst := newCopyMemoryProvider()
	ckpt := newMemCheckpoint()
	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.Checkpoint = ckpt
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	in := reflowInput{SourceURI: "s3://source-bucket/a/b.xml", SourceKey: "a/b.xml"}
	collision := &CollisionInfo{Kind: collisionConflict}
	// A stale, conflicting caller-supplied reason that must be overridden.
	details := map[string]any{
		"reason":     "stale.caller.reason",
		"source_uri": "s3://source-bucket/a/b.xml",
		"dest_uri":   "s3://dest-bucket/data/a/b.xml",
	}
	copyErr := fmt.Errorf("destination key exists with different content")

	rerr := runner.recordObjectError(context.Background(), newRunStats(), in,
		"s3://dest-bucket/data/a/b.xml", "data/a/b.xml", "collision", copyErr, details, collision)
	require.NoError(t, rerr)

	const wantReason = "collision.exists.conflict"
	require.Equal(t, wantReason, lastRecord(t, sink).Reason, "record reason")
	require.Len(t, sink.errs, 1)
	require.Equal(t, wantReason, sink.errs[0].Details["reason"],
		"event details.reason wins over the stale caller-supplied value")
	item := lastCheckpointItem(t, ckpt, "data/a/b.xml")
	require.Equal(t, wantReason, item.Reason, "durable checkpoint reason")
	require.Equal(t, "INTERNAL", item.ErrorCode, "durable error_code keeps the class")
	require.NotEmpty(t, item.ErrorMessage, "durable checkpoint keeps the sanitized cause")
}

func TestRunnerProbeHeadThrottleRetriesAndCompletes(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "same payload", "src-etag")
	dst.putFixture("data/a/b.xml", "same payload", "dest-etag")
	dst.throttleHeadsRemaining = 2
	sink := &collectSink{}
	runner, err := NewRunner(copyConfig(dst, sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":12,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	require.NoError(t, err)

	require.Equal(t, int64(2), summary.ConcurrencyThrottleBackoffs)
	require.Equal(t, int64(1), summary.Statuses["skipped"])
	require.Equal(t, int64(1), summary.Collisions["duplicate"])
}

func TestRunnerProbeHeadThrottleExhaustionFailsObject(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "new payload", "src-etag")
	dst.putFixture("data/a/b.xml", "old payload", "dest-etag")
	dst.throttleHeadsRemaining = reflowprobe.MaxAttempts
	sink := &collectSink{}
	runner, err := NewRunner(copyConfig(dst, sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":11,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, err, &objectErr)
	require.Equal(t, int64(1), objectErr.Count)

	require.Len(t, sink.errs, 1)
	require.Equal(t, ErrCodeThrottled, sink.errs[0].Code)
	require.Equal(t, int64(reflowprobe.MaxAttempts), summary.ConcurrencyThrottleBackoffs)
	require.Equal(t, int64(1), summary.Errors)
	require.Equal(t, int64(1), summary.Statuses["failed"])
	require.Equal(t, []byte("old payload"), dst.body("data/a/b.xml"), "failed probe must not overwrite the existing object")
}

func TestRunnerBodyCompareThrottleRetriesAndFindsDuplicate(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "same payload", "src-etag")
	dst.putFixture("data/a/b.xml", "same payload", "dest-etag")
	dst.throttleGetsRemaining = 1
	sink := &collectSink{}
	runner, err := NewRunner(copyConfig(dst, sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":12,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), copySource(src, line))
	require.NoError(t, err)

	require.Equal(t, int64(1), summary.ConcurrencyThrottleBackoffs)
	require.Equal(t, int64(1), summary.Statuses["skipped"])
	require.Equal(t, int64(1), summary.Collisions["duplicate"])
}

func (s *collectSink) emitted() bool {
	return len(s.runs)+len(s.records)+len(s.warnings)+len(s.errs)+len(s.summaries) > 0
}

const s3DryRunLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","dest_rel_key":"a/b.xml"}}`

// Unsupported forms or missing copy prerequisites are rejected before stream
// bytes are read, emit nothing, and leave the reader untouched.
func TestRunnerDefersBeforeReading(t *testing.T) {
	t.Run("non dry-run copy without resolver leaves reader untouched", func(t *testing.T) {
		cfg := dryRunConfig(&collectSink{})
		cfg.DryRun = false
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		reader := strings.NewReader(s3DryRunLine)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: reader})
		require.ErrorContains(t, err, "RecordStreamSource.Resolve is required")
		require.False(t, cfg.Events.(*collectSink).emitted())
		remaining, _ := io.ReadAll(reader)
		require.Equal(t, s3DryRunLine, string(remaining), "reader must be untouched for fallback")
	})

	t.Run("readonly copy rejects before reader or destination mutation", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		cfg := copyConfig(dst, sink)
		cfg.ReadOnly = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		reader := strings.NewReader(s3DryRunLine)
		_, err = runner.Run(context.Background(), RecordStreamSource{
			Records: reader,
			Resolve: func(context.Context, string) (provider.Provider, error) {
				return src, nil
			},
		})
		require.ErrorContains(t, err, "ReadOnly requires DryRun")
		require.False(t, sink.emitted())
		require.Empty(t, dst.body("data/a/b.xml"))
		require.Empty(t, dst.preconditions(), "readonly rejection must happen before IfAbsent write-probe or copy")
		remaining, _ := io.ReadAll(reader)
		require.Equal(t, s3DryRunLine, string(remaining), "reader must be untouched")
	})

	t.Run("unsupported copy collision modes defer before reader or destination mutation", func(t *testing.T) {
		// overwrite and overwrite-if-source-newer are now executed by the engine;
		// quarantine remains the unmigrated collision mode that must defer.
		for _, mode := range []string{CollisionQuarantine} {
			t.Run(mode, func(t *testing.T) {
				src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
				src.putFixture("a/b.xml", "payload", "etag-a")
				sink := &collectSink{}
				cfg := copyConfig(dst, sink)
				cfg.Collision = CollisionPolicy{Mode: mode, QuarantinePrefix: "quarantine"}
				runner, err := NewRunner(cfg)
				require.NoError(t, err)
				reader := strings.NewReader(s3DryRunLine)
				_, err = runner.Run(context.Background(), RecordStreamSource{
					Records: reader,
					Resolve: func(context.Context, string) (provider.Provider, error) {
						return src, nil
					},
				})
				require.True(t, errors.Is(err, ErrNotImplemented))
				require.False(t, sink.emitted())
				require.Empty(t, dst.body("data/a/b.xml"))
				require.Empty(t, dst.preconditions(), "unsupported collision mode must not probe or copy")
				remaining, _ := io.ReadAll(reader)
				require.Equal(t, s3DryRunLine, string(remaining), "reader must be untouched")
			})
		}
	})

	t.Run("unsupported source form", func(t *testing.T) {
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), ObjectSource{Provider: sentinelProvider{}, URI: "s3://b/k"})
		require.True(t, errors.Is(err, ErrNotImplemented))
		require.False(t, sink.emitted())
	})
}

// streamSink reports each planned record's source key over a channel so a test
// can observe streaming (emission interleaved with reading).
type streamSink struct{ keys chan string }

func (s *streamSink) OnRun(context.Context, RunRecord) error          { return nil }
func (s *streamSink) OnSource(context.Context, SourceRunRecord) error { return nil }
func (s *streamSink) OnRecord(_ context.Context, rec Record) error {
	s.keys <- rec.SourceKey
	return nil
}
func (s *streamSink) OnWarning(context.Context, Warning) error       { return nil }
func (s *streamSink) OnError(context.Context, ErrorEvent) error      { return nil }
func (s *streamSink) OnSummary(context.Context, SummaryRecord) error { return nil }

// TestRunnerDryRunStreamsRecordByRecord proves the supported dry-run path does not
// accumulate the whole stream before emitting: the first record is emitted before
// the second is written. A buffer-all implementation would block reading the
// second record and deadlock.
func TestRunnerDryRunStreamsRecordByRecord(t *testing.T) {
	pr, pw := io.Pipe()
	sink := &streamSink{keys: make(chan string, 4)}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { _, runErr := runner.Run(context.Background(), RecordStreamSource{Records: pr}); done <- runErr }()

	line := func(k string) string {
		return `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/` + k + `","source_key":"` + k + `","dest_rel_key":"` + k + `"}}` + "\n"
	}
	_, _ = pw.Write([]byte(line("k1")))
	select {
	case key := <-sink.keys:
		require.Equal(t, "k1", key)
	case <-time.After(5 * time.Second):
		t.Fatal("dry-run did not stream: first record not emitted before the second was written")
	}
	_, _ = pw.Write([]byte(line("k2")))
	require.Equal(t, "k2", <-sink.keys)
	require.NoError(t, pw.Close())
	require.NoError(t, <-done)
}

// TestRunnerDryRunInvalidInputStreams shows per-record problems surface as
// INVALID_INPUT events (CLI-equivalent) without aborting the run.
func TestRunnerDryRunInvalidInputStreams(t *testing.T) {
	sink := &collectSink{}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)
	stream := strings.Join([]string{
		s3DryRunLine,
		`{"type":"gonimbus.index.object.v1","data":{}}`,
		`not-json`,
	}, "\n")
	summary, err := runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(stream)})
	// The run completes (summary + error events emitted) but reports failure, like
	// the command path's non-zero exit on invalid inputs.
	var invErr *InvalidInputsError
	require.ErrorAs(t, err, &invErr)
	require.Equal(t, int64(2), invErr.Count)
	require.Len(t, sink.records, 1)
	require.Equal(t, "a/b.xml", sink.records[0].SourceKey)
	require.Len(t, sink.errs, 2)
	require.Equal(t, ErrCodeInvalidInput, sink.errs[0].Code)
	require.Equal(t, int64(2), summary.InvalidInputs)
	require.Len(t, sink.summaries, 1)
}

func TestRunnerDryRunQuarantineAndRewrite(t *testing.T) {
	t.Run("quarantine routing", func(t *testing.T) {
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		q := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/raw/o.xml","source_key":"raw/o.xml","routing_class":"quarantine","quarantine_prefix":"quar"}}`
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(q)})
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		require.Equal(t, "data/quar/raw/o.xml", sink.records[0].DestKey)
	})

	t.Run("rewrite when dest_rel_key absent", func(t *testing.T) {
		cfg := dryRunConfig(&collectSink{})
		cfg.Rewrite = RewriteConfig{From: "{key}", To: "renamed/{key}"}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		r := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/o.xml","source_key":"o.xml","vars":{"key":"o.xml"}}}`
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(r)})
		require.NoError(t, err)
		sink := cfg.Events.(*collectSink)
		require.Len(t, sink.records, 1)
		require.Equal(t, "data/renamed/o.xml", sink.records[0].DestKey)
	})
}
