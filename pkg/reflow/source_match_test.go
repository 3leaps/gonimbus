package reflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	producerpkg "github.com/3leaps/gonimbus/pkg/producer"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

// nonRevisionSourceProvider implements provider.Provider + provider.Getter
// without provider.RevisionGetter, used to verify fail-closed refusal.
type nonRevisionSourceProvider struct {
	inner    *copyMemoryProvider
	listed   []provider.ObjectSummary
	getCalls atomic.Int32
}

func (p *nonRevisionSourceProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	if len(p.listed) > 0 {
		return &provider.ListResult{Objects: append([]provider.ObjectSummary(nil), p.listed...)}, nil
	}
	return p.inner.List(ctx, opts)
}
func (p *nonRevisionSourceProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.inner.Head(ctx, key)
}
func (p *nonRevisionSourceProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.getCalls.Add(1)
	return p.inner.GetObject(ctx, key)
}
func (p *nonRevisionSourceProvider) Close() error { return p.inner.Close() }

var (
	_ provider.Provider     = (*nonRevisionSourceProvider)(nil)
	_ provider.ObjectGetter = (*nonRevisionSourceProvider)(nil)
)

// sourceMatchStubProvider models RevisionGetter with fine-grained control over
// Head/GetObject/GetObjectRevision counts, mid-flight mutations, follow-up HEAD
// failures, and S3-style joined 412 errors.
type sourceMatchStubProvider struct {
	mu           sync.Mutex
	providerType provider.ProviderType
	objects      map[string][]byte
	meta         map[string]provider.ObjectMeta

	headCalls     int
	getCalls      int
	revCalls      int
	failHeadAfter int
	failAllHeads  bool
	onHead        func(key string, call int)
	join412Error  bool
}

func newSourceMatchStubProvider() *sourceMatchStubProvider {
	return &sourceMatchStubProvider{
		providerType: provider.ProviderS3,
		objects:      map[string][]byte{},
		meta:         map[string]provider.ObjectMeta{},
	}
}

func (p *sourceMatchStubProvider) put(key, body, etag, version string, lastMod time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = []byte(body)
	p.meta[key] = provider.ObjectMeta{
		ObjectSummary: provider.ObjectSummary{
			Key:          key,
			Size:         int64(len(body)),
			ETag:         etag,
			Revision:     version,
			LastModified: lastMod,
		},
		Version:     version,
		ContentType: "application/octet-stream",
		Metadata:    map[string]string{"owner": "team"},
	}
}

func (p *sourceMatchStubProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}
func (p *sourceMatchStubProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.mu.Lock()
	p.headCalls++
	call := p.headCalls
	fail := p.failAllHeads || (p.failHeadAfter > 0 && call > p.failHeadAfter)
	hook := p.onHead
	meta, ok := p.meta[key]
	p.mu.Unlock()
	if hook != nil {
		hook(key, call)
	}
	if fail {
		return nil, &provider.ProviderError{Op: "Head", Provider: p.providerType, Key: key, Err: provider.ErrAccessDenied}
	}
	if !ok {
		return nil, &provider.ProviderError{Op: "Head", Provider: p.providerType, Key: key, Err: provider.ErrNotFound}
	}
	copyMeta := meta
	return &copyMeta, nil
}
func (p *sourceMatchStubProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getCalls++
	body, ok := p.objects[key]
	if !ok {
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: p.providerType, Key: key, Err: provider.ErrNotFound}
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}
func (p *sourceMatchStubProvider) GetObjectRevision(_ context.Context, key string, revision provider.SourceRevision) (io.ReadCloser, provider.ObjectMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revCalls++
	if err := revision.Validate(); err != nil {
		return nil, provider.ObjectMeta{}, err
	}
	body, ok := p.objects[key]
	if !ok {
		return nil, provider.ObjectMeta{}, &provider.ProviderError{Op: "GetObjectRevision", Provider: p.providerType, Key: key, Err: provider.ErrNotFound}
	}
	meta := p.meta[key]
	var matched bool
	switch revision.Kind {
	case provider.RevisionETag:
		matched = NormalizeETagForCompare(meta.ETag) == NormalizeETagForCompare(revision.Value)
	case provider.RevisionNative:
		matched = meta.Version == revision.Value
	default:
		return nil, provider.ObjectMeta{}, provider.ErrReplayUnverifiable
	}
	if !matched {
		innerErr := error(provider.ErrSourceChanged)
		if p.join412Error {
			innerErr = errors.Join(provider.ErrSourceChanged, errors.New("api error PreconditionFailed: status code: 412"))
		}
		return nil, provider.ObjectMeta{}, &provider.ProviderError{Op: "GetObjectRevision", Provider: p.providerType, Key: key, Err: innerErr}
	}
	return io.NopCloser(bytes.NewReader(body)), meta, nil
}
func (p *sourceMatchStubProvider) Close() error { return nil }

var _ provider.RevisionGetter = (*sourceMatchStubProvider)(nil)

func streamSourceFor(src provider.Provider, lines string) RecordStreamSource {
	return RecordStreamSource{
		Records: strings.NewReader(lines),
		Resolve: func(context.Context, string) (provider.Provider, error) {
			return src, nil
		},
	}
}

func TestRequireSourceMatchPreflightRefusesNonRevisionGetter(t *testing.T) {
	inner := newCopyMemoryProvider()
	inner.putFixture("a.txt", "hello", "etag-1")
	src := &nonRevisionSourceProvider{
		inner: inner,
		listed: []provider.ObjectSummary{
			{Key: "a.txt", Size: 5, ETag: "etag-1"},
		},
	}
	dst := newCopyMemoryProvider()
	sink := &collectSink{}

	cfg := copyConfig(dst, sink)
	cfg.RequireSourceMatch = true
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a.txt","source_key":"a.txt","source_etag":"etag-1","source_size_bytes":5,"dest_rel_key":"a.txt"}}`
	_, err = runner.Run(context.Background(), streamSourceFor(src, line))
	require.Error(t, err)
	require.ErrorIs(t, err, provider.ErrReplayUnverifiable)
	var replayErr *ReplayUnverifiableError
	require.ErrorAs(t, err, &replayErr)
	require.Zero(t, src.getCalls.Load(), "preflight must refuse before reading source")
	require.Equal(t, 0, dst.writeCount("data/a.txt"), "preflight must refuse before writing destination")
	require.Empty(t, dst.preconditions(), "preflight must refuse before any destination IfAbsent write-probe mutates the bucket")
	require.Empty(t, dst.writeCounts, "preflight must not perform any destination writes")
}

func TestRequireSourceMatchSanitizesUnsafeSourceETagInRecordAndCheckpoint(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	unsafeETags := []string{
		"Bearer super-secret-token",
		`"Bearer quoted-secret-token"`,
		`W/"Bearer weak-secret-token"`,
		`w/"Bearer lower-weak-secret-token"`,
		`W/"Bearer unmatched-weak-secret`,
		`w/"Bearer lower-unmatched-weak-secret`,
		"Bearer\u00a0nbsp-secret-token",
		"https://example.test/bucket/key?X-Amz-Signature=deadbeef",
		"token=secret-value",
		"bad\nnewline",
	}
	for _, unsafeETag := range unsafeETags {
		t.Run(unsafeETag, func(t *testing.T) {
			src := newSourceMatchStubProvider()
			src.put("a.txt", "BBBBB", "etag-v2", "", now)
			dst := newCopyMemoryProvider()
			checkpoint := newMemCheckpoint()
			sink := &collectSink{}

			cfg := copyConfig(dst, sink)
			cfg.RequireSourceMatch = true
			cfg.Checkpoint = checkpoint
			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			rawLine, err := json.Marshal(map[string]any{
				"type": "gonimbus.reflow.input.v1",
				"data": map[string]any{
					"source_uri":        "s3://source-bucket/a.txt",
					"source_key":        "a.txt",
					"source_etag":       unsafeETag,
					"source_size_bytes": 5,
					"dest_rel_key":      "a.txt",
				},
			})
			require.NoError(t, err)

			_, err = runner.Run(context.Background(), streamSourceFor(src, string(rawLine)))
			require.Error(t, err)

			for _, rec := range sink.records {
				b, marshalErr := json.Marshal(rec)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(b), unsafeETag, "unsafe source_etag must never leak into emitted records")
				require.Empty(t, rec.SourceETag)
			}
			for _, item := range checkpoint.items {
				require.Empty(t, item.SourceETag, "unsafe source_etag must never persist into checkpoint")
			}

			sidecar := BuildProvenanceSidecar("run-1", "0.5.1", "2026-11-19T00:00:00Z", ProvenanceSidecarInput{
				SourceURI:  "s3://source-bucket/a.txt",
				SourceETag: unsafeETag,
				DestURI:    "s3://dest-bucket/data/a.txt",
				DestETag:   unsafeETag,
				Collision:  &CollisionInfo{Kind: collisionConflict, DestETagObserved: unsafeETag, DecisionPath: decisionIfAbsentHead},
				Action:     "copied",
			})
			require.Empty(t, sidecar.Source.ETag, "unsafe source_etag must never persist into provenance sidecar")
			require.Empty(t, sidecar.Destination.ETag, "unsafe dest_etag must never persist into provenance sidecar")
			require.NotNil(t, sidecar.Collision)
			require.Empty(t, sidecar.Collision.DestETagObserved, "unsafe collision dest_etag_observed must never persist into provenance sidecar")

			coll := newCollisionInfo(collisionConflict, &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{ETag: unsafeETag, Size: 5}}, decisionIfAbsentHead)
			require.Empty(t, coll.DestETagObserved, "newCollisionInfo must sanitize dest_etag_observed")
		})
	}

	t.Run("adversarial repeated weak-tag prefix scales linearly", func(t *testing.T) {
		// 262,144 repeated W/ prefixes (~512 KiB) must complete in linear time
		// rather than triggering quadratic wrapper peeling.
		repeatedSafe := strings.Repeat("W/", 262144) + "x"
		start := time.Now()
		require.Equal(t, repeatedSafe, SafeReceiptHandle(repeatedSafe))
		require.Less(t, time.Since(start), 2*time.Second, "SafeReceiptHandle must scale linearly on repeated weak-tag prefixes")

		repeatedUnsafe := strings.Repeat("W/", 262144) + `"Bearer linear-secret`
		start = time.Now()
		require.Empty(t, SafeReceiptHandle(repeatedUnsafe))
		require.Less(t, time.Since(start), 2*time.Second, "SafeReceiptHandle must reject adversarial weak-tag bearer in linear time")
	})
}

func TestRequireSourceMatchMissingTokenFailsItem(t *testing.T) {
	src := newSourceMatchStubProvider()
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	src.put("missing.txt", "abc", "etag-missing", "", now)
	src.put("quoted-empty.txt", "abc", "etag-qe", "", now)
	src.put("ok.txt", "valid", "etag-ok", "v1", now)
	dst := newCopyMemoryProvider()
	sink := &collectSink{}

	cfg := copyConfig(dst, sink)
	cfg.RequireSourceMatch = true
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	lines := strings.Join([]string{
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/missing.txt","source_key":"missing.txt","source_size_bytes":3,"dest_rel_key":"missing.txt"}}`,
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/quoted-empty.txt","source_key":"quoted-empty.txt","source_etag":"\"\"","source_size_bytes":3,"dest_rel_key":"quoted-empty.txt"}}`,
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/ok.txt","source_key":"ok.txt","source_etag":"etag-ok","source_size_bytes":5,"dest_rel_key":"ok.txt"}}`,
	}, "\n")

	summary, err := runner.Run(context.Background(), streamSourceFor(src, lines))
	require.Error(t, err)
	require.Equal(t, int64(2), summary.Errors)
	require.Equal(t, int64(1), summary.Statuses["complete"])

	byKey := map[string]Record{}
	for _, rec := range sink.records {
		if rec.Status != "in_progress" {
			byKey[rec.SourceKey] = rec
		}
	}
	require.Equal(t, "failed", byKey["missing.txt"].Status)
	require.Equal(t, "source_token_missing", byKey["missing.txt"].Reason)
	require.Nil(t, byKey["missing.txt"].SourceMatch)
	require.Equal(t, 0, dst.writeCount("data/missing.txt"))

	require.Equal(t, "failed", byKey["quoted-empty.txt"].Status)
	require.Equal(t, "source_token_missing", byKey["quoted-empty.txt"].Reason)
	require.Equal(t, 0, dst.writeCount("data/quoted-empty.txt"))

	require.Equal(t, "complete", byKey["ok.txt"].Status)
	require.Equal(t, "etag-ok", byKey["ok.txt"].SourceETag)
	require.Equal(t, "v1", byKey["ok.txt"].SourceVersionID)
	require.NotNil(t, byKey["ok.txt"].SourceMatch)
	require.Equal(t, "etag-ok", byKey["ok.txt"].SourceMatch.ExpectedETag)
	require.Equal(t, "etag-ok", byKey["ok.txt"].SourceMatch.ObservedETag)
	require.Equal(t, "v1", byKey["ok.txt"].SourceMatch.ObservedVersionID)
	require.Equal(t, 0, src.getCalls, "pinned run must never call unpinned GetObject")
}

func TestRequireSourceMatchChangedDeletedAndFollowUpHeadFailure(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	t.Run("same-size replacement populates observed_etag via follow-up HEAD", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.join412Error = true
		src.put("doc.bin", "BBBBB", "etag-v2", "ver-2", now)
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/doc.bin","source_key":"doc.bin","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"doc.bin"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.Error(t, err)
		require.Equal(t, 0, src.getCalls, "must never fall back to unconditional GetObject after 412")
		require.Equal(t, 1, src.revCalls)
		require.Equal(t, 0, dst.writeCount("data/doc.bin"))

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "source_changed", terminal.Reason)
		require.NotNil(t, terminal.SourceMatch)
		require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
		require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
		require.Equal(t, "ver-2", terminal.SourceMatch.ObservedVersionID)
		require.Empty(t, terminal.DestSHA256)
	})

	t.Run("follow-up HEAD failure leaves observed_etag omitted", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.failAllHeads = true
		src.put("doc.bin", "BBBBB", "etag-v2", "ver-2", now)
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/doc.bin","source_key":"doc.bin","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"doc.bin"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.Error(t, err)

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "source_changed", terminal.Reason)
		require.NotNil(t, terminal.SourceMatch)
		require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
		require.Empty(t, terminal.SourceMatch.ObservedETag)
		require.Empty(t, terminal.SourceMatch.ObservedVersionID)
	})

	t.Run("deleted source emits not_found rather than source_changed", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/gone.bin","source_key":"gone.bin","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"gone.bin"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.Error(t, err)

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "not_found", terminal.Reason)
		require.Nil(t, terminal.SourceMatch)
	})

	t.Run("null source version id is omitted from receipt", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.put("unversioned.bin", "hello", "d41d8cd98f00b204e9800998ecf8427e-3", "null", now)
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/unversioned.bin","source_key":"unversioned.bin","source_etag":"\"d41d8cd98f00b204e9800998ecf8427e-3\"","source_size_bytes":5,"dest_rel_key":"unversioned.bin"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.NoError(t, err)

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "complete", terminal.Status)
		require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", terminal.SourceETag)
		require.Empty(t, terminal.SourceVersionID)
		require.NotNil(t, terminal.SourceMatch)
		require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", terminal.SourceMatch.ExpectedETag)
		require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", terminal.SourceMatch.ObservedETag)
		require.Empty(t, terminal.SourceMatch.ObservedVersionID)
	})
}

func TestRequireSourceMatchHeadPathPreservesExpectedETag(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	t.Run("changed before HEAD early-fails with 0 GETs", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.put("meta.txt", "BBBBB", "etag-v2", "ver-2", now)
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		cfg.Metadata.Policy = MetadataPolicyPreserve
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/meta.txt","source_key":"meta.txt","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"meta.txt"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.Error(t, err)
		require.Equal(t, 1, src.headCalls)
		require.Equal(t, 0, src.revCalls, "early mismatch on HEAD must avoid GET")
		require.Equal(t, 0, src.getCalls)

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "source_changed", terminal.Reason)
		require.NotNil(t, terminal.SourceMatch)
		require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
		require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
	})

	t.Run("changed between HEAD and GET fails via pinned GET without overwriting expectedETag", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.put("race.txt", "AAAAA", "etag-v1", "ver-1", now)
		src.onHead = func(key string, call int) {
			if call == 1 {
				src.put(key, "BBBBB", "etag-v2", "ver-2", now.Add(time.Minute))
			}
		}
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		cfg.Metadata.Policy = MetadataPolicyPreserve
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/race.txt","source_key":"race.txt","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"race.txt"}}`
		_, err = runner.Run(context.Background(), streamSourceFor(src, line))
		require.Error(t, err)
		require.Equal(t, 1, src.revCalls)
		require.Equal(t, 0, src.getCalls)
		require.Equal(t, 0, dst.writeCount("data/race.txt"))

		terminal := sink.records[len(sink.records)-1]
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "source_changed", terminal.Reason)
		require.NotNil(t, terminal.SourceMatch)
		require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
		require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
	})
}

func TestRequireSourceMatchSource412NeverMisclassifiedAsDestCollision(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []string{CollisionSkipIfDuplicate, CollisionFail, CollisionOverwriteIfSourceNewer} {
		t.Run(mode, func(t *testing.T) {
			src := newSourceMatchStubProvider()
			src.join412Error = true
			src.put("item.bin", "NEW!!", "etag-v2", "", now)
			dst := newCopyMemoryProvider()
			if mode == CollisionOverwriteIfSourceNewer {
				dst.putFixture("data/item.bin", "OLD!!", "dest-etag")
			}
			sink := &collectSink{}

			cfg := copyConfig(dst, sink)
			cfg.RequireSourceMatch = true
			cfg.Collision.Mode = mode
			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			line := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/item.bin","source_key":"item.bin","source_etag":"etag-v1","source_size_bytes":5,"source_last_modified":"%s","dest_rel_key":"item.bin"}}`, now.Add(time.Hour).Format(time.RFC3339))
			_, err = runner.Run(context.Background(), streamSourceFor(src, line))
			require.Error(t, err)

			terminal := sink.records[len(sink.records)-1]
			require.Equal(t, "failed", terminal.Status, "source 412 must never become skipped/collision")
			require.Equal(t, "source_changed", terminal.Reason)
			require.Nil(t, terminal.Collision)
			require.NotNil(t, terminal.SourceMatch)
			require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
			require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
		})
	}
}

func TestRequireSourceMatchCollisionSkipOmitsSourceMatch(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	src := newSourceMatchStubProvider()
	src.put("dup.bin", "hello", "etag-1", "", now)
	dst := newCopyMemoryProvider()
	dst.putFixture("data/dup.bin", "hello", "etag-1")
	sink := &collectSink{}

	cfg := copyConfig(dst, sink)
	cfg.RequireSourceMatch = true
	cfg.Collision.Mode = CollisionSkipIfDuplicate
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/dup.bin","source_key":"dup.bin","source_etag":"etag-1","source_size_bytes":5,"dest_rel_key":"dup.bin"}}`
	_, err = runner.Run(context.Background(), streamSourceFor(src, line))
	require.NoError(t, err)

	terminal := sink.records[len(sink.records)-1]
	require.Equal(t, "skipped", terminal.Status)
	require.Nil(t, terminal.SourceMatch, "collision skip without source read must not emit source_match")
}

func TestRequireSourceMatchOffPreservesV050RecordShape(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	src := newSourceMatchStubProvider()
	src.put("legacy.bin", "hello", "etag-1", "ver-1", now)
	dst := newCopyMemoryProvider()
	sink := &collectSink{}

	cfg := copyConfig(dst, sink)
	cfg.RequireSourceMatch = false
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/legacy.bin","source_key":"legacy.bin","source_etag":"etag-1","source_size_bytes":5,"dest_rel_key":"legacy.bin"}}`
	_, err = runner.Run(context.Background(), streamSourceFor(src, line))
	require.NoError(t, err)

	terminal := sink.records[len(sink.records)-1]
	raw, err := json.Marshal(terminal)
	require.NoError(t, err)
	const wantExactV050 = `{"source_uri":"s3://source-bucket/legacy.bin","source_bucket":"source-bucket","source_key":"legacy.bin","source_etag":"etag-1","source_size_bytes":5,"dest_sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","dest_etag":"dest-data/legacy.bin","dest_uri":"s3://dest-bucket/data/legacy.bin","dest_key":"data/legacy.bin","bytes":5,"status":"complete"}`
	require.Equal(t, wantExactV050, string(raw), "flag-off complete receipt must remain byte-identical to v0.5.0")
}

type failingFinalLineReader struct {
	data []byte
	err  error
	done bool
}

func (r *failingFinalLineReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(p, r.data)
	return n, r.err
}

type countingOversizedReader struct {
	remaining int
	bytesRead int
}

func (r *countingOversizedReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'a'
	}
	r.remaining -= n
	r.bytesRead += n
	return n, nil
}

func TestRequireSourceMatchPreflightHardening(t *testing.T) {
	validLine := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a.txt","source_key":"a.txt","source_etag":"etag-1","source_size_bytes":5,"dest_rel_key":"a.txt"}}`

	t.Run("non-EOF reader error alongside valid final record propagates without copying", func(t *testing.T) {
		src := newSourceMatchStubProvider()
		src.put("a.txt", "hello", "etag-1", "", time.Now().UTC())
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		r := &failingFinalLineReader{data: []byte(validLine), err: io.ErrUnexpectedEOF}
		_, err = runner.Run(context.Background(), RecordStreamSource{
			Records: r,
			Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
		})
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Empty(t, dst.preconditions(), "failing stream must refuse before destination probe")
		require.Empty(t, dst.writeCounts)
	})

	t.Run("blank preamble exceeding 1MiB before non-RevisionGetter refuses before destination probe", func(t *testing.T) {
		inner := newCopyMemoryProvider()
		inner.putFixture("a.txt", "hello", "etag-1")
		src := &nonRevisionSourceProvider{inner: inner}
		dst := newCopyMemoryProvider()
		sink := &collectSink{}

		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		stream := strings.Repeat("\n", (1<<20)+64) + validLine + "\n"
		_, err = runner.Run(context.Background(), streamSourceFor(src, stream))
		require.ErrorIs(t, err, provider.ErrReplayUnverifiable)
		require.Empty(t, dst.preconditions(), "sniff budget exhaustion must refuse before destination probe")
		require.Empty(t, dst.writeCounts)
	})

	t.Run("oversized unterminated first line is bounded during read", func(t *testing.T) {
		dst := newCopyMemoryProvider()
		sink := &collectSink{}
		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		r := &countingOversizedReader{remaining: 4 << 20}
		_, err = runner.Run(context.Background(), RecordStreamSource{
			Records: r,
			Resolve: func(context.Context, string) (provider.Provider, error) { return newSourceMatchStubProvider(), nil },
		})
		require.ErrorIs(t, err, provider.ErrReplayUnverifiable)
		require.LessOrEqual(t, r.bytesRead, (1<<20)+(128<<10), "preflight peek must bound reads instead of reading the full 4MiB line")
		require.Empty(t, dst.preconditions())
	})

	t.Run("initial source Resolve error refuses before destination probe", func(t *testing.T) {
		dst := newCopyMemoryProvider()
		sink := &collectSink{}
		cfg := copyConfig(dst, sink)
		cfg.RequireSourceMatch = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		resolveErr := errors.New("synthetic source connect failure")
		_, err = runner.Run(context.Background(), RecordStreamSource{
			Records: strings.NewReader(validLine + "\n"),
			Resolve: func(context.Context, string) (provider.Provider, error) { return nil, resolveErr },
		})
		require.ErrorIs(t, err, resolveErr)
		require.Empty(t, dst.preconditions(), "source preflight connection failure must refuse before destination probe")
		require.Empty(t, dst.writeCounts)
	})
}

func TestPinnedSourceRevisionNeverFallsBackOnNonRevisionGetter(t *testing.T) {
	authority, ref := laneAuthority(t, "non-fallback-admitted-rev")
	enumerator := &scriptedLaneEnumerator{objects: []producerpkg.LaneObject{{
		Lane: ref, Object: obj("a/one.xml", "admitted-etag", 5),
	}}}
	inner := newCopyMemoryProvider()
	inner.putFixture("a/one.xml", "hello", "admitted-etag")
	src := &nonRevisionSourceProvider{inner: inner}
	dst := newCopyMemoryProvider()
	checkpoint := newMemCheckpoint()
	checkpoint.resume = false // non-resume path: reaches limitedCopyConditional with admitted SourceRevision
	sink := &collectSink{}

	cfg := positionalCopyConfig(t, dst, sink, checkpoint)
	cfg.RequireSourceMatch = false
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), PrefixSource{
		Provider:   src,
		URI:        prefixSelectorURI,
		Enumerator: enumerator,
		Authority:  authority,
	})
	require.Error(t, err)
	require.Zero(t, summary.InvalidInputs, "must reach copy path rather than failing destination planning")
	terminal := terminalRecord(t, sink.records, "a/one.xml")
	require.Equal(t, "failed", terminal.Status)
	require.Equal(t, "replay_unverifiable", terminal.Reason)
	require.Zero(t, src.getCalls.Load(), "pinned SourceRevision must never silently fall back to unpinned GetObject")
	require.Equal(t, 0, dst.writeCount("data/a/one.xml"))
}

func TestRequireSourceMatchCollisionDuplicateCompareUsesPinnedRead(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	src := newSourceMatchStubProvider()
	src.join412Error = true
	// Source mutated in-place to match the existing destination bytes (same size 5).
	src.put("item.bin", "NEW!!", "etag-v2", "ver-2", now)

	dst := newCopyMemoryProvider()
	// Existing destination with non-matching multipart ETag forces objectBodiesEqual.
	dst.putFixture("data/item.bin", "NEW!!", "deadbeef-2")
	// Force HEAD fallback so handleExistingDestination runs before conditional PUT
	// (modeling a known destination / IfAbsent-fallback collision probe).
	dst.condCaps = &provider.ConditionalWriteCapabilities{}

	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.RequireSourceMatch = true
	cfg.Collision.Mode = CollisionSkipIfDuplicate
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/item.bin","source_key":"item.bin","source_etag":"etag-v1","source_size_bytes":5,"dest_rel_key":"item.bin"}}`
	_, err = runner.Run(context.Background(), streamSourceFor(src, line))
	require.Error(t, err)
	require.Equal(t, 0, src.getCalls, "collision duplicate body comparison must never call unpinned GetObject")
	require.GreaterOrEqual(t, src.revCalls, 1)

	terminal := sink.records[len(sink.records)-1]
	require.Equal(t, "failed", terminal.Status, "mutated source matching dest must fail as source_changed, not skipped/duplicate")
	require.Equal(t, "source_changed", terminal.Reason)
	require.NotNil(t, terminal.SourceMatch)
	require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
	require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
}

func TestRequireSourceMatchDeletedSourceWithExistingDestPreservesNotFound(t *testing.T) {
	destMod := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	srcMod := destMod.Add(time.Hour)

	for _, mode := range []string{CollisionOverwrite, CollisionOverwriteIfSourceNewer} {
		t.Run(mode, func(t *testing.T) {
			src := newSourceMatchStubProvider() // source key intentionally deleted
			dst := newCopyMemoryProvider()
			dst.putFixtureAt("data/item.bin", "OLD!!", "dest-etag", destMod)
			if mode == CollisionOverwriteIfSourceNewer {
				// Force HEAD fallback so handleExistingDestination runs conflict resolution
				// against the existing dest and then hits deleted source on conditional overwrite.
				dst.condCaps = &provider.ConditionalWriteCapabilities{IfMatchETag: true}
			}
			sink := &collectSink{}
			cfg := copyConfig(dst, sink)
			cfg.RequireSourceMatch = true
			cfg.Collision.Mode = mode
			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			line := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/item.bin","source_key":"item.bin","source_etag":"etag-v1","source_size_bytes":5,"source_last_modified":"%s","dest_rel_key":"item.bin"}}`, srcMod.Format(time.RFC3339))
			_, err = runner.Run(context.Background(), streamSourceFor(src, line))
			require.Error(t, err)

			terminal := sink.records[len(sink.records)-1]
			require.Equal(t, "failed", terminal.Status)
			require.Equal(t, "not_found", terminal.Reason, "deleted source must preserve not_found even when destination collision metadata exists")
			require.Nil(t, terminal.SourceMatch)
		})
	}
}
