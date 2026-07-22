package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

// multipartOverThresholdBytes is just over the 64 MiB multipart threshold, so the
// copy path routes through multipart. The source streams this full, truthful
// length (not a short body) so the test does not depend on the uploader treating a
// short known-size body as a complete upload.
const multipartOverThresholdBytes = int64(64<<20) + 1

// fixedSizeReader yields exactly n bytes without allocating them, so a source can
// stream a multipart-sized object truthfully without moving a real 64 MiB buffer.
type fixedSizeReader struct{ remaining int64 }

func (r *fixedSizeReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	return int(n), nil
}

func (r *fixedSizeReader) Close() error { return nil }

// largeSourceProvider serves one object whose body streams its full declared size
// (just over the multipart threshold).
type largeSourceProvider struct {
	key          string
	etag         string
	lastModified time.Time
	declaredSize int64
}

func (p *largeSourceProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *largeSourceProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	if key != p.key {
		return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderS3, Key: key, Err: provider.ErrNotFound}
	}
	return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: p.declaredSize, ETag: p.etag, LastModified: p.lastModified}}, nil
}

func (p *largeSourceProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	if key != p.key {
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: provider.ProviderS3, Key: key, Err: provider.ErrNotFound}
	}
	// Stream the full declared length so transferred bytes match the known size.
	return &fixedSizeReader{remaining: p.declaredSize}, p.declaredSize, nil
}

func (p *largeSourceProvider) Close() error { return nil }

// reflowMultipartDest is a destination that performs multipart uploads and can
// complete them conditionally (If-Match). It mutates the stored object once
// before the next If-Match multipart completion, modelling a destination changed
// between the head and the multipart completion, so the compare-and-swap fails
// closed rather than clobbering the concurrent write.
type reflowMultipartDest struct {
	*reflowMemoryProvider
	mpMu                          sync.Mutex
	createCalls                   int
	abortCalls                    int
	conditionalCompletePreconds   []provider.PutPrecondition
	mutateBeforeMultipartComplete bool
}

func newReflowMultipartDest() *reflowMultipartDest {
	return &reflowMultipartDest{reflowMemoryProvider: newReflowMemoryProvider()}
}

// ConditionalWriteCapabilities advertises conditional multipart completion in
// addition to the single-PUT predicates, so the source-newer capability gate
// admits this destination for large objects.
func (p *reflowMultipartDest) ConditionalWriteCapabilities() provider.ConditionalWriteCapabilities {
	return provider.ConditionalWriteCapabilities{IfAbsent: true, IfMatchETag: true, ConditionalMultipartCompletion: true}
}

func (p *reflowMultipartDest) CreateMultipartUpload(_ context.Context, _ string) (string, error) {
	p.mpMu.Lock()
	defer p.mpMu.Unlock()
	p.createCalls++
	return fmt.Sprintf("mp-%d", p.createCalls), nil
}

func (p *reflowMultipartDest) UploadPart(_ context.Context, _, _ string, partNumber int32, body io.Reader, _ int64) (provider.PartETag, error) {
	_, _ = io.Copy(io.Discard, body)
	return provider.PartETag{PartNumber: partNumber, ETag: fmt.Sprintf("part-%d", partNumber)}, nil
}

func (p *reflowMultipartDest) CompleteMultipartUpload(_ context.Context, key, _ string, _ []provider.PartETag) (provider.PutResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.storeMultipartLocked(key), nil
}

func (p *reflowMultipartDest) AbortMultipartUpload(_ context.Context, _, _ string) error {
	p.mpMu.Lock()
	defer p.mpMu.Unlock()
	p.abortCalls++
	return nil
}

// CompleteMultipartUploadConditional applies the same compare-and-swap semantics
// as the single-PUT If-Match path, plus the mutate-before-completion hook.
func (p *reflowMultipartDest) CompleteMultipartUploadConditional(_ context.Context, key, _ string, _ []provider.PartETag, precond provider.PutPrecondition) (provider.PutResult, error) {
	if err := precond.Validate(); err != nil {
		return provider.PutResult{}, err
	}
	p.mpMu.Lock()
	p.conditionalCompletePreconds = append(p.conditionalCompletePreconds, precond)
	mutate := p.mutateBeforeMultipartComplete && precond.IfMatchETag != nil
	if mutate {
		p.mutateBeforeMultipartComplete = false
	}
	p.mpMu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if mutate {
		p.objects[key] = []byte("concurrent mutation")
		p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len("concurrent mutation")), ETag: "mutated-" + key, LastModified: time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC)}}
	}
	if precond.IfAbsent {
		if _, ok := p.objects[key]; ok {
			return provider.PutResult{}, &provider.ProviderError{Op: "CompleteMultipartUpload", Provider: provider.ProviderFile, Key: key, Err: provider.ErrAlreadyExists}
		}
		return p.storeMultipartLocked(key), nil
	}
	if precond.IfMatchETag != nil {
		meta, ok := p.meta[key]
		if !ok || meta.ETag != *precond.IfMatchETag {
			return provider.PutResult{}, &provider.ProviderError{Op: "CompleteMultipartUpload", Provider: provider.ProviderFile, Key: key, Err: provider.ErrPreconditionFailed}
		}
		return p.storeMultipartLocked(key), nil
	}
	return provider.PutResult{}, errors.New("unsupported test precondition")
}

// storeMultipartLocked records a successful multipart landing with a small marker
// body. The caller must hold reflowMemoryProvider.mu.
func (p *reflowMultipartDest) storeMultipartLocked(key string) provider.PutResult {
	etag := "dest-mp-" + key
	p.objects[key] = []byte("multipart-complete")
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len("multipart-complete")), ETag: etag, LastModified: time.Now().UTC()}}
	return provider.PutResult{ETag: etag}
}

func (p *reflowMultipartDest) abortCount() int {
	p.mpMu.Lock()
	defer p.mpMu.Unlock()
	return p.abortCalls
}

func (p *reflowMultipartDest) createCount() int {
	p.mpMu.Lock()
	defer p.mpMu.Unlock()
	return p.createCalls
}

func (p *reflowMultipartDest) ifMatchMultipartCompletions() int {
	p.mpMu.Lock()
	defer p.mpMu.Unlock()
	n := 0
	for _, pc := range p.conditionalCompletePreconds {
		if pc.IfMatchETag != nil {
			n++
		}
	}
	return n
}

// reflowItemStatus reads the single persisted reflow_items (status, reason) for a
// source URI from a checkpoint database — the durable evidence of a terminal.
func reflowItemStatus(t *testing.T, checkpointPath, sourceURI string) (status, reason string) {
	t.Helper()
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(checkpointPath)}).String()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT status, COALESCE(reason, '') FROM reflow_items WHERE source_uri = ?", sourceURI).Scan(&status, &reason))
	return status, reason
}

// TestTransferReflowSourceNewerMultipartConcurrentMutation pins the multipart
// guardrail for overwrite-if-source-newer (E-CVG-S2 multipart): a >threshold
// source-newer overwrite routes through multipart and completes with an If-Match
// precondition. When the destination mutates between the head and the multipart
// completion, the conditional completion fails closed — the parts are aborted,
// the concurrent write is not clobbered, and the item is a concurrent-mutation
// skip with the same terminal/checkpoint evidence as the single-PUT path.
func TestTransferReflowSourceNewerMultipartConcurrentMutation(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)

	withTransferReflowTestState(t)
	src := &largeSourceProvider{
		key:          "src/big.xml",
		etag:         "newer-etag",
		lastModified: t2,
		declaredSize: multipartOverThresholdBytes,
	}
	dst := newReflowMultipartDest()
	dst.putFixture("data/merged.xml", "T0-ORIG", "e0", t0)
	dst.mutateBeforeMultipartComplete = true

	reflowResourceProbeForRun = reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) { return int64(512) << 20, "test_override", nil },
		FDSoftLimit:      func() (int64, error) { return 100000, nil },
	}
	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			switch cfg.Bucket {
			case "source-bucket":
				return src, nil
			case "dest-bucket":
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
			}
		},
	})

	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	line := reflowInputLineFull("src/big.xml", "merged.xml", "newer-etag", int64Ptr(multipartOverThresholdBytes), t2.Format(time.RFC3339))
	stdout, err := runTransferReflowRawStdin(t, line+"\n", "--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "1", "--checkpoint", checkpointPath, "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err, stdout)

	rec := soleTerminalReflowRecord(t, stdout)
	require.Equal(t, "skipped", rec.Status)
	require.Equal(t, "collision.skipped_concurrent_mutation", rec.Reason)

	// The overwrite genuinely routed through multipart and attempted an If-Match
	// completion (not a single PUT), then aborted the parts on the stale predicate.
	require.Positive(t, dst.createCount(), "the >threshold overwrite must route through multipart")
	require.Positive(t, dst.ifMatchMultipartCompletions(), "the conditional overwrite must attempt an If-Match multipart completion")
	require.Positive(t, dst.abortCount(), "a failed conditional completion must abort the multipart upload")

	// No clobber: the destination holds the concurrent write, never the source.
	require.Equal(t, "concurrent mutation", string(dst.mustObject("data/merged.xml")))

	// Checkpoint carries the same terminal evidence as the single-PUT path.
	status, reason := reflowItemStatus(t, checkpointPath, "s3://source-bucket/src/big.xml")
	require.Equal(t, "skipped", status)
	require.Equal(t, "collision.skipped_concurrent_mutation", reason)
}
