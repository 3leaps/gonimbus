package reflow

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// headOnlyDest is a destination provider that implements only the base Provider
// interface — no ConditionalPutter and no ConditionalCapabilityReporter — so the
// source-newer If-Match capability check must refuse it.
type headOnlyDest struct{}

func (headOnlyDest) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (headOnlyDest) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderS3, Err: provider.ErrNotFound}
}

func (headOnlyDest) Close() error { return nil }

// countingReader tracks how many bytes were consumed from the wrapped reader, so
// a refusal test can prove the record stream was never read.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

const sourceNewerCapabilityLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","dest_rel_key":"a/b.xml","source_size_bytes":7,"source_last_modified":"2026-01-15T20:53:44Z"}}`

// multipartNoConditionalDest is a destination that honors If-Match on single-PUT
// and can perform multipart uploads, but cannot complete them conditionally (no
// ConditionalMultipartCompleter). A large overwrite would upload parts and then
// fail at completion, so the capability check must refuse it up front.
type multipartNoConditionalDest struct {
	*copyMemoryProvider
}

func (multipartNoConditionalDest) CreateMultipartUpload(context.Context, string) (string, error) {
	return "upload-id", nil
}

func (multipartNoConditionalDest) UploadPart(context.Context, string, string, int32, io.Reader, int64) (provider.PartETag, error) {
	return provider.PartETag{}, nil
}

func (multipartNoConditionalDest) CompleteMultipartUpload(context.Context, string, string, []provider.PartETag) (provider.PutResult, error) {
	return provider.PutResult{}, nil
}

func (multipartNoConditionalDest) AbortMultipartUpload(context.Context, string, string) error {
	return nil
}

// ifMatchClaimNoPutterDest declares If-Match support but implements only an
// unconditional ObjectPutter — no ConditionalPutter — so it cannot actually issue
// the conditional overwrite it advertises. It records unconditional lands so a
// live test can prove the gate refuses before any fresh-key mutation (the
// contradiction has the same live fresh-key impact as RR1 via the head fallback).
type ifMatchClaimNoPutterDest struct {
	mu     sync.Mutex
	landed map[string][]byte
}

func newIfMatchClaimNoPutterDest() *ifMatchClaimNoPutterDest {
	return &ifMatchClaimNoPutterDest{landed: map[string][]byte{}}
}

func (d *ifMatchClaimNoPutterDest) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (d *ifMatchClaimNoPutterDest) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderS3, Err: provider.ErrNotFound}
}

func (d *ifMatchClaimNoPutterDest) Close() error { return nil }

func (d *ifMatchClaimNoPutterDest) PutObject(_ context.Context, key string, body io.Reader, _ int64) error {
	data, _ := io.ReadAll(body)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.landed[key] = data
	return nil
}

func (d *ifMatchClaimNoPutterDest) ConditionalWriteCapabilities() provider.ConditionalWriteCapabilities {
	return provider.ConditionalWriteCapabilities{IfMatchETag: true}
}

func (d *ifMatchClaimNoPutterDest) landedKeys() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.landed)
}

// TestRequireSourceNewerCapabilityMultipart pins the ConditionalMultipartCompletion
// arm of the predicate-specific capability authority (E-CVG-S2 multipart
// guardrail): a MultipartUploader that cannot complete conditionally is refused
// before any user-key effect, even though it honors single-PUT If-Match; a
// provider with no multipart path is accepted on single-PUT If-Match alone.
func TestRequireSourceNewerCapabilityMultipart(t *testing.T) {
	t.Run("multipart not declaring conditional completion is refused", func(t *testing.T) {
		dst := multipartNoConditionalDest{copyMemoryProvider: newCopyMemoryProvider()} // ProviderS3, IfMatch-capable single-PUT
		err := RequireSourceNewerCapability(dst)
		require.Error(t, err)
		var capErr *MissingConditionalCapabilityError
		require.ErrorAs(t, err, &capErr)
		require.Equal(t, "ConditionalMultipartCompleter.IfMatchETag", capErr.MissingCapability)
	})

	t.Run("non-multipart if-match provider is accepted", func(t *testing.T) {
		require.NoError(t, RequireSourceNewerCapability(newCopyMemoryProvider()))
	})
}

// TestRequireSourceNewerCapabilityRequiresCallableInterface pins E-CVG-S2-RR2: a
// declared capability boolean must be paired with the callable interface that
// exercises it. Local method availability is fully knowable and fails closed —
// distinct from the remote-endpoint trust boundary — so a contradictory adapter
// cannot pass the pre-I/O gate and then discover it has no callable method.
func TestRequireSourceNewerCapabilityRequiresCallableInterface(t *testing.T) {
	t.Run("declares If-Match but implements no ConditionalPutter", func(t *testing.T) {
		err := RequireSourceNewerCapability(newIfMatchClaimNoPutterDest())
		require.Error(t, err)
		var capErr *MissingConditionalCapabilityError
		require.ErrorAs(t, err, &capErr)
		require.Equal(t, "ConditionalPutter.IfMatchETag", capErr.MissingCapability)
	})

	t.Run("declares conditional multipart completion but implements no ConditionalMultipartCompleter", func(t *testing.T) {
		base := newCopyMemoryProvider()
		base.condCaps = &provider.ConditionalWriteCapabilities{IfAbsent: true, IfMatchETag: true, ConditionalMultipartCompletion: true}
		dst := multipartNoConditionalDest{copyMemoryProvider: base} // MultipartUploader, ConditionalPutter, but no ConditionalMultipartCompleter
		err := RequireSourceNewerCapability(dst)
		require.Error(t, err)
		var capErr *MissingConditionalCapabilityError
		require.ErrorAs(t, err, &capErr)
		require.Equal(t, "ConditionalMultipartCompleter.IfMatchETag", capErr.MissingCapability)
	})
}

// TestRunnerSourceNewerRequiresIfMatchCapability pins the library-owned
// (ADR-0006 authority) predicate-specific refusal of overwrite-if-source-newer
// against a destination that cannot PROVE it honors the If-Match write
// precondition. The authority is the provider's own capability declaration, not
// the presence of ConditionalPutter: an IfAbsent-only ConditionalPutter exposes
// the interface yet cannot honor If-Match and must be refused before any stream
// read, event emission, IfAbsent probe, or destination mutation — including a
// FRESH-key run, so a direct embedder cannot mutate keys until the first conflict
// happens to need the predicate. A destination that genuinely honors If-Match is
// accepted and its compare-and-swap is exercised live, not merely in dry-run.
func TestRunnerSourceNewerRequiresIfMatchCapability(t *testing.T) {
	sourceNewer := func(cfg Config) Config {
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
		return cfg
	}

	t.Run("gcs conditional-put-for-ifabsent-only is refused", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newGCSCapabilityProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		runner, err := NewRunner(sourceNewer(gcsCopyConfig(dst, sink)))
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.Error(t, runErr)
		require.NotErrorIs(t, runErr, ErrNotImplemented)
		require.Contains(t, runErr.Error(), CollisionOverwriteIfSourceNewer)
		require.False(t, sink.emitted(), "refusal must precede any event emission")
		require.Empty(t, dst.preconditions(), "no conditional PUT / IfAbsent probe on a refused config")
		require.Empty(t, dst.body("data/a/b.xml"), "a fresh key must not be mutated on a refused config")
	})

	t.Run("gcs refused in dry-run too (matches the CLI contract)", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newGCSCapabilityProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		cfg := sourceNewer(gcsCopyConfig(dst, sink))
		cfg.DryRun = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.Error(t, runErr)
		require.Contains(t, runErr.Error(), CollisionOverwriteIfSourceNewer)
		require.False(t, sink.emitted())
	})

	t.Run("provider without ConditionalPutter or capability declaration is refused", func(t *testing.T) {
		src := newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		cfg := sourceNewer(copyConfig(newCopyMemoryProvider(), sink))
		cfg.Destination.Provider = headOnlyDest{} // no ConditionalPutter, no reporter
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.Error(t, runErr)
		require.NotErrorIs(t, runErr, ErrNotImplemented)
		require.Contains(t, runErr.Error(), "If-Match")
		require.False(t, sink.emitted())
	})

	t.Run("s3-shaped IfAbsent-only ConditionalPutter is refused before any effect", func(t *testing.T) {
		// The gap E-CVG-S2-P1/RR1 flagged: an S3-shaped provider that implements
		// ConditionalPutter and honors IfAbsent but NOT If-Match. The old
		// interface-presence check accepted it and let it mutate a fresh key live.
		src := newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		dst := newCopyMemoryProvider() // ProviderS3
		dst.condCaps = &provider.ConditionalWriteCapabilities{IfAbsent: true, IfMatchETag: false}
		sink := &collectSink{}
		runner, err := NewRunner(sourceNewer(copyConfig(dst, sink)))
		require.NoError(t, err)

		reader := &countingReader{r: strings.NewReader(sourceNewerCapabilityLine)}
		_, runErr := runner.Run(context.Background(), RecordStreamSource{
			Records: reader,
			Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
		})
		require.Error(t, runErr)
		require.NotErrorIs(t, runErr, ErrNotImplemented)
		require.Contains(t, runErr.Error(), CollisionOverwriteIfSourceNewer)
		require.Contains(t, runErr.Error(), "If-Match")
		require.Zero(t, reader.n, "the record stream must not be read on a refused config")
		require.False(t, sink.emitted(), "refusal must precede any event emission")
		require.Empty(t, dst.preconditions(), "no IfAbsent write-probe or conditional PUT on a refused config")
		require.Empty(t, dst.body("data/a/b.xml"), "a fresh key must not be mutated on a refused config")
	})

	t.Run("declares If-Match but implements no ConditionalPutter is refused before any effect", func(t *testing.T) {
		// The RR2 contradiction with the same live fresh-key impact as RR1: the
		// adapter advertises If-Match but can only issue an unconditional PutObject,
		// so a passed gate would land the fresh key through the head fallback.
		src := newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		dst := newIfMatchClaimNoPutterDest()
		sink := &collectSink{}
		cfg := sourceNewer(copyConfig(newCopyMemoryProvider(), sink))
		cfg.Destination.Provider = dst
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		reader := &countingReader{r: strings.NewReader(sourceNewerCapabilityLine)}
		_, runErr := runner.Run(context.Background(), RecordStreamSource{
			Records: reader,
			Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
		})
		require.Error(t, runErr)
		require.NotErrorIs(t, runErr, ErrNotImplemented)
		require.Contains(t, runErr.Error(), "If-Match")
		require.Zero(t, reader.n, "the record stream must not be read on a refused config")
		require.False(t, sink.emitted(), "refusal must precede any event emission")
		require.Zero(t, dst.landedKeys(), "no fresh key may be landed through the head fallback on a refused config")
	})

	t.Run("if-match-capable s3 destination overwrites live via compare-and-swap", func(t *testing.T) {
		older := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		dst.putFixtureAt("data/a/b.xml", "OLDBODY", "dest-e0", older) // older, different body
		sink := &collectSink{}
		runner, err := NewRunner(sourceNewer(copyConfig(dst, sink)))
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.NoError(t, runErr, "a genuinely If-Match-capable destination must not be refused")
		require.Equal(t, []byte("payload"), dst.body("data/a/b.xml"), "the newer source lands via the If-Match compare-and-swap")
		// The overwrite PUT carried an If-Match on the observed destination ETag.
		var sawIfMatch bool
		for _, pc := range dst.preconditions() {
			if pc.IfMatchETag != nil && *pc.IfMatchETag == "dest-e0" {
				sawIfMatch = true
			}
		}
		require.True(t, sawIfMatch, "the overwrite must be an If-Match compare-and-swap on the observed ETag")
	})

	t.Run("if-match stale mismatch does not clobber the destination", func(t *testing.T) {
		older := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		dst.putFixtureAt("data/a/b.xml", "OLDBODY", "dest-e0", older)
		dst.mutateBeforeIfMatch = true // the dest changes between the head and the PUT
		sink := &collectSink{}
		runner, err := NewRunner(sourceNewer(copyConfig(dst, sink)))
		require.NoError(t, err)

		summary, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.NoError(t, runErr)
		require.Equal(t, []byte("concurrent mutation"), dst.body("data/a/b.xml"),
			"a stale If-Match must not clobber the concurrently-mutated destination")
		require.NotEqual(t, []byte("payload"), dst.body("data/a/b.xml"))
		require.Equal(t, int64(1), summary.Statuses["skipped"], "the stale contender skips rather than overwriting")
	})
}

// fanInForcedDest signals the first successful If-Match land so a test can
// serialize a second contender strictly behind it, forcing a chosen
// gate-acquisition order deterministically (no scheduling reliance).
type fanInForcedDest struct {
	*copyMemoryProvider
	firstLand chan struct{}
	once      sync.Once
}

func (p *fanInForcedDest) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	res, err := p.copyMemoryProvider.PutObjectConditional(ctx, key, body, contentLength, precond)
	if err == nil && precond.IfMatchETag != nil {
		p.once.Do(func() { close(p.firstLand) })
	}
	return res, err
}

// TestRunnerSourceNewerFanInForcedOrder forces each gate-acquisition order and
// proves the winner-take-all resolution contract: whichever contender acquires
// the per-key gate first lands its overwrite and becomes the terminal authority,
// and the second contender — resolving only after the first commit — re-evaluates
// against the committed copy (copy-time LastModified) and skips as src_older.
// Both orders are covered, including the order where the OLDER business-date
// source wins, demonstrating that newest-source-wins is explicitly NOT claimed.
func TestRunnerSourceNewerFanInForcedOrder(t *testing.T) {
	orig := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)

	lineFor := func(srcKey, etag, body string, mod time.Time) string {
		return `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/` + srcKey +
			`","source_key":"` + srcKey + `","source_etag":"` + etag +
			`","dest_rel_key":"merged.xml","source_size_bytes":` + strconv.Itoa(len(body)) +
			`,"source_last_modified":"` + mod.Format(time.RFC3339) + `"}}`
	}

	for _, tc := range []struct {
		name                  string
		winnerBody, loserBody string
		winnerMod, loserMod   time.Time
	}{
		{"older_source_wins_the_gate", "OLDER-BODY", "NEWER-BODY", older, newer},
		{"newer_source_wins_the_gate", "NEWER-BODY", "OLDER-BODY", newer, older},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newCopyMemoryProvider()
			src.putFixture("src/win.xml", tc.winnerBody, "win-etag")
			src.putFixture("src/lose.xml", tc.loserBody, "lose-etag")
			base := newCopyMemoryProvider()
			base.putFixtureAt("data/merged.xml", "ORIG-BODY", "e0", orig)
			dst := &fanInForcedDest{copyMemoryProvider: base, firstLand: make(chan struct{})}
			sink := &collectSink{}

			cfg := copyConfig(base, sink)
			cfg.Destination.Provider = dst
			cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
			cfg.Concurrency = ResolveConcurrency(2, true, DefaultResourceProbe())
			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			// The winner line is first; the loser resolves only after the winner's
			// commit signal — forcing the gate-acquisition order deterministically.
			stream := lineFor("src/win.xml", "win-etag", tc.winnerBody, tc.winnerMod) + "\n" +
				lineFor("src/lose.xml", "lose-etag", tc.loserBody, tc.loserMod) + "\n"
			source := RecordStreamSource{
				Records: strings.NewReader(stream),
				Resolve: func(ctx context.Context, uri string) (provider.Provider, error) {
					if strings.Contains(uri, "lose.xml") {
						select {
						case <-dst.firstLand:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return src, nil
				},
			}

			summary, runErr := runner.Run(context.Background(), source)
			require.NoError(t, runErr)
			require.Equal(t, int64(1), summary.Statuses["complete"], "the gate winner lands exactly once")
			require.Equal(t, int64(1), summary.Statuses["skipped"], "the loser skips against the committed copy")
			require.Equal(t, []byte(tc.winnerBody), dst.body("data/merged.xml"), "the first gate winner is the terminal authority")
		})
	}
}
