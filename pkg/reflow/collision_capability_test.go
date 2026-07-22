package reflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// headOnlyDest is a destination provider that implements only the base Provider
// interface — no ConditionalPutter — so the source-newer If-Match capability
// check must refuse it.
type headOnlyDest struct{}

func (headOnlyDest) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (headOnlyDest) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderS3, Err: provider.ErrNotFound}
}

func (headOnlyDest) Close() error { return nil }

const sourceNewerCapabilityLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","dest_rel_key":"a/b.xml","source_size_bytes":7,"source_last_modified":"2026-01-15T20:53:44Z"}}`

// TestRunnerSourceNewerRequiresIfMatchCapability pins the library-owned
// (ADR-0006 authority) refusal of overwrite-if-source-newer against a
// destination that cannot honor the If-Match write precondition — including a
// FRESH-key run, which the pre-check must refuse before any stream read, event
// emission, IfAbsent probe, or mutation. A direct embedder must not be able to
// mutate keys until the first conflict happens to need the predicate.
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

	t.Run("provider without ConditionalPutter is refused", func(t *testing.T) {
		src := newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		cfg := sourceNewer(copyConfig(newCopyMemoryProvider(), sink))
		cfg.Destination.Provider = headOnlyDest{} // no ConditionalPutter
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.Error(t, runErr)
		require.NotErrorIs(t, runErr, ErrNotImplemented)
		require.Contains(t, runErr.Error(), "ConditionalPutter")
		require.False(t, sink.emitted())
	})

	t.Run("if-match-capable s3 destination is accepted", func(t *testing.T) {
		// Dry-run isolates the capability decision from the copy path: the capable
		// provider must pass the pre-check and proceed to planning, not be refused.
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		sink := &collectSink{}
		cfg := sourceNewer(copyConfig(dst, sink))
		cfg.DryRun = true
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		_, runErr := runner.Run(context.Background(), copySource(src, sourceNewerCapabilityLine))
		require.NoError(t, runErr, "a ConditionalPutter destination must not be refused")
		require.True(t, sink.emitted(), "the accepted config proceeds to emit records")
	})
}
