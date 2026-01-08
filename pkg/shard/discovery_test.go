package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type fakeLister struct {
	children map[string][]string
}

func (f *fakeLister) List(_ context.Context, _ provider.ListOptions) (*provider.ListResult, error) {
	panic("not used")
}

func (f *fakeLister) Head(_ context.Context, _ string) (*provider.ObjectMeta, error) {
	panic("not used")
}

func (f *fakeLister) Close() error { return nil }

func (f *fakeLister) ListCommonPrefixes(_ context.Context, opts provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	return &provider.ListCommonPrefixesResult{Prefixes: f.children[opts.Prefix]}, nil
}

// errorLister is a fake that returns an error for a specific prefix
type errorLister struct {
	children    map[string][]string
	errOnPrefix string
	err         error
}

func (e *errorLister) List(_ context.Context, _ provider.ListOptions) (*provider.ListResult, error) {
	panic("not used")
}

func (e *errorLister) Head(_ context.Context, _ string) (*provider.ObjectMeta, error) {
	panic("not used")
}

func (e *errorLister) Close() error { return nil }

func (e *errorLister) ListCommonPrefixes(_ context.Context, opts provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	if opts.Prefix == e.errOnPrefix {
		return nil, e.err
	}
	return &provider.ListCommonPrefixesResult{Prefixes: e.children[opts.Prefix]}, nil
}

func TestDiscover_Depth1(t *testing.T) {
	p := &fakeLister{children: map[string][]string{"": {"a/", "b/"}}}

	out, err := Discover(context.Background(), p, "", Config{Enabled: true, Depth: 1, Delimiter: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/", "b/"}, out)
}

func TestDiscover_Depth2(t *testing.T) {
	p := &fakeLister{children: map[string][]string{"": {"a/", "b/"}, "a/": {"a/0/", "a/1/"}}}

	out, err := Discover(context.Background(), p, "", Config{Enabled: true, Depth: 2, Delimiter: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/0/", "a/1/", "b/"}, out)
}

func TestDiscover_MaxShards(t *testing.T) {
	p := &fakeLister{children: map[string][]string{"": {"a/", "b/", "c/"}}}

	out, err := Discover(context.Background(), p, "", Config{Enabled: true, Depth: 1, MaxShards: 2, Delimiter: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/", "b/"}, out)
}

// --- Stage 2: Parallel discovery tests ---

func TestDiscover_ParallelConcurrency(t *testing.T) {
	// Create a wide fan-out: 16 top-level prefixes, each with 4 children
	children := map[string][]string{
		"": make([]string, 16),
	}
	for i := 0; i < 16; i++ {
		prefix := string(rune('a'+i)) + "/"
		children[""][i] = prefix
		children[prefix] = []string{
			prefix + "0/", prefix + "1/", prefix + "2/", prefix + "3/",
		}
	}

	p := &fakeLister{children: children}

	// Run with concurrency=8 (should parallelize level-2 expansion)
	out, err := Discover(context.Background(), p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 8,
		Delimiter:       "/",
	})
	require.NoError(t, err)
	assert.Len(t, out, 64) // 16 * 4 = 64 leaf prefixes
	// Verify sorted (deterministic)
	assert.Equal(t, "a/0/", out[0])
	assert.Equal(t, "p/3/", out[63])
}

func TestDiscover_DeterministicOutput(t *testing.T) {
	// Same setup - verify multiple runs produce identical output
	children := map[string][]string{
		"":   {"z/", "m/", "a/", "f/"},
		"z/": {"z/2/", "z/1/"},
		"m/": {"m/x/", "m/a/"},
		"a/": {"a/9/", "a/0/"},
		"f/": {"f/b/", "f/a/"},
	}

	p := &fakeLister{children: children}
	cfg := Config{Enabled: true, Depth: 2, ListConcurrency: 4, Delimiter: "/"}

	// Run 5 times and verify identical output
	var first []string
	for i := 0; i < 5; i++ {
		out, err := Discover(context.Background(), p, "", cfg)
		require.NoError(t, err)
		if first == nil {
			first = out
		} else {
			assert.Equal(t, first, out, "run %d should match first run", i)
		}
	}

	// Verify sorted order
	expected := []string{"a/0/", "a/9/", "f/a/", "f/b/", "m/a/", "m/x/", "z/1/", "z/2/"}
	assert.Equal(t, expected, first)
}

func TestDiscover_ConcurrencyOne(t *testing.T) {
	// Verify ListConcurrency=1 still works (sequential fallback)
	children := map[string][]string{
		"":   {"a/", "b/"},
		"a/": {"a/1/", "a/2/"},
		"b/": {"b/1/"},
	}

	p := &fakeLister{children: children}

	out, err := Discover(context.Background(), p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 1,
		Delimiter:       "/",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/1/", "a/2/", "b/1/"}, out)
}

func TestDiscover_MaxShardsWithConcurrency(t *testing.T) {
	// Verify max_shards cap works with parallel discovery
	children := map[string][]string{
		"": {"a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/"},
	}
	for _, prefix := range children[""] {
		children[prefix] = []string{prefix + "1/", prefix + "2/", prefix + "3/", prefix + "4/"}
	}

	p := &fakeLister{children: children}

	out, err := Discover(context.Background(), p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 4,
		MaxShards:       10,
		Delimiter:       "/",
	})
	require.NoError(t, err)
	assert.Len(t, out, 10) // Capped at 10
	// First 10 in sorted order
	assert.Equal(t, "a/1/", out[0])
}

func TestDiscover_EmptyChildren(t *testing.T) {
	// Verify leaf prefixes (no children) are preserved
	children := map[string][]string{
		"":   {"a/", "b/", "c/"},
		"a/": {"a/1/"},
		"b/": {}, // No children - should keep "b/"
		"c/": {"c/1/", "c/2/"},
	}

	p := &fakeLister{children: children}

	out, err := Discover(context.Background(), p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 2,
		Delimiter:       "/",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/1/", "b/", "c/1/", "c/2/"}, out)
}

func TestDiscover_ErrorPropagation(t *testing.T) {
	// Test that errors from parallel workers propagate correctly
	testErr := errors.New("simulated list error")

	p := &errorLister{
		children: map[string][]string{
			"":   {"a/", "b/", "c/", "d/"},
			"a/": {"a/1/"},
			"b/": {"b/1/"},
			// "c/" will return error
			"d/": {"d/1/"},
		},
		errOnPrefix: "c/",
		err:         testErr,
	}

	_, err := Discover(context.Background(), p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 4,
		Delimiter:       "/",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, testErr)
}

func TestDiscover_ContextCancellation(t *testing.T) {
	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	children := map[string][]string{
		"":   {"a/", "b/"},
		"a/": {"a/1/"},
		"b/": {"b/1/"},
	}

	p := &fakeLister{children: children}

	out, err := Discover(ctx, p, "", Config{
		Enabled:         true,
		Depth:           2,
		ListConcurrency: 2,
		Delimiter:       "/",
	})
	// With cancelled context, we may get:
	// - An error (context.Canceled)
	// - Partial results (workers processed before seeing cancellation)
	// - Empty results (nil slice)
	// The key is no panic and graceful handling
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
	// Either way, the function should not panic
	_ = out
}
