package shard

import (
	"context"
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
