package provider_test

import (
	"math"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestResolveConnectionPool(t *testing.T) {
	t.Parallel()

	t.Run("negative rejected", func(t *testing.T) {
		t.Parallel()
		_, err := provider.ResolveConnectionPool(-1)
		require.Error(t, err)
	})

	t.Run("zero is sdk default", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(0)
		require.NoError(t, err)
		require.Equal(t, 0, p.MaxIdleConnsPerHost)
		require.Equal(t, 0, p.MaxConnsPerHost)
	})

	t.Run("one is sdk default", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(1)
		require.NoError(t, err)
		require.Equal(t, 0, p.MaxIdleConnsPerHost)
		require.Equal(t, 0, p.MaxConnsPerHost)
	})

	t.Run("two maps both fields", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(2)
		require.NoError(t, err)
		require.Equal(t, 2, p.MaxIdleConnsPerHost)
		require.Equal(t, 2, p.MaxConnsPerHost)
	})

	t.Run("ordinary parallel maps both fields", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(32)
		require.NoError(t, err)
		require.Equal(t, 32, p.MaxIdleConnsPerHost)
		require.Equal(t, 32, p.MaxConnsPerHost)
	})

	t.Run("large positive pass through no soft cap", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(10_000)
		require.NoError(t, err)
		require.Equal(t, 10_000, p.MaxIdleConnsPerHost)
		require.Equal(t, 10_000, p.MaxConnsPerHost)
	})

	t.Run("max int pass through", func(t *testing.T) {
		t.Parallel()
		p, err := provider.ResolveConnectionPool(math.MaxInt)
		require.NoError(t, err)
		require.Equal(t, math.MaxInt, p.MaxIdleConnsPerHost)
		require.Equal(t, math.MaxInt, p.MaxConnsPerHost)
	})
}
