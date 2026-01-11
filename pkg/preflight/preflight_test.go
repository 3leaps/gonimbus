package preflight_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider"
)

type denyMultipartProvider struct{}

func (p *denyMultipartProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{Objects: nil, IsTruncated: false, ContinuationToken: ""}, nil
}

func (p *denyMultipartProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p *denyMultipartProvider) Close() error {
	return nil
}

func (p *denyMultipartProvider) CreateMultipartUpload(ctx context.Context, key string) (string, error) {
	return "", provider.ErrAccessDenied
}

func (p *denyMultipartProvider) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	return nil
}

func TestWriteProbe_MultipartAbort_Denied_Unit(t *testing.T) {
	ctx := context.Background()
	p := &denyMultipartProvider{}

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbeMultipartAbort,
		ProbePrefix:   "_gonimbus/probe/",
	})
	require.Error(t, err)
	require.NotNil(t, rec)

	var sawDenied bool
	for _, r := range rec.Results {
		if r.Capability == preflight.CapTargetWrite {
			sawDenied = true
			assert.False(t, r.Allowed)
			assert.Equal(t, "CreateMultipartUpload+Abort", r.Method)
			assert.Equal(t, "ACCESS_DENIED", r.ErrorCode)
		}
	}
	assert.True(t, sawDenied)
}
