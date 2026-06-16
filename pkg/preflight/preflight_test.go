package preflight_test

import (
	"context"
	"io"
	"strings"
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

func (p *denyMultipartProvider) UploadPart(ctx context.Context, key, uploadID string, partNumber int32, body io.Reader, size int64) (provider.PartETag, error) {
	return provider.PartETag{}, provider.ErrAccessDenied
}

func (p *denyMultipartProvider) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []provider.PartETag) (provider.PutResult, error) {
	return provider.PutResult{}, provider.ErrAccessDenied
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

type conditionalProbeProvider struct {
	denyMultipartProvider
	putKeys        []string
	deleteKeys     []string
	preconds       []provider.PutPrecondition
	objects        map[string]bool
	ignoreIfAbsent bool
	failDelete     bool
}

func (p *conditionalProbeProvider) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	if p.objects == nil {
		p.objects = map[string]bool{}
	}
	p.putKeys = append(p.putKeys, key)
	p.preconds = append(p.preconds, precond)
	if precond.IfAbsent && p.objects[key] && !p.ignoreIfAbsent {
		return provider.PutResult{}, provider.ErrAlreadyExists
	}
	p.objects[key] = true
	return provider.PutResult{ETag: "etag"}, nil
}

func TestProbeIfAbsentSemantics_DetectsHonoredAndCleansUp(t *testing.T) {
	ctx := context.Background()
	p := &conditionalProbeProvider{}

	result := preflight.ProbeIfAbsentSemantics(ctx, p, preflight.Spec{ProbePrefix: ".gonimbus-preflight/"})
	require.Equal(t, preflight.IfAbsentProbeHonored, result.Status)
	require.NoError(t, result.Err)
	require.Len(t, p.putKeys, 2)
	require.Equal(t, p.putKeys[0], p.putKeys[1])
	require.Equal(t, []string{p.putKeys[0]}, p.deleteKeys)
	require.False(t, p.objects[p.putKeys[0]])
	require.True(t, *result.Honored())
}

func TestProbeIfAbsentSemantics_DetectsNotHonoredAndCleansUp(t *testing.T) {
	ctx := context.Background()
	p := &conditionalProbeProvider{ignoreIfAbsent: true}

	result := preflight.ProbeIfAbsentSemantics(ctx, p, preflight.Spec{ProbePrefix: ".gonimbus-preflight/"})
	require.Equal(t, preflight.IfAbsentProbeNotHonored, result.Status)
	require.NoError(t, result.Err)
	require.Len(t, p.putKeys, 2)
	require.Equal(t, p.putKeys[0], p.putKeys[1])
	require.Equal(t, []string{p.putKeys[0]}, p.deleteKeys)
	require.False(t, p.objects[p.putKeys[0]])
	require.False(t, *result.Honored())
}

func TestProbeIfAbsentSemantics_CleanupFailureIsInconclusive(t *testing.T) {
	ctx := context.Background()
	p := &conditionalProbeProvider{failDelete: true}

	result := preflight.ProbeIfAbsentSemantics(ctx, p, preflight.Spec{ProbePrefix: ".gonimbus-preflight/"})
	require.Equal(t, preflight.IfAbsentProbeInconclusive, result.Status)
	require.Error(t, result.Err)
	require.Nil(t, result.Honored())
	require.Len(t, p.putKeys, 2)
	require.Equal(t, p.putKeys[0], p.putKeys[1])
	require.Equal(t, []string{p.putKeys[0]}, p.deleteKeys)
	require.True(t, p.objects[p.putKeys[0]])
}

func (p *conditionalProbeProvider) DeleteObject(ctx context.Context, key string) error {
	p.deleteKeys = append(p.deleteKeys, key)
	if p.failDelete {
		return provider.ErrAccessDenied
	}
	delete(p.objects, key)
	return nil
}

func TestWriteProbe_PutDelete_UsesConditionalCreateAndCleansUp(t *testing.T) {
	ctx := context.Background()
	p := &conditionalProbeProvider{}

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbePutDelete,
		ProbePrefix:   ".gonimbus-preflight/",
	})
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Len(t, p.putKeys, 1)
	require.True(t, strings.HasPrefix(p.putKeys[0], ".gonimbus-preflight/preflight-"))
	require.Equal(t, p.putKeys, p.deleteKeys)
	require.Len(t, p.preconds, 1)
	require.True(t, p.preconds[0].IfAbsent)
	require.False(t, p.objects[p.putKeys[0]])

	var sawPut bool
	for _, result := range rec.Results {
		if result.Capability == preflight.CapTargetWrite {
			sawPut = true
			assert.True(t, result.Allowed)
			assert.Equal(t, "PutObjectConditional(IfAbsent,0 bytes)", result.Method)
		}
	}
	assert.True(t, sawPut)
}
