package content

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type fakeProvider struct {
	meta provider.ObjectMeta
	data []byte
}

func (p *fakeProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p *fakeProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	m := p.meta
	m.Key = key
	return &m, nil
}

func (p *fakeProvider) Close() error { return nil }

func (p *fakeProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return io.NopCloser(bytes.NewReader(p.data)), int64(len(p.data)), nil
}

func TestHeadBytes_FallbackGetObject(t *testing.T) {
	p := &fakeProvider{meta: provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Size: 11, LastModified: time.Now()}}, data: []byte("hello world")}
	b, meta, err := HeadBytes(context.Background(), p, "k", 5)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, []byte("hello"), b)
}
