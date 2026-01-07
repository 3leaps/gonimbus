package transfer

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nopCloser struct{ *bytes.Reader }

func (n nopCloser) Close() error { return nil }

func TestNewRetryableBody_InMemory_IsSeekable(t *testing.T) {
	src := nopCloser{bytes.NewReader([]byte("hello"))}
	b, err := newRetryableBody(context.Background(), src, 5, 1024)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()

	out1, err := io.ReadAll(b.Reader())
	require.NoError(t, err)
	assert.Equal(t, "hello", string(out1))

	_, err = b.Reader().Seek(0, io.SeekStart)
	require.NoError(t, err)
	out2, err := io.ReadAll(b.Reader())
	require.NoError(t, err)
	assert.Equal(t, "hello", string(out2))
}

func TestNewRetryableBody_SpoolsToFile_CleansUp(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 1024)
	src := nopCloser{bytes.NewReader(payload)}

	b, err := newRetryableBody(context.Background(), src, int64(len(payload)), 16)
	require.NoError(t, err)

	file, ok := b.Reader().(*os.File)
	require.True(t, ok)
	name := file.Name()

	out1, err := io.ReadAll(b.Reader())
	require.NoError(t, err)
	assert.Len(t, out1, len(payload))

	_, err = b.Reader().Seek(0, io.SeekStart)
	require.NoError(t, err)
	out2, err := io.ReadAll(b.Reader())
	require.NoError(t, err)
	assert.Len(t, out2, len(payload))

	require.NoError(t, b.Close())
	_, statErr := os.Stat(name)
	assert.Error(t, statErr)
}
