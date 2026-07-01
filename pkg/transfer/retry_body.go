package transfer

import (
	"bytes"
	"context"
	"io"
)

const (
	// DefaultRetryBufferMaxMemoryBytes controls how large an object we buffer in memory
	// to make PUT retries seekable. Larger objects are spooled to a temp file.
	DefaultRetryBufferMaxMemoryBytes int64 = 16 << 20 // 16 MiB
)

type retryableBody struct {
	reader  io.ReadSeeker
	cleanup func() error
}

func (b *retryableBody) Reader() io.ReadSeeker { return b.reader }

func (b *retryableBody) Close() error {
	if b.cleanup == nil {
		return nil
	}
	return b.cleanup()
}

func newRetryableBody(ctx context.Context, src io.ReadCloser, size int64, maxMemoryBytes int64) (*retryableBody, error) {
	return newRetryableBodyWithTempDir(ctx, src, size, maxMemoryBytes, "")
}

func newRetryableBodyWithTempDir(ctx context.Context, src io.ReadCloser, size int64, maxMemoryBytes int64, tempDir string) (*retryableBody, error) {
	_ = ctx
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = DefaultRetryBufferMaxMemoryBytes
	}

	defer func() { _ = src.Close() }()

	// Unknown size: treat as "large" and spool.
	if size < 0 {
		size = maxMemoryBytes + 1
	}

	if size <= maxMemoryBytes {
		buf := make([]byte, 0, minInt64(size, maxMemoryBytes))
		data, err := io.ReadAll(io.LimitReader(src, size))
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
		return &retryableBody{reader: bytes.NewReader(buf), cleanup: func() error { return nil }}, nil
	}

	f, cleanup, err := createSecureTempFile(tempDir, "gonimbus-put-buffer-*")
	if err != nil {
		return nil, err
	}

	_, copyErr := io.Copy(f, src)
	if copyErr != nil {
		_ = cleanup()
		return nil, copyErr
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = cleanup()
		return nil, err
	}

	return &retryableBody{
		reader:  f,
		cleanup: cleanup,
	}, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
