package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	size    int64
	sha256  string
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

	var prefix []byte
	if size <= maxMemoryBytes {
		// Declared size is a buffering hint, not permission to truncate the
		// source. Detect growth within the memory budget and spill when needed.
		data, err := io.ReadAll(io.LimitReader(src, maxMemoryBytes+1))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) <= maxMemoryBytes {
			digest := sha256.Sum256(data)
			return &retryableBody{reader: bytes.NewReader(data), cleanup: func() error { return nil }, size: int64(len(data)), sha256: hex.EncodeToString(digest[:])}, nil
		}
		prefix = data
	}

	f, cleanup, err := createSecureTempFile(tempDir, "gonimbus-put-buffer-*")
	if err != nil {
		return nil, err
	}

	digest := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, digest), io.MultiReader(bytes.NewReader(prefix), src))
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
		size:    n,
		sha256:  hex.EncodeToString(digest.Sum(nil)),
	}, nil
}
