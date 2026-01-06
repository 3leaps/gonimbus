package provider

import (
	"context"
	"io"
)

// Optional provider capability interfaces.
//
// These interfaces are used for feature detection (type assertions). The core
// Provider interface remains intentionally small.

// ObjectPutter can create/overwrite objects.
//
// For v0.1.x this is primarily used for write-probe preflight operations.
type ObjectPutter interface {
	PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error
}

// ObjectDeleter can delete objects.
//
// For v0.1.x this is primarily used for write-probe preflight operations.
type ObjectDeleter interface {
	DeleteObject(ctx context.Context, key string) error
}

// MultipartUploader can create and abort multipart uploads.
//
// This provides a low-side-effect write probe when supported.
type MultipartUploader interface {
	CreateMultipartUpload(ctx context.Context, key string) (uploadID string, err error)
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// ObjectGetter can download objects as a stream.
//
// For v0.1.x this is used for streaming transfer operations.
type ObjectGetter interface {
	GetObject(ctx context.Context, key string) (body io.ReadCloser, contentLength int64, err error)
}
