package transfer

import (
	"context"
	"errors"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// CopyObject streams a single object from srcKey to dstKey.
//
// expectedSize is optional; when > 0 it is compared against the content length
// reported by GetObject to detect stale list/index metadata.
func CopyObject(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64) (bytesTransferred int64, err error) {
	return CopyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, provider.PutOptions{})
}

// CopyObjectWithOptions streams a single object from srcKey to dstKey using
// provider-specific destination metadata options when requested.
func CopyObjectWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions) (bytesTransferred int64, err error) {
	getter, ok := src.(provider.ObjectGetter)
	if !ok {
		return 0, errors.New("source provider does not support GetObject")
	}
	putter, ok := dst.(provider.ObjectPutter)
	if !ok {
		return 0, errors.New("target provider does not support PutObject")
	}

	body, gotSize, err := getter.GetObject(ctx, srcKey)
	if err != nil {
		return 0, err
	}

	// validate=size: compare expected listing size vs GetObject content length.
	if expectedSize > 0 && gotSize >= 0 && expectedSize != gotSize {
		_ = body.Close()
		return 0, &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: gotSize}
	}
	defer func() { _ = body.Close() }()

	result, err := UploadReaderWithSize(ctx, putter, dstKey, body, gotSize, UploadOptions{
		RetryBufferBytes: retryBufferMaxMemoryBytes,
		PutOptions:       opts,
	})
	if err != nil {
		return 0, err
	}

	return result.Bytes, nil
}

// CopyObjectConditional streams a single object from srcKey to dstKey using an
// atomic provider write precondition.
func CopyObjectConditional(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition) (bytesTransferred int64, result provider.PutResult, err error) {
	return CopyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, provider.PutOptions{})
}

// CopyObjectConditionalWithOptions streams a single object from srcKey to dstKey
// using an atomic provider write precondition and destination metadata options
// when requested.
func CopyObjectConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions) (bytesTransferred int64, result provider.PutResult, err error) {
	getter, ok := src.(provider.ObjectGetter)
	if !ok {
		return 0, provider.PutResult{}, errors.New("source provider does not support GetObject")
	}
	if _, ok := dst.(provider.ConditionalPutter); !ok {
		return 0, provider.PutResult{}, errors.New("target provider does not support conditional PutObject")
	}
	objectPutter, ok := dst.(provider.ObjectPutter)
	if !ok {
		return 0, provider.PutResult{}, errors.New("target provider does not support PutObject")
	}

	body, gotSize, err := getter.GetObject(ctx, srcKey)
	if err != nil {
		return 0, provider.PutResult{}, err
	}

	if expectedSize > 0 && gotSize >= 0 && expectedSize != gotSize {
		_ = body.Close()
		return 0, provider.PutResult{}, &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: gotSize}
	}
	defer func() { _ = body.Close() }()

	uploadResult, err := UploadReaderWithSize(ctx, objectPutter, dstKey, body, gotSize, UploadOptions{
		RetryBufferBytes: retryBufferMaxMemoryBytes,
		Precondition:     precond,
		PutOptions:       opts,
	})
	if err != nil {
		return 0, provider.PutResult{}, err
	}

	return uploadResult.Bytes, provider.PutResult{ETag: uploadResult.ETag, Version: uploadResult.Version}, nil
}
