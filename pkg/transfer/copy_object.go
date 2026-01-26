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

	retryBody, err := newRetryableBody(ctx, body, gotSize, retryBufferMaxMemoryBytes)
	if err != nil {
		return 0, err
	}
	defer func() { _ = retryBody.Close() }()

	if err := putter.PutObject(ctx, dstKey, retryBody.Reader(), gotSize); err != nil {
		return 0, err
	}

	return gotSize, nil
}
