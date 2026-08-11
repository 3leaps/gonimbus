package transfer

import (
	"context"
	"errors"
	"io"

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
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, nil, nil)
}

// CopyObjectWithGate is CopyObjectWithOptions with an optional per-phase
// CopyGate. When gate is non-nil, source-read and dest-write for the
// single-part path acquire independently so concurrent copies can overlap a
// Get of one object with a Put of another under the same concurrency ceiling.
func CopyObjectWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, gate CopyGate) (bytesTransferred int64, err error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, nil, gate)
}

// CopyObjectRevisionWithOptions streams exactly the admitted source revision.
func CopyObjectRevisionWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision provider.SourceRevision) (bytesTransferred int64, err error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, &revision, nil)
}

// CopyObjectRevisionWithGate streams exactly the admitted source revision under
// an optional per-phase CopyGate.
func CopyObjectRevisionWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision provider.SourceRevision, gate CopyGate) (bytesTransferred int64, err error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, &revision, gate)
}

func copyObjectWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision *provider.SourceRevision, gate CopyGate) (bytesTransferred int64, err error) {
	putter, ok := dst.(provider.ObjectPutter)
	if !ok {
		return 0, errors.New("target provider does not support PutObject")
	}
	gate = resolveCopyGate(gate)
	uploadOpts := normalizeUploadOptions(putter, UploadOptions{
		RetryBufferBytes: retryBufferMaxMemoryBytes,
		PutOptions:       opts,
	})

	// When listing size already implies multipart, keep source open while
	// streaming parts (coupled path). Small-object authority workloads take the
	// phase-split path below.
	if expectedSize > 0 && shouldUseMultipart(expectedSize, uploadOpts.MultipartThreshold, putter) {
		var bytes int64
		err := gate.Do(ctx, CopyStageCoupled, func(ctx context.Context) error {
			n, copyErr := copyObjectCoupled(ctx, src, putter, srcKey, dstKey, expectedSize, uploadOpts, revision)
			bytes = n
			return copyErr
		})
		return bytes, err
	}

	var (
		buffered *retryableBody
		gotSize  int64
	)
	if err := gate.Do(ctx, CopyStageSourceRead, func(ctx context.Context) error {
		body, size, getErr := getSourceObject(ctx, src, srcKey, revision)
		if getErr != nil {
			return getErr
		}
		// validate=size: compare expected listing size vs GetObject content length.
		if expectedSize > 0 && size >= 0 && expectedSize != size {
			_ = body.Close()
			return &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: size}
		}
		gotSize = size
		var bufErr error
		buffered, bufErr = newRetryableBodyWithTempDir(ctx, body, size, uploadOpts.RetryBufferBytes, uploadOpts.TempDir)
		return bufErr
	}); err != nil {
		return 0, err
	}
	defer func() { _ = buffered.Close() }()

	var bytes int64
	err = gate.Do(ctx, CopyStageDestWrite, func(ctx context.Context) error {
		// Size discovered only after Get may still require multipart; upload
		// from the already-materialized body (source stage is complete).
		if gotSize >= 0 && shouldUseMultipart(gotSize, uploadOpts.MultipartThreshold, putter) {
			result, putErr := uploadMultipartKnownSize(ctx, putter, dstKey, buffered.Reader(), gotSize, uploadOpts)
			if putErr != nil {
				return putErr
			}
			bytes = result.Bytes
			return nil
		}
		result, putErr := putSingle(ctx, putter, dstKey, buffered.Reader(), gotSize, uploadOpts)
		if putErr != nil {
			return putErr
		}
		_ = result
		if gotSize >= 0 {
			bytes = gotSize
		}
		return nil
	})
	return bytes, err
}

func copyObjectCoupled(ctx context.Context, src provider.Provider, putter provider.ObjectPutter, srcKey, dstKey string, expectedSize int64, uploadOpts UploadOptions, revision *provider.SourceRevision) (int64, error) {
	body, gotSize, err := getSourceObject(ctx, src, srcKey, revision)
	if err != nil {
		return 0, err
	}
	if expectedSize > 0 && gotSize >= 0 && expectedSize != gotSize {
		_ = body.Close()
		return 0, &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: gotSize}
	}
	defer func() { _ = body.Close() }()

	result, err := UploadReaderWithSize(ctx, putter, dstKey, body, gotSize, uploadOpts)
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
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, nil, nil)
}

// CopyObjectConditionalWithGate is CopyObjectConditionalWithOptions with an
// optional per-phase CopyGate (see CopyObjectWithGate).
func CopyObjectConditionalWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, gate CopyGate) (bytesTransferred int64, result provider.PutResult, err error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, nil, gate)
}

// CopyObjectRevisionConditionalWithOptions streams exactly the admitted source
// revision using an atomic destination write precondition.
func CopyObjectRevisionConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision provider.SourceRevision) (bytesTransferred int64, result provider.PutResult, err error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, &revision, nil)
}

// CopyObjectRevisionConditionalWithGate streams an admitted revision under an
// optional per-phase CopyGate.
func CopyObjectRevisionConditionalWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision provider.SourceRevision, gate CopyGate) (bytesTransferred int64, result provider.PutResult, err error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, &revision, gate)
}

func copyObjectConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision *provider.SourceRevision, gate CopyGate) (bytesTransferred int64, result provider.PutResult, err error) {
	if _, ok := dst.(provider.ConditionalPutter); !ok {
		return 0, provider.PutResult{}, errors.New("target provider does not support conditional PutObject")
	}
	objectPutter, ok := dst.(provider.ObjectPutter)
	if !ok {
		return 0, provider.PutResult{}, errors.New("target provider does not support PutObject")
	}
	gate = resolveCopyGate(gate)
	uploadOpts := normalizeUploadOptions(objectPutter, UploadOptions{
		RetryBufferBytes: retryBufferMaxMemoryBytes,
		Precondition:     precond,
		PutOptions:       opts,
	})

	if expectedSize > 0 && shouldUseMultipart(expectedSize, uploadOpts.MultipartThreshold, objectPutter) {
		var (
			bytes  int64
			putRes provider.PutResult
		)
		err := gate.Do(ctx, CopyStageCoupled, func(ctx context.Context) error {
			n, res, copyErr := copyObjectConditionalCoupled(ctx, src, objectPutter, srcKey, dstKey, expectedSize, uploadOpts, revision)
			bytes, putRes = n, res
			return copyErr
		})
		return bytes, putRes, err
	}

	var (
		buffered *retryableBody
		gotSize  int64
	)
	if err := gate.Do(ctx, CopyStageSourceRead, func(ctx context.Context) error {
		body, size, getErr := getSourceObject(ctx, src, srcKey, revision)
		if getErr != nil {
			return getErr
		}
		if expectedSize > 0 && size >= 0 && expectedSize != size {
			_ = body.Close()
			return &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: size}
		}
		gotSize = size
		var bufErr error
		buffered, bufErr = newRetryableBodyWithTempDir(ctx, body, size, uploadOpts.RetryBufferBytes, uploadOpts.TempDir)
		return bufErr
	}); err != nil {
		return 0, provider.PutResult{}, err
	}
	defer func() { _ = buffered.Close() }()

	var (
		bytes  int64
		putRes provider.PutResult
	)
	err = gate.Do(ctx, CopyStageDestWrite, func(ctx context.Context) error {
		if gotSize >= 0 && shouldUseMultipart(gotSize, uploadOpts.MultipartThreshold, objectPutter) {
			uploadResult, putErr := uploadMultipartKnownSize(ctx, objectPutter, dstKey, buffered.Reader(), gotSize, uploadOpts)
			if putErr != nil {
				return putErr
			}
			bytes = uploadResult.Bytes
			putRes = provider.PutResult{ETag: uploadResult.ETag, Version: uploadResult.Version}
			return nil
		}
		res, putErr := putSingle(ctx, objectPutter, dstKey, buffered.Reader(), gotSize, uploadOpts)
		if putErr != nil {
			return putErr
		}
		putRes = res
		if gotSize >= 0 {
			bytes = gotSize
		}
		return nil
	})
	return bytes, putRes, err
}

func copyObjectConditionalCoupled(ctx context.Context, src provider.Provider, putter provider.ObjectPutter, srcKey, dstKey string, expectedSize int64, uploadOpts UploadOptions, revision *provider.SourceRevision) (int64, provider.PutResult, error) {
	body, gotSize, err := getSourceObject(ctx, src, srcKey, revision)
	if err != nil {
		return 0, provider.PutResult{}, err
	}
	if expectedSize > 0 && gotSize >= 0 && expectedSize != gotSize {
		_ = body.Close()
		return 0, provider.PutResult{}, &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: gotSize}
	}
	defer func() { _ = body.Close() }()

	uploadResult, err := UploadReaderWithSize(ctx, putter, dstKey, body, gotSize, uploadOpts)
	if err != nil {
		return 0, provider.PutResult{}, err
	}
	return uploadResult.Bytes, provider.PutResult{ETag: uploadResult.ETag, Version: uploadResult.Version}, nil
}

func getSourceObject(ctx context.Context, src provider.Provider, key string, revision *provider.SourceRevision) (io.ReadCloser, int64, error) {
	if revision != nil {
		getter, ok := src.(provider.RevisionGetter)
		if !ok {
			return nil, 0, provider.ErrReplayUnverifiable
		}
		body, meta, err := getter.GetObjectRevision(ctx, key, *revision)
		return body, meta.Size, err
	}
	getter, ok := src.(provider.ObjectGetter)
	if !ok {
		return nil, 0, errors.New("source provider does not support GetObject")
	}
	return getter.GetObject(ctx, key)
}
