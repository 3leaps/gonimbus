package transfer

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// CopyReceipt describes an acknowledged streamed write, not independent
// destination read-back verification. Unknown provider handles remain empty.
type CopyReceipt struct {
	UploadResult
	SourceLastModified time.Time
}

// CopyReceiptOptions preserves revision admission, write preconditions and
// phase budgets while requesting the write's payload receipt.
type CopyReceiptOptions struct {
	Upload   UploadOptions
	Revision *provider.SourceRevision
	Gate     CopyGate
}

// CopyObjectWithReceipt streams an object using the same bounded upload and
// phase-gate paths as CopyObject. It never issues a HEAD to fill receipt fields.
func CopyObjectWithReceipt(ctx context.Context, src, dst provider.Provider, srcKey, dstKey string, expectedSize int64, opts CopyReceiptOptions) (CopyReceipt, error) {
	putter, ok := dst.(provider.ObjectPutter)
	if !ok {
		return CopyReceipt{}, errors.New("target provider does not support PutObject")
	}
	if hasPrecondition(opts.Upload.Precondition) {
		if _, ok := dst.(provider.ConditionalPutter); !ok {
			return CopyReceipt{}, errors.New("target provider does not support conditional PutObject")
		}
	}
	uploadOpts := normalizeUploadOptions(putter, opts.Upload)
	gate := resolveCopyGate(opts.Gate)
	var receipt CopyReceipt
	readSource := func(ctx context.Context) (io.ReadCloser, provider.ObjectMeta, error) {
		body, meta, err := getReceiptSource(ctx, src, srcKey, opts.Revision)
		if err != nil {
			return nil, provider.ObjectMeta{}, err
		}
		if expectedSize > 0 && meta.Size >= 0 && expectedSize != meta.Size {
			_ = body.Close()
			return nil, provider.ObjectMeta{}, &SizeMismatchError{Key: srcKey, Expected: expectedSize, Got: meta.Size}
		}
		receipt.SourceLastModified = meta.LastModified
		return body, meta, nil
	}
	if expectedSize > 0 && shouldUseMultipart(expectedSize, uploadOpts.MultipartThreshold, putter) {
		err := gate.Do(ctx, CopyStageCoupled, func(ctx context.Context) error {
			body, meta, err := readSource(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = body.Close() }()
			receipt.UploadResult, err = UploadReaderWithSize(ctx, putter, dstKey, body, meta.Size, uploadOpts)
			return err
		})
		if err != nil {
			return CopyReceipt{}, err
		}
		return receipt, nil
	}
	var buffered *retryableBody
	err := gate.Do(ctx, CopyStageSourceRead, func(ctx context.Context) error {
		body, meta, err := readSource(ctx)
		if err != nil {
			return err
		}
		buffered, err = newRetryableBodyWithTempDir(ctx, body, meta.Size, uploadOpts.RetryBufferBytes, uploadOpts.TempDir)
		return err
	})
	if err != nil {
		return CopyReceipt{}, err
	}
	defer func() { _ = buffered.Close() }()
	err = gate.Do(ctx, CopyStageDestWrite, func(ctx context.Context) error {
		if shouldUseMultipart(buffered.size, uploadOpts.MultipartThreshold, putter) {
			result, err := uploadMultipartKnownSize(ctx, putter, dstKey, buffered.Reader(), buffered.size, uploadOpts)
			if err != nil {
				return err
			}
			receipt.UploadResult = result
			return nil
		}
		result, err := putSingle(ctx, putter, dstKey, buffered.Reader(), buffered.size, uploadOpts)
		if err != nil {
			return err
		}
		receipt.UploadResult = UploadResult{Bytes: buffered.size, SHA256: buffered.sha256, ETag: result.ETag, Version: result.Version, Mode: "single"}
		return nil
	})
	if err != nil {
		return CopyReceipt{}, err
	}
	return receipt, nil
}

func getReceiptSource(ctx context.Context, src provider.Provider, key string, revision *provider.SourceRevision) (io.ReadCloser, provider.ObjectMeta, error) {
	if revision != nil {
		getter, ok := src.(provider.RevisionGetter)
		if !ok {
			return nil, provider.ObjectMeta{}, provider.ErrReplayUnverifiable
		}
		return getter.GetObjectRevision(ctx, key, *revision)
	}
	if getter, ok := src.(provider.VersionedGetter); ok {
		return getter.GetObjectVersioned(ctx, key)
	}
	getter, ok := src.(provider.ObjectGetter)
	if !ok {
		return nil, provider.ObjectMeta{}, errors.New("source provider does not support GetObject")
	}
	body, size, err := getter.GetObject(ctx, key)
	return body, provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Size: size}}, err
}
