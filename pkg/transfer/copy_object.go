package transfer

import (
	"context"
	"errors"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// CopyObject streams a single object from srcKey to dstKey.
// expectedSize, when positive, is compared to the source read metadata.
func CopyObject(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64) (int64, error) {
	return CopyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, provider.PutOptions{})
}

// CopyObjectWithOptions streams an object with destination metadata options.
func CopyObjectWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions) (int64, error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, nil, nil)
}

// CopyObjectWithGate independently gates the source and destination phases
// when the source can be buffered before the destination write.
func CopyObjectWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, gate CopyGate) (int64, error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, nil, gate)
}

// CopyObjectRevisionWithOptions streams exactly the admitted source revision.
func CopyObjectRevisionWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision provider.SourceRevision) (int64, error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, &revision, nil)
}

// CopyObjectRevisionWithGate streams an admitted revision under phase budgets.
func CopyObjectRevisionWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision provider.SourceRevision, gate CopyGate) (int64, error) {
	return copyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, opts, &revision, gate)
}

func copyObjectWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, opts provider.PutOptions, revision *provider.SourceRevision, gate CopyGate) (int64, error) {
	receipt, err := CopyObjectWithReceipt(ctx, src, dst, srcKey, dstKey, expectedSize, CopyReceiptOptions{
		Upload: UploadOptions{RetryBufferBytes: retryBufferMaxMemoryBytes, PutOptions: opts}, Revision: revision, Gate: gate,
	})
	return receipt.Bytes, err
}

// CopyObjectConditional streams an object with an atomic write precondition.
func CopyObjectConditional(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition) (int64, provider.PutResult, error) {
	return CopyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, provider.PutOptions{})
}

// CopyObjectConditionalWithOptions preserves atomic destination predicates.
func CopyObjectConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions) (int64, provider.PutResult, error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, nil, nil)
}

// CopyObjectConditionalWithGate applies phase budgets to a conditional copy.
func CopyObjectConditionalWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, gate CopyGate) (int64, provider.PutResult, error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, nil, gate)
}

// CopyObjectRevisionConditionalWithOptions pins the admitted source revision.
func CopyObjectRevisionConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision provider.SourceRevision) (int64, provider.PutResult, error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, &revision, nil)
}

// CopyObjectRevisionConditionalWithGate preserves revision and phase admission.
func CopyObjectRevisionConditionalWithGate(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision provider.SourceRevision, gate CopyGate) (int64, provider.PutResult, error) {
	return copyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, retryBufferMaxMemoryBytes, precond, opts, &revision, gate)
}

func copyObjectConditionalWithOptions(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, retryBufferMaxMemoryBytes int64, precond provider.PutPrecondition, opts provider.PutOptions, revision *provider.SourceRevision, gate CopyGate) (int64, provider.PutResult, error) {
	if _, ok := dst.(provider.ConditionalPutter); !ok {
		return 0, provider.PutResult{}, errors.New("target provider does not support conditional PutObject")
	}
	receipt, err := CopyObjectWithReceipt(ctx, src, dst, srcKey, dstKey, expectedSize, CopyReceiptOptions{
		Upload: UploadOptions{RetryBufferBytes: retryBufferMaxMemoryBytes, Precondition: precond, PutOptions: opts}, Revision: revision, Gate: gate,
	})
	return receipt.Bytes, provider.PutResult{ETag: receipt.ETag, Version: receipt.Version}, err
}
