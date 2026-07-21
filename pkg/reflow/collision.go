package reflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	collisionDuplicate     = "duplicate"
	collisionConflict      = "conflict"
	collisionOverwritten   = "overwritten"
	collisionSrcOlder      = "skipped_src_older"
	collisionConcurrentMut = "skipped_concurrent_mutation"

	decisionIfAbsentHead = "ifabsent_then_head"
	decisionOverwrite    = "unconditional_overwrite"
	decisionHeadCompare  = "head_compare_then_conditional_overwrite"
	decisionHeadFallback = "head_compare_fallback"

	reasonSrcNewer         = "src_newer"
	reasonSrcOlder         = "src_older"
	reasonEqualSizeDiffers = "equal_time_size_differs"
	reasonConcurrentMut    = "concurrent_mutation"
)

func newCollisionInfo(kind string, destMeta *provider.ObjectMeta, decisionPath string) *CollisionInfo {
	info := &CollisionInfo{Kind: kind, DecisionPath: decisionPath}
	if destMeta != nil {
		size := destMeta.Size
		info.DestETagObserved = destMeta.ETag
		info.DestSizeObserved = &size
	}
	return info
}

// newSourceNewerCollisionInfo extends newCollisionInfo with the source and
// destination LastModified timestamps and the decision reason that drove an
// overwrite-if-source-newer outcome. It mirrors the CLI pool constructor of the
// same name so both execution paths emit byte-identical collision metadata.
func newSourceNewerCollisionInfo(kind string, destMeta *provider.ObjectMeta, srcLastModified time.Time, decisionPath, decisionReason string) *CollisionInfo {
	info := newCollisionInfo(kind, destMeta, decisionPath)
	if !srcLastModified.IsZero() {
		t := srcLastModified.UTC()
		info.SrcLastModified = &t
	}
	if destMeta != nil && !destMeta.LastModified.IsZero() {
		t := destMeta.LastModified.UTC()
		info.DestLastModifiedObserved = &t
	}
	info.DecisionReason = decisionReason
	return info
}

func recordWithCollision(rec Record, collision *CollisionInfo) Record {
	if collision == nil {
		return rec
	}
	rec.Collision = collision
	return rec
}

func isConditionalExists(err error) bool {
	return provider.IsAlreadyExists(err) || provider.IsPreconditionFailed(err)
}

func isDuplicateCollision(sourceProvider, destProvider, sourceETag string, sourceSize int64, destMeta *provider.ObjectMeta) bool {
	if !collisionETagsComparable(sourceProvider, destProvider) || destMeta == nil || sourceETag == "" || destMeta.ETag == "" || sourceETag != destMeta.ETag {
		return false
	}
	if isMultipartETag(sourceETag) || isMultipartETag(destMeta.ETag) {
		return false
	}
	return sourceSize <= 0 || destMeta.Size <= 0 || sourceSize == destMeta.Size
}

func collisionETagsComparable(sourceProvider, destProvider string) bool {
	sourceProvider = strings.TrimSpace(sourceProvider)
	if sourceProvider == "" {
		sourceProvider = string(provider.ProviderS3)
	}
	return sourceProvider == destProvider && sourceProvider != string(provider.ProviderFile)
}

func isMultipartETag(etag string) bool {
	etag = strings.Trim(strings.TrimSpace(etag), `"`)
	if etag == "" {
		return false
	}
	idx := strings.LastIndex(etag, "-")
	if idx <= 0 || idx == len(etag)-1 {
		return false
	}
	for _, r := range etag[idx+1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isDuplicateCollisionForReflow(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey, sourceProvider, destProvider, sourceETag string, sourceSize int64, destMeta *provider.ObjectMeta) (bool, error) {
	if isDuplicateCollision(sourceProvider, destProvider, sourceETag, sourceSize, destMeta) {
		return true, nil
	}
	if destMeta == nil || sourceSize != destMeta.Size || !canCompareObjectBodies(src, dst) {
		return false, nil
	}
	return objectBodiesEqual(ctx, src, dst, srcKey, dstKey)
}

func canCompareObjectBodies(src provider.Provider, dst provider.Provider) bool {
	_, srcOK := src.(provider.ObjectGetter)
	_, dstOK := dst.(provider.ObjectGetter)
	return srcOK && dstOK
}

func objectBodiesEqual(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string) (bool, error) {
	srcGetter, ok := src.(provider.ObjectGetter)
	if !ok {
		return false, fmt.Errorf("source provider does not support GetObject")
	}
	dstGetter, ok := dst.(provider.ObjectGetter)
	if !ok {
		return false, fmt.Errorf("destination provider does not support GetObject")
	}

	srcBody, _, err := srcGetter.GetObject(ctx, srcKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = srcBody.Close() }()

	dstBody, _, err := dstGetter.GetObject(ctx, dstKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = dstBody.Close() }()

	srcHash := sha256.New()
	if _, err := io.Copy(srcHash, srcBody); err != nil {
		return false, err
	}
	dstHash := sha256.New()
	if _, err := io.Copy(dstHash, dstBody); err != nil {
		return false, err
	}
	return bytes.Equal(srcHash.Sum(nil), dstHash.Sum(nil)), nil
}
